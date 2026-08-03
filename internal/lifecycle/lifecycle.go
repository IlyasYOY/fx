// Copyright (c) 2022 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/fx/fxevent"
	"go.uber.org/fx/internal/fxclock"
	"go.uber.org/fx/internal/fxreflect"
	"go.uber.org/multierr"
)

// Reflection types for each of the supported hook function signatures. These
// are used in cases in which the Callable constraint matches a user-defined
// function type that cannot be converted to an underlying function type with
// a conventional conversion or type switch.
var (
	_reflFunc             = reflect.TypeOf(Func(nil))
	_reflErrorFunc        = reflect.TypeOf(ErrorFunc(nil))
	_reflContextFunc      = reflect.TypeOf(ContextFunc(nil))
	_reflContextErrorFunc = reflect.TypeOf(ContextErrorFunc(nil))
)

// Discrete function signatures that are allowed as part of a [Callable].
type (
	// A Func can be converted to a ContextErrorFunc.
	Func = func()
	// An ErrorFunc can be converted to a ContextErrorFunc.
	ErrorFunc = func() error
	// A ContextFunc can be converted to a ContextErrorFunc.
	ContextFunc = func(context.Context)
	// A ContextErrorFunc is used as a [Hook.OnStart] or [Hook.OnStop]
	// function.
	ContextErrorFunc = func(context.Context) error
)

// A Callable is a constraint that matches functions that are, or can be
// converted to, functions suitable for a Hook.
//
// Callable must be identical to [fx.HookFunc].
type Callable interface {
	~Func | ~ErrorFunc | ~ContextFunc | ~ContextErrorFunc
}

// Wrap wraps x into a ContextErrorFunc suitable for a Hook.
func Wrap[T Callable](x T) (ContextErrorFunc, string) {
	if x == nil {
		return nil, ""
	}

	switch fn := any(x).(type) {
	case Func:
		return func(context.Context) error {
			fn()
			return nil
		}, fxreflect.FuncName(x)
	case ErrorFunc:
		return func(context.Context) error {
			return fn()
		}, fxreflect.FuncName(x)
	case ContextFunc:
		return func(ctx context.Context) error {
			fn(ctx)
			return nil
		}, fxreflect.FuncName(x)
	case ContextErrorFunc:
		return fn, fxreflect.FuncName(x)
	}

	// Since (1) we're already using reflect in Fx, (2) we're not particularly
	// concerned with performance, and (3) unsafe would require discrete build
	// targets for appengine (etc), just use reflect to convert user-defined
	// function types to their underlying function types and then call Wrap
	// again with the converted value.
	reflVal := reflect.ValueOf(x)
	switch {
	case reflVal.CanConvert(_reflFunc):
		return Wrap(reflVal.Convert(_reflFunc).Interface().(Func))
	case reflVal.CanConvert(_reflErrorFunc):
		return Wrap(reflVal.Convert(_reflErrorFunc).Interface().(ErrorFunc))
	case reflVal.CanConvert(_reflContextFunc):
		return Wrap(reflVal.Convert(_reflContextFunc).Interface().(ContextFunc))
	default:
		// Is already convertible to ContextErrorFunc.
		return Wrap(reflVal.Convert(_reflContextErrorFunc).Interface().(ContextErrorFunc))
	}
}

// A Hook is a pair of start and stop callbacks, either of which can be nil,
// plus a string identifying the supplier of the hook.
type Hook struct {
	OnStart     func(context.Context) error
	OnStop      func(context.Context) error
	OnStartName string
	OnStopName  string

	callerFrame fxreflect.Frame
}

type appState int

const (
	stopped appState = iota
	starting
	incompleteStart
	started
	stopping
)

func (as appState) String() string {
	switch as {
	case stopped:
		return "stopped"
	case starting:
		return "starting"
	case incompleteStart:
		return "incompleteStart"
	case started:
		return "started"
	case stopping:
		return "stopping"
	default:
		return "invalidState"
	}
}

// Lifecycle coordinates application lifecycle hooks.
type Lifecycle struct {
	clock        fxclock.Clock
	logger       fxevent.Logger
	state        appState
	hooks        []hookEntry
	numStarted   int
	startRecords HookRecords
	stopRecords  HookRecords
	runningHook  Hook
	parallelism  int
	dependencies map[int][]int
	mu           sync.Mutex
	eventMu      sync.Mutex
}

type hookEntry struct {
	Hook
	owner   int
	started bool
}

// New constructs a new Lifecycle.
func New(logger fxevent.Logger, clock fxclock.Clock) *Lifecycle {
	return &Lifecycle{logger: logger, clock: clock, parallelism: 1}
}

// Append adds a Hook to the lifecycle.
func (l *Lifecycle) Append(hook Hook) {
	l.AppendWithOwner(hook, 0)
}

// AppendWithOwner adds a Hook associated with a dependency graph component.
// An owner of zero marks a hook whose graph ownership is unknown.
func (l *Lifecycle) AppendWithOwner(hook Hook, owner int) {
	// Save the caller's stack frame to report file/line number.
	if f := fxreflect.CallerStack(2, 0); len(f) > 0 {
		hook.callerFrame = f[0]
	}
	l.mu.Lock()
	l.hooks = append(l.hooks, hookEntry{Hook: hook, owner: owner})
	l.mu.Unlock()
}

// SetParallelism sets the maximum number of hooks that may execute at once.
func (l *Lifecycle) SetParallelism(parallelism int) {
	l.mu.Lock()
	l.parallelism = parallelism
	l.mu.Unlock()
}

// SetDependencies replaces the component dependency graph. Each map key is a
// component and each value lists the components it depends on.
func (l *Lifecycle) SetDependencies(dependencies map[int][]int) {
	cloned := make(map[int][]int, len(dependencies))
	for component, deps := range dependencies {
		cloned[component] = append([]int(nil), deps...)
	}
	l.mu.Lock()
	l.dependencies = cloned
	l.mu.Unlock()
}

// Start runs all OnStart hooks. In parallel mode, it stops scheduling new
// hooks after an error and waits for hooks that are already running.
func (l *Lifecycle) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("called OnStart with nil context")
	}

	l.mu.Lock()
	if l.state != stopped {
		defer l.mu.Unlock()
		return fmt.Errorf("attempted to start lifecycle when in state: %v", l.state)
	}
	l.numStarted = 0
	l.state = starting

	l.startRecords = make(HookRecords, 0, len(l.hooks))
	for i := range l.hooks {
		l.hooks[i].started = false
	}
	parallelism := l.parallelism
	l.mu.Unlock()

	returnState := incompleteStart
	defer func() {
		l.mu.Lock()
		l.state = returnState
		l.mu.Unlock()
	}()

	var err error
	if parallelism > 1 {
		err = l.startParallel(ctx)
	} else {
		err = l.startSequential(ctx)
	}
	if err != nil {
		return err
	}

	returnState = started
	return nil
}

func (l *Lifecycle) startSequential(ctx context.Context) error {
	l.mu.Lock()
	hooks := append([]hookEntry(nil), l.hooks...)
	l.mu.Unlock()

	for i, entry := range hooks {
		// if ctx has cancelled, bail out of the loop.
		if err := ctx.Err(); err != nil {
			return err
		}

		hook := entry.Hook
		if hook.OnStart != nil {
			l.mu.Lock()
			l.runningHook = hook
			l.mu.Unlock()

			runtime, err := l.runStartHook(ctx, hook)
			if err != nil {
				return err
			}

			l.mu.Lock()
			l.startRecords = append(l.startRecords, HookRecord{
				CallerFrame: hook.callerFrame,
				Func:        hook.OnStart,
				Runtime:     runtime,
			})
			l.mu.Unlock()
		}
		l.mu.Lock()
		l.hooks[i].started = true
		l.numStarted++
		l.mu.Unlock()
	}
	return nil
}

func (l *Lifecycle) runStartHook(ctx context.Context, hook Hook) (runtime time.Duration, err error) {
	funcName := hook.OnStartName
	if len(funcName) == 0 {
		funcName = fxreflect.FuncName(hook.OnStart)
	}

	l.logEvent(&fxevent.OnStartExecuting{
		CallerName:   hook.callerFrame.Function,
		FunctionName: funcName,
	})
	defer func() {
		l.logEvent(&fxevent.OnStartExecuted{
			CallerName:   hook.callerFrame.Function,
			FunctionName: funcName,
			Runtime:      runtime,
			Err:          err,
		})
	}()

	begin := l.clock.Now()
	err = hook.OnStart(ctx)
	return l.clock.Since(begin), err
}

// Stop runs any OnStop hooks whose OnStart counterpart succeeded. OnStop
// hooks run in reverse order.
func (l *Lifecycle) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("called OnStop with nil context")
	}

	l.mu.Lock()
	if l.state != started && l.state != incompleteStart && l.state != starting {
		defer l.mu.Unlock()
		return nil
	}
	l.state = stopping
	parallelism := l.parallelism
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		l.state = stopped
		l.mu.Unlock()
	}()

	if parallelism > 1 {
		return l.stopParallel(ctx)
	}
	return l.stopSequential(ctx)
}

func (l *Lifecycle) stopSequential(ctx context.Context) error {
	l.mu.Lock()
	l.stopRecords = make(HookRecords, 0, l.numStarted)
	allHooks := append([]hookEntry(nil), l.hooks...)
	numStarted := l.numStarted
	l.mu.Unlock()

	var errs []error
	for ; numStarted > 0; numStarted-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		hook := allHooks[numStarted-1].Hook
		if hook.OnStop == nil {
			continue
		}

		l.mu.Lock()
		l.runningHook = hook
		l.mu.Unlock()

		runtime, err := l.runStopHook(ctx, hook)
		if err != nil {
			// For best-effort cleanup, keep going after errors.
			errs = append(errs, err)
		}

		l.mu.Lock()
		l.stopRecords = append(l.stopRecords, HookRecord{
			CallerFrame: hook.callerFrame,
			Func:        hook.OnStop,
			Runtime:     runtime,
		})
		l.mu.Unlock()
	}

	return multierr.Combine(errs...)
}

func (l *Lifecycle) runStopHook(ctx context.Context, hook Hook) (runtime time.Duration, err error) {
	funcName := hook.OnStopName
	if len(funcName) == 0 {
		funcName = fxreflect.FuncName(hook.OnStop)
	}

	l.logEvent(&fxevent.OnStopExecuting{
		CallerName:   hook.callerFrame.Function,
		FunctionName: funcName,
	})
	defer func() {
		l.logEvent(&fxevent.OnStopExecuted{
			CallerName:   hook.callerFrame.Function,
			FunctionName: funcName,
			Runtime:      runtime,
			Err:          err,
		})
	}()

	begin := l.clock.Now()
	err = hook.OnStop(ctx)
	return l.clock.Since(begin), err
}

func (l *Lifecycle) logEvent(event fxevent.Event) {
	l.eventMu.Lock()
	l.logger.LogEvent(event)
	l.eventMu.Unlock()
}

type hookPart struct {
	// indices contains the positions of owned hooks preceding the barrier.
	indices []int
	// barrier is the position of the following ownerless hook, or -1 if absent.
	barrier int
}

type indexedError struct {
	index int
	err   error
}

type ownerResult struct {
	owner int
	errs  []indexedError
}

func (l *Lifecycle) startParallel(ctx context.Context) error {
	l.mu.Lock()
	hooks := append([]hookEntry(nil), l.hooks...)
	dependencies := cloneDependencies(l.dependencies)
	parallelism := l.parallelism
	l.mu.Unlock()

	ownerDeps := lifecycleOwnerDependencies(hooks, dependencies)
	for _, part := range splitHookParts(hooks) {
		if len(part.indices) > 0 {
			errs, err := l.runStartSegment(ctx, hooks, part.indices, ownerDeps, parallelism)
			if err != nil {
				return err
			}
			if len(errs) > 0 {
				return combineIndexedErrors(errs, false)
			}
		}
		if part.barrier >= 0 {
			result := l.runStartOwner(ctx, hooks, []int{part.barrier}, 0)
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(result.errs) > 0 {
				return combineIndexedErrors(result.errs, false)
			}
		}
	}
	return nil
}

// runStartSegment starts the owned hooks at indices, respecting owner
// dependencies and the parallelism limit. Hooks belonging to the same owner run
// sequentially. After a hook fails, it waits for running owners to finish but
// does not start any more owners.
func (l *Lifecycle) runStartSegment(
	ctx context.Context,
	hooks []hookEntry,
	indices []int,
	ownerDeps map[int][]int,
	parallelism int,
) ([]indexedError, error) {
	jobs := groupHookIndices(hooks, indices)
	indegree, next := segmentEdges(jobs, ownerDeps, false)
	ready := readyOwners(jobs, indegree, false)
	results := make(chan ownerResult, len(jobs))

	var (
		running  int
		launched int
		failed   bool
		errs     []indexedError
	)
	for running > 0 || (!failed && len(ready) > 0) {
		for !failed && ctx.Err() == nil && running < parallelism && len(ready) > 0 {
			owner := ready[0]
			ready = ready[1:]
			running++
			launched++
			go func() {
				results <- l.runStartOwner(ctx, hooks, jobs[owner], owner)
			}()
		}

		if running == 0 {
			break
		}
		result := <-results
		running--
		if len(result.errs) > 0 {
			failed = true
			errs = append(errs, result.errs...)
			continue
		}
		for _, dependent := range next[result.owner] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		sortOwners(ready, jobs, false)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !failed && launched != len(jobs) {
		return nil, errors.New("lifecycle hook dependency graph contains a cycle")
	}
	return errs, nil
}

// runStartOwner starts an owner's hooks sequentially in append order. It stops
// at the first hook or context error and records each successful start.
func (l *Lifecycle) runStartOwner(
	ctx context.Context,
	hooks []hookEntry,
	indices []int,
	owner int,
) ownerResult {
	result := ownerResult{owner: owner}
	for _, index := range indices {
		if err := ctx.Err(); err != nil {
			result.errs = append(result.errs, indexedError{index: index, err: err})
			return result
		}

		hook := hooks[index].Hook
		if hook.OnStart != nil {
			l.mu.Lock()
			l.runningHook = hook
			l.mu.Unlock()

			runtime, err := l.runStartHook(ctx, hook)
			if err != nil {
				result.errs = append(result.errs, indexedError{index: index, err: err})
				return result
			}
			l.mu.Lock()
			l.startRecords = append(l.startRecords, HookRecord{
				CallerFrame: hook.callerFrame,
				Func:        hook.OnStart,
				Runtime:     runtime,
			})
			l.mu.Unlock()
		}

		l.mu.Lock()
		if index < len(l.hooks) {
			l.hooks[index].started = true
		}
		l.numStarted++
		l.mu.Unlock()
	}
	return result
}

func (l *Lifecycle) stopParallel(ctx context.Context) error {
	l.mu.Lock()
	hooks := append([]hookEntry(nil), l.hooks...)
	dependencies := cloneDependencies(l.dependencies)
	parallelism := l.parallelism
	l.stopRecords = make(HookRecords, 0, len(hooks))
	l.mu.Unlock()

	ownerDeps := lifecycleOwnerDependencies(hooks, dependencies)
	parts := splitHookParts(hooks)
	var errs []indexedError
	for i := len(parts) - 1; i >= 0; i-- {
		part := parts[i]
		if part.barrier >= 0 && hooks[part.barrier].started {
			result := l.runStopOwner(ctx, hooks, []int{part.barrier}, 0)
			errs = append(errs, result.errs...)
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		started := part.indices[:0]
		for _, index := range part.indices {
			if hooks[index].started {
				started = append(started, index)
			}
		}
		if len(started) == 0 {
			continue
		}
		segmentErrs, err := l.runStopSegment(ctx, hooks, started, ownerDeps, parallelism)
		if err != nil {
			return err
		}
		errs = append(errs, segmentErrs...)
	}
	return combineIndexedErrors(errs, true)
}

func (l *Lifecycle) runStopSegment(
	ctx context.Context,
	hooks []hookEntry,
	indices []int,
	ownerDeps map[int][]int,
	parallelism int,
) ([]indexedError, error) {
	jobs := groupHookIndices(hooks, indices)
	indegree, next := segmentEdges(jobs, ownerDeps, true)
	ready := readyOwners(jobs, indegree, true)
	results := make(chan ownerResult, len(jobs))
	var (
		running   int
		completed int
		errs      []indexedError
	)
	for running > 0 || len(ready) > 0 {
		for ctx.Err() == nil && running < parallelism && len(ready) > 0 {
			owner := ready[0]
			ready = ready[1:]
			running++
			go func() {
				results <- l.runStopOwner(ctx, hooks, jobs[owner], owner)
			}()
		}
		if running == 0 {
			break
		}

		result := <-results
		running--
		completed++
		errs = append(errs, result.errs...)
		for _, dependent := range next[result.owner] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		sortOwners(ready, jobs, true)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if completed != len(jobs) {
		return nil, errors.New("lifecycle hook dependency graph contains a cycle")
	}
	return errs, nil
}

func (l *Lifecycle) runStopOwner(
	ctx context.Context,
	hooks []hookEntry,
	indices []int,
	owner int,
) ownerResult {
	result := ownerResult{owner: owner}
	for i := len(indices) - 1; i >= 0; i-- {
		index := indices[i]
		if err := ctx.Err(); err != nil {
			result.errs = append(result.errs, indexedError{index: index, err: err})
			return result
		}

		hook := hooks[index].Hook
		if hook.OnStop == nil {
			continue
		}
		l.mu.Lock()
		l.runningHook = hook
		l.mu.Unlock()

		runtime, err := l.runStopHook(ctx, hook)
		if err != nil {
			result.errs = append(result.errs, indexedError{index: index, err: err})
		}
		l.mu.Lock()
		l.stopRecords = append(l.stopRecords, HookRecord{
			CallerFrame: hook.callerFrame,
			Func:        hook.OnStop,
			Runtime:     runtime,
		})
		l.mu.Unlock()
	}
	return result
}

// splitHookParts divides hooks into ordered parts of owned hooks followed by an
// optional ownerless barrier hook. Barriers keep ownerless hooks in append order
// while allowing the owned hooks between them to be scheduled in parallel. A
// trailing group of owned hooks has a barrier index of -1.
func splitHookParts(hooks []hookEntry) []hookPart {
	var (
		parts   []hookPart
		indices []int
	)
	flush := func(barrier int) {
		if len(indices) == 0 && barrier < 0 {
			return
		}
		parts = append(parts, hookPart{
			indices: append([]int(nil), indices...),
			barrier: barrier,
		})
		indices = indices[:0]
	}
	for index, hook := range hooks {
		if hook.owner == 0 {
			flush(index)
			continue
		}
		indices = append(indices, index)
	}
	flush(-1)
	return parts
}

func groupHookIndices(hooks []hookEntry, indices []int) map[int][]int {
	jobs := make(map[int][]int)
	for _, index := range indices {
		owner := hooks[index].owner
		jobs[owner] = append(jobs[owner], index)
	}
	return jobs
}

// lifecycleOwnerDependencies returns the lifecycle owners that each owner must
// wait for. It walks transitively through components without hooks, stopping at
// the first component on each path that owns hooks. The result contains only
// owners with hooks, excludes self-dependencies, and sorts each dependency list
// for deterministic scheduling.
func lifecycleOwnerDependencies(hooks []hookEntry, dependencies map[int][]int) map[int][]int {
	hasHooks := make(map[int]bool)
	for _, hook := range hooks {
		if hook.owner > 0 {
			hasHooks[hook.owner] = true
		}
	}

	result := make(map[int][]int, len(hasHooks))
	for owner := range hasHooks {
		seen := make(map[int]bool)
		var visit func(int)
		visit = func(component int) {
			for _, dependency := range dependencies[component] {
				if dependency == owner || seen[dependency] {
					continue
				}
				seen[dependency] = true
				if hasHooks[dependency] {
					result[owner] = append(result[owner], dependency)
					continue
				}
				visit(dependency)
			}
		}
		visit(owner)
		sort.Ints(result[owner])
	}
	return result
}

func segmentEdges(
	jobs map[int][]int,
	ownerDeps map[int][]int,
	reverse bool,
) (map[int]int, map[int][]int) {
	indegree := make(map[int]int, len(jobs))
	next := make(map[int][]int, len(jobs))
	for owner := range jobs {
		indegree[owner] = 0
	}
	for owner := range jobs {
		for _, dependency := range ownerDeps[owner] {
			if _, ok := jobs[dependency]; !ok {
				continue
			}
			from, to := dependency, owner
			if reverse {
				from, to = to, from
			}
			next[from] = append(next[from], to)
			indegree[to]++
		}
	}
	return indegree, next
}

func readyOwners(jobs map[int][]int, indegree map[int]int, reverse bool) []int {
	ready := make([]int, 0, len(jobs))
	for owner := range jobs {
		if indegree[owner] == 0 {
			ready = append(ready, owner)
		}
	}
	sortOwners(ready, jobs, reverse)
	return ready
}

func sortOwners(owners []int, jobs map[int][]int, reverse bool) {
	sort.Slice(owners, func(i, j int) bool {
		left := jobs[owners[i]][0]
		right := jobs[owners[j]][0]
		if reverse {
			return left > right
		}
		return left < right
	})
}

func combineIndexedErrors(errs []indexedError, reverse bool) error {
	if len(errs) == 0 {
		return nil
	}
	sort.SliceStable(errs, func(i, j int) bool {
		if reverse {
			return errs[i].index > errs[j].index
		}
		return errs[i].index < errs[j].index
	})
	combined := make([]error, 0, len(errs))
	for _, indexed := range errs {
		combined = append(combined, indexed.err)
	}
	return multierr.Combine(combined...)
}

func cloneDependencies(dependencies map[int][]int) map[int][]int {
	cloned := make(map[int][]int, len(dependencies))
	for component, deps := range dependencies {
		cloned[component] = append([]int(nil), deps...)
	}
	return cloned
}

// RunningHookCaller returns the name of the hook that was running when a Start/Stop
// hook timed out.
func (l *Lifecycle) RunningHookCaller() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.runningHook.callerFrame.Function
}

// HookRecord keeps track of each Hook's execution time, the caller that appended the Hook, and function that ran as the Hook.
type HookRecord struct {
	CallerFrame fxreflect.Frame             // stack frame of the caller
	Func        func(context.Context) error // function that ran as sanitized name
	Runtime     time.Duration               // how long the hook ran
}

// HookRecords is a Stringer wrapper of HookRecord slice.
type HookRecords []HookRecord

func (rs HookRecords) Len() int {
	return len(rs)
}

func (rs HookRecords) Less(i, j int) bool {
	// Sort by runtime, greater ones at top.
	return rs[i].Runtime > rs[j].Runtime
}

func (rs HookRecords) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// Used for logging startup errors.
func (rs HookRecords) String() string {
	var b strings.Builder
	for _, r := range rs {
		fmt.Fprintf(&b, "%s took %v from %s",
			fxreflect.FuncName(r.Func), r.Runtime, r.CallerFrame)
	}
	return b.String()
}

// Format implements fmt.Formatter to handle "%+v".
func (rs HookRecords) Format(w fmt.State, c rune) {
	if !w.Flag('+') {
		// Without %+v, fall back to String().
		io.WriteString(w, rs.String())
		return
	}

	for _, r := range rs {
		fmt.Fprintf(w, "\n%s took %v from:\n\t%+v",
			fxreflect.FuncName(r.Func),
			r.Runtime,
			r.CallerFrame)
	}
	fmt.Fprintf(w, "\n")
}
