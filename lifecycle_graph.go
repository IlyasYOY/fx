// Copyright (c) 2026 Uber Technologies, Inc.
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

package fx

import (
	"reflect"
	"strings"
	"sync"

	"go.uber.org/dig"
)

type hookScope struct {
	parent *hookScope
}

// hookKey preserves type identity even when two packages share a name.
// Group membership is separate from the group name because a group may have
// an empty name (for example, group:",flatten").
type hookKey struct {
	typ       reflect.Type
	name      string
	group     string
	isGrouped bool
}

type hookComponentKind int

const (
	hookProvider hookComponentKind = iota
	hookDecorator
	hookInvoke
)

type hookComponent struct {
	id                int
	kind              hookComponentKind
	scope             *hookScope
	outputScope       *hookScope
	inputs            []hookKey
	outputs           []hookKey
	constructionOrder int
}

// hookGraph mirrors the portion of Dig's graph needed to order lifecycle
// hooks. Dig owns value construction; this graph only records component
// ownership and dependency edges for lifecycle execution.
type hookGraph struct {
	mu sync.Mutex

	root        *hookScope
	components  []*hookComponent
	owners      []int
	constructed int
}

func newHookGraph() *hookGraph {
	root := new(hookScope)
	return &hookGraph{root: root}
}

func (g *hookGraph) newScope(parent *hookScope) *hookScope {
	return &hookScope{parent: parent}
}

func (g *hookGraph) newComponent(scope *hookScope, kind hookComponentKind) *hookComponent {
	g.mu.Lock()
	defer g.mu.Unlock()

	c := &hookComponent{
		id:    len(g.components) + 1,
		kind:  kind,
		scope: scope,
	}
	g.components = append(g.components, c)
	return c
}

func (g *hookGraph) begin(c *hookComponent) {
	g.mu.Lock()
	g.owners = append(g.owners, c.id)
	g.mu.Unlock()
}

func (g *hookGraph) end(c *hookComponent) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if n := len(g.owners); n > 0 && g.owners[n-1] == c.id {
		g.owners = g.owners[:n-1]
	}
	if c.constructionOrder == 0 {
		g.constructed++
		c.constructionOrder = g.constructed
	}
}

func (g *hookGraph) currentOwner() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.owners) == 0 {
		return 0
	}
	return g.owners[len(g.owners)-1]
}

// hookContainer observes the final functions passed to Dig, after annotation
// transformation. Dig's display-oriented metadata is still used for logging.
// Only successfully validated functions are inspected here.
type hookContainer struct {
	container
	graph     *hookGraph
	component *hookComponent
	exported  bool
	name      string
	group     string
}

func (g *hookGraph) container(c container, component *hookComponent, exported bool) *hookContainer {
	return &hookContainer{container: c, graph: g, component: component, exported: exported}
}

func (c *hookContainer) Provide(fn any, opts ...dig.ProvideOption) error {
	if err := c.container.Provide(fn, opts...); err != nil {
		return err
	}
	c.record(fn)
	return nil
}

func (c *hookContainer) Decorate(fn any, opts ...dig.DecorateOption) error {
	if err := c.container.Decorate(fn, opts...); err != nil {
		return err
	}
	c.record(fn)
	return nil
}

func (c *hookContainer) Invoke(fn any, opts ...dig.InvokeOption) error {
	if err := c.container.Invoke(fn, opts...); err != nil {
		return err
	}
	c.record(fn)
	return nil
}

func (c *hookContainer) record(fn any) {
	g := c.graph
	g.mu.Lock()
	defer g.mu.Unlock()

	component := c.component
	ft := reflect.TypeOf(fn)
	numIn := ft.NumIn()
	if ft.IsVariadic() {
		// Dig does not resolve ordinary variadic arguments. Annotated
		// variadics have already been transformed into non-variadic functions.
		numIn--
	}
	for i := 0; i < numIn; i++ {
		component.inputs = append(component.inputs, hookInputs(ft.In(i), "", "")...)
	}
	if component.kind == hookInvoke {
		return
	}
	for i := 0; i < ft.NumOut(); i++ {
		output := ft.Out(i)
		if output.Implements(_typeOfError) {
			continue
		}
		component.outputs = append(component.outputs,
			hookOutputs(output, c.name, c.group, component.kind == hookDecorator)...)
	}
	component.outputScope = component.scope
	if c.exported {
		component.outputScope = g.root
	}
}

func hookInputs(t reflect.Type, name, group string) []hookKey {
	if !isIn(t) {
		return []hookKey{newHookKey(t, name, group, true)}
	}
	var result []hookKey
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		// Dig already validated the struct, so any unexported fields here
		// must have been explicitly ignored with ignore-unexported.
		if field.Type == _typeOfIn || field.PkgPath != "" {
			continue
		}
		result = append(result, hookInputs(field.Type, field.Tag.Get("name"), field.Tag.Get("group"))...)
	}
	return result
}

func hookOutputs(t reflect.Type, name, group string, decorator bool) []hookKey {
	// A grouped result field is an opaque member, even if its type embeds
	// Out or implements error. Only top-level error results are discarded.
	if group != "" || !isOut(t) {
		return []hookKey{newHookKey(t, name, group, decorator)}
	}
	var result []hookKey
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Type == reflect.TypeOf(Out{}) {
			continue
		}
		result = append(result,
			hookOutputs(field.Type, field.Tag.Get("name"), field.Tag.Get("group"), decorator)...)
	}
	return result
}

func newHookKey(t reflect.Type, name, group string, groupSlice bool) hookKey {
	key := hookKey{typ: t, name: name, isGrouped: group != ""}
	if !key.isGrouped {
		return key
	}
	parts := strings.Split(group, ",")
	key.group = parts[0]
	for _, option := range parts[1:] {
		if option == "flatten" {
			groupSlice = true
		}
	}
	// Consumers and decorators represent a whole group as a slice. A
	// provider's slice is itself a member unless flatten was specified.
	// In either case remove exactly one dimension.
	if groupSlice && t.Kind() == reflect.Slice {
		key.typ = t.Elem()
	}
	return key
}

func (g *hookGraph) dependencies() map[int][]int {
	g.mu.Lock()
	defer g.mu.Unlock()

	providers := make(map[*hookScope]map[hookKey][]int)
	decorators := make(map[*hookScope]map[hookKey]int)
	for _, c := range g.components {
		if len(c.outputs) == 0 {
			continue
		}

		scope := c.outputScope
		if scope == nil {
			scope = c.scope
		}
		switch c.kind {
		case hookDecorator:
			byKey := decorators[scope]
			if byKey == nil {
				byKey = make(map[hookKey]int)
				decorators[scope] = byKey
			}
			for _, output := range c.outputs {
				byKey[output] = c.id
			}
		default:
			byKey := providers[scope]
			if byKey == nil {
				byKey = make(map[hookKey][]int)
				providers[scope] = byKey
			}
			for _, output := range c.outputs {
				byKey[output] = append(byKey[output], c.id)
			}
		}
	}

	deps := make(map[int][]int, len(g.components))
	for _, c := range g.components {
		seen := make(map[int]struct{})
		for _, input := range c.inputs {
			for _, dep := range resolveHookInput(c, input, providers, decorators, g.components) {
				if dep == c.id {
					continue
				}
				seen[dep] = struct{}{}
			}
		}
		for dep := range seen {
			deps[c.id] = append(deps[c.id], dep)
		}
	}
	return deps
}

func resolveHookInput(
	c *hookComponent,
	input hookKey,
	providers map[*hookScope]map[hookKey][]int,
	decorators map[*hookScope]map[hookKey]int,
	components []*hookComponent,
) []int {
	var found []int
	for scope := c.scope; scope != nil; scope = scope.parent {
		if decorator := decorators[scope][input]; decorator != 0 && decorator != c.id &&
			constructedBefore(components, decorator, c) {
			found = append(found, decorator)
			if !input.isGrouped {
				return found
			}
		}
	}
	if len(found) > 0 {
		return found
	}

	for scope := c.scope; scope != nil; scope = scope.parent {
		if scoped := providers[scope][input]; len(scoped) > 0 {
			for _, provider := range scoped {
				if !input.isGrouped || constructedBefore(components, provider, c) {
					found = append(found, provider)
				}
			}
			if !input.isGrouped {
				return found
			}
		}
	}
	return found
}

func constructedBefore(components []*hookComponent, dependency int, dependent *hookComponent) bool {
	if dependency <= 0 || dependency > len(components) {
		return false
	}
	// Dig constructs every hard group provider before its consumer. A soft
	// group may leave registered providers unconstructed until a later invoke;
	// those providers did not contribute a value to this consumer.
	order := components[dependency-1].constructionOrder
	return order > 0 && order < dependent.constructionOrder
}
