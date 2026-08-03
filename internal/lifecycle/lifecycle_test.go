// Copyright (c) 2019 Uber Technologies, Inc.
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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxevent"
	"go.uber.org/fx/internal/fxclock"
	"go.uber.org/fx/internal/fxlog"
	"go.uber.org/fx/internal/fxreflect"
	"go.uber.org/fx/internal/testutil"
	"go.uber.org/goleak"
	"go.uber.org/multierr"
)

func testLogger(t *testing.T) fxevent.Logger {
	return fxlog.DefaultLogger(testutil.WriteSyncer{T: t})
}

func TestLifecycleStart(t *testing.T) {
	t.Parallel()

	t.Run("ExecutesInOrder", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		count := 0

		l.Append(Hook{
			OnStart: func(context.Context) error {
				count++
				assert.Equal(t, 1, count, "expected this starter to be executed first")
				return nil
			},
		})
		l.Append(Hook{
			OnStart: func(context.Context) error {
				count++
				assert.Equal(t, 2, count, "expected this starter to be executed second")
				return nil
			},
		})

		assert.NoError(t, l.Start(context.Background()))
		assert.Equal(t, 2, count)
	})

	t.Run("ErrHaltsChainAndRollsBack", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		err := errors.New("a starter error")
		starterCount := 0
		stopperCount := 0

		// this event's starter succeeded, so no matter what the stopper should run
		l.Append(Hook{
			OnStart: func(context.Context) error {
				starterCount++
				return nil
			},
			OnStop: func(context.Context) error {
				stopperCount++
				return nil
			},
		})
		// this event's starter fails, so the stopper shouldnt run
		l.Append(Hook{
			OnStart: func(context.Context) error {
				starterCount++
				return err
			},
			OnStop: func(context.Context) error {
				t.Error("this stopper shouldnt run, since the starter in this event failed")
				return nil
			},
		})
		// this event is last in the chain, so it should never run since the previous failed
		l.Append(Hook{
			OnStart: func(context.Context) error {
				t.Error("this starter should never run, since the previous event failed")
				return nil
			},
			OnStop: func(context.Context) error {
				t.Error("this stopper should never run, since the previous event failed")
				return nil
			},
		})

		assert.Error(t, err, l.Start(context.Background()))
		assert.NoError(t, l.Stop(context.Background()))

		assert.Equal(t, 2, starterCount, "expected the first and second starter to execute")
		assert.Equal(t, 1, stopperCount, "expected the first stopper to execute since the second starter failed")
	})

	t.Run("DoNotRunStartHooksWithExpiredCtx", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		l.Append(Hook{
			OnStart: func(context.Context) error {
				assert.Fail(t, "this hook should not run")
				return nil
			},
			OnStop: func(context.Context) error {
				assert.Fail(t, "this hook should not run")
				return nil
			},
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := l.Start(ctx)
		require.Error(t, err)
		// Note: Stop does not return an error here because no hooks
		// have been started, so we don't end up any of the corresponding
		// stop hooks.
		require.NoError(t, l.Stop(ctx))
	})

	t.Run("StartWhileStartedErrors", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		assert.NoError(t, l.Start(context.Background()))
		err := l.Start(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "attempted to start lifecycle when in state: started")
		assert.NoError(t, l.Stop(context.Background()))
		assert.NoError(t, l.Start(context.Background()))
	})
}

func TestLifecycleStop(t *testing.T) {
	t.Parallel()

	t.Run("DoesNothingWithoutHooks", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		l.Start(context.Background())
		assert.Nil(t, l.Stop(context.Background()), "no lifecycle hooks should have resulted in stop returning nil")
	})

	t.Run("DoesNothingWhenNotStarted", func(t *testing.T) {
		t.Parallel()

		hook := Hook{
			OnStop: func(context.Context) error {
				assert.Fail(t, "OnStop should not be called if lifecycle was never started")
				return nil
			},
		}
		l := New(testLogger(t), fxclock.System)
		l.Append(hook)
		l.Stop(context.Background())
	})

	t.Run("ExecutesInReverseOrder", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		count := 2

		l.Append(Hook{
			OnStop: func(context.Context) error {
				count--
				assert.Equal(t, 0, count, "this stopper was added first, so should execute last")
				return nil
			},
		})
		l.Append(Hook{
			OnStop: func(context.Context) error {
				count--
				assert.Equal(t, 1, count, "this stopper was added last, so should execute first")
				return nil
			},
		})

		assert.NoError(t, l.Start(context.Background()))
		assert.NoError(t, l.Stop(context.Background()))
		assert.Equal(t, 0, count)
	})

	t.Run("ErrDoesntHaltChain", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		count := 0

		l.Append(Hook{
			OnStop: func(context.Context) error {
				count++
				return nil
			},
		})
		err := errors.New("some stop error")
		l.Append(Hook{
			OnStop: func(context.Context) error {
				count++
				return err
			},
		})

		assert.NoError(t, l.Start(context.Background()))
		assert.Equal(t, err, l.Stop(context.Background()))
		assert.Equal(t, 2, count)
	})
	t.Run("GathersAllErrs", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)

		err := errors.New("some stop error")
		err2 := errors.New("some other stop error")

		l.Append(Hook{
			OnStop: func(context.Context) error {
				return err2
			},
		})
		l.Append(Hook{
			OnStop: func(context.Context) error {
				return err
			},
		})

		assert.NoError(t, l.Start(context.Background()))
		assert.Equal(t, multierr.Combine(err, err2), l.Stop(context.Background()))
	})
	t.Run("AllowEmptyHooks", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		l.Append(Hook{})
		l.Append(Hook{})

		assert.NoError(t, l.Start(context.Background()))
		assert.NoError(t, l.Stop(context.Background()))
	})

	t.Run("DoesNothingIfStartFailed", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		err := errors.New("some start error")

		l.Append(Hook{
			OnStart: func(context.Context) error {
				return err
			},
			OnStop: func(context.Context) error {
				assert.Fail(t, "OnStop should not be called if start failed")
				return nil
			},
		})

		assert.Equal(t, err, l.Start(context.Background()))
		l.Stop(context.Background())
	})

	t.Run("DoNotRunStopHooksWithExpiredCtx", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		l.Append(Hook{
			OnStart: func(context.Context) error {
				return nil
			},
			OnStop: func(context.Context) error {
				assert.Fail(t, "this hook should not run")
				return nil
			},
		})
		ctx, cancel := context.WithCancel(context.Background())
		err := l.Start(ctx)
		require.NoError(t, err)
		cancel()
		require.Error(t, l.Stop(ctx))
	})

	t.Run("nil ctx", func(t *testing.T) {
		t.Parallel()

		l := New(testLogger(t), fxclock.System)
		l.Append(Hook{
			OnStart: func(context.Context) error {
				assert.Fail(t, "this hook should not run")
				return nil
			},
			OnStop: func(context.Context) error {
				assert.Fail(t, "this hook should not run")
				return nil
			},
		})
		err := l.Start(nil) //nolint:staticcheck // SA1012 this test specifically tests for the lint failure
		require.Error(t, err)
		assert.Contains(t, err.Error(), "called OnStart with nil context")

		err = l.Stop(nil) //nolint:staticcheck // SA1012 this test specifically tests for the lint failure
		require.Error(t, err)
		assert.Contains(t, err.Error(), "called OnStop with nil context")
	})
}

func TestLifecycleParallelDependencyOrder(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(2)
	l.SetDependencies(map[int][]int{3: {1, 2}})

	branchesReady := make(chan struct{})
	var ready int
	var mu sync.Mutex
	done := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	for i := range 2 {
		i := i
		l.AppendWithOwner(Hook{OnStart: func(context.Context) error {
			mu.Lock()
			ready++
			if ready == 2 {
				close(branchesReady)
			}
			mu.Unlock()
			<-branchesReady
			close(done[i])
			return nil
		}}, i+1)
	}

	l.AppendWithOwner(Hook{OnStart: func(context.Context) error {
		for i := range done {
			select {
			case <-done[i]:
			default:
				return fmt.Errorf("dependency %d has not started", i)
			}
		}
		return nil
	}}, 3)

	require.NoError(t, l.Start(t.Context()))
	require.NoError(t, l.Stop(t.Context()))
}

func TestLifecycleParallelStopReversesDAG(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(2)
	l.SetDependencies(map[int][]int{3: {1, 2}})

	dependentStopped := make(chan struct{})
	branchesStopped := make(chan struct{})
	var stopped int
	var mu sync.Mutex
	for owner := 1; owner <= 2; owner++ {
		l.AppendWithOwner(Hook{OnStop: func(context.Context) error {
			select {
			case <-dependentStopped:
			default:
				return errors.New("dependency stopped before dependent")
			}
			mu.Lock()
			stopped++
			if stopped == 2 {
				close(branchesStopped)
			}
			mu.Unlock()
			return nil
		}}, owner)
	}
	l.AppendWithOwner(Hook{OnStop: func(context.Context) error {
		close(dependentStopped)
		return nil
	}}, 3)

	require.NoError(t, l.Start(t.Context()))
	require.NoError(t, l.Stop(t.Context()))
	select {
	case <-branchesStopped:
	default:
		t.Fatal("dependency stop hooks did not run")
	}
}

func TestLifecycleParallelPreservesOwnerHookOrder(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(4)

	var order []string
	var mu sync.Mutex
	appendOrder := func(value string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			order = append(order, value)
			mu.Unlock()
			return nil
		}
	}
	l.AppendWithOwner(Hook{OnStart: appendOrder("start-1"), OnStop: appendOrder("stop-1")}, 1)
	l.AppendWithOwner(Hook{OnStart: appendOrder("start-2"), OnStop: appendOrder("stop-2")}, 1)

	require.NoError(t, l.Start(t.Context()))
	require.NoError(t, l.Stop(t.Context()))
	assert.Equal(t, []string{"start-1", "start-2", "stop-2", "stop-1"}, order)
}

func TestLifecycleParallelStartErrors(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(2)
	l.SetDependencies(map[int][]int{3: {1}})

	ready := make(chan struct{})
	var count int
	var mu sync.Mutex
	for owner, message := range []string{"first", "second"} {
		owner, message := owner+1, message
		l.AppendWithOwner(Hook{OnStart: func(context.Context) error {
			mu.Lock()
			count++
			if count == 2 {
				close(ready)
			}
			mu.Unlock()
			<-ready
			return errors.New(message)
		}}, owner)
	}
	l.AppendWithOwner(Hook{OnStart: func(context.Context) error {
		t.Fatal("dependent hook must not run")
		return nil
	}}, 3)
	l.AppendWithOwner(Hook{OnStart: func(context.Context) error {
		t.Fatal("queued independent hook must not run after an error")
		return nil
	}}, 4)

	err := l.Start(t.Context())
	require.EqualError(t, err, "first; second")
}

func TestLifecycleParallelCancellation(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(2)
	for owner := 1; owner <= 2; owner++ {
		l.AppendWithOwner(Hook{OnStart: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}, owner)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, l.Start(ctx), context.DeadlineExceeded)
	require.NoError(t, l.Stop(t.Context()))
}

func TestLifecycleParallelStopCancellation(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(2)
	for owner := 1; owner <= 2; owner++ {
		l.AppendWithOwner(Hook{OnStop: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}, owner)
	}

	require.NoError(t, l.Start(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, l.Stop(ctx), context.DeadlineExceeded)
}

func TestLifecycleParallelUnknownHookIsBarrier(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(2)

	branchesReady := make(chan struct{})
	var ready int
	var mu sync.Mutex
	for owner := 1; owner <= 2; owner++ {
		l.AppendWithOwner(Hook{OnStart: func(context.Context) error {
			mu.Lock()
			ready++
			if ready == 2 {
				close(branchesReady)
			}
			mu.Unlock()
			<-branchesReady
			return nil
		}}, owner)
	}

	barrierDone := make(chan struct{})
	l.Append(Hook{OnStart: func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if ready != 2 {
			return errors.New("barrier ran before earlier hooks completed")
		}
		close(barrierDone)
		return nil
	}})
	l.AppendWithOwner(Hook{OnStart: func(context.Context) error {
		select {
		case <-barrierDone:
			return nil
		default:
			return errors.New("later hook ran before barrier")
		}
	}}, 3)

	require.NoError(t, l.Start(t.Context()))
	require.NoError(t, l.Stop(t.Context()))
}

func TestLifecycleParallelStopGathersErrorsAndContinues(t *testing.T) {
	t.Parallel()

	l := New(testLogger(t), fxclock.System)
	l.SetParallelism(2)
	l.SetDependencies(map[int][]int{3: {1, 2}})

	for owner, message := range []string{"dependency-1", "dependency-2"} {
		l.AppendWithOwner(Hook{OnStop: func(context.Context) error {
			return errors.New(message)
		}}, owner+1)
	}
	l.AppendWithOwner(Hook{OnStop: func(context.Context) error {
		return errors.New("dependent")
	}}, 3)

	require.NoError(t, l.Start(t.Context()))
	require.EqualError(t, l.Stop(t.Context()), "dependent; dependency-2; dependency-1")
}

func TestHookRecordsFormat(t *testing.T) {
	t.Parallel()

	t.Run("SortRecords", func(t *testing.T) {
		t.Parallel()

		t1, err := time.ParseDuration("10ms")
		require.NoError(t, err)
		t2, err := time.ParseDuration("20ms")
		require.NoError(t, err)

		f := fxreflect.Frame{
			Function: "someFunc",
			File:     "somefunc.go",
			Line:     1,
		}

		r := HookRecords{
			HookRecord{
				CallerFrame: f,
				Func: func(context.Context) error {
					return nil
				},
				Runtime: t1,
			},
			HookRecord{
				CallerFrame: f,
				Func: func(context.Context) error {
					return nil
				},
				Runtime: t2,
			},
		}

		sort.Sort(r)
		for _, format := range []string{"%v", "%+v", "%s"} {
			s := fmt.Sprintf(format, r)
			hook1Idx := strings.Index(s, "TestHookRecordsFormat.func1.1()")
			hook2Idx := strings.Index(s, "TestHookRecordsFormat.func1.2()")
			assert.Greater(t, hook1Idx, hook2Idx, "second hook must appear first in the formatted string")
			assert.Contains(t, s, "somefunc.go:1", "file name and line should be reported")
		}
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
