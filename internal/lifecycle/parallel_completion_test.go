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

package lifecycle_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxevent"
	"go.uber.org/fx/internal/fxclock"
	"go.uber.org/fx/internal/lifecycle"
)

type completionLogger func(fxevent.Event)

func (f completionLogger) LogEvent(event fxevent.Event) { f(event) }

func awaitLifecycleResult(t *testing.T, call func(context.Context) error) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- call(context.Background()) }()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle did not return after its worker exited")
		return nil
	}
}

func TestParallelFailureStopsRemainingHooks(t *testing.T) {
	for _, exit := range []bool{false, true} {
		name := "error"
		if exit {
			name = "goexit"
		}
		t.Run(name, func(t *testing.T) {
			failed := make(chan struct{})
			firstRunning := make(chan struct{})
			startErr := errors.New("start failed")
			if exit {
				startErr = lifecycle.ErrHookCallbackExited
			}
			logger := completionLogger(func(event fxevent.Event) {
				if event, ok := event.(*fxevent.OnStartExecuted); ok && event.Err != nil {
					close(failed)
				}
			})
			l := lifecycle.New(logger, fxclock.System)
			l.SetParallelism(2)
			var mu sync.Mutex
			var stopped []string
			stop := func(name string) func(context.Context) error {
				return func(context.Context) error {
					mu.Lock()
					defer mu.Unlock()
					stopped = append(stopped, name)
					return nil
				}
			}
			l.AppendWithOwner(lifecycle.Hook{
				OnStart: func(context.Context) error {
					close(firstRunning)
					<-failed
					return nil
				},
				OnStop: stop("completed"),
			}, 1)
			unexpectedStart := func(context.Context) error {
				t.Error("hook admitted after startup failure")
				return nil
			}
			l.AppendWithOwner(lifecycle.Hook{OnStart: unexpectedStart, OnStop: stop("later")}, 1)
			l.AppendWithOwner(lifecycle.Hook{OnStop: stop("stop-only later")}, 1)
			l.AppendWithOwner(lifecycle.Hook{
				OnStart: func(context.Context) error {
					<-firstRunning
					if exit {
						runtime.Goexit()
					}
					return startErr
				},
				OnStop: stop("failed"),
			}, 2)
			l.AppendWithOwner(lifecycle.Hook{OnStart: unexpectedStart, OnStop: stop("queued")}, 3)

			require.ErrorIs(t, awaitLifecycleResult(t, l.Start), startErr)
			require.NoError(t, awaitLifecycleResult(t, l.Stop))
			assert.Equal(t, []string{"completed"}, stopped)
		})
	}
}

func TestParallelStartGoexitReturnsAndPreservesRollback(t *testing.T) {
	l := lifecycle.New(fxevent.NopLogger, fxclock.System)
	l.SetParallelism(2)
	l.SetDependencies(map[int][]int{2: {1}})
	var stopped []string
	l.AppendWithOwner(lifecycle.Hook{
		OnStart: func(context.Context) error { return nil },
		OnStop: func(context.Context) error {
			stopped = append(stopped, "completed")
			return nil
		},
	}, 1)
	unexpected := func(context.Context) error {
		t.Error("uncompleted hook must remain skipped")
		return nil
	}
	l.AppendWithOwner(lifecycle.Hook{
		OnStart: func(context.Context) error { runtime.Goexit(); return nil },
		OnStop:  unexpected,
	}, 1)
	l.AppendWithOwner(lifecycle.Hook{OnStart: unexpected, OnStop: unexpected}, 1)
	l.AppendWithOwner(lifecycle.Hook{OnStart: unexpected, OnStop: unexpected}, 2)

	err := awaitLifecycleResult(t, l.Start)
	require.EqualError(t, err, "goroutine exited without returning")
	assert.ErrorIs(t, err, lifecycle.ErrHookCallbackExited)
	require.NoError(t, awaitLifecycleResult(t, l.Stop))
	assert.Equal(t, []string{"completed"}, stopped)
}

func TestParallelStopGoexitPreservesErrorsAndContinues(t *testing.T) {
	l := lifecycle.New(fxevent.NopLogger, fxclock.System)
	l.SetParallelism(2)
	l.SetDependencies(map[int][]int{2: {1}})
	firstErr := errors.New("first stop error")
	var order []string
	l.AppendWithOwner(lifecycle.Hook{OnStop: func(context.Context) error {
		order = append(order, "dependency")
		return nil
	}}, 1)
	l.AppendWithOwner(lifecycle.Hook{OnStop: func(context.Context) error {
		t.Error("remaining hooks of exited owner must be skipped")
		return nil
	}}, 2)
	l.AppendWithOwner(lifecycle.Hook{OnStop: func(context.Context) error {
		order = append(order, "exit")
		runtime.Goexit()
		return nil
	}}, 2)
	l.AppendWithOwner(lifecycle.Hook{OnStop: func(context.Context) error {
		order = append(order, "error")
		return firstErr
	}}, 2)

	require.NoError(t, awaitLifecycleResult(t, l.Start))
	err := awaitLifecycleResult(t, l.Stop)
	require.EqualError(t, err, "first stop error; goroutine exited without returning")
	assert.ErrorIs(t, err, firstErr)
	assert.ErrorIs(t, err, lifecycle.ErrHookCallbackExited)
	assert.Equal(t, []string{"error", "exit", "dependency"}, order)
}
