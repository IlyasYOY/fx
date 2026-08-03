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

package fx_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	. "go.uber.org/fx"
)

type (
	parallelHooksB    struct{}
	parallelHooksC    struct{}
	parallelHooksA    struct{}
	parallelDecorated struct{}
	parallelGrouped   struct{}
	parallelConsumer  struct{}
	parallelPrivate   struct{}
	parallelTrigger   struct{}
)

type parallelGroupParams struct {
	In

	Items []*parallelGrouped `group:"parallel"`
}

type parallelSoftGroupParams struct {
	In

	Items []*parallelGrouped `group:"parallel-soft,soft"`
}

type parallelSoftGroupResult struct {
	Out

	Item    *parallelGrouped `group:"parallel-soft"`
	Trigger *parallelTrigger
}

type parallelDecoratedGroupResult struct {
	Out

	Items []*parallelGrouped `group:"parallel"`
}

func TestParallelHooksOption(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "fx.ParallelHooks(3)", fmt.Sprint(ParallelHooks(3)))
	assert.ErrorContains(t, New(NopLogger, ParallelHooks(0)).Err(), "positive limit")
	assert.ErrorContains(t,
		New(NopLogger, Module("child", ParallelHooks(2))).Err(),
		"top-level App",
	)
}

func TestParallelHooksFollowDependencyDAG(t *testing.T) {
	t.Parallel()

	branchesReady := make(chan struct{})
	branchDone := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	dependentStopped := make(chan struct{})
	var ready atomic.Int32

	branchHook := func(index int) Hook {
		return Hook{
			OnStart: func(context.Context) error {
				if ready.Add(1) == 2 {
					close(branchesReady)
				}
				<-branchesReady
				close(branchDone[index])
				return nil
			},
			OnStop: func(context.Context) error {
				select {
				case <-dependentStopped:
					return nil
				default:
					return fmt.Errorf("branch %d stopped before its dependent", index)
				}
			},
		}
	}

	app := New(
		NopLogger,
		ParallelHooks(2),
		Provide(
			func(lc Lifecycle) *parallelHooksB {
				lc.Append(branchHook(0))
				return new(parallelHooksB)
			},
			func(lc Lifecycle) *parallelHooksC {
				lc.Append(branchHook(1))
				return new(parallelHooksC)
			},
			func(lc Lifecycle, _ *parallelHooksB, _ *parallelHooksC) *parallelHooksA {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						for index := range branchDone {
							select {
							case <-branchDone[index]:
							default:
								return fmt.Errorf("branch %d has not started", index)
							}
						}
						return nil
					},
					OnStop: func(context.Context) error {
						close(dependentStopped)
						return nil
					},
				})
				return new(parallelHooksA)
			},
		),
		Invoke(func(*parallelHooksA) {}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	require.NoError(t, app.Stop(ctx))
}

func TestParallelHooksDefaultIsSequential(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	app := New(
		NopLogger,
		Invoke(func(lc Lifecycle) {
			lc.Append(Hook{OnStart: func(context.Context) error {
				close(firstStarted)
				<-releaseFirst
				return nil
			}})
		}),
		Invoke(func(lc Lifecycle) {
			lc.Append(Hook{OnStart: func(context.Context) error {
				close(secondStarted)
				return nil
			}})
		}),
	)

	started := make(chan error, 1)
	go func() { started <- app.Start(t.Context()) }()
	<-firstStarted
	select {
	case <-secondStarted:
		t.Fatal("second hook started before the first hook completed")
	default:
	}
	close(releaseFirst)
	require.NoError(t, <-started)
	require.NoError(t, app.Stop(t.Context()))
}

func TestParallelHooksLimit(t *testing.T) {
	t.Parallel()

	var running, maximum atomic.Int32
	reached := make(chan struct{}, 3)
	release := make(chan struct{})
	newHook := func(lc Lifecycle) {
		lc.Append(Hook{OnStart: func(context.Context) error {
			current := running.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			reached <- struct{}{}
			<-release
			running.Add(-1)
			return nil
		}})
	}
	app := New(
		NopLogger,
		ParallelHooks(2),
		Invoke(newHook),
		Invoke(newHook),
		Invoke(newHook),
	)

	started := make(chan error, 1)
	go func() { started <- app.Start(t.Context()) }()
	<-reached
	<-reached
	select {
	case <-reached:
		t.Fatal("parallel hook limit was exceeded")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-reached
	require.NoError(t, <-started)
	assert.Equal(t, int32(2), maximum.Load())
	require.NoError(t, app.Stop(t.Context()))
}

func TestParallelHooksTrackDecoratorAndInvokeOwners(t *testing.T) {
	t.Parallel()

	providerStarted := make(chan struct{})
	decoratorStarted := make(chan struct{})
	app := New(
		NopLogger,
		ParallelHooks(3),
		Provide(func(lc Lifecycle) *parallelDecorated {
			lc.Append(Hook{OnStart: func(context.Context) error {
				close(providerStarted)
				return nil
			}})
			return new(parallelDecorated)
		}),
		Decorate(func(lc Lifecycle, value *parallelDecorated) *parallelDecorated {
			lc.Append(Hook{OnStart: func(context.Context) error {
				select {
				case <-providerStarted:
					close(decoratorStarted)
					return nil
				default:
					return fmt.Errorf("decorator started before provider")
				}
			}})
			return value
		}),
		Invoke(func(lc Lifecycle, _ *parallelDecorated) {
			lc.Append(Hook{OnStart: func(context.Context) error {
				select {
				case <-decoratorStarted:
					return nil
				default:
					return fmt.Errorf("invoke hook started before decorator")
				}
			}})
		}),
	)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
}

func TestParallelHooksTrackAnnotatedAndGroupedDependencies(t *testing.T) {
	t.Parallel()

	var annotatedStarted, groupsStarted atomic.Int32
	groupHook := func(context.Context) error {
		groupsStarted.Add(1)
		return nil
	}
	app := New(
		NopLogger,
		ParallelHooks(2),
		Provide(
			Annotate(
				func() *parallelHooksB { return new(parallelHooksB) },
				OnStart(func(context.Context, *parallelHooksB) error {
					annotatedStarted.Add(1)
					return nil
				}),
			),
			Annotate(
				func(*parallelHooksB) *parallelHooksA { return new(parallelHooksA) },
				OnStart(func(context.Context, *parallelHooksA) error {
					if annotatedStarted.Load() != 1 {
						return fmt.Errorf("annotated dependency has not started")
					}
					return nil
				}),
			),
			Annotate(
				func(lc Lifecycle) *parallelGrouped {
					lc.Append(Hook{OnStart: groupHook})
					return new(parallelGrouped)
				},
				ResultTags(`group:"parallel"`),
			),
			Annotate(
				func(lc Lifecycle) *parallelGrouped {
					lc.Append(Hook{OnStart: groupHook})
					return new(parallelGrouped)
				},
				ResultTags(`group:"parallel"`),
			),
			func(lc Lifecycle, _ parallelGroupParams) *parallelConsumer {
				lc.Append(Hook{OnStart: func(context.Context) error {
					if groupsStarted.Load() != 2 {
						return fmt.Errorf("group dependencies have not started")
					}
					return nil
				}})
				return new(parallelConsumer)
			},
		),
		Invoke(func(*parallelHooksA, *parallelConsumer) {}),
	)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
}

func TestParallelHooksTrackDecoratedGroupDependencies(t *testing.T) {
	t.Parallel()

	decoratorStartStarted := make(chan struct{})
	releaseDecoratorStart := make(chan struct{})
	consumerStartStarted := make(chan struct{})
	consumerStopStarted := make(chan struct{})
	releaseConsumerStop := make(chan struct{})
	decoratorStopStarted := make(chan struct{})

	app := New(
		NopLogger,
		ParallelHooks(2),
		Provide(
			Annotate(
				func() *parallelGrouped { return new(parallelGrouped) },
				ResultTags(`group:"parallel"`),
			),
			func(lc Lifecycle, _ parallelGroupParams) *parallelConsumer {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						close(consumerStartStarted)
						return nil
					},
					OnStop: func(context.Context) error {
						close(consumerStopStarted)
						<-releaseConsumerStop
						return nil
					},
				})
				return new(parallelConsumer)
			},
		),
		Decorate(func(lc Lifecycle, group parallelGroupParams) parallelDecoratedGroupResult {
			lc.Append(Hook{
				OnStart: func(context.Context) error {
					close(decoratorStartStarted)
					<-releaseDecoratorStart
					return nil
				},
				OnStop: func(context.Context) error {
					close(decoratorStopStarted)
					return nil
				},
			})
			return parallelDecoratedGroupResult{Items: group.Items}
		}),
		Invoke(func(*parallelConsumer) {}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	started := make(chan error, 1)
	go func() { started <- app.Start(ctx) }()
	<-decoratorStartStarted
	consumerStartedEarly := false
	select {
	case <-consumerStartStarted:
		consumerStartedEarly = true
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseDecoratorStart)
	require.NoError(t, <-started)
	assert.False(t, consumerStartedEarly, "group consumer started before its decorator completed")

	stopped := make(chan error, 1)
	go func() { stopped <- app.Stop(ctx) }()
	<-consumerStopStarted
	decoratorStoppedEarly := false
	select {
	case <-decoratorStopStarted:
		decoratorStoppedEarly = true
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseConsumerStop)
	require.NoError(t, <-stopped)
	assert.False(t, decoratorStoppedEarly, "group decorator stopped before its consumer completed")
}

func TestParallelHooksDoNotWaitForUnresolvedSoftGroupProviders(t *testing.T) {
	t.Parallel()

	consumerStarted := make(chan struct{})
	app := New(
		NopLogger,
		ParallelHooks(2),
		Provide(
			func(lc Lifecycle, _ parallelSoftGroupParams) *parallelConsumer {
				lc.Append(Hook{OnStart: func(context.Context) error {
					close(consumerStarted)
					return nil
				}})
				return new(parallelConsumer)
			},
			func(lc Lifecycle) parallelSoftGroupResult {
				lc.Append(Hook{OnStart: func(ctx context.Context) error {
					select {
					case <-consumerStarted:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}})
				return parallelSoftGroupResult{
					Item:    new(parallelGrouped),
					Trigger: new(parallelTrigger),
				}
			},
		),
		Invoke(func(*parallelConsumer) {}),
		Invoke(func(*parallelTrigger) {}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	require.NoError(t, app.Stop(ctx))
}

func TestParallelHooksTrackPrivateModuleDependencies(t *testing.T) {
	t.Parallel()

	privateStarted := make(chan struct{})
	app := New(
		NopLogger,
		ParallelHooks(2),
		Module("private",
			Provide(func(lc Lifecycle) *parallelPrivate {
				lc.Append(Hook{OnStart: func(context.Context) error {
					close(privateStarted)
					return nil
				}})
				return new(parallelPrivate)
			}, Private),
			Provide(func(lc Lifecycle, _ *parallelPrivate) *parallelConsumer {
				lc.Append(Hook{OnStart: func(context.Context) error {
					select {
					case <-privateStarted:
						return nil
					default:
						return fmt.Errorf("private dependency has not started")
					}
				}})
				return new(parallelConsumer)
			}),
			Invoke(func(*parallelConsumer) {}),
		),
	)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
}
