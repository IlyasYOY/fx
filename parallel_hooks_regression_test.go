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
	mathrand "math/rand"
	randv2 "math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	. "go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

type parallelRandIntermediate struct {
	legacy *mathrand.Rand
}

type parallelDecoratorA struct {
	value string
}

type parallelDecoratorB struct {
	a *parallelDecoratorA
}

type (
	parallelSliceGroupItem       struct{ value string }
	parallelFlattenedGroupItem   struct{ value string }
	parallelSliceGroupConsumer   struct{}
	parallelFlattenGroupConsumer struct{}
)

type parallelSliceGroupParams struct {
	In

	Items [][]*parallelSliceGroupItem `group:"slice-members"`
}

type parallelFlattenedGroupParams struct {
	In

	Items [][]*parallelFlattenedGroupItem `group:"flattened-members"`
}

type parallelDecoratedSliceGroupResult struct {
	Out

	Items [][]*parallelSliceGroupItem `group:"decorated-slice-members"`
}

type parallelDecoratedSliceGroupParams struct {
	In

	Items [][]*parallelSliceGroupItem `group:"decorated-slice-members"`
}

type parallelHookCompletionLogger struct {
	decoratorStartCompleted chan struct{}
	consumerStopCompleted   chan struct{}
	decoratorStartOnce      sync.Once
	consumerStopOnce        sync.Once
}

func (l *parallelHookCompletionLogger) LogEvent(event fxevent.Event) {
	switch event := event.(type) {
	case *fxevent.OnStartExecuted:
		if event.Err == nil && strings.Contains(event.FunctionName, "decoratorStart") {
			l.decoratorStartOnce.Do(func() { close(l.decoratorStartCompleted) })
		}
	case *fxevent.OnStopExecuted:
		if event.Err == nil && strings.Contains(event.FunctionName, "consumerStop") {
			l.consumerStopOnce.Do(func() { close(l.consumerStopCompleted) })
		}
	}
}

type parallelDecoratedSliceGroupHooks struct {
	providerStarted              chan struct{}
	decoratorStartEntered        chan struct{}
	releaseDecoratorStart        chan struct{}
	decoratorStartCompletedEvent chan struct{}
	consumerStarted              chan struct{}
	consumerStopEntered          chan struct{}
	releaseConsumerStop          chan struct{}
	consumerStopCompletedEvent   chan struct{}
	decoratorStopped             chan struct{}
}

func (h *parallelDecoratedSliceGroupHooks) providerStart(context.Context) error {
	close(h.providerStarted)
	return nil
}

func (h *parallelDecoratedSliceGroupHooks) providerStop(context.Context) error {
	select {
	case <-h.decoratorStopped:
		return nil
	default:
		return fmt.Errorf("slice group provider stopped before decorator")
	}
}

func (h *parallelDecoratedSliceGroupHooks) decoratorStart(context.Context) error {
	select {
	case <-h.providerStarted:
	default:
		return fmt.Errorf("slice group decorator started before provider")
	}
	close(h.decoratorStartEntered)
	<-h.releaseDecoratorStart
	return nil
}

func (h *parallelDecoratedSliceGroupHooks) consumerStart(context.Context) error {
	select {
	case <-h.decoratorStartCompletedEvent:
		close(h.consumerStarted)
		return nil
	default:
		return fmt.Errorf("slice group consumer started before decorator completed")
	}
}

func (h *parallelDecoratedSliceGroupHooks) consumerStop(context.Context) error {
	close(h.consumerStopEntered)
	<-h.releaseConsumerStop
	return nil
}

func (h *parallelDecoratedSliceGroupHooks) decoratorStop(context.Context) error {
	select {
	case <-h.consumerStopCompletedEvent:
		close(h.decoratorStopped)
		return nil
	default:
		return fmt.Errorf("slice group decorator stopped before consumer completed")
	}
}

func TestParallelHooksPreserveDependencyTypeIdentity(t *testing.T) {
	t.Parallel()

	legacyStarted := make(chan struct{})
	intermediateStarted := make(chan struct{})
	modernStarted := make(chan struct{})
	modernStopped := make(chan struct{})
	intermediateStopped := make(chan struct{})

	app := New(
		NopLogger,
		ParallelHooks(3),
		Provide(
			func(lc Lifecycle) *mathrand.Rand {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						close(legacyStarted)
						return nil
					},
					OnStop: func(context.Context) error {
						select {
						case <-intermediateStopped:
							return nil
						default:
							return fmt.Errorf("legacy rand stopped before its dependent")
						}
					},
				})
				return mathrand.New(mathrand.NewSource(1))
			},
			func(lc Lifecycle, legacy *mathrand.Rand) *parallelRandIntermediate {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						select {
						case <-legacyStarted:
							close(intermediateStarted)
							return nil
						default:
							return fmt.Errorf("intermediate started before legacy rand")
						}
					},
					OnStop: func(context.Context) error {
						select {
						case <-modernStopped:
							close(intermediateStopped)
							return nil
						default:
							return fmt.Errorf("intermediate stopped before modern rand")
						}
					},
				})
				return &parallelRandIntermediate{legacy: legacy}
			},
			func(lc Lifecycle, _ *parallelRandIntermediate) *randv2.Rand {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						select {
						case <-intermediateStarted:
							close(modernStarted)
							return nil
						default:
							return fmt.Errorf("modern rand started before intermediate")
						}
					},
					OnStop: func(context.Context) error {
						close(modernStopped)
						return nil
					},
				})
				return randv2.New(randv2.NewPCG(1, 2))
			},
		),
		Invoke(func(*randv2.Rand) {}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	select {
	case <-modernStarted:
	default:
		t.Fatal("modern rand start hook was not run")
	}
	require.NoError(t, app.Stop(ctx))
}

func TestParallelHooksScalarDecoratorUsesConstructedDependency(t *testing.T) {
	t.Parallel()

	providerStarted := make(chan struct{})
	bStarted := make(chan struct{})
	decoratorStarted := make(chan struct{})
	decoratorStopped := make(chan struct{})
	bStopped := make(chan struct{})

	app := New(
		NopLogger,
		ParallelHooks(3),
		Provide(
			func(lc Lifecycle) *parallelDecoratorA {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						close(providerStarted)
						return nil
					},
					OnStop: func(context.Context) error {
						select {
						case <-bStopped:
							return nil
						default:
							return fmt.Errorf("A provider stopped before B")
						}
					},
				})
				return &parallelDecoratorA{value: "provider"}
			},
			func(lc Lifecycle, a *parallelDecoratorA) *parallelDecoratorB {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						select {
						case <-providerStarted:
							close(bStarted)
							return nil
						default:
							return fmt.Errorf("B started before the A provider")
						}
					},
					OnStop: func(context.Context) error {
						select {
						case <-decoratorStopped:
							close(bStopped)
							return nil
						default:
							return fmt.Errorf("B stopped before the A decorator")
						}
					},
				})
				return &parallelDecoratorB{a: a}
			},
		),
		Decorate(func(lc Lifecycle, a *parallelDecoratorA, b *parallelDecoratorB) *parallelDecoratorA {
			require.Equal(t, "provider", b.a.value, "B must receive the undecorated A")
			lc.Append(Hook{
				OnStart: func(context.Context) error {
					select {
					case <-bStarted:
						close(decoratorStarted)
						return nil
					default:
						return fmt.Errorf("A decorator started before B")
					}
				},
				OnStop: func(context.Context) error {
					close(decoratorStopped)
					return nil
				},
			})
			return &parallelDecoratorA{value: a.value + "+decorated"}
		}),
		Invoke(func(a *parallelDecoratorA) {
			require.Equal(t, "provider+decorated", a.value)
		}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	select {
	case <-decoratorStarted:
	default:
		t.Fatal("decorator start hook was not run")
	}
	require.NoError(t, app.Stop(ctx))
}

func TestParallelHooksScalarDecoratorFallsBackToAncestorDecorator(t *testing.T) {
	t.Parallel()

	providerStarted := make(chan struct{})
	ancestorStarted := make(chan struct{})
	bStarted := make(chan struct{})
	childStarted := make(chan struct{})
	childStopped := make(chan struct{})
	bStopped := make(chan struct{})
	ancestorStopped := make(chan struct{})

	app := New(
		NopLogger,
		ParallelHooks(4),
		Provide(func(lc Lifecycle) *parallelDecoratorA {
			lc.Append(Hook{
				OnStart: func(context.Context) error {
					close(providerStarted)
					return nil
				},
				OnStop: func(context.Context) error {
					select {
					case <-ancestorStopped:
						return nil
					default:
						return fmt.Errorf("provider stopped before ancestor decorator")
					}
				},
			})
			return &parallelDecoratorA{value: "provider"}
		}),
		Decorate(func(lc Lifecycle, a *parallelDecoratorA) *parallelDecoratorA {
			lc.Append(Hook{
				OnStart: func(context.Context) error {
					select {
					case <-providerStarted:
						close(ancestorStarted)
						return nil
					default:
						return fmt.Errorf("ancestor decorator started before provider")
					}
				},
				OnStop: func(context.Context) error {
					select {
					case <-bStopped:
						close(ancestorStopped)
						return nil
					default:
						return fmt.Errorf("ancestor decorator stopped before B")
					}
				},
			})
			return &parallelDecoratorA{value: a.value + "+ancestor"}
		}),
		Module("child",
			Provide(func(lc Lifecycle, a *parallelDecoratorA) *parallelDecoratorB {
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						select {
						case <-ancestorStarted:
							close(bStarted)
							return nil
						default:
							return fmt.Errorf("B started before ancestor decorator")
						}
					},
					OnStop: func(context.Context) error {
						select {
						case <-childStopped:
							close(bStopped)
							return nil
						default:
							return fmt.Errorf("B stopped before child decorator")
						}
					},
				})
				return &parallelDecoratorB{a: a}
			}),
			Decorate(func(lc Lifecycle, a *parallelDecoratorA, b *parallelDecoratorB) *parallelDecoratorA {
				require.Equal(t, "provider+ancestor", b.a.value, "B must receive the ancestor-decorated A")
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						select {
						case <-bStarted:
							close(childStarted)
							return nil
						default:
							return fmt.Errorf("child decorator started before B")
						}
					},
					OnStop: func(context.Context) error {
						close(childStopped)
						return nil
					},
				})
				return &parallelDecoratorA{value: a.value + "+child"}
			}),
			Invoke(func(a *parallelDecoratorA) {
				require.Equal(t, "provider+ancestor+child", a.value)
			}),
		),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	select {
	case <-childStarted:
	default:
		t.Fatal("child decorator start hook was not run")
	}
	require.NoError(t, app.Stop(ctx))
}

func TestParallelHooksTrackSliceValuedGroupMembers(t *testing.T) {
	t.Parallel()

	providerStarted := make(chan struct{})
	consumerStarted := make(chan struct{})
	consumerStopped := make(chan struct{})

	app := New(
		NopLogger,
		ParallelHooks(2),
		Provide(
			Annotated{
				Group: "slice-members",
				Target: func(lc Lifecycle) []*parallelSliceGroupItem {
					lc.Append(Hook{
						OnStart: func(context.Context) error {
							close(providerStarted)
							return nil
						},
						OnStop: func(context.Context) error {
							select {
							case <-consumerStopped:
								return nil
							default:
								return fmt.Errorf("slice group provider stopped before consumer")
							}
						},
					})
					return []*parallelSliceGroupItem{{value: "member"}}
				},
			},
			func(lc Lifecycle, params parallelSliceGroupParams) *parallelSliceGroupConsumer {
				require.Len(t, params.Items, 1)
				require.Equal(t, "member", params.Items[0][0].value)
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						select {
						case <-providerStarted:
							close(consumerStarted)
							return nil
						default:
							return fmt.Errorf("slice group consumer started before provider")
						}
					},
					OnStop: func(context.Context) error {
						close(consumerStopped)
						return nil
					},
				})
				return new(parallelSliceGroupConsumer)
			},
		),
		Invoke(func(*parallelSliceGroupConsumer) {}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	select {
	case <-consumerStarted:
	default:
		t.Fatal("slice group consumer start hook was not run")
	}
	require.NoError(t, app.Stop(ctx))
}

func TestParallelHooksTrackFlattenedSliceGroupMembers(t *testing.T) {
	t.Parallel()

	providerStarted := make(chan struct{})
	consumerStarted := make(chan struct{})
	consumerStopped := make(chan struct{})

	app := New(
		NopLogger,
		ParallelHooks(2),
		Provide(
			Annotated{
				Group: "flattened-members,flatten",
				Target: func(lc Lifecycle) [][]*parallelFlattenedGroupItem {
					lc.Append(Hook{
						OnStart: func(context.Context) error {
							close(providerStarted)
							return nil
						},
						OnStop: func(context.Context) error {
							select {
							case <-consumerStopped:
								return nil
							default:
								return fmt.Errorf("flattened group provider stopped before consumer")
							}
						},
					})
					return [][]*parallelFlattenedGroupItem{
						{{value: "first"}},
						{{value: "second"}},
					}
				},
			},
			func(lc Lifecycle, params parallelFlattenedGroupParams) *parallelFlattenGroupConsumer {
				require.Len(t, params.Items, 2)
				require.Len(t, params.Items[0], 1)
				require.Len(t, params.Items[1], 1)
				lc.Append(Hook{
					OnStart: func(context.Context) error {
						select {
						case <-providerStarted:
							close(consumerStarted)
							return nil
						default:
							return fmt.Errorf("flattened group consumer started before provider")
						}
					},
					OnStop: func(context.Context) error {
						close(consumerStopped)
						return nil
					},
				})
				return new(parallelFlattenGroupConsumer)
			},
		),
		Invoke(func(*parallelFlattenGroupConsumer) {}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	select {
	case <-consumerStarted:
	default:
		t.Fatal("flattened group consumer start hook was not run")
	}
	require.NoError(t, app.Stop(ctx))
}

func TestParallelHooksTrackDecoratedGroupsOfSlices(t *testing.T) {
	t.Parallel()

	hooks := &parallelDecoratedSliceGroupHooks{
		providerStarted:              make(chan struct{}),
		decoratorStartEntered:        make(chan struct{}),
		releaseDecoratorStart:        make(chan struct{}),
		decoratorStartCompletedEvent: make(chan struct{}),
		consumerStarted:              make(chan struct{}),
		consumerStopEntered:          make(chan struct{}),
		releaseConsumerStop:          make(chan struct{}),
		consumerStopCompletedEvent:   make(chan struct{}),
		decoratorStopped:             make(chan struct{}),
	}
	eventLogger := &parallelHookCompletionLogger{
		decoratorStartCompleted: hooks.decoratorStartCompletedEvent,
		consumerStopCompleted:   hooks.consumerStopCompletedEvent,
	}

	app := New(
		WithLogger(func() fxevent.Logger { return eventLogger }),
		ParallelHooks(3),
		Provide(
			Annotate(
				func(lc Lifecycle) []*parallelSliceGroupItem {
					lc.Append(Hook{
						OnStart: hooks.providerStart,
						OnStop:  hooks.providerStop,
					})
					return []*parallelSliceGroupItem{{value: "member"}}
				},
				ResultTags(`group:"decorated-slice-members"`),
			),
			func(lc Lifecycle, params parallelDecoratedSliceGroupParams) *parallelSliceGroupConsumer {
				require.Len(t, params.Items, 1)
				lc.Append(Hook{
					OnStart: hooks.consumerStart,
					OnStop:  hooks.consumerStop,
				})
				return new(parallelSliceGroupConsumer)
			},
		),
		Decorate(func(lc Lifecycle, params parallelDecoratedSliceGroupParams) parallelDecoratedSliceGroupResult {
			lc.Append(Hook{
				OnStart: hooks.decoratorStart,
				OnStop:  hooks.decoratorStop,
			})
			return parallelDecoratedSliceGroupResult{Items: params.Items}
		}),
		Invoke(func(*parallelSliceGroupConsumer) {}),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	started := make(chan error, 1)
	go func() { started <- app.Start(ctx) }()
	select {
	case <-hooks.decoratorStartEntered:
	case err := <-started:
		require.NoError(t, err)
		t.Fatal("application started without running the slice group decorator")
	case <-ctx.Done():
		t.Fatal("slice group decorator did not start")
	}
	close(hooks.releaseDecoratorStart)
	require.NoError(t, <-started)
	select {
	case <-hooks.consumerStarted:
	default:
		t.Error("slice group consumer start hook was not run")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- app.Stop(ctx) }()
	select {
	case <-hooks.consumerStopEntered:
	case err := <-stopped:
		require.NoError(t, err)
		t.Fatal("application stopped without running the slice group consumer")
	case <-ctx.Done():
		t.Fatal("slice group consumer did not begin stopping")
	}
	close(hooks.releaseConsumerStop)
	require.NoError(t, <-stopped)
	select {
	case <-hooks.decoratorStopped:
	default:
		t.Error("slice group decorator stop hook was not run")
	}
}
