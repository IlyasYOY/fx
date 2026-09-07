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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	. "go.uber.org/fx"
)

type parallelGroupedOutMember struct {
	Out
	Value int
}

type parallelGroupedOutFields struct {
	Out
	Member parallelGroupedOutMember `group:"out-members"`
}

type parallelGroupedErrorFields struct {
	Out
	Member error `group:"error-members"`
}

type parallelGroupedOutFieldParams struct {
	In
	Members []parallelGroupedOutMember `group:"out-members"`
}

type parallelGroupedErrorFieldParams struct {
	In
	Members []error `group:"error-members"`
}

func TestParallelHooksGroupedSpecialFields(t *testing.T) {
	for _, groupedOut := range []bool{true, false} {
		name := "error"
		if groupedOut {
			name = "out"
		}
		t.Run(name, func(t *testing.T) {
			providerStarted := make(chan struct{})
			releaseProvider := make(chan struct{})
			consumerStopped := make(chan struct{})
			var release sync.Once
			unblockProvider := func() { release.Do(func() { close(releaseProvider) }) }
			providerHook := Hook{
				OnStart: func(context.Context) error {
					<-releaseProvider
					close(providerStarted)
					return nil
				},
				OnStop: func(context.Context) error {
					select {
					case <-consumerStopped:
						return nil
					default:
						return errors.New("group provider stopped before its consumer")
					}
				},
			}
			consumerHook := Hook{
				OnStart: func(context.Context) error {
					defer unblockProvider()
					select {
					case <-providerStarted:
						return nil
					default:
						return errors.New("group consumer started before its provider completed")
					}
				},
				OnStop: func(context.Context) error {
					close(consumerStopped)
					return nil
				},
			}
			var grouped Option
			if groupedOut {
				grouped = Options(
					Provide(func(lc Lifecycle) parallelGroupedOutFields {
						lc.Append(providerHook)
						return parallelGroupedOutFields{Member: parallelGroupedOutMember{Value: 42}}
					}),
					Invoke(func(lc Lifecycle, params parallelGroupedOutFieldParams) {
						require.Len(t, params.Members, 1)
						require.Equal(t, 42, params.Members[0].Value)
						lc.Append(consumerHook)
					}),
				)
			} else {
				member := errors.New("group member")
				grouped = Options(
					Provide(func(lc Lifecycle) parallelGroupedErrorFields {
						lc.Append(providerHook)
						return parallelGroupedErrorFields{Member: member}
					}),
					Invoke(func(lc Lifecycle, params parallelGroupedErrorFieldParams) {
						require.Len(t, params.Members, 1)
						require.ErrorIs(t, params.Members[0], member)
						lc.Append(consumerHook)
					}),
				)
			}
			app := New(NopLogger, ParallelHooks(2), grouped,
				// The independent owner releases the provider. With a missing
				// edge, the consumer occupies its worker slot and observes the
				// blocked provider, making the ordering failure deterministic.
				Invoke(func(lc Lifecycle) {
					lc.Append(Hook{OnStart: func(context.Context) error {
						unblockProvider()
						return nil
					}})
				}),
			)
			require.NoError(t, app.Err())
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, app.Start(ctx))
			require.NoError(t, app.Stop(ctx))
		})
	}
}
