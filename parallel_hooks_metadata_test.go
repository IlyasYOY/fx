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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	. "go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

type parallelMetadataValue struct{}

func (*parallelMetadataValue) String() string { return "metadata" }

// Each test builds a chain. Checking the trace verifies both lifecycle edges
// and the hooks installed by transformed constructors and invokes.
func parallelMetadataHooks(t *testing.T, count int) func(int) Hook {
	var mu sync.Mutex
	var trace []int
	t.Cleanup(func() {
		want := make([]int, 0, 2*count)
		for i := 1; i <= count; i++ {
			want = append(want, i)
		}
		for i := count; i > 0; i-- {
			want = append(want, -i)
		}
		require.Equal(t, want, trace)
	})
	return func(index int) Hook {
		record := func(value int) func(context.Context) error {
			return func(context.Context) error {
				mu.Lock()
				defer mu.Unlock()
				trace = append(trace, value)
				return nil
			}
		}
		return Hook{OnStart: record(index), OnStop: record(-index)}
	}
}

func runParallelMetadataApp(t *testing.T, opts ...Option) {
	t.Helper()
	app := New(append([]Option{NopLogger, ParallelHooks(4)}, opts...)...)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))
}

func TestParallelHooksMetadataNestedNamedOptional(t *testing.T) {
	t.Parallel()
	type innerParams struct {
		In     `ignore-unexported:"true"`
		Value  *parallelMetadataValue `name:"source" optional:"true"`
		hidden *parallelHooksA
	}
	type params struct {
		In
		Nested innerParams
	}
	type innerResult struct {
		Out
		Value *parallelHooksB `name:"result"`
	}
	type result struct {
		Out
		Nested innerResult
	}
	hook := parallelMetadataHooks(t, 3)
	runParallelMetadataApp(t,
		Provide(Annotated{Name: "source", Target: func(lc Lifecycle) *parallelMetadataValue {
			lc.Append(hook(1))
			return new(parallelMetadataValue)
		}}),
		Provide(func(lc Lifecycle, p params) result {
			require.NotNil(t, p.Nested.Value)
			require.Nil(t, p.Nested.hidden)
			lc.Append(hook(2))
			return result{Nested: innerResult{Value: new(parallelHooksB)}}
		}),
		Invoke(Annotate(func(lc Lifecycle, value *parallelHooksB) {
			require.NotNil(t, value)
			lc.Append(hook(3))
		}, ParamTags("", `name:"result"`))),
	)
}

func TestParallelHooksMetadataAsAndFrom(t *testing.T) {
	t.Parallel()
	for _, from := range []bool{false, true} {
		t.Run(fmt.Sprintf("From=%v", from), func(t *testing.T) {
			hook := parallelMetadataHooks(t, 2)
			var provider any = func(lc Lifecycle) *parallelMetadataValue {
				lc.Append(hook(1))
				return new(parallelMetadataValue)
			}
			var consumer any = func(lc Lifecycle, value fmt.Stringer) {
				require.Equal(t, "metadata", value.String())
				lc.Append(hook(2))
			}
			if from {
				consumer = Annotate(consumer, From(new(Lifecycle), new(*parallelMetadataValue)))
			} else {
				provider = Annotate(provider, As(new(fmt.Stringer)))
			}
			runParallelMetadataApp(t, Provide(provider), Invoke(consumer))
		})
	}
}

func TestParallelHooksMetadataSupplyAndReplace(t *testing.T) {
	t.Parallel()
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("Replace=%v", replace), func(t *testing.T) {
			hook := parallelMetadataHooks(t, 2)
			first := hook(1)
			value := Annotate(new(parallelMetadataValue),
				As(new(fmt.Stringer)), OnStart(first.OnStart), OnStop(first.OnStop))
			opt := Supply(value)
			if replace {
				opt = Options(Supply(Annotate(new(parallelMetadataValue), As(new(fmt.Stringer)))), Replace(value))
			}
			runParallelMetadataApp(t, opt, Invoke(func(lc Lifecycle, value fmt.Stringer) {
				require.Equal(t, "metadata", value.String())
				lc.Append(hook(2))
			}))
		})
	}
}

func TestParallelHooksMetadataLogger(t *testing.T) {
	t.Parallel()
	hook := parallelMetadataHooks(t, 3)
	runParallelMetadataApp(t,
		Provide(func(lc Lifecycle) *parallelMetadataValue {
			lc.Append(hook(1))
			return new(parallelMetadataValue)
		}),
		WithLogger(func(lc Lifecycle, _ *parallelMetadataValue) fxevent.Logger {
			lc.Append(hook(2))
			return fxevent.NopLogger
		}),
		Invoke(func(lc Lifecycle, _ fxevent.Logger) { lc.Append(hook(3)) }),
	)
}

func TestParallelHooksMetadataIgnoresOrdinaryVariadic(t *testing.T) {
	t.Parallel()
	hook := parallelMetadataHooks(t, 2)
	runParallelMetadataApp(t,
		Provide(func(lc Lifecycle, unused ...*parallelHooksB) *parallelHooksA {
			require.Empty(t, unused)
			lc.Append(hook(1))
			return new(parallelHooksA)
		}, func(lc Lifecycle, _ *parallelHooksA) *parallelHooksB {
			lc.Append(hook(2))
			return new(parallelHooksB)
		}),
		Invoke(func(*parallelHooksB) {}),
	)
}
