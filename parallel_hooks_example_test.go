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

	"go.uber.org/fx"
)

func ExampleParallelHooks() {
	type cache struct{}
	type database struct{}
	type server struct{}

	newCache := func(lc fx.Lifecycle) *cache {
		lc.Append(fx.StartHook(func(context.Context) error { return nil }))
		return new(cache)
	}
	newDatabase := func(lc fx.Lifecycle) *database {
		lc.Append(fx.StartHook(func(context.Context) error { return nil }))
		return new(database)
	}
	newServer := func(lc fx.Lifecycle, _ *cache, _ *database) *server {
		lc.Append(fx.StartHook(func(context.Context) error { return nil }))
		return new(server)
	}

	fx.New(
		fx.ParallelHooks(2),
		fx.Provide(newCache, newDatabase, newServer),
		fx.Invoke(func(*server) {}),
	).Run()
}
