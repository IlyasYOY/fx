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
	"strings"
	"sync"

	"go.uber.org/dig"
)

type hookScope struct {
	parent *hookScope
}

type hookInput struct {
	key   string
	group bool
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
	inputs            []hookInput
	outputs           []string
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

func (g *hookGraph) setProvideInfo(c *hookComponent, info dig.ProvideInfo, exported bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	c.inputs = hookInputs(info.Inputs)
	c.outputs = hookOutputs(info.Outputs)
	c.outputScope = c.scope
	if exported {
		c.outputScope = g.root
	}
}

func (g *hookGraph) setDecorateInfo(c *hookComponent, info dig.DecorateInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	c.inputs = hookInputs(info.Inputs)
	c.outputs = hookOutputs(info.Outputs)
	c.outputScope = c.scope
}

func (g *hookGraph) setInvokeInfo(c *hookComponent, info dig.InvokeInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c.inputs = hookInputs(info.Inputs)
}

func hookInputs(inputs []*dig.Input) []hookInput {
	result := make([]hookInput, 0, len(inputs))
	for _, input := range inputs {
		key, group := normalizeHookKey(input.String(), true)
		result = append(result, hookInput{key: key, group: group})
	}
	return result
}

func hookOutputs(outputs []*dig.Output) []string {
	result := make([]string, 0, len(outputs))
	for _, output := range outputs {
		key, _ := normalizeHookKey(output.String(), false)
		result = append(result, key)
	}
	return result
}

func normalizeHookKey(key string, input bool) (string, bool) {
	group := strings.Contains(key, "[group = ")
	key = strings.Replace(key, "[optional, ", "[", 1)
	key = strings.Replace(key, "[optional]", "", 1)
	if input && group {
		key = strings.TrimPrefix(key, "[]")
	}
	return key, group
}

func (g *hookGraph) dependencies() map[int][]int {
	g.mu.Lock()
	defer g.mu.Unlock()

	providers := make(map[*hookScope]map[string][]int)
	decorators := make(map[*hookScope]map[string]int)
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
				byKey = make(map[string]int)
				decorators[scope] = byKey
			}
			for _, output := range c.outputs {
				byKey[output] = c.id
			}
		default:
			byKey := providers[scope]
			if byKey == nil {
				byKey = make(map[string][]int)
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
	input hookInput,
	providers map[*hookScope]map[string][]int,
	decorators map[*hookScope]map[string]int,
	components []*hookComponent,
) []int {
	var found []int
	for scope := c.scope; scope != nil; scope = scope.parent {
		if decorator := decorators[scope][input.key]; decorator != 0 && decorator != c.id {
			if !input.group || constructedBefore(components, decorator, c) {
				found = append(found, decorator)
			}
			if !input.group {
				return found
			}
		}
	}
	if len(found) > 0 {
		return found
	}

	for scope := c.scope; scope != nil; scope = scope.parent {
		if scoped := providers[scope][input.key]; len(scoped) > 0 {
			for _, provider := range scoped {
				if !input.group || constructedBefore(components, provider, c) {
					found = append(found, provider)
				}
			}
			if !input.group {
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
