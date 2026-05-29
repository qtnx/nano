// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package component

import "sync"

type CompWithOptions struct {
	Comp Component
	Opts []Option
}

type Components struct {
	mu    sync.RWMutex
	comps []CompWithOptions
}

// Register registers a component to hub with options
func (cs *Components) Register(c Component, options ...Option) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.comps = append(cs.comps, CompWithOptions{c, options})
}

// List returns all components with it's options. It returns a copy of the
// backing slice so callers may iterate while Register runs concurrently and
// cannot mutate the hub's internal state through the returned slice.
func (cs *Components) List() []CompWithOptions {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	out := make([]CompWithOptions, len(cs.comps))
	copy(out, cs.comps)
	return out
}
