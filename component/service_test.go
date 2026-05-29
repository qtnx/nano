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

import (
	"strings"
	"sync"
	"testing"

	"github.com/lonng/nano/session"
)

// DupComp has two handler methods that, when lower-cased by a name func,
// collapse to the same route name ("ping"). Used by L6. Must be exported so
// ExtractHandler proceeds past the exported-type check.
type DupComp struct{}

func (d *DupComp) Init()           {}
func (d *DupComp) AfterInit()      {}
func (d *DupComp) BeforeShutdown() {}
func (d *DupComp) Shutdown()       {}

func (d *DupComp) Ping(s *session.Session, msg []byte) error { return nil }
func (d *DupComp) PING(s *session.Session, msg []byte) error { return nil }

// OkComp is a well-formed component with a single handler.
type OkComp struct{}

func (o *OkComp) Init()                                     {}
func (o *OkComp) AfterInit()                                {}
func (o *OkComp) BeforeShutdown()                           {}
func (o *OkComp) Shutdown()                                 {}
func (o *OkComp) Ping(s *session.Session, msg []byte) error { return nil }

// L5: registering a nil / typed-nil component must return an error from
// ExtractHandler instead of panicking during construction or extraction.
func TestExtractHandler_NilComponent(t *testing.T) {
	// untyped nil interface
	s := NewService(nil, nil)
	if err := s.ExtractHandler(); err == nil {
		t.Fatalf("expected error for nil component, got nil")
	}

	// typed-nil pointer
	var typed *OkComp
	s2 := NewService(typed, nil)
	if err := s2.ExtractHandler(); err == nil {
		t.Fatalf("expected error for typed-nil component, got nil")
	}
}

// L6: two handler methods renamed to the same route must produce an
// extraction error rather than silently overwriting one another.
func TestExtractHandler_DuplicateRouteName(t *testing.T) {
	s := NewService(&DupComp{}, []Option{WithNameFunc(strings.ToLower)})
	err := s.ExtractHandler()
	if err == nil {
		t.Fatalf("expected duplicate route name error, got nil; handlers=%v", s.Handlers)
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate-route error, got: %v", err)
	}
}

// sanity: well-formed component still extracts cleanly.
func TestExtractHandler_OK(t *testing.T) {
	s := NewService(&OkComp{}, nil)
	if err := s.ExtractHandler(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := s.Handlers["Ping"]; !ok {
		t.Fatalf("expected Ping handler to be registered, got %v", s.Handlers)
	}
}

// M26: List() must not expose the live backing slice, and Register/List
// must be safe under concurrent access (run with -race).
func TestComponents_ListIsCopyAndConcurrencySafe(t *testing.T) {
	cs := &Components{}
	cs.Register(&OkComp{})

	// Mutating the returned slice must not affect the hub's internal state.
	got := cs.List()
	if len(got) != 1 {
		t.Fatalf("expected 1 component, got %d", len(got))
	}
	got[0] = CompWithOptions{}
	again := cs.List()
	if again[0].Comp == nil {
		t.Fatalf("List returned the live backing slice; external mutation leaked into hub state")
	}

	// Concurrent Register while iterating List must be race-free.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			cs.Register(&OkComp{})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			for _, c := range cs.List() {
				_ = c.Comp
			}
		}
	}()
	wg.Wait()
}
