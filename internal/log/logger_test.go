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

package log

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lonng/nano/internal/env"
	"github.com/rs/zerolog"
)

// newBufLogger swaps the package-level logger for one that writes JSON to buf so
// the emitted message can be asserted deterministically (no color/timestamp).
func newBufLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	SetLogger(New(zerolog.New(buf)))
	return buf
}

// L3: log.Debug must interpolate format strings instead of dumping the raw arg
// slice with the literal verbs. Callers like internal/codec/codec.go:57 use
// log.Debug("forward header: %v, type=%v, size=%v", ...).
func TestDebugInterpolatesFormatString(t *testing.T) {
	defer func(prev bool) { env.Debug = prev }(env.Debug)
	env.Debug = true
	buf := newBufLogger(t)

	// Use a non-constant format so `go vet` does not statically reject the
	// (intentional) Printf-style misuse this test reproduces.
	format := "forward header: %v, type=%v, size=%v"
	Debug(format, []byte{1, 2}, 3, 4)

	out := buf.String()
	if strings.Contains(out, "%v") {
		t.Fatalf("Debug rendered literal format verbs: %s", out)
	}
	if !strings.Contains(out, "type=3") || !strings.Contains(out, "size=4") {
		t.Fatalf("Debug did not interpolate args: %s", out)
	}
}

// L1: log.Debugf passed v (a []interface{}) instead of v..., so every verb
// received the whole slice, producing malformed %!-style output.
func TestDebugfInterpolatesArgs(t *testing.T) {
	defer func(prev bool) { env.Debug = prev }(env.Debug)
	env.Debug = true
	buf := newBufLogger(t)

	Debugf("value=%d name=%s", 42, "alice")

	out := buf.String()
	if strings.Contains(out, "%!") {
		t.Fatalf("Debugf produced malformed verbs: %s", out)
	}
	if !strings.Contains(out, "value=42") || !strings.Contains(out, "name=alice") {
		t.Fatalf("Debugf did not interpolate args correctly: %s", out)
	}
}

// Print-style callers (e.g. cluster.go:245 log.Debug("Ping node: ", label))
// must keep working after routing Debug through a formatting path.
func TestDebugPrintStyle(t *testing.T) {
	defer func(prev bool) { env.Debug = prev }(env.Debug)
	env.Debug = true
	buf := newBufLogger(t)

	Debug("Ping node: ", "labelX")

	out := buf.String()
	if strings.Contains(out, "%!") {
		t.Fatalf("print-style Debug produced malformed output: %s", out)
	}
	if !strings.Contains(out, "Ping node: ") || !strings.Contains(out, "labelX") {
		t.Fatalf("print-style Debug broke: %s", out)
	}
}

// A single already-formatted argument must be emitted verbatim even if it
// contains a stray percent sign (it must not be reinterpreted as a verb).
func TestDebugSingleArgWithPercent(t *testing.T) {
	defer func(prev bool) { env.Debug = prev }(env.Debug)
	env.Debug = true
	buf := newBufLogger(t)

	msg := "progress 100% done"
	Debug(msg)

	out := buf.String()
	if strings.Contains(out, "%!") {
		t.Fatalf("single-arg Debug mishandled literal percent: %s", out)
	}
	if !strings.Contains(out, "progress 100% done") {
		t.Fatalf("single-arg Debug altered message: %s", out)
	}
}

// Debug/Debugf must emit nothing when debug logging is disabled.
func TestDebugSuppressedWhenDisabled(t *testing.T) {
	defer func(prev bool) { env.Debug = prev }(env.Debug)
	env.Debug = false
	buf := newBufLogger(t)

	Debug("should not appear")
	Debugf("should not appear")

	if buf.Len() != 0 {
		t.Fatalf("expected no output when env.Debug is false, got: %s", buf.String())
	}
}
