package cluster

import (
	"testing"
	"time"

	"github.com/lonng/nano/internal/log"
	"github.com/valyala/fasthttp"
)

// --- M13: master heartbeat-checker goroutine must stop on shutdown ----------

// Before the fix the ticker loop only returned when the node stopped being
// master, so Shutdown could not terminate it and it leaked while continuing to
// mutate stale cluster state. The checker now selects on a stop channel; this
// test proves stopHeartbeatChecker terminates the goroutine (joined via
// heartbeatDone) and is idempotent.
func TestHeartbeatCheckerStops(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c := newCluster(&Node{Options: Options{IsMaster: true}})

	c.stopHeartbeatChecker()
	select {
	case <-c.heartbeatDone:
	case <-time.After(2 * time.Second):
		t.Fatal("master heartbeat-checker goroutine did not stop after stopHeartbeatChecker")
	}

	// Idempotent: a second stop (e.g. a double Shutdown) must not panic.
	c.stopHeartbeatChecker()
}

func TestStopHeartbeatCheckerNoopWhenNotStarted(t *testing.T) {
	c := newCluster(&Node{}) // non-master: checker never started
	c.stopHeartbeatChecker()  // must be a safe no-op
}

// --- M37: /api waits on a signal, not a 1ms busy-poll -----------------------

func TestWaitForHTTPResponseWakesOnSignal(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	ctx := &fasthttp.RequestCtx{}
	const mid = uint64(7)
	a.AttackHttpRequestCtx(mid, ctx)
	observe := a.GetFastHttpContextObserve(mid)
	if observe == nil {
		t.Fatal("observe was not registered")
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = a.ResponseMid(mid, []byte(`{"ok":true}`))
	}()

	start := time.Now()
	n.waitForHTTPResponse(ctx, observe, mid, 5*time.Second)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("waitForHTTPResponse did not wake on the done signal (took %v)", elapsed)
	}
	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected 200 after a signalled success, got %d", ctx.Response.StatusCode())
	}
}

func TestWaitForHTTPResponseTimesOut(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	ctx := &fasthttp.RequestCtx{}
	const mid = uint64(8)
	a.AttackHttpRequestCtx(mid, ctx)
	observe := a.GetFastHttpContextObserve(mid)

	start := time.Now()
	n.waitForHTTPResponse(ctx, observe, mid, 50*time.Millisecond)
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("waitForHTTPResponse returned before its timeout (took %v)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("waitForHTTPResponse hung past its timeout (took %v)", elapsed)
	}
	if ctx.Response.StatusCode() != fasthttp.StatusRequestTimeout {
		t.Fatalf("expected 408 on timeout, got %d", ctx.Response.StatusCode())
	}
}
