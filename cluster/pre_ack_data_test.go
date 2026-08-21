package cluster

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/metrics"
	"github.com/lonng/nano/session"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type PreAckProbeComponent struct {
	component.Base
	calls   atomic.Int32
	payload chan []byte
}

func (c *PreAckProbeComponent) Handle(_ *session.Session, data []byte) error {
	c.calls.Add(1)
	c.payload <- append([]byte(nil), data...)
	return nil
}

func newPreAckTestHandler(t *testing.T) (*LocalHandler, *agent, *PreAckProbeComponent) {
	t.Helper()
	cache()
	ensureScheduler()

	node := newTestNode()
	probe := &PreAckProbeComponent{payload: make(chan []byte, 2)}
	if err := node.handler.register(probe, nil); err != nil {
		t.Fatalf("register probe: %v", err)
	}
	return node.handler, newAgent(newCountConn(), nil, node.handler.remoteProcess), probe
}

func handshakePacket(t *testing.T, h *LocalHandler, a *agent) {
	t.Helper()
	if err := h.processPacket(a, &packet.Packet{Type: packet.Handshake}); err != nil {
		t.Fatalf("Handshake: %v", err)
	}
}

func preAckDataPacket(t *testing.T, data []byte) *packet.Packet {
	t.Helper()
	encoded, err := message.Encode(&message.Message{
		Type:  message.Notify,
		Route: "PreAckProbeComponent.Handle",
		Data:  data,
	})
	if err != nil {
		t.Fatalf("encode data message: %v", err)
	}
	return &packet.Packet{Type: packet.Data, Data: encoded}
}

func requireProbeCall(t *testing.T, probe *PreAckProbeComponent, want []byte) {
	t.Helper()
	select {
	case got := <-probe.payload:
		if !bytes.Equal(got, want) {
			t.Fatalf("handler payload = %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler was not invoked")
	}
	if got := probe.calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestHandshakeAckBeforeHandshakeRejectsData(t *testing.T) {
	h, a, probe := newPreAckTestHandler(t)

	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err == nil {
		t.Fatal("HandshakeAck before Handshake was accepted")
	}
	if got := a.status(); got == statusWorking {
		t.Fatalf("agent status = %d after invalid HandshakeAck, must not be working", got)
	}
	if err := h.processPacket(a, preAckDataPacket(t, []byte("must-not-dispatch"))); err == nil {
		t.Fatal("Data after invalid HandshakeAck was accepted")
	}
	if got := probe.calls.Load(); got != 0 {
		t.Fatalf("handler calls after invalid HandshakeAck = %d, want 0", got)
	}
}

func TestHandshakeCannotResurrectClosedAgent(t *testing.T) {
	h, a, _ := newPreAckTestHandler(t)
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := h.processPacket(a, &packet.Packet{Type: packet.Handshake}); err == nil {
		t.Fatal("Handshake after Close was accepted")
	}
	if got := a.status(); got != statusClosed {
		t.Fatalf("agent status after Handshake race = %d, want statusClosed", got)
	}
}

func TestPreAckDataRejectsBeforeHandshake(t *testing.T) {
	h, a, _ := newPreAckTestHandler(t)
	rejectedBefore := testutil.ToFloat64(metrics.PreAckDataRejected)

	if err := h.processPacket(a, preAckDataPacket(t, []byte("before-handshake"))); err == nil {
		t.Fatal("Data before Handshake was accepted")
	}
	if got := testutil.ToFloat64(metrics.PreAckDataRejected); got != rejectedBefore+1 {
		t.Fatalf("pre-ACK rejected counter = %v, want %v", got, rejectedBefore+1)
	}
}

func TestPreAckDataBuffersAndDrainsAfterAck(t *testing.T) {
	bufferedBefore := testutil.ToFloat64(metrics.PreAckDataBuffered)
	drainedBefore := testutil.ToFloat64(metrics.PreAckDataDrained)
	h, a, probe := newPreAckTestHandler(t)
	handshakePacket(t, h, a)
	data := preAckDataPacket(t, []byte("buffered"))

	if err := h.processPacket(a, data); err != nil {
		t.Fatalf("Data during Handshake: %v", err)
	}
	if got := testutil.ToFloat64(metrics.PreAckDataBuffered); got != bufferedBefore+1 {
		t.Fatalf("pre-ACK buffered counter = %v, want %v", got, bufferedBefore+1)
	}
	select {
	case <-probe.payload:
		t.Fatal("Data dispatched before HandshakeAck")
	default:
	}
	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err != nil {
		t.Fatalf("HandshakeAck: %v", err)
	}
	requireProbeCall(t, probe, []byte("buffered"))
	if got := testutil.ToFloat64(metrics.PreAckDataDrained); got != drainedBefore+1 {
		t.Fatalf("pre-ACK drained counter = %v, want %v", got, drainedBefore+1)
	}
	if _, ok := a.takePreAckData(); ok {
		t.Fatal("pending Data was retained after HandshakeAck")
	}
}

func TestPreAckDataRejectsSecondPacket(t *testing.T) {
	h, a, _ := newPreAckTestHandler(t)
	handshakePacket(t, h, a)

	if err := h.processPacket(a, preAckDataPacket(t, []byte("first"))); err != nil {
		t.Fatalf("first Data during Handshake: %v", err)
	}
	if err := h.processPacket(a, preAckDataPacket(t, []byte("second"))); err == nil {
		t.Fatal("second Data during Handshake was accepted")
	}
}

func TestPreAckDataRejectsOversizePacket(t *testing.T) {
	h, a, _ := newPreAckTestHandler(t)
	handshakePacket(t, h, a)

	if err := h.processPacket(a, &packet.Packet{Type: packet.Data, Data: make([]byte, 64*1024+1)}); err == nil {
		t.Fatal("oversize Data during Handshake was accepted")
	}
}

func TestPreAckDataClearsOnClose(t *testing.T) {
	h, a, _ := newPreAckTestHandler(t)
	handshakePacket(t, h, a)
	if err := h.processPacket(a, preAckDataPacket(t, []byte("discard-on-close"))); err != nil {
		t.Fatalf("Data during Handshake: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, ok := a.takePreAckData(); ok {
		t.Fatal("pending Data was retained after Close")
	}
}

func TestPreAckDataRejectsAfterCloseWithoutRetention(t *testing.T) {
	h, a, probe := newPreAckTestHandler(t)
	handshakePacket(t, h, a)
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := h.processPacket(a, preAckDataPacket(t, []byte("closed"))); err == nil {
		t.Fatal("Data after Close was accepted")
	}
	if err := a.bufferPreAckData([]byte("closed")); err == nil {
		t.Fatal("closed agent buffered pre-ACK Data")
	}
	if _, ok := a.takePreAckData(); ok {
		t.Fatal("closed agent retained pre-ACK Data")
	}
	if got := probe.calls.Load(); got != 0 {
		t.Fatalf("handler calls after Close = %d, want 0", got)
	}
}

func TestPreAckDataMalformedPacketFailsOnlyWhenDrained(t *testing.T) {
	drainedBefore := testutil.ToFloat64(metrics.PreAckDataDrained)
	h, a, probe := newPreAckTestHandler(t)
	handshakePacket(t, h, a)

	if err := h.processPacket(a, &packet.Packet{Type: packet.Data, Data: []byte{0}}); err != nil {
		t.Fatalf("malformed Data was decoded before HandshakeAck: %v", err)
	}
	if got := probe.calls.Load(); got != 0 {
		t.Fatalf("handler calls before HandshakeAck = %d, want 0", got)
	}
	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err == nil {
		t.Fatal("HandshakeAck accepted a malformed buffered Data packet")
	}
	if got := probe.calls.Load(); got != 0 {
		t.Fatalf("handler calls after malformed drain = %d, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.PreAckDataDrained); got != drainedBefore {
		t.Fatalf("pre-ACK drained counter = %v after malformed packet, want %v", got, drainedBefore)
	}
	if _, ok := a.takePreAckData(); ok {
		t.Fatal("malformed pending Data was retained after drain")
	}
}

func TestClosedAgentCannotTransitionToWorking(t *testing.T) {
	_, a, _ := newPreAckTestHandler(t)
	a.setStatus(statusHandshake)
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if a.transitionHandshakeToWorking() {
		t.Fatal("closed agent transitioned to working")
	}
	if got := a.status(); got != statusClosed {
		t.Fatalf("closed agent status = %d, want statusClosed", got)
	}
}

func TestPreAckDataCopiesDecoderOwnedBytes(t *testing.T) {
	h, a, probe := newPreAckTestHandler(t)
	handshakePacket(t, h, a)
	dataPacket := preAckDataPacket(t, []byte("original"))

	if err := h.processPacket(a, dataPacket); err != nil {
		t.Fatalf("Data during Handshake: %v", err)
	}
	for i := range dataPacket.Data {
		dataPacket.Data[i] = 0
	}
	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err != nil {
		t.Fatalf("HandshakeAck: %v", err)
	}
	requireProbeCall(t, probe, []byte("original"))
}

func TestDataAfterAckStillDispatchesNormally(t *testing.T) {
	h, a, probe := newPreAckTestHandler(t)
	handshakePacket(t, h, a)
	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err != nil {
		t.Fatalf("HandshakeAck: %v", err)
	}
	if err := h.processPacket(a, preAckDataPacket(t, []byte("normal"))); err != nil {
		t.Fatalf("Data after HandshakeAck: %v", err)
	}
	requireProbeCall(t, probe, []byte("normal"))
}
