package codec

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"reflect"
	"testing"

	. "github.com/lonng/nano/internal/packet"
)

func TestPack(t *testing.T) {
	data := []byte("hello world")
	p1 := &Packet{Type: Handshake, Data: data, Length: len(data)}
	pp1, err := Encode(Handshake, data)
	if err != nil {
		t.Error(err.Error())
	}

	d1 := NewDecoder()
	packets, err := d1.Decode(pp1)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(packets) < 1 {
		t.Fatal("packets should not empty")
	}
	if !reflect.DeepEqual(p1, packets[0]) {
		t.Fatalf("expect: %v, got: %v", p1, packets[0])
	}

	p2 := &Packet{Type: Type(5), Data: data, Length: len(data)}
	pp2, err := Encode(Kick, data)
	if err != nil {
		t.Error(err.Error())
	}

	d2 := NewDecoder()
	upp2, err := d2.Decode(pp2)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(upp2) < 1 {
		t.Fatal("packets should not empty")
	}
	if !reflect.DeepEqual(p2, upp2[0]) {
		t.Fatalf("expect: %v, got: %v", p2, upp2[0])
	}

	_ = &Packet{Type: Type(0), Data: data, Length: len(data)}
	if _, err := Encode(Type(0), data); err == nil {
		t.Error("should err")
	}

	_ = &Packet{Type: Type(6), Data: data, Length: len(data)}
	if _, err = Encode(Type(6), data); err == nil {
		t.Error("should err")
	}

	p5 := &Packet{Type: Type(5), Data: data, Length: len(data)}
	pp5, err := Encode(Kick, data)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("********** D3 **************")
	d3 := NewDecoder()
	upp5, err := d3.Decode(append(pp5, []byte{0x01, 0x00, 0x00, 0x00}...))
	if err != nil {
		log.Err(err)
		t.Fatal(err.Error())
	}
	if len(upp5) < 1 {
		t.Fatal("packets should not empty")
	}

	if !reflect.DeepEqual(p5, upp5[0]) {
		t.Fatalf("expect: %v, got: %v", p2, upp5[0])
	}
}

func BenchmarkDecoder_Decode(b *testing.B) {
	data := []byte("hello world")
	pp1, err := Encode(Handshake, data)
	if err != nil {
		b.Error(err.Error())
	}

	b.ReportAllocs()
	d1 := NewDecoder()
	for i := 0; i < b.N; i++ {
		packets, err := d1.Decode(pp1)
		if err != nil {
			b.Fatal(err)
		}
		if len(packets) != 1 {
			b.Fatal("decode error")
		}
	}
}

func TestEncodeAllocationsStayLow(t *testing.T) {
	data := []byte("hello world")
	var encoded []byte
	var err error

	allocs := testing.AllocsPerRun(1000, func() {
		encoded, err = Encode(Data, data)
	})
	if err != nil {
		t.Fatal(err)
	}
	if allocs > 1 {
		t.Fatalf("Encode allocations = %.1f, want <= 1", allocs)
	}

	want := []byte{byte(Data), 0x00, 0x00, byte(len(data))}
	want = append(want, data...)
	if !reflect.DeepEqual(want, encoded) {
		t.Fatalf("encoded packet changed: want %v, got %v", want, encoded)
	}
}

func TestDecodeMalformedPacketsReturnExpectedErrors(t *testing.T) {
	t.Run("fragmented header waits for more data", func(t *testing.T) {
		d := NewDecoder()
		packets, err := d.Decode([]byte{byte(Data), 0x00})
		if err != nil {
			t.Fatalf("Decode error = %v, want nil", err)
		}
		if len(packets) != 0 {
			t.Fatalf("Decode packets = %d, want 0", len(packets))
		}
	})

	t.Run("wrong packet type", func(t *testing.T) {
		d := NewDecoder()
		_, err := d.Decode([]byte{0x00, 0x00, 0x00, 0x00})
		if err != ErrWrongPacketType {
			t.Fatalf("Decode error = %v, want %v", err, ErrWrongPacketType)
		}
	})

	t.Run("oversized packet", func(t *testing.T) {
		d := NewDecoder()
		size := MaxPacketSize + 1
		header := []byte{byte(Data), byte((size >> 16) & 0xff), byte((size >> 8) & 0xff), byte(size & 0xff)}
		_, err := d.Decode(header)
		if err != ErrPacketSizeExcced {
			t.Fatalf("Decode error = %v, want %v", err, ErrPacketSizeExcced)
		}
	})
}

// TestEncodeMaxPacketSize covers M1: Encode must reject payloads larger than
// MaxPacketSize instead of silently allocating and wrapping the 3-byte length.
func TestEncodeMaxPacketSize(t *testing.T) {
	if _, err := Encode(Data, make([]byte, MaxPacketSize+1)); err != ErrPacketSizeExcced {
		t.Fatalf("expected ErrPacketSizeExcced for oversized payload, got %v", err)
	}

	// A payload at exactly MaxPacketSize must still encode (boundary is inclusive,
	// matching the decoder's `size > MaxPacketSize` check).
	if _, err := Encode(Data, make([]byte, MaxPacketSize)); err != nil {
		t.Fatalf("encode at MaxPacketSize boundary should succeed, got %v", err)
	}
}

func TestEncodeDecodeAllowsLargeResponsePayload(t *testing.T) {
	if MaxPacketSize != 512*1024 {
		t.Fatalf("MaxPacketSize = %d, want 512KB cap for large responses", MaxPacketSize)
	}

	payload := make([]byte, 88*1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	encoded, err := Encode(Data, payload)
	if err != nil {
		t.Fatalf("Encode 88KB response payload error = %v, want nil", err)
	}

	packets, err := NewDecoder().Decode(encoded)
	if err != nil {
		t.Fatalf("Decode 88KB response packet error = %v, want nil", err)
	}
	if len(packets) != 1 {
		t.Fatalf("Decode packet count = %d, want 1", len(packets))
	}
	if packets[0].Type != Data {
		t.Fatalf("Decode packet type = %v, want %v", packets[0].Type, Data)
	}
	if packets[0].Length != len(payload) {
		t.Fatalf("Decode packet length = %d, want %d", packets[0].Length, len(payload))
	}
	if !reflect.DeepEqual(packets[0].Data, payload) {
		t.Fatal("Decode packet data changed for 88KB response payload")
	}
}

// TestDecoderResetAfterError covers L8: after a forward() error the decoder must
// not retain stale frame state (oversized size / consumed header), so a reused
// decoder can still parse a subsequent valid packet.
func TestDecoderResetAfterError(t *testing.T) {
	d := NewDecoder()

	// Hand-craft an oversized frame header (valid type, length > MaxPacketSize).
	oversize := make([]byte, HeadLength)
	oversize[0] = byte(Data)
	copy(oversize[1:], intToBytes(MaxPacketSize+1))
	if _, err := d.Decode(oversize); err != ErrPacketSizeExcced {
		t.Fatalf("expected ErrPacketSizeExcced, got %v", err)
	}

	// The same decoder must recover and decode a fresh, valid packet.
	data := []byte("hello world")
	valid, err := Encode(Kick, data)
	if err != nil {
		t.Fatal(err.Error())
	}
	packets, err := d.Decode(valid)
	if err != nil {
		t.Fatalf("unexpected error decoding after recovery: %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("expected 1 packet after decoder recovery, got %d", len(packets))
	}
	want := &Packet{Type: Type(Kick), Length: len(data), Data: data}
	if !reflect.DeepEqual(want, packets[0]) {
		t.Fatalf("expect: %v, got: %v", want, packets[0])
	}
}
