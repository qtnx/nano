package message

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestEncode(t *testing.T) {
	dict := map[string]uint16{
		"test.test.test":  100,
		"test.test.test1": 101,
		"test.test.test2": 102,
		"test.test.test3": 103,
	}
	SetDictionary(dict)
	m1 := &Message{
		Type:       Request,
		ID:         100,
		Route:      "test.test.test",
		Data:       []byte(`hello world`),
		compressed: true,
	}
	em1, err := m1.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm1, err := Decode(em1)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m1, dm1) {
		t.Error("not equal")
	}

	m2 := &Message{
		Type:  Request,
		ID:    100,
		Route: "test.test.test4",
		Data:  []byte(`hello world`),
	}
	em2, err := m2.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm2, err := Decode(em2)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m2, dm2) {
		t.Error("not equal")
	}

	m3 := &Message{
		Type: Response,
		ID:   100,
		Data: []byte(`hello world`),
	}
	em3, err := m3.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm3, err := Decode(em3)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m3, dm3) {
		t.Error("not equal")
	}

	m4 := &Message{
		Type: Response,
		ID:   100,
		Data: []byte(`hello world`),
	}
	em4, err := m4.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm4, err := Decode(em4)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m4, dm4) {
		t.Error("not equal")
	}

	m5 := &Message{
		Type:       Notify,
		Route:      "test.test.test",
		Data:       []byte(`hello world`),
		compressed: true,
	}
	em5, err := m5.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm5, err := Decode(em5)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m5, dm5) {
		t.Error("not equal")
	}

	m6 := &Message{
		Type:  Notify,
		Route: "test.test.test20",
		Data:  []byte(`hello world`),
	}
	em6, err := m6.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm6, err := Decode(em6)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m6, dm6) {
		t.Error("not equal")
	}

	m7 := &Message{
		Type:  Push,
		Route: "test.test.test9",
		Data:  []byte(`hello world`),
	}
	em7, err := m7.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm7, err := Decode(em7)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m7, dm7) {
		t.Error("not equal")
	}

	m8 := &Message{
		Type:       Push,
		Route:      "test.test.test3",
		Data:       []byte(`hello world`),
		compressed: true,
	}
	em8, err := m8.Encode()
	if err != nil {
		t.Error(err.Error())
	}
	dm8, err := Decode(em8)
	if err != nil {
		t.Error(err.Error())
	}

	if !reflect.DeepEqual(m8, dm8) {
		t.Error("not equal")
	}
}

// C1: a frame with the route-compress bit set but fewer than two bytes left
// after the header must be rejected, not crash the read goroutine with an
// out-of-range slice panic.
func TestDecodeCompressedRouteTruncated(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Decode panicked on truncated compressed route: %v", r)
		}
	}()
	// flag 0x03 = Notify (type 0x01) with compress bit set; only one trailing byte.
	_, err := Decode([]byte{0x03, 0xAB})
	if err == nil {
		t.Fatal("expected error for truncated compressed route, got nil")
	}
}

// M1: a non-compressed route longer than 255 bytes cannot be encoded with a
// single length byte; Encode must reject it instead of silently truncating.
func TestEncodeRouteTooLong(t *testing.T) {
	longRoute := strings.Repeat("a", 256)
	m := &Message{Type: Notify, Route: longRoute, Data: []byte("x")}
	if _, err := m.Encode(); err == nil {
		t.Fatal("expected error encoding route longer than 255 bytes, got nil")
	}
}

// M2: a message-id varint that never terminates (all continuation bytes) must
// be rejected, not silently parsed with a bogus id and offset.
func TestDecodeMessageIDVarintUnterminated(t *testing.T) {
	// flag 0x04 = Response (type 0x02); 0x80 0x81 are both continuation bytes.
	if _, err := Decode([]byte{0x04, 0x80, 0x81}); err == nil {
		t.Fatal("expected error for unterminated message-id varint, got nil")
	}
}

// M2: a message-id varint longer than 10 bytes overflows a uint64 and must be
// rejected rather than wrapping.
func TestDecodeMessageIDVarintTooLong(t *testing.T) {
	data := []byte{0x04} // Response
	for i := 0; i < 11; i++ {
		data = append(data, 0x80) // 11 continuation bytes
	}
	data = append(data, 0x01)        // terminator (12-byte varint, exceeds uint64)
	data = append(data, 0xFF, 0xFF)  // trailing payload so offset < len pre-fix
	if _, err := Decode(data); err == nil {
		t.Fatal("expected error for over-long message-id varint, got nil")
	}
}

// M6: Encode should allocate a single buffer per call instead of repeatedly
// growing it (and converting the route string to []byte separately).
func TestEncodeAllocations(t *testing.T) {
	m := &Message{Type: Push, Route: "alloc.bench.route", Data: []byte("hello world payload")}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := Encode(m); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("Encode allocated %.0f times per call, want <= 1", allocs)
	}
}

// H8: SetDictionary mutating the global route maps concurrently with
// Encode/Decode reads is a data race / fatal concurrent map access. Run under -race.
func TestDictionaryConcurrentAccess(t *testing.T) {
	stop := make(chan struct{})

	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			SetDictionary(map[string]uint16{fmt.Sprintf("race.route.%d", i): uint16(i % 4000)})
		}
	}()

	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			m := &Message{Type: Notify, Route: "test.test.test", Data: []byte("payload")}
			for i := 0; i < 3000; i++ {
				b, err := m.Encode()
				if err != nil {
					continue
				}
				_, _ = Decode(b)
			}
		}()
	}

	readers.Wait()
	close(stop)
	writer.Wait()
}