package message

import (
	"errors"
	"reflect"
	"testing"
)

func appendStringParts(dst []byte, parts ...string) []byte {
	for _, part := range parts {
		dst = append(dst, part...)
	}
	return dst
}

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

func TestEncodeAllocationsStayLow(t *testing.T) {
	SetDictionary(map[string]uint16{
		"alloc.compressed": 0x1234,
	})

	tests := []struct {
		name string
		msg  *Message
		want []byte
	}{
		{
			name: "uncompressed request",
			msg: &Message{
				Type:  Request,
				ID:    300,
				Route: "alloc.request",
				Data:  []byte("payload"),
			},
			want: appendStringParts(
				[]byte{byte(Request << 1), 0xac, 0x02, byte(len("alloc.request"))},
				"alloc.request",
				"payload",
			),
		},
		{
			name: "compressed notify",
			msg: &Message{
				Type:  Notify,
				Route: "alloc.compressed",
				Data:  []byte("payload"),
			},
			want: appendStringParts(
				[]byte{byte(Notify<<1) | msgRouteCompressMask, 0x12, 0x34},
				"payload",
			),
		},
		{
			name: "response",
			msg: &Message{
				Type: Response,
				ID:   300,
				Data: []byte("payload"),
			},
			want: appendStringParts(
				[]byte{byte(Response << 1), 0xac, 0x02},
				"payload",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var encoded []byte
			var err error
			allocs := testing.AllocsPerRun(1000, func() {
				encoded, err = Encode(tt.msg)
			})
			if err != nil {
				t.Fatal(err)
			}
			if allocs > 1 {
				t.Fatalf("Encode allocations = %.1f, want <= 1", allocs)
			}
			if !reflect.DeepEqual(tt.want, encoded) {
				t.Fatalf("encoded message changed: want %v, got %v", tt.want, encoded)
			}
		})
	}
}

func TestDecodeMalformedCompressedRouteReturnsError(t *testing.T) {
	data := []byte{byte(Notify<<1) | msgRouteCompressMask, 0x12}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Decode panicked on truncated compressed route: %v", r)
		}
	}()

	_, err := Decode(data)
	if !errors.Is(err, ErrWrongMessage) {
		t.Fatalf("Decode error = %v, want %v", err, ErrWrongMessage)
	}
}

func TestDecodeMalformedMessagesReturnExpectedErrors(t *testing.T) {
	delete(codes, 0xffff)

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{
			name: "too short",
			data: []byte{byte(Notify << 1)},
			want: ErrInvalidMessage,
		},
		{
			name: "wrong type",
			data: []byte{byte(0x04 << 1), 0x00},
			want: ErrWrongMessageType,
		},
		{
			name: "uncompressed route length exceeds payload",
			data: []byte{byte(Notify << 1), 0x05, 'a', 'b'},
			want: ErrWrongMessage,
		},
		{
			name: "unknown compressed route code",
			data: []byte{byte(Notify<<1) | msgRouteCompressMask, 0xff, 0xff},
			want: ErrRouteInfoNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.data)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Decode error = %v, want %v", err, tt.want)
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	SetDictionary(map[string]uint16{
		"bench.compressed": 0x2345,
	})

	tests := []struct {
		name string
		msg  *Message
	}{
		{
			name: "uncompressed_request",
			msg: &Message{
				Type:  Request,
				ID:    300,
				Route: "bench.request",
				Data:  []byte("payload"),
			},
		},
		{
			name: "compressed_notify",
			msg: &Message{
				Type:  Notify,
				Route: "bench.compressed",
				Data:  []byte("payload"),
			},
		},
		{
			name: "response",
			msg: &Message{
				Type: Response,
				ID:   300,
				Data: []byte("payload"),
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Encode(tt.msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
