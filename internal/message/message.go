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

package message

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/lonng/nano/internal/log"
)

// Type represents the type of message, which could be Request/Notify/Response/Push
type Type byte

// Message types
const (
	Request  Type = 0x00
	Notify        = 0x01
	Response      = 0x02
	Push          = 0x03
)

const (
	msgRouteCompressMask = 0x01
	msgTypeMask          = 0x07
	msgRouteLengthMask   = 0xFF
	msgHeadLength        = 0x02
)

var types = map[Type]string{
	Request:  "Request",
	Notify:   "Notify",
	Response: "Response",
	Push:     "Push",
}

func (t Type) String() string {
	return types[t]
}

var (
	routesMu sync.RWMutex              // guards routes and codes
	routes   = make(map[string]uint16) // route map to code
	codes    = make(map[uint16]string) // code map to route
)

// Errors that could be occurred in message codec
var (
	ErrWrongMessageType  = errors.New("wrong message type")
	ErrInvalidMessage    = errors.New("invalid message")
	ErrRouteInfoNotFound = errors.New("route info not found in dictionary")
	ErrWrongMessage      = errors.New("wrong message")
	ErrRouteTooLong      = errors.New("route length exceeds 255 bytes")
)

// Message represents a unmarshaled message or a message which to be marshaled
type Message struct {
	Type       Type   // message type
	ID         uint64 // unique id, zero while notify mode
	Route      string // route for locating service
	Data       []byte // payload
	compressed bool   // is message compressed
}

// New returns a new message instance
func New() *Message {
	return &Message{}
}

// String, implementation of fmt.Stringer interface
func (m *Message) String() string {
	return fmt.Sprintf("%s %s (%dbytes)", types[m.Type], m.Route, len(m.Data))
}

// Encode marshals message to binary format.
func (m *Message) Encode() ([]byte, error) {
	return Encode(m)
}

func routable(t Type) bool {
	return t == Request || t == Notify || t == Push
}

func invalidType(t Type) bool {
	return t < Request || t > Push

}

func messageIDSize(id uint64) int {
	size := 1
	for id >>= 7; id != 0; id >>= 7 {
		size++
	}
	return size
}

func encodedSize(m *Message, compressed bool) int {
	size := 1 + len(m.Data)
	if m.Type == Request || m.Type == Response {
		size += messageIDSize(m.ID)
	}
	if routable(m.Type) {
		if compressed {
			size += 2
		} else {
			size += 1 + len(m.Route)
		}
	}
	return size
}

// Encode marshals message to binary format. Different message types is corresponding to
// different message header, message types is identified by 2-4 bit of flag field. The
// relationship between message types and message header is presented as follows:
// ------------------------------------------
// |   type   |  flag  |       other        |
// |----------|--------|--------------------|
// | request  |----000-|<message id>|<route>|
// | notify   |----001-|<route>             |
// | response |----010-|<message id>        |
// | push     |----011-|<route>             |
// ------------------------------------------
// The figure above indicates that the bit does not affect the type of message.
// See ref: https://github.com/lonnng/nano/blob/master/docs/communication_protocol.md
func Encode(m *Message) ([]byte, error) {
	if invalidType(m.Type) {
		return nil, ErrWrongMessageType
	}

	var code uint16
	compressed := false
	if routable(m.Type) {
		routesMu.RLock()
		code, compressed = routes[m.Route]
		routesMu.RUnlock()

		if !compressed && len(m.Route) > msgRouteLengthMask {
			return nil, ErrRouteTooLong
		}
	}
	flag := byte(m.Type) << 1

	if compressed {
		flag |= msgRouteCompressMask
	}

	buf := make([]byte, 0, encodedSize(m, compressed))
	buf = append(buf, flag)

	if m.Type == Request || m.Type == Response {
		n := m.ID
		// variant length encode
		for {
			b := byte(n % 128)
			n >>= 7
			if n != 0 {
				buf = append(buf, b+128)
			} else {
				buf = append(buf, b)
				break
			}
		}
	}

	if routable(m.Type) {
		if compressed {
			buf = append(buf, byte((code>>8)&0xFF))
			buf = append(buf, byte(code&0xFF))
		} else {
			buf = append(buf, byte(len(m.Route)))
			buf = append(buf, m.Route...)
		}
	}

	buf = append(buf, m.Data...)
	return buf, nil
}

// Decode unmarshal the bytes slice to a message
// See ref: https://github.com/lonnng/nano/blob/master/docs/communication_protocol.md
func Decode(data []byte) (*Message, error) {
	if len(data) < msgHeadLength {
		return nil, ErrInvalidMessage
	}
	m := New()
	flag := data[0]
	offset := 1
	m.Type = Type((flag >> 1) & msgTypeMask)

	if invalidType(m.Type) {
		return nil, ErrWrongMessageType
	}

	if m.Type == Request || m.Type == Response {
		id := uint64(0)
		terminated := false
		// little end byte order
		// WARNING: must can be stored in 64 bits integer
		// variant length encode
		for i := offset; i < len(data); i++ {
			b := data[i]
			n := i - offset
			// A uint64 varint is at most 10 bytes; reject anything longer to
			// avoid silent wraparound.
			if n >= 10 {
				return nil, ErrWrongMessage
			}
			id += uint64(b&0x7F) << uint64(7*n)
			if b < 128 {
				offset = i + 1
				terminated = true
				break
			}
		}
		// A varint with no terminating byte leaves offset unchanged and would
		// cause the remaining bytes to be misparsed as route/data.
		if !terminated {
			return nil, ErrWrongMessage
		}
		m.ID = id
	}

	if routable(m.Type) && offset >= len(data) {
		return nil, ErrWrongMessage
	}

	if routable(m.Type) {
		if flag&msgRouteCompressMask == 1 {
			// Need two bytes for the compressed route code.
			if offset+2 > len(data) {
				return nil, ErrWrongMessage
			}
			m.compressed = true
			code := binary.BigEndian.Uint16(data[offset:(offset + 2)])
			routesMu.RLock()
			route, ok := codes[code]
			routesMu.RUnlock()
			if !ok {
				return nil, ErrRouteInfoNotFound
			}
			m.Route = route
			offset += 2
		} else {
			m.compressed = false
			rl := data[offset]
			offset++
			if offset+int(rl) > len(data) {
				return nil, ErrWrongMessage
			}
			m.Route = string(data[offset:(offset + int(rl))])
			offset += int(rl)
		}
	}

	if offset > len(data) {
		return nil, ErrWrongMessage
	}
	m.Data = data[offset:]
	return m, nil
}

// SetDictionary set routes map which be used to compress route.
// TODO(warning): set dictionary in runtime would be a dangerous operation!!!!!!
func SetDictionary(dict map[string]uint16) {
	routesMu.Lock()
	defer routesMu.Unlock()
	for route, code := range dict {
		r := strings.TrimSpace(route)

		if oldCode, ok := routes[r]; ok {
			if oldCode != code {
				delete(codes, oldCode)
			}
			log.Println(fmt.Sprintf("duplicated route(route: %s, code: %d)", r, code))
		}

		if oldRoute, ok := codes[code]; ok {
			if oldRoute != r {
				delete(routes, oldRoute)
			}
			log.Println(fmt.Sprintf("duplicated route(route: %s, code: %d)", r, code))
		}

		// update map, using last value when key duplicated
		routes[r] = code
		codes[code] = r
	}
}

func GetDictionary() (map[string]uint16, bool) {
	routesMu.RLock()
	defer routesMu.RUnlock()
	if len(routes) <= 0 {
		return nil, false
	}
	// Return a copy so callers cannot mutate the live maps concurrently with
	// Encode/Decode reads.
	cp := make(map[string]uint16, len(routes))
	for route, code := range routes {
		cp[route] = code
	}
	return cp, true
}
