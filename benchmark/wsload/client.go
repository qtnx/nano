package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
)

var (
	handshakeFrame    = mustPacket(packet.Handshake, nil)
	handshakeAckFrame = mustPacket(packet.HandshakeAck, nil)
	heartbeatFrame    = mustPacket(packet.Heartbeat, nil)
)

func mustPacket(typ packet.Type, data []byte) []byte {
	frame, err := codec.Encode(typ, data)
	if err != nil {
		panic(err)
	}
	return frame
}

type loadClient struct {
	conn         *websocket.Conn
	decoder      *codec.Decoder
	writeMu      sync.Mutex
	requestFrame []byte
	stats        *stats

	handshake chan struct{}
	readDone  chan clientClose
	closeOnce sync.Once
	planned   atomic.Bool
	connected atomic.Bool

	requestMu      sync.Mutex
	requestStarted time.Time
	requestPending bool
}

type clientClose struct {
	category string
	err      error
}

func runClient(ctx context.Context, cfg Config, stats *stats) {
	stats.attempt()
	dialStarted := time.Now()
	dialer := websocket.Dialer{HandshakeTimeout: cfg.ConnectTimeout}
	conn, _, err := dialer.DialContext(ctx, cfg.URL, nil)
	if err != nil {
		stats.fail(classifyDialError(err))
		return
	}

	client := &loadClient{
		conn:      conn,
		decoder:   codec.NewDecoder(),
		handshake: make(chan struct{}),
		readDone:  make(chan clientClose, 1),
		stats:     stats,
	}
	if cfg.RequestRoute != "" {
		client.requestFrame, err = requestFrame(cfg.RequestRoute, cfg.RequestJSON)
		if err != nil {
			client.closePlanned()
			stats.fail(errorProtocol)
			return
		}
	}

	go client.read()
	if err := client.write(handshakeFrame); err != nil {
		client.closePlanned()
		stats.fail(errorWrite)
		return
	}

	setupTimer := time.NewTimer(cfg.ConnectTimeout)
	defer setupTimer.Stop()
	select {
	case <-client.handshake:
		client.connected.Store(true)
	case close := <-client.readDone:
		stats.fail(nonEmpty(close.category, errorHandshake))
		return
	case <-setupTimer.C:
		client.closePlanned()
		stats.fail(errorHandshakeTimeout)
		return
	case <-ctx.Done():
		client.closePlanned()
		return
	}

	connectedAt := time.Now()
	stats.connect(time.Since(dialStarted))
	defer func() {
		client.closePlanned()
		stats.disconnect(time.Since(connectedAt), !client.planned.Load())
	}()

	var requestTicker *time.Ticker
	var requestTicks <-chan time.Time
	if client.requestFrame != nil {
		requestTicker = time.NewTicker(cfg.RequestEvery)
		requestTicks = requestTicker.C
		defer requestTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case close := <-client.readDone:
			if !client.planned.Load() {
				stats.error(nonEmpty(close.category, errorRead))
				stats.markUnexpectedDisconnect()
			}
			return
		case <-requestTicks:
			if started := client.startRequest(); !started {
				continue
			}
			if err := client.write(client.requestFrame); err != nil {
				client.clearRequest()
				stats.error(errorWrite)
			}
		}
	}
}

func (c *loadClient) read() {
	for {
		_, frame, err := c.conn.ReadMessage()
		if err != nil {
			c.readDone <- clientClose{category: errorRead, err: err}
			return
		}
		packets, err := c.decoder.Decode(frame)
		if err != nil {
			c.readDone <- clientClose{category: errorProtocol, err: err}
			return
		}
		for _, p := range packets {
			switch p.Type {
			case packet.Handshake:
				if err := c.write(handshakeAckFrame); err != nil {
					c.readDone <- clientClose{category: errorWrite, err: err}
					return
				}
				select {
				case <-c.handshake:
				default:
					close(c.handshake)
				}
			case packet.Heartbeat:
				if err := c.write(heartbeatFrame); err != nil {
					c.readDone <- clientClose{category: errorWrite, err: err}
					return
				}
			case packet.Data:
				msg, err := message.Decode(p.Data)
				if err != nil {
					c.readDone <- clientClose{category: errorProtocol, err: err}
					return
				}
				if msg.Type == message.Response && msg.ID == 1 {
					c.finishRequest()
				}
			case packet.Kick:
				c.readDone <- clientClose{category: errorKick, err: errors.New("nano server kick")}
				return
			}
		}
	}
}

func (c *loadClient) write(frame []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(websocket.BinaryMessage, frame)
}

func (c *loadClient) closePlanned() {
	c.closeOnce.Do(func() {
		c.planned.Store(true)
		_ = c.conn.Close()
	})
}

func (c *loadClient) startRequest() bool {
	c.requestMu.Lock()
	defer c.requestMu.Unlock()
	if c.requestPending {
		return false
	}
	c.requestStarted = time.Now()
	c.requestPending = true
	return true
}

func (c *loadClient) clearRequest() {
	c.requestMu.Lock()
	c.requestPending = false
	c.requestMu.Unlock()
}

func (c *loadClient) finishRequest() {
	c.requestMu.Lock()
	defer c.requestMu.Unlock()
	if !c.requestPending {
		return
	}
	c.stats.addRequest(time.Since(c.requestStarted))
	c.requestPending = false
}

func requestFrame(route string, raw []byte) ([]byte, error) {
	msg := &message.Message{Type: message.Request, ID: 1, Route: route, Data: raw}
	data, err := msg.Encode()
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	return codec.Encode(packet.Data, data)
}

func nonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
