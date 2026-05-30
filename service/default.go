package service

import (
	"sync/atomic"

	"github.com/bwmarrin/snowflake"
)

// implement Connection
type defaultConnectionServer struct {
	count int64
	node  atomic.Pointer[snowflake.Node]
}

func newDefaultConnectionServer(node uint64) *defaultConnectionServer {
	dcs := &defaultConnectionServer{count: 0}
	dcs.resetNode(node)
	return dcs
}

// resetNode atomically swaps the snowflake node used to generate session ids,
// so a runtime ResetNodeId can never race concurrent SessionID readers.
func (dcs *defaultConnectionServer) resetNode(node uint64) {
	n := int64(node % 1000) // safety node value
	sn, _ := snowflake.NewNode(n)
	dcs.node.Store(sn)
}

// Increment increment the connection count
func (dcs *defaultConnectionServer) Increment() {
	atomic.AddInt64(&dcs.count, 1)
}

// Decrement decrement the connection count
func (dcs *defaultConnectionServer) Decrement() {
	atomic.AddInt64(&dcs.count, -1)
}

// Count returns the connection numbers in current
func (dcs *defaultConnectionServer) Count() int64 {
	return atomic.LoadInt64(&dcs.count)
}

// Reset reset the connection service status
func (dcs *defaultConnectionServer) Reset() {
	atomic.StoreInt64(&dcs.count, 0)
}

// SessionID returns the session id, (snowflake impl)
func (dcs *defaultConnectionServer) SessionID() int64 {
	return dcs.node.Load().Generate().Int64()
}
