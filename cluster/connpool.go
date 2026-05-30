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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lonng/nano/internal/env"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type connPool struct {
	index uint32
	v     []*grpc.ClientConn
}

type rpcClient struct {
	sync.RWMutex
	isClosed bool
	pools    map[string]*connPool
	// dialing serializes pool construction per address so only one goroutine
	// dials a given address and, crucially, the heavy newConnArray runs without
	// the global lock held — otherwise one slow address stalls every concurrent
	// getConnPool caller (routing / heartbeat / session-close) (M15).
	dialing map[string]*sync.Mutex
}

// clusterAuthClientInterceptor attaches the configured shared token to every
// outbound cluster RPC as `authorization` metadata so an authenticated server
// (env.ClusterAuthToken set) accepts it. It is a no-op when no token is set (H9).
func clusterAuthClientInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if token := env.ClusterAuthToken; token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, clusterAuthMetadataKey, token)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func newConnArray(maxSize uint, addr string) (*connPool, error) {
	a := &connPool{
		index: 0,
		v:     make([]*grpc.ClientConn, maxSize),
	}
	if err := a.init(addr); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connPool) init(addr string) error {
	// Attach the cluster auth client interceptor so outbound RPCs carry the
	// shared token when configured (H9). Copy env.GrpcOptions so the shared
	// slice is never mutated.
	opts := make([]grpc.DialOption, 0, len(env.GrpcOptions)+1)
	opts = append(opts, env.GrpcOptions...)
	opts = append(opts, grpc.WithChainUnaryInterceptor(clusterAuthClientInterceptor))
	for i := range a.v {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(
			ctx,
			fmt.Sprintf("dns:///%s", addr),
			opts...,
		)
		cancel()
		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return err
		}
		a.v[i] = conn

	}
	return nil
}

func (a *connPool) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connPool) Close() {
	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			if err != nil {
				// TODO: error handling
			}
			a.v[i] = nil
		}
	}
}

func newRPCClient() *rpcClient {
	return &rpcClient{
		pools:   make(map[string]*connPool),
		dialing: make(map[string]*sync.Mutex),
	}
}

func (c *rpcClient) getConnPool(addr string) (*connPool, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.New("rpc client is closed")
	}
	array, ok := c.pools[addr]
	c.RUnlock()
	if !ok {
		var err error
		array, err = c.createConnPool(addr)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (c *rpcClient) createConnPool(addr string) (*connPool, error) {
	// Grab (or create) the per-address dial lock under the global lock, then
	// release the global lock before dialing so concurrent getConnPool callers
	// for other addresses are never blocked by a slow dial (M15).
	c.Lock()
	if c.isClosed {
		c.Unlock()
		return nil, errors.New("rpc client is closed")
	}
	if array, ok := c.pools[addr]; ok {
		c.Unlock()
		return array, nil
	}
	dm, ok := c.dialing[addr]
	if !ok {
		dm = &sync.Mutex{}
		c.dialing[addr] = dm
	}
	c.Unlock()

	// Only one goroutine dials a given address at a time.
	dm.Lock()
	defer dm.Unlock()

	// Re-check: another goroutine may have built the pool while we waited.
	c.RLock()
	array, ok := c.pools[addr]
	c.RUnlock()
	if ok {
		return array, nil
	}

	// TODO: make conn count configurable
	array, err := newConnArray(10, addr)
	if err != nil {
		return nil, err
	}

	c.Lock()
	if c.isClosed {
		c.Unlock()
		array.Close()
		return nil, errors.New("rpc client is closed")
	}
	c.pools[addr] = array
	c.Unlock()
	return array, nil
}

func (c *rpcClient) closePool() {
	c.Lock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, array := range c.pools {
			array.Close()
		}
	}
	c.Unlock()
}

// removePool closes and removes the connection pool for a single address. It is
// called when a member departs the cluster so its gRPC ClientConns/goroutines
// and the map entry are reclaimed instead of being retained for the process
// lifetime (M3). The connections are closed outside the lock so a slow
// Close cannot stall concurrent getConnPool callers.
func (c *rpcClient) removePool(addr string) {
	c.Lock()
	pool, ok := c.pools[addr]
	if ok {
		delete(c.pools, addr)
	}
	c.Unlock()
	if ok {
		pool.Close()
	}
}
