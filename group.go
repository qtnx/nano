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

package nano

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lonng/nano/cluster"

	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
)

const (
	groupStatusWorking = 0
	groupStatusClosed  = 1
)

// SessionFilter represents a filter which was used to filter session when Multicast,
// the session will receive the message while filter returns true.
type SessionFilter func(*session.Session) bool

// Group represents a session group which used to manage a number of
// sessions, data send to the group will send to all session in it.
type Group struct {
	mu       sync.RWMutex
	status   int32                      // channel current status
	name     string                     // channel name
	sessions map[int64]*session.Session // session id map to session instance
}

// NewGroup returns a new group instance
func NewGroup(n string) *Group {
	return &Group{
		status:   groupStatusWorking,
		name:     n,
		sessions: make(map[int64]*session.Session),
	}
}

// FindMember Find a member with customer filter
func (c *Group) FindMember(filter func(ses *session.Session) bool) (*session.Session, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, s := range c.sessions {
		if filter(s) {
			return s, nil
		}
	}

	return nil, ErrMemberNotFound
}

// Member returns specified UID's session
func (c *Group) Member(uid int64) (*session.Session, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, s := range c.sessions {
		if s.UID() == uid {
			return s, nil
		}
	}

	return nil, ErrMemberNotFound
}

// MemberBySessionID Member by session id
func (c *Group) MemberBySessionID(id int64) (*session.Session, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.sessions[id]
	if !ok {
		return nil, ErrMemberNotFound
	}

	return s, nil
}

// Members returns all member's UID in current group
func (c *Group) Members() []int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var members []int64
	for _, s := range c.sessions {
		members = append(members, s.UID())
	}

	return members
}

// Multicast  push  the message to the filtered clients
func (c *Group) Multicast(route string, v interface{}, filter SessionFilter) error {
	if c.isClosed() {
		return ErrClosedGroup
	}

	data, err := message.Serialize(v)
	if err != nil {
		return err
	}

	if env.Debug {
		log.Println(fmt.Sprintf("Multicast %s, Data=%+v", route, v))
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, s := range c.sessions {
		if !filter(s) {
			continue
		}
		if err = s.Push(route, data); err != nil {
			if errors.Is(err, cluster.ErrBrokenPipe) {
				return err
			} else {
				log.Println(fmt.Sprintf("Session Multicast message error, ID=%d, UID=%d, Error=%s", s.ID(), s.UID(), err.Error()))
			}
		}
	}

	return nil
}

// Singlecast push the message to the specified client decided by session id
func (c *Group) Singlecast(route string, v interface{}, id int64) error {
	if c.isClosed() {
		return ErrClosedGroup
	}
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}

	if env.Debug {
		log.Println(fmt.Sprintf("Singlecast %s, Data=%+v", route, v))
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.sessions[id]
	if !ok {
		return ErrMemberNotFound
	}

	if err = s.Push(route, data); err != nil {
		if errors.Is(err, cluster.ErrBrokenPipe) {
			return err
		} else {
			log.Println(err.Error())
		}
		return err
	}
	return nil
}

// Broadcast push  the message(s) to  all members
func (c *Group) Broadcast(route string, v interface{}) error {
	if c.isClosed() {
		return ErrClosedGroup
	}

	data, err := message.Serialize(v)
	if err != nil {
		return err
	}

	if env.Debug {
		log.Println(fmt.Sprintf("Broadcast %s, Data=%+v", route, v))
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.sessions {
		if err = s.Push(route, data); err != nil {
			if !errors.Is(err, cluster.ErrBrokenPipe) {
				log.Println(fmt.Sprintf("Session push message error, ID=%d, UID=%d, Error=%s", s.ID(), s.UID(), err.Error()))
			}

		}
	}
	return err
}

// BroadcastWithFallbackClosedSession push  the message(s) to  all members
func (c *Group) BroadcastWithFallbackClosedSession(route string, v interface{}) ([]int64, error) {
	closedSession := make([]int64, 0)
	if c.isClosed() {
		return closedSession, ErrClosedGroup
	}

	data, err := message.Serialize(v)
	if err != nil {
		return closedSession, err
	}

	if env.Debug {
		log.Println(fmt.Sprintf("Broadcast %s, Data=%+v", route, v))
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, s := range c.sessions {
		if err = s.Push(route, data); err != nil {
			if errors.Is(err, cluster.ErrBrokenPipe) {
				closedSession = append(closedSession, s.ID())
			} else {
				log.Println(fmt.Sprintf("Session push message error, ID=%d, UID=%d, Error=%s", s.ID(), s.UID(), err.Error()))
			}

		}
	}
	if env.Debug {
		log.Println(fmt.Sprintf("Broadcast done %s, Data=%+v, closedSession=%+v", route, v, closedSession))
	}

	return closedSession, err
}

// Contains check whether a UID is contained in current group or not
func (c *Group) Contains(uid int64) bool {
	_, err := c.Member(uid)
	return err == nil
}

// Add add session to group
func (c *Group) Add(session *session.Session) error {
	if c.isClosed() {
		return ErrClosedGroup
	}

	if env.Debug {
		log.Println(fmt.Sprintf("Add session to group %s, ID=%d, UID=%d", c.name, session.ID(), session.UID()))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	id := session.ID()
	_, ok := c.sessions[id]
	if ok {
		return ErrSessionDuplication
	}

	log.Debugf("Add session to group %s, ID=%d, UID=%d", c.name, id, session.UID())

	c.sessions[id] = session
	return nil
}

// Leave remove specified UID related session from group
func (c *Group) Leave(s *session.Session) error {
	if c.isClosed() {
		return ErrClosedGroup
	}

	if env.Debug {
		log.Println(fmt.Sprintf("Remove session from group %s, UID=%d", c.name, s.UID()))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.sessions, s.ID())
	return nil
}

// LeaveWithSID remove specified UID related session from group with SID
func (c *Group) LeaveWithSID(ID int64) error {
	if c.isClosed() {
		return ErrClosedGroup
	}

	if env.Debug {
		log.Println(fmt.Sprintf("Remove session from group %s, UID=%d", c.name, ID))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.sessions, ID)
	return nil
}

// LeaveAll clear all sessions in the group
func (c *Group) LeaveAll() error {
	if c.isClosed() {
		return ErrClosedGroup
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sessions = make(map[int64]*session.Session)
	return nil
}

// Count get current member amount in the group
func (c *Group) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.sessions)
}

func (c *Group) isClosed() bool {
	if atomic.LoadInt32(&c.status) == groupStatusClosed {
		return true
	}
	return false
}

// Close destroy group, which will release all resource in the group
func (c *Group) Close() error {
	if c.isClosed() {
		return ErrCloseClosedGroup
	}

	atomic.StoreInt32(&c.status, groupStatusClosed)

	// release all reference
	c.sessions = make(map[int64]*session.Session)
	return nil
}
