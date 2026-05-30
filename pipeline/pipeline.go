package pipeline

import (
	"sync"

	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
)

type (
	// Message is the alias of `message.Message`
	Message = message.Message

	Func func(s *session.Session, msg *message.Message) error

	Pipeline interface {
		Outbound() Channel
		Inbound() Channel
	}

	pipeline struct {
		outbound, inbound *pipelineChannel
	}

	Channel interface {
		PushFront(h Func)
		PushBack(h Func)
		Process(s *session.Session, msg *message.Message) error
	}

	pipelineChannel struct {
		mu       sync.RWMutex
		handlers []Func
	}
)

func New() Pipeline {
	return &pipeline{
		outbound: &pipelineChannel{},
		inbound:  &pipelineChannel{},
	}
}

func (p *pipeline) Outbound() Channel { return p.outbound }
func (p *pipeline) Inbound() Channel  { return p.inbound }

// PushFront push a function to the front of the pipeline
func (p *pipelineChannel) PushFront(h Func) {
	p.mu.Lock()
	defer p.mu.Unlock()
	handlers := make([]Func, len(p.handlers)+1)
	handlers[0] = h
	copy(handlers[1:], p.handlers)
	p.handlers = handlers
}

// PushFront push a function to the end of the pipeline
func (p *pipelineChannel) PushBack(h Func) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers = append(p.handlers, h)
}

// Process process message with all pipeline functions
func (p *pipelineChannel) Process(s *session.Session, msg *message.Message) error {
	p.mu.RLock()
	if len(p.handlers) < 1 {
		p.mu.RUnlock()
		return nil
	}
	// Snapshot the handlers and release the lock before invoking them, so a
	// slow handler cannot block PushFront/PushBack and a handler that mutates
	// the channel does not deadlock (RLock -> Lock on the same goroutine).
	handlers := make([]Func, len(p.handlers))
	copy(handlers, p.handlers)
	p.mu.RUnlock()
	for _, h := range handlers {
		err := h(s, msg)
		if err != nil {
			return err
		}
	}
	return nil
}
