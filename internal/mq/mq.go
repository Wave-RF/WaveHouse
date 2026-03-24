package mq

import (
	"context"
	"time"
)

// Message represents a message received from the queue.
type Message struct {
	Subject   string
	Data      []byte
	Timestamp time.Time
	ack       func()
	nak       func()
}

// NewMessage constructs a Message with ack/nak callbacks.
func NewMessage(subject string, data []byte, ts time.Time, ack, nak func()) *Message {
	return &Message{Subject: subject, Data: data, Timestamp: ts, ack: ack, nak: nak}
}

// Ack acknowledges successful processing.
func (m *Message) Ack() {
	if m.ack != nil {
		m.ack()
	}
}

// Nak signals processing failure for redelivery.
func (m *Message) Nak() {
	if m.nak != nil {
		m.nak()
	}
}

// Publisher publishes messages to a subject.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
	Close() error
}

// Subscriber subscribes to messages on a subject.
type Subscriber interface {
	Subscribe(ctx context.Context, subject, consumerName string, handler func(msg *Message) error) error
	Close() error
}

// StreamName returns the JetStream stream name used by all MQ implementations.
func StreamName() string {
	return "BEACHHOUSE"
}
