// Message.OnSettled, apart from mq.go so the e2e coverage gate, whose
// stack runs the embedded broker, can leave it to the suites that consume a
// sharded queue (.testcoverage.yml).

package mq

import (
	"context"
	"sync"
	"time"
)

// OnSettled arranges for fn to run once, after the first DoubleAck, Ack,
// Nak or NakWithDelay, whether or not the broker confirms it: the consumer
// has let go of the message either way, and one it failed to settle comes
// back from the broker as a new delivery. A consumer handing a shard on to
// another process waits for it, and one that caps the messages it holds
// frees a slot. Call it before the message is shared with another goroutine.
func (m *Message) OnSettled(fn func()) {
	var once sync.Once
	settle := func(err error) error {
		once.Do(fn)
		return err
	}
	doubleAck, ack, nak, nakDelay := m.doubleAckFn, m.ackFn, m.nakFn, m.nakDelayFn
	m.doubleAckFn = func(ctx context.Context) error {
		if doubleAck == nil {
			return settle(nil)
		}
		return settle(doubleAck(ctx))
	}
	m.ackFn = func() error {
		if ack == nil {
			return settle(nil)
		}
		return settle(ack())
	}
	m.nakFn = func() error {
		if nak == nil {
			return settle(nil)
		}
		return settle(nak())
	}
	if nakDelay != nil {
		m.nakDelayFn = func(d time.Duration) error { return settle(nakDelay(d)) }
	}
}
