package mq

import (
	"strconv"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

// feedLog is a liveFeed whose deliveries and loss reports are recorded in the
// order they happen.
type feedLog struct {
	*liveFeed
	events    []string
	connected bool
}

func newFeedLog() *feedLog {
	l := &feedLog{connected: true}
	l.liveFeed = newLiveFeed(
		func(m *nats.Msg) { l.events = append(l.events, string(m.Data)) },
		func() { l.events = append(l.events, "lost") },
		func() bool { return l.connected },
	)
	return l
}

// row is a republished row: data names it, and the server's headers say which
// stream stored it and at what sequence.
func (l *feedLog) row(stream string, seq uint64) {
	m := nats.NewMsg("wh.live.acme.events")
	m.Header.Set(republishedStream, stream)
	m.Header.Set(republishedSequence, strconv.FormatUint(seq, 10))
	m.Data = []byte(stream + ":" + strconv.FormatUint(seq, 10))
	l.receive(m)
}

// The live feed is a plain subscription, so nothing redelivers a row it was
// not sent. Each stream's rows carry its sequence, which rises by one a row:
// a break is reported before the row that shows it is delivered, and so is a
// connection that was lost, once it is back.
func TestLiveFeed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(l *feedLog)
		want []string
	}{
		{"rows in sequence are delivered", func(l *feedLog) {
			l.row("A", 7)
			l.row("A", 8)
			l.row("A", 9)
		}, []string{"A:7", "A:8", "A:9"}},
		{"a skipped row is reported before the next is delivered", func(l *feedLog) {
			l.row("A", 1)
			l.row("A", 2)
			l.row("A", 4)
			l.row("A", 5)
		}, []string{"A:1", "A:2", "lost", "A:4", "A:5"}},
		{"a repeated row is a break too", func(l *feedLog) {
			l.row("A", 1)
			l.row("A", 1)
		}, []string{"A:1", "lost", "A:1"}},
		{"each stream has its own sequence", func(l *feedLog) {
			l.row("A", 1)
			l.row("B", 40)
			l.row("A", 2)
			l.row("B", 41)
		}, []string{"A:1", "B:40", "A:2", "B:41"}},
		{"a break on one stream starts every stream over", func(l *feedLog) {
			l.row("A", 1)
			l.row("B", 40)
			l.row("A", 3)
			// The readers were sent back to the replay, so B's earlier
			// sequence means nothing to the ones that follow.
			l.row("B", 45)
			l.row("B", 46)
		}, []string{"A:1", "B:40", "lost", "A:3", "B:45", "B:46"}},
		{"a message no stream republished is dropped", func(l *feedLog) {
			l.row("A", 1)
			forged := nats.NewMsg("wh.live.acme.events")
			forged.Data = []byte("forged")
			l.receive(forged)
			unnumbered := nats.NewMsg("wh.live.acme.events")
			unnumbered.Header.Set(republishedStream, "A")
			unnumbered.Header.Set(republishedSequence, "two")
			unnumbered.Data = []byte("unnumbered")
			l.receive(unnumbered)
			l.row("A", 2)
		}, []string{"A:1", "A:2"}},
		{"a lost connection is reported once it is back", func(l *feedLog) {
			l.row("A", 1)
			l.disconnected()
			l.connected = false
			l.connected = true
			l.reconnected()
			l.reconnected()
			// Whatever the stream stored meanwhile was skipped, and reported.
			l.row("A", 9)
		}, []string{"A:1", "lost", "A:9"}},
		{"a row that outruns the reconnect report carries it", func(l *feedLog) {
			l.row("A", 1)
			l.disconnected()
			l.row("A", 2)
			l.reconnected()
		}, []string{"A:1", "lost", "A:2"}},
		{"nothing is reported while the connection is down", func(l *feedLog) {
			l.row("A", 1)
			l.disconnected()
			l.connected = false
			// Received before the connection went: delivered, and no reader is
			// sent to a replay the broker cannot answer.
			l.row("A", 2)
			l.connected = true
			l.reconnected()
		}, []string{"A:1", "A:2", "lost"}},
		{"a reconnect with nothing lost reports nothing", func(l *feedLog) {
			l.row("A", 1)
			l.reconnected()
			l.row("A", 2)
		}, []string{"A:1", "A:2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := newFeedLog()
			tt.run(l)
			assert.Equal(t, tt.want, l.events)
		})
	}
}
