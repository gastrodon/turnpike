package turnpike

import (
	"context"
	"fmt"
	"time"
)

// A Sender can send a message to its peer.
//
// For clients, this sends a message to the router, and for routers,
// this sends a message to the client.
type Sender interface {
	// Send a message to the peer
	Send(Message) error
}

// Peer is the interface that must be implemented by all WAMP peers.
type Peer interface {
	Sender

	// Closes the peer connection and any channel returned from Receive().
	// Multiple calls to Close() will have no effect.
	Close() error

	// Receive returns a channel of messages coming from the peer.
	Receive() <-chan Message
}

// GetMessageTimeout is a convenience function to get a single message from a
// peer within a specified period of time
func GetMessageTimeout(ctx context.Context, p Peer, t time.Duration) (Message, error) {
	timer := time.NewTimer(t)
	defer timer.Stop()
	select {
	case msg, open := <-p.Receive():
		if !open {
			return nil, fmt.Errorf("receive channel closed")
		}
		return msg, nil
	case <-timer.C:
		return nil, fmt.Errorf("timeout waiting for message")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
