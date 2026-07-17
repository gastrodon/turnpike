package turnpike

import (
	"context"
	"fmt"
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

// GetMessage is a convenience function to get a single message from a peer,
// bounded by ctx. To bound the wait by a duration, pass a ctx derived from
// context.WithTimeout.
func GetMessage(ctx context.Context, p Peer) (Message, error) {
	select {
	case msg, open := <-p.Receive():
		if !open {
			return nil, fmt.Errorf("receive channel closed")
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
