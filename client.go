package turnpike

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"time"
)

var (
	abortUnexpectedMsg = &Abort{
		Details: map[string]interface{}{},
		Reason:  "turnpike.error.unexpected_message_type",
	}
	abortNoAuthHandler = &Abort{
		Details: map[string]interface{}{},
		Reason:  "turnpike.error.no_handler_for_authmethod",
	}
	abortAuthFailure = &Abort{
		Details: map[string]interface{}{},
		Reason:  "turnpike.error.authentication_failure",
	}
	goodbyeClient = &Goodbye{
		Details: map[string]interface{}{},
		Reason:  ErrCloseRealm,
	}
)

// A Client routes messages to/from a WAMP router.
type Client struct {
	Peer
	// ReceiveTimeout is the amount of time that the client will block waiting for a response from the router.
	ReceiveTimeout time.Duration
	// Auth is a map of WAMP authmethods to functions that will handle each auth type
	Auth map[string]AuthFunc
	// ReceiveDone is notified when the client's connection to the router is lost.
	ReceiveDone chan struct{}
	listeners   map[ID]chan Message
	events      map[ID]*eventDesc
	procedures  map[ID]*procedureDesc
	acts        chan func()
}

type procedureDesc struct {
	name    string
	handler MethodHandler
}

type eventDesc struct {
	topic   string
	handler EventHandler
}

// NewWebsocketClient creates a new websocket client connected to the specified
// `url` and using the specified `serialization`.
func NewWebsocketClient(serialization Serialization, url string, requestHeader http.Header, tlscfg *tls.Config, dial DialFunc) (*Client, error) {
	p, err := NewWebsocketPeer(serialization, url, requestHeader, tlscfg, dial)
	if err != nil {
		return nil, err
	}
	return NewClient(p), nil
}

// NewClient takes a connected Peer and returns a new Client
func NewClient(p Peer) *Client {
	c := &Client{
		Peer:           p,
		ReceiveTimeout: 10 * time.Second,
		listeners:      make(map[ID]chan Message),
		events:         make(map[ID]*eventDesc),
		procedures:     make(map[ID]*procedureDesc),
		acts:           make(chan func()),
	}
	go c.run()
	return c
}

func (c *Client) run() {
	for {
		if act, ok := <-c.acts; ok {
			act()
		} else {
			for _, lc := range c.listeners {
				close(lc)
			}
			return
		}
	}
}

// do runs fn on the client's actor goroutine and blocks until it completes, so
// fn has exclusive access to the client's maps.
func (c *Client) do(fn func()) {
	sync := make(chan struct{})
	c.acts <- func() {
		fn()
		close(sync)
	}
	<-sync
}

// fail closes the peer and the actor loop, returning err. Used on the handshake
// error paths before the client is fully established.
func (c *Client) fail(err error) error {
	c.Peer.Close()
	close(c.acts)
	return err
}

// getMessage waits for a single message from the peer, bounded by ctx and the
// client's configured ReceiveTimeout, whichever elapses first.
func (c *Client) getMessage(ctx context.Context) (Message, error) {
	ctx, cancel := context.WithTimeout(ctx, c.ReceiveTimeout)
	defer cancel()
	return GetMessage(ctx, c.Peer)
}

// JoinRealm joins a WAMP realm, but does not handle challenge/response authentication.
//
// Cancelling ctx does not un-send the HELLO; if the router has already accepted,
// this closes the connection without acknowledging it.
func (c *Client) JoinRealm(ctx context.Context, realm string, details map[string]interface{}) (map[string]interface{}, error) {
	if details == nil {
		details = map[string]interface{}{}
	}
	details["roles"] = clientRoles()
	if len(c.Auth) > 0 {
		return c.joinRealmCRA(ctx, realm, details)
	}
	if err := c.Send(&Hello{Realm: URI(realm), Details: details}); err != nil {
		return nil, c.fail(err)
	}
	msg, err := c.getMessage(ctx)
	if err != nil {
		return nil, c.fail(err)
	}
	welcome, ok := msg.(*Welcome)
	if !ok {
		c.Send(abortUnexpectedMsg)
		return nil, c.fail(errors.New(formatUnexpectedMessage(msg, WELCOME)))
	}
	go c.Receive()
	return welcome.Details, nil
}

// AuthFunc takes the HELLO details and CHALLENGE details and returns the
// signature string and a details map
type AuthFunc func(hello, challenge map[string]interface{}) (string, map[string]interface{}, error)

// joinRealmCRA joins a WAMP realm and handles challenge/response authentication.
func (c *Client) joinRealmCRA(ctx context.Context, realm string, details map[string]interface{}) (map[string]interface{}, error) {
	authmethods := []interface{}{}
	for m := range c.Auth {
		authmethods = append(authmethods, m)
	}
	details["authmethods"] = authmethods
	if err := c.Send(&Hello{Realm: URI(realm), Details: details}); err != nil {
		return nil, c.fail(err)
	}

	msg, err := c.getMessage(ctx)
	if err != nil {
		return nil, c.fail(err)
	}
	challenge, ok := msg.(*Challenge)
	if !ok {
		c.Send(abortUnexpectedMsg)
		return nil, c.fail(errors.New(formatUnexpectedMessage(msg, CHALLENGE)))
	}
	authFunc, ok := c.Auth[challenge.AuthMethod]
	if !ok {
		c.Send(abortNoAuthHandler)
		return nil, c.fail(fmt.Errorf("no auth handler for method: %s", challenge.AuthMethod))
	}
	signature, authDetails, err := authFunc(details, challenge.Extra)
	if err != nil {
		c.Send(abortAuthFailure)
		return nil, c.fail(err)
	}
	if err := c.Send(&Authenticate{Signature: signature, Extra: authDetails}); err != nil {
		return nil, c.fail(err)
	}

	msg, err = c.getMessage(ctx)
	if err != nil {
		return nil, c.fail(err)
	}
	welcome, ok := msg.(*Welcome)
	if !ok {
		c.Send(abortUnexpectedMsg)
		return nil, c.fail(errors.New(formatUnexpectedMessage(msg, WELCOME)))
	}
	go c.Receive()
	return welcome.Details, nil
}

func clientRoles() map[string]map[string]interface{} {
	return map[string]map[string]interface{}{
		"publisher":  make(map[string]interface{}),
		"subscriber": make(map[string]interface{}),
		"callee":     make(map[string]interface{}),
		"caller":     make(map[string]interface{}),
	}
}

func formatUnexpectedMessage(msg Message, expected MessageType) string {
	s := fmt.Sprintf("received unexpected %s message while waiting for %s", msg.MessageType(), expected)
	switch m := msg.(type) {
	case *Abort:
		s += ": " + string(m.Reason)
		s += formatUnknownMap(m.Details)
		return s
	case *Goodbye:
		s += ": " + string(m.Reason)
		s += formatUnknownMap(m.Details)
		return s
	}
	return s
}

func formatUnknownMap(m map[string]interface{}) string {
	s := ""
	for k, v := range m {
		// TODO: reflection to recursively check map
		s += fmt.Sprintf(" %s=%v", k, v)
	}
	return s
}

// LeaveRealm leaves the current realm without closing the connection to the server.
func (c *Client) LeaveRealm() error {
	if err := c.Send(goodbyeClient); err != nil {
		return fmt.Errorf("error leaving realm: %v", err)
	}
	return nil
}

// Close closes the connection to the server.
func (c *Client) Close() error {
	if err := c.LeaveRealm(); err != nil {
		return err
	}
	if err := c.Peer.Close(); err != nil {
		return fmt.Errorf("error closing client connection: %v", err)
	}
	return nil
}

// Receive handles messages from the server until this client disconnects.
//
// This function blocks and is most commonly run in a goroutine.
func (c *Client) Receive() {
	for msg := range c.Peer.Receive() {

		switch msg := msg.(type) {

		case *Event:
			c.handleEvent(msg)

		case *Invocation:
			c.handleInvocation(msg)

		case *Registered:
			c.notifyListener(msg, msg.Request)
		case *Subscribed:
			c.notifyListener(msg, msg.Request)
		case *Unsubscribed:
			c.notifyListener(msg, msg.Request)
		case *Unregistered:
			c.notifyListener(msg, msg.Request)
		case *Result:
			c.notifyListener(msg, msg.Request)
		case *Error:
			c.notifyListener(msg, msg.Request)

		case *Goodbye:
			log.Println("client received Goodbye message")

		default:
			log.Println("unhandled message:", msg.MessageType(), msg)
		}
	}

	close(c.acts)
	log.Println("client closed")

	if c.ReceiveDone != nil {
		c.ReceiveDone <- struct{}{}
	}
}

func (c *Client) handleEvent(msg *Event) {
	c.do(func() {
		if event, ok := c.events[msg.Subscription]; ok {
			go event.handler(msg.Arguments, msg.ArgumentsKw)
		} else {
			log.Println("no handler registered for subscription:", msg.Subscription)
		}
	})
}

func (c *Client) notifyListener(msg Message, requestID ID) {
	// pass in the request ID so we don't have to do any type assertion
	var (
		l  chan Message
		ok bool
	)
	c.do(func() {
		l, ok = c.listeners[requestID]
	})
	if ok {
		l <- msg
	} else {
		log.Println("no listener for message", msg.MessageType(), requestID)
	}
}

func (c *Client) handleInvocation(msg *Invocation) {
	c.do(func() {
		if proc, ok := c.procedures[msg.Registration]; ok {
			go func() {
				result := proc.handler(msg.Arguments, msg.ArgumentsKw, msg.Details)

				var tosend Message
				tosend = &Yield{
					Request:     msg.Request,
					Options:     make(map[string]interface{}),
					Arguments:   result.Args,
					ArgumentsKw: result.Kwargs,
				}

				if result.Err != "" {
					tosend = &Error{
						Type:        INVOCATION,
						Request:     msg.Request,
						Details:     make(map[string]interface{}),
						Arguments:   result.Args,
						ArgumentsKw: result.Kwargs,
						Error:       result.Err,
					}
				}

				if err := c.Send(tosend); err != nil {
					log.Println("error sending message:", err)
				}
			}()
		} else {
			log.Println("no handler registered for registration:", msg.Registration)
			if err := c.Send(&Error{
				Type:    INVOCATION,
				Request: msg.Request,
				Details: make(map[string]interface{}),
				Error:   URI(fmt.Sprintf("no handler for registration: %v", msg.Registration)),
			}); err != nil {
				log.Println("error sending message:", err)
			}
		}
	})
}

func (c *Client) registerListener(id ID) {
	log.Println("register listener:", id)
	wait := make(chan Message, 1)
	c.do(func() {
		c.listeners[id] = wait
	})
}

// waitOnListener blocks until a message arrives for id, the ReceiveTimeout
// elapses, or ctx is cancelled — whichever comes first. The listener is always
// removed before returning, on every exit path.
func (c *Client) waitOnListener(ctx context.Context, id ID) (msg Message, err error) {
	log.Println("wait on listener:", id)
	var (
		wait chan Message
		ok   bool
	)
	c.do(func() {
		wait, ok = c.listeners[id]
	})
	if !ok {
		return nil, fmt.Errorf("unknown listener ID: %v", id)
	}
	ctx, cancel := context.WithTimeout(ctx, c.ReceiveTimeout)
	defer cancel()
	select {
	case msg, ok = <-wait:
		if !ok {
			return nil, fmt.Errorf("listener closed while waiting for message")
		}
	case <-ctx.Done():
		err = ctx.Err()
	}
	c.do(func() {
		delete(c.listeners, id)
	})
	return
}

// EventHandler handles a publish event.
type EventHandler func(args []interface{}, kwargs map[string]interface{})

// Subscribe registers the EventHandler to be called for every message in the
// provided topic, bounding the wait for the SUBSCRIBED reply on ctx in addition
// to the ReceiveTimeout.
func (c *Client) Subscribe(ctx context.Context, topic string, options map[string]interface{}, fn EventHandler) error {
	if options == nil {
		options = make(map[string]interface{})
	}
	id := NewID()
	c.registerListener(id)
	sub := &Subscribe{
		Request: id,
		Options: options,
		Topic:   URI(topic),
	}
	if err := c.Send(sub); err != nil {
		return err
	}
	msg, err := c.waitOnListener(ctx, id)
	if err != nil {
		return err
	}
	if e, ok := msg.(*Error); ok {
		return fmt.Errorf("error subscribing to topic '%v': %v", topic, e.Error)
	}
	subscribed, ok := msg.(*Subscribed)
	if !ok {
		return errors.New(formatUnexpectedMessage(msg, SUBSCRIBED))
	}
	c.do(func() {
		c.events[subscribed.Subscription] = &eventDesc{topic, fn}
	})
	return nil
}

// Unsubscribe removes the registered EventHandler from the topic, bounding the
// wait for the UNSUBSCRIBED reply on ctx in addition to the ReceiveTimeout.
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	var (
		subscriptionID ID
		found          bool
	)
	c.do(func() {
		for id, desc := range c.events {
			if desc.topic == topic {
				subscriptionID = id
				found = true
				break
			}
		}
	})
	if !found {
		return fmt.Errorf("event %s is not registered with this client", topic)
	}

	id := NewID()
	c.registerListener(id)
	sub := &Unsubscribe{
		Request:      id,
		Subscription: subscriptionID,
	}
	if err := c.Send(sub); err != nil {
		return err
	}
	msg, err := c.waitOnListener(ctx, id)
	if err != nil {
		return err
	}
	if e, ok := msg.(*Error); ok {
		return fmt.Errorf("error unsubscribing to topic '%v': %v", topic, e.Error)
	}
	if _, ok := msg.(*Unsubscribed); !ok {
		return errors.New(formatUnexpectedMessage(msg, UNSUBSCRIBED))
	}
	c.do(func() {
		delete(c.events, subscriptionID)
	})
	return nil
}

// MethodHandler is an RPC endpoint.
type MethodHandler func(
	args []interface{}, kwargs map[string]interface{}, details map[string]interface{},
) (result *CallResult)

// Register registers a MethodHandler procedure with the router, bounding the
// wait for the REGISTERED reply on ctx in addition to the ReceiveTimeout.
func (c *Client) Register(ctx context.Context, procedure string, fn MethodHandler, options map[string]interface{}) error {
	id := NewID()
	c.registerListener(id)
	register := &Register{
		Request:   id,
		Options:   options,
		Procedure: URI(procedure),
	}
	if err := c.Send(register); err != nil {
		return err
	}
	msg, err := c.waitOnListener(ctx, id)
	if err != nil {
		return err
	}
	if e, ok := msg.(*Error); ok {
		return fmt.Errorf("error registering procedure '%v': %v", procedure, e.Error)
	}
	registered, ok := msg.(*Registered)
	if !ok {
		return errors.New(formatUnexpectedMessage(msg, REGISTERED))
	}
	c.do(func() {
		c.procedures[registered.Registration] = &procedureDesc{procedure, fn}
	})
	return nil
}

// BasicMethodHandler is an RPC endpoint that doesn't expect the `Details` map
type BasicMethodHandler func(args []interface{}, kwargs map[string]interface{}) (result *CallResult)

// BasicRegister registers a BasicMethodHandler procedure with the router
func (c *Client) BasicRegister(ctx context.Context, procedure string, fn BasicMethodHandler) error {
	wrap := func(args []interface{}, kwargs map[string]interface{},
		details map[string]interface{}) (result *CallResult) {
		return fn(args, kwargs)
	}
	return c.Register(ctx, procedure, wrap, make(map[string]interface{}))
}

// Unregister removes a procedure with the router, bounding the wait for the
// UNREGISTERED reply on ctx in addition to the ReceiveTimeout.
func (c *Client) Unregister(ctx context.Context, procedure string) error {
	var (
		procedureID ID
		found       bool
	)
	c.do(func() {
		for id, p := range c.procedures {
			if p.name == procedure {
				procedureID = id
				found = true
				break
			}
		}
	})
	if !found {
		return fmt.Errorf("procedure %s is not registered with this client", procedure)
	}
	id := NewID()
	c.registerListener(id)
	unregister := &Unregister{
		Request:      id,
		Registration: procedureID,
	}
	if err := c.Send(unregister); err != nil {
		return err
	}
	msg, err := c.waitOnListener(ctx, id)
	if err != nil {
		return err
	}
	if e, ok := msg.(*Error); ok {
		return fmt.Errorf("error unregister to procedure '%v': %v", procedure, e.Error)
	}
	if _, ok := msg.(*Unregistered); !ok {
		return errors.New(formatUnexpectedMessage(msg, UNREGISTERED))
	}
	c.do(func() {
		delete(c.procedures, procedureID)
	})
	return nil
}

// Publish publishes an EVENT to all subscribed peers.
func (c *Client) Publish(topic string, options map[string]interface{}, args []interface{}, kwargs map[string]interface{}) error {
	if options == nil {
		options = make(map[string]interface{})
	}
	return c.Send(&Publish{
		Request:     NewID(),
		Options:     options,
		Topic:       URI(topic),
		Arguments:   args,
		ArgumentsKw: kwargs,
	})
}

type RPCError struct {
	ErrorMessage *Error
	Procedure    string
}

func (rpc RPCError) Error() string {
	return fmt.Sprintf("error calling procedure '%v': %v: %v: %v", rpc.Procedure, rpc.ErrorMessage.Error, rpc.ErrorMessage.Arguments, rpc.ErrorMessage.ArgumentsKw)
}

// Call calls a procedure given a URI and waits for the result. In addition to
// the ReceiveTimeout, the wait is bounded by ctx: if ctx is cancelled before a
// result arrives, Call returns ctx.Err() and stops listening for the response.
// Cancelling ctx does not unsend an already-sent CALL; it only abandons the
// wait on this side.
func (c *Client) Call(ctx context.Context, procedure string, options map[string]interface{}, args []interface{}, kwargs map[string]interface{}) (*Result, error) {
	id := NewID()
	c.registerListener(id)

	call := &Call{
		Request:     id,
		Procedure:   URI(procedure),
		Options:     options,
		Arguments:   args,
		ArgumentsKw: kwargs,
	}
	if err := c.Send(call); err != nil {
		return nil, err
	}
	msg, err := c.waitOnListener(ctx, id)
	if err != nil {
		return nil, err
	}
	if e, ok := msg.(*Error); ok {
		return nil, RPCError{e, procedure}
	}
	result, ok := msg.(*Result)
	if !ok {
		return nil, errors.New(formatUnexpectedMessage(msg, RESULT))
	}
	return result, nil
}
