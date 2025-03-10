package turnpike

import "sync"

// A Dealer routes and manages RPC calls to callees.
type Dealer interface {
	// Register a procedure on an endpoint
	Register(*Session, *Register)
	// Unregister a procedure on an endpoint
	Unregister(*Session, *Unregister)
	// Call a procedure on an endpoint
	Call(*Session, *Call)
	// Return the result of a procedure call
	Yield(*Session, *Yield)
	// Handle an ERROR message from an invocation
	Error(*Session, *Error)
	// Remove a callee's registrations
	RemoveSession(*Session)
}

type remoteProcedure struct {
	Endpoint  *Session
	Procedure URI
}

type defaultDealer struct {
	// map registration IDs to procedures
	procedures map[ID]remoteProcedure
	// map procedure URIs to registration IDs
	// TODO: this will eventually need to be `map[URI][]ID` to support
	// multiple callees for the same procedure
	registrations map[URI]ID
	// keep track of call IDs so we can send the response to the caller
	calls map[ID]*Session
	// link the invocation ID to the call ID
	invocations map[ID]ID
	// keep track of callee's registrations
	callees map[*Session]map[ID]bool
	// protect maps from concurrent access
	lock sync.Mutex
}

// NewDefaultDealer returns the default turnpike dealer implementation
func NewDefaultDealer() Dealer {
	return &defaultDealer{
		procedures:    make(map[ID]remoteProcedure),
		registrations: make(map[URI]ID),
		calls:         make(map[ID]*Session),
		invocations:   make(map[ID]ID),
		callees:       make(map[*Session]map[ID]bool),
	}
}

func (d *defaultDealer) Register(callee *Session, msg *Register) {
	reg := NewID()
	d.lock.Lock()
	if id, ok := d.registrations[msg.Procedure]; ok {
		d.lock.Unlock()
		log.Println("error: procedure already exists:", msg.Procedure, id)
		callee.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrProcedureAlreadyExists,
		})
		return
	}
	d.procedures[reg] = remoteProcedure{callee, msg.Procedure}
	d.registrations[msg.Procedure] = reg
	d.addCalleeRegistration(callee, reg)
	d.lock.Unlock()

	log.Printf("registered procedure %v [%v]", reg, msg.Procedure)
	callee.Send(&Registered{
		Request:      msg.Request,
		Registration: reg,
	})
}

func (d *defaultDealer) Unregister(callee *Session, msg *Unregister) {
	d.lock.Lock()
	if procedure, ok := d.procedures[msg.Registration]; !ok {
		d.lock.Unlock()
		// the registration doesn't exist
		log.Println("error: no such registration:", msg.Registration)
		callee.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchRegistration,
		})
	} else {
		delete(d.registrations, procedure.Procedure)
		delete(d.procedures, msg.Registration)
		d.removeCalleeRegistration(callee, msg.Registration)
		d.lock.Unlock()
		log.Printf("unregistered procedure %v [%v]", procedure.Procedure, msg.Registration)
		callee.Send(&Unregistered{
			Request: msg.Request,
		})
	}
}

func (d *defaultDealer) Call(caller *Session, msg *Call) {
	d.lock.Lock()
	if reg, ok := d.registrations[msg.Procedure]; !ok {
		d.lock.Unlock()
		caller.Send(&Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Details: make(map[string]interface{}),
			Error:   ErrNoSuchProcedure,
		})
	} else {
		if rproc, ok := d.procedures[reg]; !ok {
			// found a registration id, but doesn't match any remote procedure
			d.lock.Unlock()
			caller.Send(&Error{
				Type:    msg.MessageType(),
				Request: msg.Request,
				Details: make(map[string]interface{}),
				// TODO: what should this error be?
				Error: URI("wamp.error.internal_error"),
			})
		} else {
			// everything checks out, make the invocation request
			d.calls[msg.Request] = caller
			invocationID := NewID()
			d.invocations[invocationID] = msg.Request
			d.lock.Unlock()
			details := map[string]interface{}{}

			// Options{"disclose_me": true} -> Details{"caller": 3335656}
			if val, ok := msg.Options["disclose_me"]; ok {
				if disclose, ok := val.(bool); ok && disclose {
					details["caller"] = caller.Id
				}
			}

			// TODO deal with Details{"trustlevel": 2}
			rproc.Endpoint.Send(&Invocation{
				Request:      invocationID,
				Registration: reg,
				Details:      details,
				Arguments:    msg.Arguments,
				ArgumentsKw:  msg.ArgumentsKw,
			})
			log.Printf("dispatched CALL: %v [%v] to callee as INVOCATION %v",
				msg.Request, msg.Procedure, invocationID,
			)
		}
	}
}

func (d *defaultDealer) Yield(callee *Session, msg *Yield) {
	d.lock.Lock()
	if callID, ok := d.invocations[msg.Request]; !ok {
		d.lock.Unlock()
		// WAMP spec doesn't allow sending an error in response to a YIELD message
		log.Println("received YIELD message with invalid invocation request ID:", msg.Request)
	} else {
		delete(d.invocations, msg.Request)
		if caller, ok := d.calls[callID]; !ok {
			// found the invocation id, but doesn't match any call id
			// WAMP spec doesn't allow sending an error in response to a YIELD message
			d.lock.Unlock()
			log.Printf("received YIELD message, but unable to match it (%v) to a CALL ID", msg.Request)
		} else {
			delete(d.calls, callID)
			d.lock.Unlock()
			// return the result to the caller
			caller.Send(&Result{
				Request:     callID,
				Details:     map[string]interface{}{},
				Arguments:   msg.Arguments,
				ArgumentsKw: msg.ArgumentsKw,
			})
			log.Printf("returned YIELD %v to caller as RESULT %v", msg.Request, callID)
		}
	}
}

func (d *defaultDealer) Error(peer *Session, msg *Error) {
	d.lock.Lock()
	if callID, ok := d.invocations[msg.Request]; !ok {
		d.lock.Unlock()
		log.Println("received ERROR (INVOCATION) message with invalid invocation request ID:", msg.Request)
	} else {
		delete(d.invocations, msg.Request)
		if caller, ok := d.calls[callID]; !ok {
			d.lock.Unlock()
			log.Printf("received ERROR (INVOCATION) message, but unable to match it (%v) to a CALL ID", msg.Request)
		} else {
			delete(d.calls, callID)
			d.lock.Unlock()
			// return an error to the caller
			caller.Send(&Error{
				Type:        CALL,
				Request:     callID,
				Error:       msg.Error,
				Details:     make(map[string]interface{}),
				Arguments:   msg.Arguments,
				ArgumentsKw: msg.ArgumentsKw,
			})
			log.Printf("returned ERROR %v to caller as ERROR %v", msg.Request, callID)
		}
	}
}

func (d *defaultDealer) RemoveSession(callee *Session) {
	d.lock.Lock()
	defer d.lock.Unlock()
	for reg := range d.callees[callee] {
		if procedure, ok := d.procedures[reg]; ok {
			delete(d.registrations, procedure.Procedure)
			delete(d.procedures, reg)
		}
		d.removeCalleeRegistration(callee, reg)
	}
}

func (d *defaultDealer) addCalleeRegistration(callee *Session, reg ID) {
	if _, ok := d.callees[callee]; !ok {
		d.callees[callee] = make(map[ID]bool)
	}
	d.callees[callee][reg] = true
}

func (d *defaultDealer) removeCalleeRegistration(callee *Session, reg ID) {
	if _, ok := d.callees[callee]; !ok {
		return
	}
	delete(d.callees[callee], reg)
	if len(d.callees[callee]) == 0 {
		delete(d.callees, callee)
	}
}
