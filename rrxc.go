/*
Package rrxc is a Request/Response eXchange Controller for synchronizing an
operation against remote asynchronous backends (AKA sync/async or
sync-over-async). The package does not exclusively support sync-over-async as
an exchange in rrxc can very well be fully asynchronous, why it was named a
controller - rrxc correlates requests with responses.

Example usages:

	// In main, before starting the http server and queue consumers...

	controller := rrxc.NewController()

	// In the http server handler...

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	ctx = controller.NewExchangeContext(ctx)
	xc, err := rrxc.ExchangeFromContext(ctx)
	// handler error
	defer xc.Close()
	// Use ID in your outgoing message, the other side would have to include it in
	// their response.
	id := xc.NewCorrelID()
	reg := &rrxc.Registration{
		CorrelID: id,
		Message: "hello world",
	}
	if err := xc.RegisterRequest(reg); err != nil {
		return err
	}
	producer.Publish(queue, fmt.Sprintf(`{"id":%q,"message":%q}`, reg.CorrelID, reg.Message.(string)))
	result, err := rrxc.Wait(ctx) // wait for RegisterResponse (in msghandler below)
	if err != nil {
		return err
	}

	// In the message handler (consumer, in the same app as the http server)...

	if controller.HasTag(correlatingID, controller.GetRollbackTag()) {
		// Silently ACK a rollback-message
		return nil
	}

	if err := controller.RegisterResponse(&rrxc.Registration{
		CorrelID: correlatingID,
		Message: "hello there",
		FailOnDuplicate: true,
	}); err != nil {
		if errors.Is(err, rrxc.ErrHaveNoCorrelatedRequest) {
			// Message was not for me, requeue it?
			return err
		} else if errors.Is(err, rrxc.ErrDuplicate) {
			// Handle additional response with the same correlation ID
			return nil
		}
		return err
	}
*/
package rrxc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/sa6mwa/rrxc/pkg/anystore"
)

const (
	defaultRollbackTag         string        = "rollback"
	defaultRollbackTagLifespan time.Duration = 3 * time.Hour
)

// A controller handles multiple exchanges.
type Controller interface {
	// Controller_SetRollbackTag sets the name of the rollback tag (by default "rollback")
	// which a correlID is tagged with when a request in an exchange context is not
	// responded to when the exchange is terminated by a cancelled context.
	SetRollbackTag(tag string) Controller

	// Controller_GetRollbackTag returns the configured rollback tag (default
	// "rollback").
	GetRollbackTag() string

	// SetRollbacktagLifespan sets the timeout until the rollback tag is removed
	// (default 3h). A value of 0 disables the timeout (not recommended).
	SetRollbackTagLifespan(d time.Duration) Controller

	// Controller.NewControllerContext stores the already initiated controller in
	// ctx Values to be retrieved using rrxc.ControllerFromContext. Returns a
	// derived context.
	NewControllerContext(ctx context.Context) context.Context

	// NewExchangeContext is usually executed inside a HTTP handler function or MQ
	// message handler function.
	//
	//	ctx := controller.NewExchangeContext(context.Background())
	NewExchangeContext(ctx context.Context) context.Context

	// NewCorrelID returns a new correlation identifier to stamp on requests. The
	// returned CorrelID is guaranteed not to conflict with another request in any
	// of the controller's exchanges.
	NewCorrelID() string

	// HasRequest returns true if this controller has a request with given
	// correlID or false if not.
	HasRequest(correlID string) bool

	// GetRequestAge returns age (time.Duration) of request or error if request
	// with correlID does not exist.
	GetRequestAge(correlID string) (time.Duration, error)

	// RegisterResponse registers a response to a previous request based on
	// correlation ID. Input is a rrxc.Registration struct where CorrelID,
	// Message, OverwriteOnDuplicate and FailOnDuplicate are used. If you only
	// fill in CorrelID and Message, function will return
	// rrxc.ErrHaveNoCorrelatedRequest if there is no correlated request. If
	// OverwriteOnDuplicate is set to true and FailOnDuplicate is false, function
	// will silently return nil (without overwriting a previous response).
	RegisterResponse(r *Registration) error

	// Synchronize creates a new Exchange and configures a struct with Context,
	// Controller and Exchange. The orchestration function will wait until all
	// requests have been responded to or the context has timed out.
	//
	// Usage:
	//
	//	xcresult, err := controller.Synchronize(rrxc.ExchangeFromContext(context.WithTimeout(context.Background(), 15 * time.Second), func(sb SyncBundle) error {
	//		sb.Exchange.RegisterRequest(...)
	//	}))
	Synchronize(ctx context.Context, operation func(sb SyncBundle) error) (ExchangeResult, error)

	// Tag tags the entity with tag and - optionally - sends the entity value
	// through the channel(s) specified in notificationChannels in a separate
	// goroutine.
	Tag(entity any, tag any, notificationChannels ...chan any)

	// Untag removes tag from entity and - optionally - sends the entity value
	// through the channel(s) specified in notificationChannels in a separate
	// goroutine. Returns nil or the only error rrxc.ErrNoSuchEntity if entity was
	// not previously tagged.
	Untag(entity any, tag any, notificationChannels ...chan any) error

	// If zero tags were given HasTag will consider that you want to know whether
	// the entity is untagged or not. If it is tagged and zero tags were given,
	// HasTag will return false.
	HasTag(entity any, tags ...any) bool

	// Close the controller (optional) and terminate all child exchanges.
	Close()
}

// An exchange is a set of request/response pairs (or just one pair).
type Exchange interface {
	// Controller returns the controller attached to this exchange so that
	// you can do controller operations even if you only access to an exchange
	// context. Can not be used as a chained method as it returns error if the
	// exchange could not be retrieved.
	Controller() (Controller, error)

	// GetID returns the *ExchangeID* of this exchange (not to be confused with
	// CorrelID).
	GetID() string

	// NewCorrelID returns a new correlation identifier to stamp on requests. The
	// returned CorrelID is guaranteed not to conflict with another request in any
	// of the parent controller's other exchanges.
	NewCorrelID() string

	// HasCorrelID returns true if this exchange has correlID as an identifier of a
	// request/response. Returns false if not.
	HasCorrelID(correlID string) bool

	// RegisterRequest registers an outgoing request in the controller
	// (before you send the actual request). RegisterRequest takes a Registration
	// struct where CorrelID, Message and NotifyOnResponse are the only relevant
	// fields. Function only returns rrxc.ErrUnableToLoadExchange on error.
	//
	// notificationChannelsOnResponse are synchronous, if one in the list blocks on
	// send, the rest will wait to be notified. TODO: Not sure which behaviour is
	// more favourable: asynchronous or synchronous. It's easy to make
	// asynchronous, each channel could have it's own goroutine. Behaviour could be
	// configurable in the new-constructor.
	RegisterRequest(r *Registration) error

	// RegisterResponse registers a response to a previous request based on
	// correlation ID. Input is a rrxc.Registration struct where CorrelID, Message,
	// OverwriteOnDuplicate and FailOnDuplicate are used. If you only fill in
	// CorrelID and Message, function will return rrxc.ErrHaveNoCorrelatedRequest if
	// there is no correlated request. If OverwriteOnDuplicate is set to true and
	// FailOnDuplicate is false, function will silently return nil (without
	// overwriting a previous response).
	RegisterResponse(r *Registration) error

	// HasRequest returns true if a request in this exchange exists with specified
	// correlID.
	HasRequest(correlID string) bool

	// GetExchangeResult returns an ExchangeResult struct. It is recommended to
	// call this function after confirming the Exchange has ended.
	GetExchangeResult() ExchangeResult

	// GetRequestsAndResponses returns a slice with all RequestResponse objects in
	// this exchange.
	GetRequestsAndResponses() []RequestResponse

	// GetRequest returns the message data in request with correlID or error
	// (ErrUnableToLoadExchange or ErrNoSuchRequest).
	GetRequest(correlID string) (any, error)

	// GetResponse returns the message data in response with correlID or error
	// (ErrUnableToLoadExchange or ErrNoSuchResponse).
	GetResponse(correlID string) (any, error)

	// GetRequestAge returns age (time.Duration) of request (in this exchange) or
	// error if request with correlID does not exist.
	GetRequestAge(correlID string) (time.Duration, error)

	// Done returns a receive-only channel which is closed when all requests in
	// the exchange have been responded to.
	Done() <-chan struct{}

	// Close exchange.
	Close()
}

// Passed to the operation function in controller.Sync and controller.SyncWithoutExchange
type SyncBundle struct {
	Context    context.Context
	Controller Controller
	Exchange   Exchange
}

// type SyncBundle struct {
// 	Context    context.Context
// 	Controller Controller
// 	Exchange   Exchange
// }

// RequestResponse is returned through notificationChannelsOnResponse
type ExchangeResult struct {
	ExchangeID             string
	Created                time.Time
	Finished               time.Time
	Latency                time.Duration
	RequestsAndResponses   []RequestResponse
	AllRequestsRespondedTo bool
}

type RequestResponse struct {
	CorrelID           string
	Request            any
	Response           any
	RespondedTo        bool
	RequestRegistered  time.Time
	ResponseRegistered time.Time
	Latency            time.Duration
}

type Registration struct {
	CorrelID             string
	Message              any
	NotifyOnResponse     []chan RequestResponse
	OverwriteOnDuplicate bool
	FailOnDuplicate      bool
}

// Key used to store and load the controller interface from the AnyStore in the
// context value (of a context.Context).
type controllerKey struct{}

type controller struct {
	contexts            anystore.AnyStore
	mapOfMaps           anystore.AnyStore // map[any]any = make(map[any]any))
	rollbackTag         atomic.Value
	rollbackTagLifespan atomic.Value
	closed              atomic.Value
	done                chan struct{}
}

type tagMap map[any]struct{}

// Key used to store and load the exchange interface from the AnyStore in the
// atomix struct.
type exchangeKey struct{}

// Un-exported exchange is returned wrapped inside an atomix struct which is
// instantiated by NewExchangeContext. The exchange struct does not have a
// (public) interface as the Exchange interface is attached to the atomix
// struct.
type exchange struct {
	controller Controller
	id         string
	created    time.Time
	finished   time.Time
	latency    time.Duration
	requests   anystore.AnyStore
	responses  anystore.AnyStore
	durable    bool // Not implemented yet
	finalize   chan struct{}
	finalized  bool
	done       chan struct{}
	closed     bool
	succeeded  bool
}

// atomix wraps the exchange struct in an AnyStore and what all Exchange
// receiver functions are attached to.
type atomix struct {
	anystore.AnyStore
}

//type requestsMap map[string]requestStruct
//type responsesMap map[string]responseStruct

// An instance of requestStruct is stored as the value of the requests
// correlationIdentifierMap in an exchange instance.
type requestStruct struct {
	request                        any
	registered                     time.Time
	notificationChannelsOnResponse []chan RequestResponse
	completed                      bool
}

type responseStruct struct {
	response   any
	registered time.Time
}

// Default NewID function is NewID256 returning a hex-encoded sha256 string
// based of a random number. NewID256 can be replaced by a custom function or
// the other provided function NewID256...
//
//	rrxc.NewID = rrxc.NewID512
var NewID func() string = NewID256

var (
	ErrNoControllerContext  error = errors.New("context has no request/response controller")
	ErrNoExchangeInContext  error = errors.New("context has no request/response exchange")
	ErrUnableToLoadExchange error = errors.New("unable to load exchange, key not found")
	ErrCorrelIDConflict     error = errors.New("correlation identifier conflict: already have a request with that correlID")
	//ErrNoTagsGiven             error = errors.New("no tags given")
	ErrNoSuchEntity            error = errors.New("no such entity")
	ErrHaveNoCorrelatedRequest error = errors.New("request/response correlation failure: no request found to associate response with")
	ErrNoSuchRequest           error = errors.New("no such request")
	ErrNoSuchResponse          error = errors.New("no such response")
	ErrDuplicate               error = errors.New("duplicate response: have already registered a response for this correlation identifier")
)

// Start with all non-receiver functions

// NewController is initialized before starting HTTP servers or message
// handlers, usually in the main function. The return Controller should be used
// in HandleFuncs and message handlers.
func NewController() Controller {
	ctrlr := &controller{
		contexts:  anystore.New(),
		mapOfMaps: anystore.New(),
		done:      make(chan struct{}),
	}
	ctrlr.rollbackTag.Store(defaultRollbackTag)
	ctrlr.rollbackTagLifespan.Store(defaultRollbackTagLifespan)
	return ctrlr
}

// NewControllerContext calls NewController and stores it in the context.Values
// which is simple to load using rrxc.ControllerFromContext. Returns a derived
// context which is to be used to derive Exchange contexts via
// mycontroller.NewExchangeContext. See related ControllerFromContext to load a
// controller context.
func NewControllerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, controllerKey{}, NewController())
}

// ControllerFromContext returns the Controller stored (by NewControllerContext)
// in ctx or error rrxc.ErrNoControllerContext.
func ControllerFromContext(ctx context.Context) (Controller, error) {
	ctrlr, ok := ctx.Value(controllerKey{}).(*controller)
	if !ok {
		return nil, ErrNoControllerContext
	}
	return ctrlr, nil
}

// ExchangeFromContext returns the Exchange stored (by NewExchangeContext) in
// ctx or error rrxc.ErrNoExchangeInContext.
func ExchangeFromContext(ctx context.Context) (Exchange, error) {
	x, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return nil, ErrNoExchangeInContext
	}
	return x, nil
}

// RegisterRequestByContext registers an outgoing request in the Exchange
// context stored in ctx. RegisterRequest takes a Registration struct where
// CorrelID, Message and NotifyOnResponse are the only relevant fields. Function
// only returns rrxc.ErrUnableToLoadExchange on error.
func RegisterRequestByContext(ctx context.Context, r *Registration) error {
	xc, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return ErrNoExchangeInContext
	}
	return xc.RegisterRequest(r)
}

// RegisterResponseByContext registers a response to a previous request in the
// Exchange context ctx based on correlation ID. Input is a rrxc.Registration
// struct where CorrelID, Message, OverwriteOnDuplicate and FailOnDuplicate are
// used. If you only fill in CorrelID and Message, function will return
// rrxc.ErrHaveNoCorrelatedRequest if there is no correlated request. If
// OverwriteOnDuplicate is set to true and FailOnDuplicate is false, function
// will silently return nil (without overwriting a previous response).
func RegisterResponseByContext(ctx context.Context, r *Registration) error {
	xc, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return ErrNoExchangeInContext
	}
	return xc.RegisterResponse(r)
}

// Wait is a synchronization primitive simply waiting until the Exchange context
// ctx is finished (i.e all requests have been responded to) or the context has
// been cancelled (e.g timed out). Returns an ExchangeResult object or error.
func Wait(ctx context.Context) (ExchangeResult, error) {
	xc, err := ExchangeFromContext(ctx)
	if err != nil {
		return ExchangeResult{}, err
	}
	defer xc.Close()
	select {
	case <-ctx.Done():
	case <-xc.Done():
	}
	return xc.GetExchangeResult(), nil
}

// CloseExchangeByContext closes an exchange by it's context ctx. Returns
// ErrNoExchangeInContext in case of error.
func CloseExchangeByContext(ctx context.Context) error {
	xc, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return ErrNoExchangeInContext
	}
	xc.Close()
	return nil
}

// NewID256 returns a random hex-encoded sha256 hash as a string.
func NewID256() string {
	b := make([]byte, 128)
	rand.Read(b) // Care not about errors
	h := sha256.New()
	n := time.Now().UTC().UnixNano()
	nanobytes := []byte{
		byte(0xff & n),
		byte(0xff & (n >> 8)),
		byte(0xff & (n >> 16)),
		byte(0xff & (n >> 24)),
		byte(0xff & (n >> 32)),
		byte(0xff & (n >> 40)),
		byte(0xff & (n >> 48)),
		byte(0xff & (n >> 56)),
	}
	h.Write(append(b, nanobytes...))
	return hex.EncodeToString(h.Sum(nil))
}

// NewID512 returns a random hex-encoded sha512 hash as a string.
func NewID512() string {
	b := make([]byte, 256)
	rand.Read(b)
	h := sha512.New()
	n := time.Now().UTC().UnixNano()
	nanobytes := []byte{
		byte(0xff & n),
		byte(0xff & (n >> 8)),
		byte(0xff & (n >> 16)),
		byte(0xff & (n >> 24)),
		byte(0xff & (n >> 32)),
		byte(0xff & (n >> 40)),
		byte(0xff & (n >> 48)),
		byte(0xff & (n >> 56)),
	}
	h.Write(append(b, nanobytes...))
	return hex.EncodeToString(h.Sum(nil))
}

// Controller interface

func (c *controller) GetExchangeByCorrelID(correlID string) (Exchange, error) {
	keys, err := c.contexts.Keys()
	if err != nil {
		return nil, err
	}
	for _, exchangeID := range keys {
		val, err := c.contexts.Load(exchangeID)
		if err != nil {
			return nil, err
		}
		ctx, ok := val.(context.Context)
		if !ok {
			continue
		}
		xc, err := ExchangeFromContext(ctx)
		if err != nil {
			continue
		}
		if xc.HasRequest(correlID) {
			return xc, nil
		}
	}
	return nil, ErrHaveNoCorrelatedRequest
}

func (c *controller) SetRollbackTag(tag string) Controller {
	c.rollbackTag.Store(tag)
	return c
}

func (c *controller) GetRollbackTag() string {
	return c.rollbackTag.Load().(string)
}

func (c *controller) SetRollbackTagLifespan(d time.Duration) Controller {
	c.rollbackTagLifespan.Store(d)
	return c
}

func (c *controller) NewControllerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, controllerKey{}, c)
}

func (c *controller) NewExchangeContext(ctx context.Context) context.Context {
	xc := exchange{
		controller: c,
		id:         NewID(),
		created:    time.Now(),
		requests:   anystore.New(),
		responses:  anystore.New(),
		finalize:   make(chan struct{}),
		done:       make(chan struct{}),
	}
	axc := atomix{
		anystore.New(),
	}
	axc.Store(exchangeKey{}, xc)
	newContext := context.WithValue(ctx, exchangeKey{}, axc)
	c.contexts.Store(xc.id, newContext)
	// A goroutine will remove this exchange from the contexts map and mark it as
	// terminated when the exchange is done or the context is cancelled.
	go func() {
		tagForRollback := true
		select {
		case <-c.done:
			tagForRollback = false
		case <-xc.finalize:
		case <-newContext.Done():
		}
		c.contexts.Delete(xc.id)
		if axc.HasKey(exchangeKey{}) {
			axc.Run(func(a anystore.AnyStore) error {
				val, err := a.Load(exchangeKey{})
				if err != nil {
					return err
				}
				grxc, ok := val.(exchange)
				if ok {
					if tagForRollback {
						keys, err := grxc.requests.Keys()
						if err != nil {
							return err
						}
						for _, correlID := range keys {
							val, err := grxc.requests.Load(correlID)
							if err != nil {
								return err
							}
							r, ok := val.(requestStruct)
							if !ok {
								continue
							}
							if !r.completed {
								c.Tag(correlID, c.rollbackTag.Load().(string))
								to := c.rollbackTagLifespan.Load().(time.Duration)
								// The following goroutine will remove the rollback tag from
								// this correlID after 24 hours or when/if the controller is
								// closed (c never sends anything on c.done, just closes it).
								go func(cid string) {
									if to > 0 {
										t := time.NewTimer(to)
										select {
										case <-t.C:
										case <-c.done:
											if !t.Stop() {
												<-t.C
											}
										}
									} else {
										<-c.done
									}
									c.Untag(cid, c.rollbackTag.Load().(string))
								}(correlID.(string))
							}
						}
					}
					if !grxc.closed {
						grxc.closed = true
					}
					grxc.finished = time.Now()
					grxc.latency = grxc.finished.Sub(grxc.created)
					a.Store(exchangeKey{}, grxc)
				}
				return nil
			})
		}
		close(xc.done)
	}()
	return newContext
}

func (c *controller) NewCorrelID() string {
	correlID := NewID()
	for {
		hasCorrelID := false
		keys, err := c.contexts.Keys()
		if err != nil {
			return correlID
		}
		for _, exchangeID := range keys {
			val, err := c.contexts.Load(exchangeID)
			if err != nil {
				return correlID
			}
			ctx, ok := val.(context.Context)
			if !ok {
				continue
			}
			xc, err := ExchangeFromContext(ctx)
			if err != nil {
				log.Print("[ERROR] Unable to load exchange from context")
				continue
			}
			if xc.HasCorrelID(correlID) {
				hasCorrelID = true
				continue
			}
		}
		if !hasCorrelID {
			break
		}
	}
	return correlID
}

func (c *controller) HasRequest(correlID string) bool {
	keys, err := c.contexts.Keys()
	if err != nil {
		return false
	}
	for _, exchangeID := range keys {
		val, err := c.contexts.Load(exchangeID)
		if err != nil {
			continue
		}
		ctx, ok := val.(context.Context)
		if !ok {
			continue
		}
		xc, err := ExchangeFromContext(ctx)
		if err != nil {
			log.Print("[ERROR] Unable to load exchange from context")
			continue
		}
		if xc.HasRequest(correlID) {
			return true
		}
	}
	return false
}

func (c *controller) GetRequestAge(correlID string) (time.Duration, error) {
	keys, err := c.contexts.Keys()
	if err != nil {
		return -1, err
	}
	for _, exchangeID := range keys {
		val, err := c.contexts.Load(exchangeID)
		if err != nil {
			continue
		}
		ctx, ok := val.(context.Context)
		if !ok {
			continue
		}
		xc, err := ExchangeFromContext(ctx)
		if err != nil {
			log.Print("[ERROR] Unable to load exchange from context")
			continue
		}
		r, err := xc.GetRequestAge(correlID)
		if err != nil {
			continue
		}
		return r, nil
	}
	return -1, ErrNoSuchRequest
}

func (c *controller) RegisterResponse(r *Registration) error {
	keys, err := c.contexts.Keys()
	if err != nil {
		return err
	}
	for _, exchangeID := range keys {
		val, err := c.contexts.Load(exchangeID)
		if err != nil {
			continue
		}
		ctx, ok := val.(context.Context)
		if !ok {
			continue
		}
		xc, err := ExchangeFromContext(ctx)
		if err != nil {
			continue
		}
		if xc.HasRequest(r.CorrelID) {
			return xc.RegisterResponse(r)
		}
	}
	return ErrHaveNoCorrelatedRequest
}

// // Moved to pkg level
// func (c *controller) RegisterRequestByContext(ctx context.Context, r *Registration) error {
// 	xc, ok := ctx.Value(exchangeKey{}).(atomix)
// 	if !ok {
// 		return ErrNoExchangeInContext
// 	}
// 	return xc.RegisterRequest(r)
// }

// // Moved to pkg level
// func (c *controller) RegisterResponseByContext(ctx context.Context, r *Registration) error {
// 	xc, ok := ctx.Value(exchangeKey{}).(atomix)
// 	if !ok {
// 		return ErrNoExchangeInContext
// 	}
// 	return xc.RegisterResponse(r)
// }

func (c *controller) Synchronize(ctx context.Context, operation func(sb SyncBundle) error) (ExchangeResult, error) {
	xc, err := ExchangeFromContext(ctx)
	if err != nil {
		return ExchangeResult{}, err
	}
	defer xc.Close()
	syncBundle := SyncBundle{
		Context:    ctx,
		Controller: c,
		Exchange:   xc,
	}
	if err := operation(syncBundle); err != nil {
		return ExchangeResult{}, err
	}
	select {
	case <-ctx.Done():
	case <-xc.Done():
	}
	return xc.GetExchangeResult(), nil
}

// // Moved to pkg level
// func (c *controller) Wait(ctx context.Context) (ExchangeResult, error) {
// 	xc, err := ExchangeFromContext(ctx)
// 	if err != nil {
// 		return ExchangeResult{}, err
// 	}
// 	defer xc.Close()
// 	select {
// 	case <-ctx.Done():
// 	case <-xc.Done():
// 	}
// 	return xc.GetExchangeResult(), nil
// }

func (c *controller) Tag(entity any, tag any, notificationChannels ...chan any) {
	c.mapOfMaps.Run(func(mm anystore.AnyStore) error {
		kv := make(tagMap)
		kv[tag] = struct{}{}
		mm.Store(entity, kv)
		return nil
	})
	if len(notificationChannels) > 0 {
		go func() {
			for _, ch := range notificationChannels {
				ch <- entity
				close(ch)
			}
		}()
	}
}

func (c *controller) Untag(entity any, tag any, notificationChannels ...chan any) error {
	err := c.mapOfMaps.Run(func(mm anystore.AnyStore) error {
		val, err := c.mapOfMaps.Load(entity)
		if err != nil {
			return err
		}
		entityTagMap, ok := val.(tagMap)
		if !ok {
			return ErrNoSuchEntity
		}
		delete(entityTagMap, tag)
		// If there are no more tags on this entity, delete the entity from the
		// store, otherwise commit the updated map.
		if len(entityTagMap) == 0 {
			mm.Delete(entity)
		} else {
			mm.Store(entity, entityTagMap)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(notificationChannels) > 0 {
		go func() {
			for _, ch := range notificationChannels {
				ch <- entity
				close(ch)
			}
		}()
	}
	return nil
}

func (c *controller) HasTag(entity any, tags ...any) bool {
	val, err := c.mapOfMaps.Load(entity)
	if err != nil {
		return false
	}
	m, ok := val.(tagMap)
	if !ok {
		return false
	}
	if len(tags) == 0 {
		// Considered you want to know if the entity is untagged, meaning there
		// should be no tags on this entity in the tag map for this to return true.
		if len(m) > 0 {
			return false
		}
	} else {
		for _, tag := range tags {
			if _, found := m[tag]; !found {
				return false
			}
		}
	}
	return true
}

func (c *controller) Close() {
	if c.closed.Load() == nil {
		c.closed.Store(struct{}{})
		close(c.done)
	}
}

// Exchange interface
//
// Receiver functions attached to atomix implement the Exchange interface.

func (x atomix) Controller() (Controller, error) {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnableToLoadExchange, err)
	}
	xc, ok := val.(exchange)
	if !ok {
		return nil, ErrUnableToLoadExchange
	}
	return xc.controller, nil
}

func (x atomix) GetID() string {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return ""
	}
	xc, ok := val.(exchange)
	if !ok {
		return ""
	}
	return xc.id
}

func (x atomix) NewCorrelID() string {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		// Instead of panicking, just return a NewID, the risk that it already
		// exists as an ID or CorrelID in this exchange is still extremely remote.
		return NewID()
	}
	xc, ok := val.(exchange)
	if !ok {
		// Instead of panicking, just return a NewID, the risk that it already
		// exists as an ID or CorrelID in this exchange is still extremely remote.
		return NewID()
	}
	return xc.controller.NewCorrelID()
}

func (x atomix) HasCorrelID(correlID string) bool {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return false
	}
	xc, ok := val.(exchange)
	if !ok {
		return false
	}
	val, err = xc.requests.Load(correlID)
	if err != nil {
		return false
	}
	_, exist := val.(requestStruct)
	return exist
}

func (x atomix) RegisterRequest(r *Registration) error {
	return x.Run(func(a anystore.AnyStore) error {
		val, err := a.Load(exchangeKey{})
		if err != nil {
			return err
		}
		xc, ok := val.(exchange)
		if !ok {
			return ErrUnableToLoadExchange
		}
		xc.requests.Store(r.CorrelID, requestStruct{
			request:                        r.Message,
			registered:                     time.Now(),
			notificationChannelsOnResponse: r.NotifyOnResponse,
		})
		// Commit changes
		return a.Store(exchangeKey{}, xc)
	})
}

func (x atomix) RegisterResponse(r *Registration) error {
	return x.Run(func(a anystore.AnyStore) error {
		val, err := a.Load(exchangeKey{})
		if err != nil {
			return err
		}
		xc, ok := val.(exchange)
		if !ok {
			return ErrUnableToLoadExchange
		}
		val, err = xc.requests.Load(r.CorrelID)
		if err != nil {
			return err
		}
		request, exist := val.(requestStruct)
		if !exist {
			return ErrHaveNoCorrelatedRequest
		}
		val, err = xc.responses.Load(r.CorrelID)
		if err != nil {
			return err
		}
		if _, exist := val.(responseStruct); exist && !r.OverwriteOnDuplicate {
			if r.FailOnDuplicate {
				return ErrDuplicate
			}
			return nil
		}
		request.completed = true
		xc.requests.Store(r.CorrelID, request)
		timeRegistered := time.Now()
		xc.responses.Store(r.CorrelID, responseStruct{
			response:   r.Message,
			registered: timeRegistered,
		})
		// If all requests have been responded to, we are done, you can not add
		// another request after the last response has finished.
		closeExchange := true
		keys, err := xc.requests.Keys()
		if err != nil {
			return err
		}
		for _, cid := range keys {
			val, err := xc.requests.Load(cid)
			if err != nil {
				continue
			}
			r, ok := val.(requestStruct)
			if !ok {
				continue
			}
			if !r.completed {
				closeExchange = false
			}
		}
		if closeExchange {
			xc.succeeded = true
		}
		if closeExchange && !xc.finalized {
			xc.finalized = true
			defer close(xc.finalize)
		}
		// Commit before notifying
		a.Store(exchangeKey{}, xc)
		// Send to notification channels and close them
		if len(request.notificationChannelsOnResponse) > 0 {
			go func() {
				for _, ch := range request.notificationChannelsOnResponse {
					ch <- RequestResponse{
						CorrelID:           r.CorrelID,
						Request:            request.request,
						Response:           r.Message,
						RespondedTo:        true,
						RequestRegistered:  request.registered,
						ResponseRegistered: timeRegistered,
						Latency:            timeRegistered.Sub(request.registered),
					}
					close(ch)
				}
			}()
		}
		return nil
	})
}

func (x atomix) HasRequest(correlID string) bool {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return false
	}
	xc, ok := val.(exchange)
	if !ok {
		return false
	}
	val, err = xc.requests.Load(correlID)
	if err != nil {
		return false
	}
	_, exist := val.(requestStruct)
	return exist
}

func (x atomix) GetExchangeResult() ExchangeResult {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return ExchangeResult{}
	}
	xc, ok := val.(exchange)
	if !ok {
		return ExchangeResult{}
	}
	xresult := ExchangeResult{
		ExchangeID:             xc.id,
		Created:                xc.created,
		Finished:               xc.finished,
		Latency:                xc.latency,
		RequestsAndResponses:   x.GetRequestsAndResponses(),
		AllRequestsRespondedTo: true,
	}
	for _, r := range xresult.RequestsAndResponses {
		if !r.RespondedTo {
			xresult.AllRequestsRespondedTo = false
		}
	}
	return xresult
}

func (x atomix) GetRequestsAndResponses() []RequestResponse {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return []RequestResponse{}
	}
	xc, ok := val.(exchange)
	if !ok {
		return []RequestResponse{}
	}
	rnr := make([]RequestResponse, 0)
	keys, err := xc.requests.Keys()
	if err != nil {
		return []RequestResponse{}
	}
	for _, cid := range keys {
		val, err := xc.requests.Load(cid)
		if err != nil {
			continue
		}
		req, ok := val.(requestStruct)
		if !ok {
			continue
		}
		rr := RequestResponse{
			CorrelID:          cid.(string),
			Request:           req.request,
			RequestRegistered: req.registered,
		}
		if val, err := xc.responses.Load(cid); err == nil {
			resp, gotResponse := val.(responseStruct)
			if gotResponse {
				rr.RespondedTo = true
				rr.Response = resp.response
				rr.ResponseRegistered = resp.registered
				rr.Latency = resp.registered.Sub(req.registered)
			}
		}
		rnr = append(rnr, rr)
	}
	return rnr
}

func (x atomix) GetRequest(correlID string) (any, error) {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return nil, err
	}
	xc, ok := val.(exchange)
	if !ok {
		return nil, ErrUnableToLoadExchange
	}
	val, err = xc.requests.Load(correlID)
	if err != nil {
		return nil, err
	}
	r, found := val.(requestStruct)
	if !found {
		return nil, ErrNoSuchRequest
	}
	return r.request, nil
}

func (x atomix) GetResponse(correlID string) (any, error) {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return nil, err
	}
	xc, ok := val.(exchange)
	if !ok {
		return nil, ErrUnableToLoadExchange
	}
	val, err = xc.responses.Load(correlID)
	if err != nil {
		return nil, err
	}
	r, found := val.(responseStruct)
	if !found {
		return nil, ErrNoSuchResponse
	}
	return r.response, nil
}

func (x atomix) GetRequestAge(correlID string) (time.Duration, error) {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		return -1, err
	}
	xc, ok := val.(exchange)
	if !ok {
		return -1, ErrUnableToLoadExchange
	}
	val, err = xc.requests.Load(correlID)
	if err != nil {
		return -1, err
	}
	r, found := val.(requestStruct)
	if !found {
		return -1, ErrNoSuchRequest
	}
	return time.Now().Sub(r.registered), nil
}

func (x atomix) Done() <-chan struct{} {
	val, err := x.Load(exchangeKey{})
	if err != nil {
		// If we are here, just deliver a closed channel
		closedChannel := make(chan struct{})
		close(closedChannel)
		return closedChannel
	}
	xc, ok := val.(exchange)
	if !ok {
		// If we are here, just deliver a closed channel
		closedChannel := make(chan struct{})
		close(closedChannel)
		return closedChannel
	}
	return xc.done
}

func (x atomix) Close() {
	x.Run(func(a anystore.AnyStore) error {
		val, err := a.Load(exchangeKey{})
		if err != nil {
			return err
		}
		xc, ok := val.(exchange)
		if ok {
			if !xc.finalized {
				xc.finalized = true
				close(xc.finalize)
			}
		}
		a.Store(exchangeKey{}, xc)
		return nil
	})
}
