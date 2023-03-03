// Package rrxc is a Request/Response eXchange Controller for synchronizing an
// operation against remote asynchronous backends (AKA sync/async or
// sync-over-async). The package does not exclusively support sync-over-async as
// an exchange in rrxc can very well be fully asynchronous, why it was named a
// controller - rrxc correlates requests with responses.
package rrxc

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"time"

	"github.com/sa6mwa/rrxc/pkg/anystore"
	"golang.org/x/net/context"
)

// Main exchange handler below. Works like context.Context, returns an
// interface.

// A controller handles multiple exchanges.
type Controller interface {
	NewControllerContext(ctx context.Context) context.Context
	NewExchangeContext(ctx context.Context) context.Context
	RegisterResponse(correlID string, response any, dropDuplicates ...bool) error
	RegisterRequestByContext(ctx context.Context, correlID string, request any, notificationChannelsOnResponse ...chan RequestResponse) error
	RegisterResponseByContext(ctx context.Context, correlID string, response any, dropDuplicates ...bool) error
	Synchronize(ctx context.Context, operation func(sb SyncBundle) error) (ExchangeResult, error)
	Wait(ctx context.Context) (ExchangeResult, error)
	Tag(entity any, tag any, notificationChannels ...chan any)
	Untag(entity any, tag any, notificationChannels ...chan any) error
	HasTag(entity any, tags ...any) bool
	Close()
}

// An exchange is a set of request/response pairs (or just one pair).
type Exchange interface {
	GetID() string
	NewCorrelID() (correlID string)
	RegisterRequest(correlID string, request any, notificationChannelsOnResponse ...chan RequestResponse) error
	RegisterResponse(correlID string, response any, dropDuplicates ...bool) error
	HasRequest(correlID string) bool
	GetExchangeResult() ExchangeResult
	GetRequestsAndResponses() []RequestResponse
	GetRequest(correlID string) (any, error)
	GetResponse(correlID string) (any, error)

	// Done returns a receive-only channel which is closed when there has been at
	// least one successfully exchanged request/response pair (or context has been
	// cancelled).
	Done() <-chan struct{}
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

// Key used to store and load the controller interface from the AnyStore in the
// context value (of a context.Context).
type controllerKey struct{}

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
	requests   requestsMap
	responses  responsesMap
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

type requestsMap map[string]requestStruct
type responsesMap map[string]responseStruct

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

type controller struct {
	contexts  anystore.AnyStore
	mapOfMaps anystore.AnyStore // map[any]any = make(map[any]any))
	done      chan struct{}
}

type tagMap map[any]struct{}

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
	//ErrNoTagsGiven             error = errors.New("no tags given, must at least provide one")
	ErrNoSuchEntity            error = errors.New("no such entity")
	ErrHaveNoCorrelatedRequest error = errors.New("request/response correlation failure: no request found to associate response with")
	ErrNoSuchRequest           error = errors.New("no such request")
	ErrNoSuchResponse          error = errors.New("no such response")
	ErrDuplicate               error = errors.New("duplicate response: have already registered a response for this correlation identifier")
)

// NewController is initialized before starting HTTP servers or message
// handlers, usually in the main function. The return Controller should be used
// in HandleFuncs and message handlers.
func NewController() Controller {
	return &controller{ // Does not need to be a pointer, but became confusing if it wasn't.
		contexts:  anystore.NewAnyStore(),
		mapOfMaps: anystore.NewAnyStore(),
		done:      make(chan struct{}),
	}
}

// Controller.NewControllerContext stores the already initiated controller in
// ctx Values to be retrieved using rrxc.ControllerFromContext. Returns a
// derived context.
func (c *controller) NewControllerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, controllerKey{}, c)
}

// NewControllerContext calls NewController and stores it in the context.Values
// which is simple to load using rrxc.ControllerFromContext. Returns a derived
// context which is to be used to derive Exchange contexts via
// mycontroller.NewExchangeContext. See related ControllerFromContext to load a
// controller context.
func NewControllerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, controllerKey{}, NewController())
}

func ControllerFromContext(ctx context.Context) (Controller, error) {
	ctrlr, ok := ctx.Value(controllerKey{}).(*controller)
	if !ok {
		return nil, ErrNoControllerContext
	}
	return ctrlr, nil
}

// NewExchangeContext is usually executed inside a HTTP handler function or MQ
// message handler function.
//
//	ctx := NewExchangeContext(context.Background())
func (c *controller) NewExchangeContext(ctx context.Context) context.Context {
	xc := exchange{
		controller: c,
		id:         NewID(),
		created:    time.Now(),
		requests:   make(requestsMap),
		responses:  make(responsesMap),
		finalize:   make(chan struct{}),
		done:       make(chan struct{}),
	}
	axc := atomix{
		anystore.NewAnyStore(),
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
				grxc, ok := a.Load(exchangeKey{}).(exchange)
				if ok {
					if tagForRollback {
						for correlID, r := range grxc.requests {
							if !r.completed {
								c.Tag(correlID, "rollback")
								// The following goroutine will remove the rollback tag from
								// this correlID after 24 hours or when/if the controller is
								// closed (c never sends anything on c.done, just closes it).
								go func() {
									t := time.NewTimer(24 * time.Hour)
									select {
									case <-t.C:
									case <-c.done:
										if !t.Stop() {
											<-t.C
										}
									}
									c.Untag(correlID, "rollback")
								}()
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

func ExchangeFromContext(ctx context.Context) (Exchange, error) {
	x, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return nil, ErrNoExchangeInContext
	}
	return x, nil
}

func (c *controller) RegisterResponse(correlID string, response any, dropDuplicates ...bool) error {
	for _, exchangeId := range c.contexts.Keys() {
		ctx, ok := c.contexts.Load(exchangeId).(context.Context)
		if !ok {
			continue
		}
		xc, err := ExchangeFromContext(ctx)
		if err != nil {
			continue
		}
		if xc.HasRequest(correlID) {
			return xc.RegisterResponse(correlID, response, dropDuplicates...)
		}
	}
	return ErrHaveNoCorrelatedRequest
}

func (c *controller) RegisterRequestByContext(ctx context.Context, correlID string, request any, notificationChannelsOnResponse ...chan RequestResponse) error {
	xc, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return ErrNoExchangeInContext
	}
	return xc.RegisterRequest(correlID, request, notificationChannelsOnResponse...)
}

func (c *controller) RegisterResponseByContext(ctx context.Context, correlID string, response any, dropDuplicates ...bool) error {
	xc, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return ErrNoExchangeInContext
	}
	return xc.RegisterResponse(correlID, response, dropDuplicates...)
}

// Usage:
//
//	ctrl := rrxc.NewController()
//	ctrl.Sync(rrxc.ExchangeFromContext(context.WithTimeout(context.Background(), 15 * time.Second), func( context.Context, ) ))
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

func (c *controller) Wait(ctx context.Context) (ExchangeResult, error) {
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

// Tag tags the entity with tag and - optionally - sends the entity value
// through the channel(s) specified in notificationChannels in a separate
// goroutine.
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

// Untag removes tag from entity and - optionally - sends the entity value
// through the channel(s) specified in notificationChannels in a separate
// goroutine. Returns rrxc.ErrNoSuchEntity if entity was not previously tagged.
func (c *controller) Untag(entity any, tag any, notificationChannels ...chan any) error {
	err := c.mapOfMaps.Run(func(mm anystore.AnyStore) error {
		entityTagMap, ok := c.mapOfMaps.Load(entity).(tagMap)
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

// If zero tags were given HasTag will consider that you want to know whether
// the entity is untagged or not. If it is tagged and zero tags were given,
// HasTag will return false.
func (c *controller) HasTag(entity any, tags ...any) bool {
	m, ok := c.mapOfMaps.Load(entity).(tagMap)
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

func CloseExchangeByContext(ctx context.Context) error {
	xc, ok := ctx.Value(exchangeKey{}).(atomix)
	if !ok {
		return ErrNoExchangeInContext
	}
	xc.Close()
	return nil
}

func (c *controller) Close() {
	close(c.done)
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

// Receiver functions attached to atomix implement the Exchange interface.

func (x atomix) GetID() string {
	xc, ok := x.Load(exchangeKey{}).(exchange)
	if !ok {
		return ""
	}
	return xc.id
}

func (x atomix) NewCorrelID() (correlID string) {
	xc, ok := x.Load(exchangeKey{}).(exchange)
	if !ok {
		// Instead of panicking, just return a NewID, the risk that it already
		// exists as an ID or CorrelID in this exchange is still extremely remote.
		return NewID()
	}
	for {
		correlID = NewID()
		if correlID == xc.id {
			continue
		}
		_, exist := xc.requests[correlID]
		if !exist {
			break
		}
	}
	return
}

// notificationChannelsOnResponse are synchronous, if one in the list blocks on
// send, the rest will wait to be notified. TODO: Not sure which behaviour is
// more favourable: asynchronous or synchronous. It's easy to make
// asynchronous, each channel could have it's own goroutine. Behaviour could be
// configurable in the new-constructor.
func (x atomix) RegisterRequest(correlID string, request any, notificationChannelsOnResponse ...chan RequestResponse) error {
	return x.Run(func(a anystore.AnyStore) error {
		xc, ok := a.Load(exchangeKey{}).(exchange)
		if !ok {
			return ErrUnableToLoadExchange
		}
		xc.requests[correlID] = requestStruct{ // requestStruct should not be a pointer
			request:                        request,
			registered:                     time.Now(),
			notificationChannelsOnResponse: notificationChannelsOnResponse,
		}
		// Commit changes
		a.Store(exchangeKey{}, xc)
		return nil
	})
}

func (x atomix) RegisterResponse(correlID string, response any, dropDuplicates ...bool) error {
	return x.Run(func(a anystore.AnyStore) error {
		xc, ok := a.Load(exchangeKey{}).(exchange)
		if !ok {
			return ErrUnableToLoadExchange
		}
		request, exist := xc.requests[correlID]
		if !exist {
			return ErrHaveNoCorrelatedRequest
		}
		if _, exist := xc.responses[correlID]; exist && len(dropDuplicates) > 0 && dropDuplicates[0] {
			return ErrDuplicate
		}
		request.completed = true
		xc.requests[correlID] = request
		timeRegistered := time.Now()
		xc.responses[correlID] = responseStruct{
			response:   response,
			registered: timeRegistered,
		}
		// If all requests have been responded to, we are done, you can not add
		// another request after the last response has finished.
		closeExchange := true
		for _, r := range xc.requests {
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
						CorrelID:           correlID,
						Request:            request.request,
						Response:           response,
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
	xc, ok := x.Load(exchangeKey{}).(exchange)
	if !ok {
		return false
	}
	_, exist := xc.requests[correlID]
	return exist
}

func (x atomix) GetExchangeResult() ExchangeResult {
	xc, ok := x.Load(exchangeKey{}).(exchange)
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
	xc, ok := x.Load(exchangeKey{}).(exchange)
	if !ok {
		return []RequestResponse{}
	}
	rnr := make([]RequestResponse, 0)
	for cid, req := range xc.requests {
		rr := RequestResponse{
			CorrelID:          cid,
			Request:           req.request,
			RequestRegistered: req.registered,
		}
		resp, gotResponse := xc.responses[cid]
		if gotResponse {
			rr.RespondedTo = true
			rr.Response = resp.response
			rr.ResponseRegistered = resp.registered
			rr.Latency = resp.registered.Sub(req.registered)
		}
		rnr = append(rnr, rr)
	}
	return rnr
}

func (x atomix) GetRequest(correlID string) (any, error) {
	xc, ok := x.Load(exchangeKey{}).(exchange)
	if !ok {
		return nil, ErrUnableToLoadExchange
	}
	r, found := xc.requests[correlID]
	if !found {
		return nil, ErrNoSuchRequest
	}
	return r.request, nil
}

func (x atomix) GetResponse(correlID string) (any, error) {
	xc, ok := x.Load(exchangeKey{}).(exchange)
	if !ok {
		return nil, ErrUnableToLoadExchange
	}
	r, found := xc.responses[correlID]
	if !found {
		return nil, ErrNoSuchResponse
	}
	return r.response, nil
}

func (x atomix) Done() <-chan struct{} {
	xc, ok := x.Load(exchangeKey{}).(exchange)
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
		xc, ok := a.Load(exchangeKey{}).(exchange)
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
