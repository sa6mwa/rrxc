# rrxc

Package rrxc is a Request/Response eXchange Controller for synchronizing an
operation against remote asynchronous backends (AKA sync/async or
sync-over-async). The package does not exclusively support sync-over-async as an
exchange in rrxc can very well be fully asynchronous, why it was named a
controller - rrxc correlates requests with responses.

This package is currently very much a MVP, examples and proper unit testing are
in the backlog.

See [rrxc_test.go](rrxc_test.go) for crude examples.

## Walkthrough

A new controller is instantiated via `ctrl := rrxc.NewController()` in the
`main` function. In each call to the server handler that is to make asynchronous
requests against remote system(s), a new exchange is initiated and attached to a
`context.Context` (preferably with a timeout), for example...

```go
ctrl := rrxc.NewController()
ctx, cancel := context.WithTimeout(context.Background(), 15 * time.Second)
defer cancel()
ctx = ctrl.NewExchangeContext(ctx)
```

A new *correlation identifier* or `correlID` needs to be requested to pair-up a
response with a request using, for example, `correlID := ctrl.NewCorrelID()`.
You use this `correlID` in your requests to your remote system(s) and they are
expected to include this ID in their response. Requests are registered in the
exchange using one of the registration functions, e.g...

```go
correlID := ctrl.NewCorrelID()
if err := rrxc.RegisterRequestByContext(ctx, &rrxc.Registration{
	CorrelID: correlID,
	Message: "my message",
}); err != nil {
	// handle error...
}
// Send your request to remote system...
```

If you are making more asynchronous requests in this exchange, you
need to fetch another `correlID` using `ctrl.NewCorrelID()` and register the
request the same way (via the controller or the exchange receiver function;
`RegisterRequestByContext`, `RegisterRequest`). In the other server goroutine
(perhaps a messaging queue consumer or a callback handler/path in the API
server) you register responses via the controller (directly or derived from a
controller context via `rrxc.ControllerFromContext(ctx)`)...

```go
// Controller direct method...

if err := ctrl.RegisterResponse(&rrxc.Registration{
	CorrelID: myID,
	Message: "hello world",
}); err != nil {
	// Handle error
}

// Controller from context...

ctrl := rrxc.NewController()
ctx := ctrl.NewControllerContext(context.Background())
// Use ctx in your response handler and derive the controller...
c, err := rrxc.ControllerFromContext(ctx)
// Register response (controller will search for an exchange having a request
// with this correlID and put the response there)...
if err := c.RegisterResponse(&rrxc.Registration{
	CorrelID: myID,
	Message: "hello world",
}); err != nil {
	// Handle error
}
```

A response can simply be registered via `ctrl.RegisterResponse(id, &rrxc.Registration{})`
and the controller will figure out which exchange the response should be
registered in.

In the requesting server handler the function `rrxc.Wait(exchangeContext)` can
be used to wait until all responses have arrived. An alternative approach is to
wrap everything in the `ctrl.Synchronize` function which will not exit until all
requests are done or the context is cancelled (timed out). The results are
returned by both `Synchronize` and `Wait` as an `ExchangeResult` struct.

If any of the requests fail, the whole exchange will fail and could potentially
be handled as a transaction to rollback. The controller keeps a map of *tags*.
If the exchange is closed or the context is cancelled and has requests without
responses, each `correlID` is tagged with the default rollback tag (`rollback`).
These tags can be looked up in, for example, the message consumer handler if the
remote system is setup to requeue un-acknowledged messages. That way you can
handle the message and acknowledge it. The rollback tags are automatically
removed from the tag map after `defaultRollbackTagLifespan` which is `3 *
time.Hour` or `ctrl.SetRollbackTagLifespan(duration)`.

## Illustration

A drawing illustrating the connection between controller(s), exchange(s) and
request(s)/response(s).

```
      ┌───────────────────────────┐     ┌───────────────────────────┐
      │                           │     │                           │
      │ a := rrxc.NewController() │     │ b := rrxc.NewController() │ ...
      │                           │     │                           │
      └──┬────────┬───────┬───────┘     └──┬─────────┬────────┬─────┘
         │        │       │                │         │        │
         │        │       │                │         │        │
         │        │       │                │         │        │
  ca := a.NewExchangeContext(ctx)      cb := b.NewExchangeContext(ctx)
         │        │       │                │         │        │
         │        │       │                │         │        │
         │        │       │                │         │        │
     ┌───▼──┐ ┌───▼──┐ ┌──▼───┐         ┌──▼───┐ ┌───▼──┐ ┌───▼──┐
     │ Xchg │ │ Xchg │ │ Xchg │ ...     │ Xchg │ │ Xchg │ │ Xchg │ ...
     └┬─┬─┬─┘ └┬─┬─┬─┘ └──────┘         └──────┘ └──┬───┘ └──────┘
      │ │ │    │ │ │                                │
      │ │ │    │ │ │                                │
rrxc.RegisterRequestByContext(ca,...)  rrxc.RegisterRequestByContext(cb,...)
      │ │ │    │ │ │                                │
      │ │ │    │ │ └─────────┐                      │
      │ │ │    │ │           │                      │
      │ │ │    │ └────┐      │                      │
      │ │ │    │      │      │                   ┌──▼─┐
      │ │ │  ┌─▼──┐ ┌─▼──┐ ┌─▼──┐                │ RR │ ...
      │ │ │  │ RR │ │ RR │ │ RR │ ...            └────┘
      │ │ │  └────┘ └────┘ └────┘            Request/Response
      │ │ │
   ┌──┘ │ └─────┐
   │    │       │
┌──▼─┐ ┌▼───┐ ┌─▼──┐
│ RR │ │ RR │ │ RR │ ...
└────┘ └────┘ └────┘
  Request/Response
```


## Bugs

The following appeared during load testing (fixed in
8ea22054224122f3f887404b2aca9428cb2e5dce). Cause was using a map incorrectly,
resolved by implementing `anystore.AnyStore` for the map instead.

```
fatal error: concurrent map read and map write

goroutine 47431 [running]:
github.com/sa6mwa/rrxc.atomix.HasCorrelID({{0x7d1520?, 0xc0000a5440?}}, {0xc0002d5a00, 0x40})
        /some/dir/go/pkg/mod/github.com/sa6mwa/rrxc@v0.0.0-20230305211820-0bf9a9e490b8/rrxc.go:849 +0xfa
github.com/sa6mwa/rrxc.(*controller).NewCorrelID(0xc0000602a0)
        /some/dir/go/pkg/mod/github.com/sa6mwa/rrxc@v0.0.0-20230305211820-0bf9a9e490b8/rrxc.go:619 +0x1a7
main.main.func2({0x7d0128, 0xc0000a1a40}, 0xc00041fc00)
        /some/dir/lab/cmd/calculator-api-server/main.go:237 +0x33a
net/http.HandlerFunc.ServeHTTP(0xc000072af0?, {0x7d0128?, 0xc0000a1a40?}, 0x0?)
        /usr/local/go/src/net/http/server.go:2109 +0x2f
net/http.(*ServeMux).ServeHTTP(0xc000023ade?, {0x7d0128, 0xc0000a1a40}, 0xc00041fc00)
        /usr/local/go/src/net/http/server.go:2487 +0x149
net/http.serverHandler.ServeHTTP({0xc000387500?}, {0x7d0128, 0xc0000a1a40}, 0xc00041fc00)
        /usr/local/go/src/net/http/server.go:2947 +0x30c
```
