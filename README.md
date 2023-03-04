# rrxc

Package rrxc is a Request/Response eXchange Controller for synchronizing an
operation against remote asynchronous backends (AKA sync/async or
sync-over-async). The package does not exclusively support sync-over-async as an
exchange in rrxc can very well be fully asynchronous, why it was named a
controller - rrxc correlates requests with responses.

This package is currently very much a MVP, examples and proper unit testing are
in the backlog.

See [rrxc_test.go](rrxc_test.go) for crude examples.

## Design

A new controller is instantiated via `ctrl := rrxc.NewController()` in the
`main` function. In each call to the server handler that is to make asynchronous
requests against remote system(s), a new exchange is initiated and attached to a
`context.Context` (preferably with a timeout). The server handler initiates a
new exchange by calling `ctx = ctrl.NewExchangeContext(ctx)`. A new *correlation
identifier* or `correlID` needs to be requested using, for example, `correlID :=
ctrl.NewCorrelID()`. Requests are registered in the exchange using one of the
registration functions, e.g `rrxc.RegisterRequestByContext(ctx, correlID, "my
message")`. If you are making more asynchronous requests in this exchange, you
need to fetch another `correlID` using `ctrl.NewCorrelID()` and register the
request the same way (via the controller or the exchange receiver function;
`RegisterRequestByContext`, `RegisterRequest`). In the other server goroutine
(perhaps a messaging queue consumer) you register responses via either the
controller or the exchange interface stored in the context using
`ExchangeFromContext(ctx)`. A response can simply be registered via `err :=
ctrl.RegisterResponse(id, "my response")` and the controller will figure out
which exchange the response should be registered in. In the requesting server
handler function `ctrl.Wait()` can be used to wait until all responses have
arrived. An alternative approach is to wrap everything in the `ctrl.Synchronize`
function which will not exit until all requests are done or the context is
cancelled (timed out). The results are returned by both `Synchronize` and `Wait`
as an `ExchangeResult` struct. If any of the requests fail, the whole exchange
will fail and could potentially be handled as a transaction to rollback. The
controller keeps a map of *tags*. If the exchange is closed or the context is
cancelled and has requests without responses, each `correlID` is tagged with the
default rollback tag (`rollback`). These tags can be looked up in, for example,
the message consumer handler if the remote system is setup to requeue
un-acknowledged messages. That way you can handle the message and acknowledge
it. The rollback tags are automatically removed from the tag map after
`defaultRollbackTagLifespan` which is `3 * time.Hour` or
`ctrl.SetRollbackTagLifespan(duration)`.

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
 a.RegisterRequestByContext(ca,...)   b.RegisterRequestByContext(cb,...)
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
