# rrxc

Package rrxc is a Request/Response eXchange Controller for synchronizing an
operation against remote asynchronous backends (AKA sync/async or
sync-over-async). The package does not exclusively support sync-over-async as an
exchange in rrxc can very well be fully asynchronous, why it was named a
controller - rrxc correlates requests with responses.

This package is currently very much a MVP, examples and proper unit testing are
in the backlog.

See [rrxc_test.go](rrxc_test.go) for a crude example.


```
ok      github.com/sa6mwa/rrxc  0.713s  coverage: 57.3% of statements
ok      github.com/sa6mwa/rrxc/pkg/anystore     0.003s  coverage: 63.0% of statements


Request/Response eXchange Controller handles sync-over-async as well as fully
asynchronous patterns correlating requests with responses.
rrxc.go:196:11: Error return value of `axc.Run` is not checked (errcheck)
			axc.Run(func(a anystore.AnyStore) error {
			       ^
rrxc.go:215:17: Error return value of `c.Untag` is not checked (errcheck)
									c.Untag(correlID, "rollback")
									       ^
rrxc.go:272:17: Error return value of `c.mapOfMaps.Run` is not checked (errcheck)
	c.mapOfMaps.Run(func(mm anystore.AnyStore) error {
	               ^
rrxc.go:352:11: Error return value of `rand.Read` is not checked (errcheck)
	rand.Read(b) // Care not about errors
	         ^
rrxc.go:372:11: Error return value of `rand.Read` is not checked (errcheck)
	rand.Read(b)
	         ^
rrxc.go:580:7: Error return value of `x.Run` is not checked (errcheck)
	x.Run(func(a anystore.AnyStore) error {
	     ^
rrxc.go:99:2: field `durable` is unused (unused)
	durable   bool // Not implemented yet
	^
pkg/anystore/anystore.go:119:10: S1005: unnecessary assignment to the blank identifier (gosimple)
		for k, _ := range kv {
```		       ^
