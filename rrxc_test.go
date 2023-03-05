// Yes, these tests are useless. Proper unit testing is on the todo list...
// TODO Proper unit testing
package rrxc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/sa6mwa/rrxc"
)

func TestExchangeFromContext(t *testing.T) {
	c := rrxc.NewController()
	defer c.Close()
	ctx := c.NewExchangeContext(context.Background())

	c.Tag("hello", "something")

	xc, err := rrxc.ExchangeFromContext(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer xc.Close()

	ch := make(chan rrxc.RequestResponse)
	go func() {
		requestResponse := <-ch
		if requestResponse != (rrxc.RequestResponse{}) {
			t.Logf("correlID: %v\n\trequest: %v\n\tresponse: %v\n\tlatency: %s\n", requestResponse.CorrelID, requestResponse.Request, requestResponse.Response, requestResponse.Latency)
		} else {
			t.Log("empty requestResponse struct")
		}
	}()

	go func() {
		select {
		case <-xc.Done():
			t.Logf("%+v\n", xc.GetExchangeResult())
		case <-ctx.Done():
			t.Log("context timed out")
		}
	}()

	id := xc.NewCorrelID()

	if err := xc.RegisterRequest(&rrxc.Registration{
		CorrelID:         id,
		Message:          "hello world",
		NotifyOnResponse: []chan rrxc.RequestResponse{ch},
	}); err != nil {
		log.Fatal(err)
	}
	//time.Sleep(100 * time.Millisecond)

	if err := xc.RegisterResponse(&rrxc.Registration{
		CorrelID: id,
		Message:  "yes, hello there",
	}); err != nil {
		log.Fatal(err)
	}

	//time.Sleep(100 * time.Millisecond)
}

func TestController_Synchronize(t *testing.T) {
	c := rrxc.NewController()
	intermediateCTX, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx := c.NewControllerContext(intermediateCTX)

	ready := make(chan struct{})

	go func() {
		<-ready
		controller, err := rrxc.ControllerFromContext(ctx)
		if err != nil {
			t.Log(err)
			return
		}
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("abc%d", i)
			if err := controller.RegisterResponse(&rrxc.Registration{
				CorrelID: id,
				Message:  "Hi there, " + id,
			}); err != nil {
				t.Log(err)
			}
		}
	}()

	xcresult, err := c.Synchronize(c.NewExchangeContext(ctx), func(sb rrxc.SyncBundle) error {
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("abc%d", i)
			//id := sb.Exchange.NewCorrelID()

			if err := sb.Exchange.RegisterRequest(&rrxc.Registration{
				CorrelID: id,
				Message:  "hello world",
			}); err != nil {
				t.Fatal(err)
			}

		}
		close(ready)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	j, err := json.MarshalIndent(xcresult, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(j))
}

func TestController_Wait(t *testing.T) {
	intermediateCTX, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ctx := rrxc.NewControllerContext(intermediateCTX)

	c, err := rrxc.ControllerFromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ready := make(chan struct{})

	go func() {
		<-ready
		controller, err := rrxc.ControllerFromContext(ctx)
		if err != nil {
			t.Log(err)
			return
		}
		for i := 0; i < 99; i++ {
			id := fmt.Sprintf("abc%d", i)
			if err := controller.RegisterResponse(&rrxc.Registration{
				CorrelID: id,
				Message:  "Hello there, " + id,
			}); err != nil {
				t.Log(err)
			}
		}
	}()

	ctx = c.NewExchangeContext(ctx)

	// xc, err := rrxc.ExchangeFromContext(ctx)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("abc%d", i)
		//id := xc.NewCorrelID()
		//if err := xc.RegisterRequest(id, "hello world"); err != nil {
		if err := rrxc.RegisterRequestByContext(ctx, &rrxc.Registration{
			CorrelID: id,
			Message:  "hello world",
		}); err != nil {
			t.Fatal(err)
		}
	}
	close(ready)

	xcresult, err := rrxc.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if xcresult.AllRequestsRespondedTo {
		t.Fatal("expected that not all requests were responded to, but results says they were")
	}

	// As the context has timed out, all goroutines waiting for ctx.Done() ran in
	// parallel which means the tag-for-rollback code can run after the check
	// below, so sleep a bit to let the tag be created.
	time.Sleep(10 * time.Millisecond)

	taggedForRollback := "abc99"
	if !c.HasTag(taggedForRollback, "rollback") {
		t.Fatalf("%s not tagged for rollback", taggedForRollback)
	}

	if err := rrxc.CloseExchangeByContext(ctx); err != nil {
		t.Fatal(err)
	}

	j, err := json.MarshalIndent(xcresult, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(j))

	c.Close()

	// Wait just enough to let one of the internal goroutines to exit.
	// NumGoRoutine should be down to 2.
	time.Sleep(10 * time.Millisecond)

	t.Log("goroutines:", runtime.NumGoroutine())

	if runtime.NumGoroutine() != 2 {
		t.Errorf("runtime.NumGoroutine != 2")
	}
}

func TestController_SetRollbackTag(t *testing.T) {
	want := "hello world"
	controller := rrxc.NewController()
	controller.SetRollbackTag(want)
	if tag := controller.GetRollbackTag(); tag != want {
		t.Errorf("got %q, expected %q", tag, want)
	}
}

func TestController_GetRollbackTag(t *testing.T) {
	want := "rollback"
	controller := rrxc.NewController()
	if tag := controller.GetRollbackTag(); tag != want {
		t.Errorf("got %q, expected %q", tag, want)
	}
}

func TestController_NewCorrelID(t *testing.T) {
	exchanges := 10
	requestsPerExchange := 100
	correlIDsToGenerate := 100
	controller := rrxc.NewController()
	initialGoroutines := runtime.NumGoroutine()
	for i := 0; i < exchanges; i++ {
		ctx := controller.NewExchangeContext(context.Background())
		xc, err := rrxc.ExchangeFromContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for ii := 0; ii < requestsPerExchange; ii++ {
			if err := xc.RegisterRequest(&rrxc.Registration{
				CorrelID: xc.NewCorrelID(),
				Message:  "hello world",
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	cids := make(map[string]interface{})
	for i := 0; i < correlIDsToGenerate; i++ {
		cid := controller.NewCorrelID()
		if cids[cid] != nil {
			t.Errorf("correlID %q already exist", cid)
		}
		cids[cid] = struct{}{}
	}
	gr := runtime.NumGoroutine()
	if initialGoroutines+exchanges != gr {
		t.Errorf("initial goroutine count was %d, now it is %d", initialGoroutines, gr)
	}
}

func TestExchange_NewCorrelID(t *testing.T) {
	exchanges := 10
	requestsPerExchange := 100
	correlIDsToGenerate := 100
	controller := rrxc.NewController()
	initialGoroutines := runtime.NumGoroutine()
	var firstContext context.Context
	for i := 0; i < exchanges; i++ {
		ctx := controller.NewExchangeContext(context.Background())
		if firstContext == nil {
			firstContext = ctx
		}
		xc, err := rrxc.ExchangeFromContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for ii := 0; ii < requestsPerExchange; ii++ {
			if err := xc.RegisterRequest(&rrxc.Registration{
				CorrelID: xc.NewCorrelID(),
				Message:  "hello world",
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	xc, err := rrxc.ExchangeFromContext(firstContext)
	if err != nil {
		t.Fatal(err)
	}
	cids := make(map[string]interface{})
	for i := 0; i < correlIDsToGenerate; i++ {
		cid := xc.NewCorrelID()
		if cids[cid] != nil {
			t.Errorf("correlID %q already exist", cid)
		}
		cids[cid] = struct{}{}
	}
	gr := runtime.NumGoroutine()
	if initialGoroutines+exchanges != gr {
		t.Errorf("initial goroutine count was %d, now it is %d", initialGoroutines, gr)
	}
}

func TestController_HasRequest(t *testing.T) {
	correlIDsToGenerate := 10
	controller := rrxc.NewController()
	ctx := controller.NewExchangeContext(context.Background())
	cids := make(map[string]bool)
	for i := 0; i < correlIDsToGenerate; i++ {
		cid := controller.NewCorrelID()
		if cids[cid] {
			t.Errorf("correlID %q already exist", cid)
		}
		cids[cid] = true
	}
	for cid, _ := range cids {
		if err := rrxc.RegisterRequestByContext(ctx, &rrxc.Registration{
			CorrelID: cid,
			Message:  "hello world",
		}); err != nil {
			t.Error(err)
		}
	}
	for cid, _ := range cids {
		if !controller.HasRequest(cid) {
			t.Errorf("request with correlID %q does not exist in exchange", cid)
		}
	}
}

func TestController_GetRequestAge(t *testing.T) {
	correlIDsToGenerate := 10
	pauseDuration := 5 * time.Millisecond
	controller := rrxc.NewController()
	ctx := controller.NewExchangeContext(context.Background())
	cids := make(map[string]bool)
	for i := 0; i < correlIDsToGenerate; i++ {
		cid := controller.NewCorrelID()
		if cids[cid] {
			t.Errorf("correlID %q already exist", cid)
		}
		cids[cid] = true
	}
	for cid, _ := range cids {
		if err := rrxc.RegisterRequestByContext(ctx, &rrxc.Registration{
			CorrelID: cid,
			Message:  "hello world",
		}); err != nil {
			t.Error(err)
		}
	}
	time.Sleep(pauseDuration)
	for cid, _ := range cids {
		d, err := controller.GetRequestAge(cid)
		if err != nil {
			t.Fatal(err)
		}
		if d < pauseDuration {
			t.Errorf("reqeust with correlID %q is %s old which is younger than %s", cid, d, pauseDuration)
		}
	}
}

func FuzzController_RegisterResponse(f *testing.F) {
	controller := rrxc.NewController()
	ctx := controller.NewExchangeContext(context.Background())

	if err := rrxc.RegisterRequestByContext(ctx, &rrxc.Registration{
		CorrelID: "0123456789",
		Message:  "hello world",
	}); err != nil {
		f.Fatal(err)
	}

	maxAllowedGoroutines := 50

	f.Add("abcdefghijklmnopqrstuvwxyz0123456789")
	f.Fuzz(func(t *testing.T, a string) {
		exchange, err := rrxc.ExchangeFromContext(ctx)
		if err != nil {
			t.Error(err)
		}
		if err := exchange.RegisterResponse(&rrxc.Registration{
			CorrelID: a,
			Message:  "this is my response",
		}); err != nil {
			switch {
			case errors.Is(err, rrxc.ErrNoSuchRequest), errors.Is(err, rrxc.ErrHaveNoCorrelatedRequest):
			default:
				t.Error(err)
			}
		}
		gr := runtime.NumGoroutine()
		if gr > maxAllowedGoroutines {
			t.Errorf("runtime.NumGoroutine == %d, more than limit of %d", gr, maxAllowedGoroutines)
		}
	})
}
