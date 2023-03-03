// Yes, these tests are useless. Proper unit testing is on the todo list...
// TODO Proper unit testing
package rrxc_test

import (
	"context"
	"encoding/json"
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

	if err := xc.RegisterRequest(id, "hello world", ch); err != nil {
		log.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := xc.RegisterResponse(id, "yes, hello there"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
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
			if err := controller.RegisterResponse(id, "Hi there, "+id); err != nil {
				t.Log(err)
			}
		}
	}()

	xcresult, err := c.Synchronize(c.NewExchangeContext(ctx), func(sb rrxc.SyncBundle) error {
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("abc%d", i)
			//id := sb.Exchange.NewCorrelID()
			if err := sb.Exchange.RegisterRequest(id, "hello world"); err != nil {
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
	intermediateCTX, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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
			if err := controller.RegisterResponse(id, "Hello there, "+id); err != nil {
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
		if err := c.RegisterRequestByContext(ctx, id, "hello world"); err != nil {
			t.Fatal(err)
		}
	}
	close(ready)

	xcresult, err := c.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if xcresult.AllRequestsRespondedTo {
		t.Fatal("expected that not all requests were responded to, but results says they were")
	}

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

	time.Sleep(200 * time.Millisecond)

	t.Log("goroutines:", runtime.NumGoroutine())
}
