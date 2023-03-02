// Yes, these tests are useless. Proper unit testing is on the todo list...
// TODO Proper unit testing
package rrxc_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/sa6mwa/rrxc"
)

func TestExchangeFromContext(t *testing.T) {
	c := rrxc.NewController()
	ctx := c.NewExchangeContext(context.Background())

	c.Tag("hello", "something")

	xc, err := rrxc.ExchangeFromContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

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

	xc.Close()

	time.Sleep(500 * time.Millisecond)
}

func TestControllerSync(t *testing.T) {
	c := rrxc.NewController()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	xcresult, err := c.Synchronize(c.NewExchangeContext(ctx), func(sb rrxc.SyncBundle) error {
		for i := 0; i < 100; i++ {
			id := sb.Exchange.NewCorrelID()
			if err := sb.Exchange.RegisterRequest(id, "hello world"); err != nil {
				t.Fatal(err)
			}

			if err := sb.Exchange.RegisterResponse(id, "hello there", true); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v\n", xcresult)

}
