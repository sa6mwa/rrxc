package anystore_test

import (
	"errors"
	"testing"

	"github.com/sa6mwa/rrxc/pkg/anystore"
)

func TestRun(t *testing.T) {
	errTesting := errors.New("this error")
	a := anystore.NewAnyStore()
	a.Store("hello", "world")
	err := a.Run(func(as anystore.AnyStore) error {
		if !as.HasKey("hello") {
			t.Error("expected key not found in store")
		}
		as.Store(struct{}{}, "okilidokili")
		val, ok := as.Load(struct{}{}).(string)
		if !ok {
			t.Fatalf("expected key \"struct{}{}\" with value of type string not found in store")
		}
		if val != "okilidokili" {
			t.Errorf("expected okilidokili, but got %q", val)
		}
		if as.Len() != 2 {
			t.Errorf("expected Len() == %q, got %q", 2, as.Len())
		}
		as.Store(struct{}{}, "completely")
		as.Store(struct{}{}, "different")
		as.Delete(struct{}{})
		if as.Len() != 1 {
			t.Errorf("expected Len() == %q, got %q", 2, as.Len())
		}
		as.Store(struct{}{}, "okilidokili")
		return errTesting
	})
	if err != errTesting {
		t.Errorf("expected error %v, but got %v", errTesting, err)
	}
	o := a.Load(struct{}{})
	expected := "okilidokili"
	if o != expected {
		t.Fatalf("expected key %q with value %q not found in store", "struct{}{}", expected)
	}
	nilVal := a.Load("keyNotPresent")
	if nilVal != nil {
		t.Errorf("expected nil, but got %T", nilVal)
	}
}
