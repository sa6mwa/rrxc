package anystore_test

import (
	"os"
	"testing"

	"github.com/sa6mwa/rrxc/pkg/anystore"
)

func TestIsUnixTerminal(t *testing.T) {
	if anystore.IsUnixTerminal(os.Stdin) {
		t.Fatal("go test is not a terminal")
	}
}
