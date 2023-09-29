.PHONY: test govulncheck capslock

all: govulncheck test


test:
	go test ./...
	CGO_ENABLED=1 go test -race -v ./...

govulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

capslock:
	go run github.com/google/capslock/cmd/capslock@latest
