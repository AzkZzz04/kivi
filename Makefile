.PHONY: all build test clean lint vet bench

all: test

build:
	go build ./...

test:
	go test -race -count=1 ./...

bench:
	go test -bench=. -benchmem ./...

lint:
	golangci-lint run

vet:
	go vet ./...

clean:
	go clean ./...
	rm -rf runs/

# Phase 0 specific
test-unit:
	go test -v ./internal/metrics ./internal/testutil

