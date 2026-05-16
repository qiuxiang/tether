.PHONY: build test fmt

build:
	go build -o tether ./

test:
	go test ./...

fmt:
	gofmt -w .
