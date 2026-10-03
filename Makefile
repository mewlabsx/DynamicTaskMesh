.PHONY: test build fuzz verify

FUZZTIME ?= 5s

test:
	go test ./...

build:
	go build ./cmd/...

fuzz:
	go test ./internal/mesh/protocol -fuzz=FuzzDecodePresence -fuzztime=$(FUZZTIME)
	go test ./internal/mesh/handshake -fuzz=FuzzHandshakeRequest -fuzztime=$(FUZZTIME)
	go test ./internal/mesh/membership -fuzz=FuzzObserveAndTick -fuzztime=$(FUZZTIME)
	go test ./internal/mesh/resourceview -fuzz=FuzzResourceAdvertisement -fuzztime=$(FUZZTIME)
	go test ./internal/task -fuzz=FuzzNewTask -fuzztime=$(FUZZTIME)
	go test ./internal/transport/grpcapi -fuzz=FuzzNativeInvocationRequest -fuzztime=$(FUZZTIME)

verify:
	go test ./...
	go build ./cmd/...
	go vet ./...
	$(MAKE) fuzz
