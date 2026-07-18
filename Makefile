GO ?= go
BIN := bin/lasd
PKGS := ./...
RUNTIME_IMAGE ?= lasd/sandbox-runtime:dev
VERSION ?= dev
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test race vet fmt lint tidy integration runtime-image clean

build:
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/lasd

test:
	$(GO) test $(PKGS)
	cd runtime && $(GO) test ./...

race:
	$(GO) test -race ./internal/store/... ./internal/kubefacade/... ./internal/reconciler/... ./internal/router/...

vet:
	$(GO) vet $(PKGS)
	cd runtime && $(GO) vet ./...

fmt:
	gofmt -w internal/ cmd/ test/ runtime/

tidy:
	$(GO) mod tidy

lint: fmt vet

# Docker-backed integration + E2E tests (opt in). -p 1 serializes the test
# packages: they share one Docker daemon and their cleanups sweep las.managed
# containers, so parallel package binaries interfere.
integration:
	LASD_DOCKER_TESTS=1 $(GO) test -count=1 -timeout 20m -p 1 ./internal/driver/... ./internal/reconciler/... ./internal/portforward/... ./test/e2e/...

# Build the bundled runtime image directly (lasd also builds it on demand).
runtime-image:
	docker build -t $(RUNTIME_IMAGE) runtime

clean:
	rm -rf bin dist
