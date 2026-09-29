.PHONY: fmt fmt-check test race coverage vet build check fuzz bench vuln lint

VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)
FUZZTIME ?= 10s
COVERAGE_MIN ?= 90

fmt:
	gofmt -w ./cmd ./internal

fmt-check:
	@test -z "$$(gofmt -l ./cmd ./internal)" || (gofmt -l ./cmd ./internal; exit 1)

test:
	go test -timeout 3m ./...

race:
	go test -race -timeout 3m ./...

coverage:
	go test -race -timeout 3m -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@go tool cover -func=coverage.out | awk -v minimum=$(COVERAGE_MIN) '/^total:/ { sub(/%/, "", $$3); if ($$3+0 < minimum) { print "Coverage below " minimum "%"; exit 1 } found=1 } END { if (!found) exit 1 }'

vet:
	go vet ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; running go vet"; \
		go vet ./...; \
	fi

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o storage-cli ./cmd/storage-cli

# Verification must not silently rewrite source files.
check: fmt-check vet coverage build

fuzz:
	go test ./internal/storage -run '^$$' -fuzz '^FuzzObjectPath$$' -fuzztime $(FUZZTIME) -parallel 2
	go test ./internal/storage -run '^$$' -fuzz '^FuzzTempName$$' -fuzztime $(FUZZTIME) -parallel 2
	go test ./cmd/storage-cli -run '^$$' -fuzz '^FuzzParsePairs$$' -fuzztime $(FUZZTIME) -parallel 2

bench:
	go test ./internal/storage -run '^$$' -bench . -benchmem

# Downloads the pinned analysis tool; production dependencies stay standard-library-only.
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
