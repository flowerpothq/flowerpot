VERSION ?= dev
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS  = -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)"

export GOFLAGS := -buildvcs=false

.PHONY: build install test test-integ lint clean check

build:
	go build $(LDFLAGS) -o bin/flowerpot ./cmd/flowerpot

test:
	go test ./...

test-integ:
	FLOWERPOT_INTEGRATION=1 go test ./...

lint:
	golangci-lint run

check: build test
	@echo ""
	@echo "=== Smoke test ==="
	@./bin/flowerpot version
	@./bin/flowerpot validate testdata/examples/sql-duckdb-basic/flowerpot.yaml
	@echo ""
	@echo "All checks passed."

clean:
	rm -rf bin/ .flowerpot/
