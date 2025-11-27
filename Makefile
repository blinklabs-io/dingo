# Determine root directory
ROOT_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# Gather all .go files for use in dependencies below
GO_FILES=$(shell find $(ROOT_DIR) -name '*.go')

# Gather list of expected binaries
BINARIES=$(shell cd $(ROOT_DIR)/cmd && ls -1 | grep -v ^common)

# Extract Go module name from go.mod
GOMODULE=$(shell grep ^module $(ROOT_DIR)/go.mod | awk '{ print $$2 }')

# Set version strings: use env vars if set, else git
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null)
COMMIT_HASH ?= $(shell git rev-parse --short HEAD)
GO_LDFLAGS=-ldflags "-s -w -X '$(GOMODULE)/internal/version.Version=$(VERSION)' -X '$(GOMODULE)/internal/version.CommitHash=$(COMMIT_HASH)'"

.PHONY: all build mod-tidy clean format golines test bench test-load-profile

# Default target
all: format golines build test

# Build target
build: $(BINARIES)

# Builds and installs binary in ~/.local/bin
install: build
	mkdir -p $(HOME)/.local/bin
	mv $(BINARIES) $(HOME)/.local/bin

uninstall:
	rm -f $(HOME)/.local/bin/$(BINARIES)

mod-tidy:
	# Needed to fetch new dependencies and add them to go.mod
	go mod tidy

clean:
	rm -f $(BINARIES)

format:
	go fmt ./...
	gofmt -s -w $(GO_FILES)

golines:
	golines -w --ignore-generated --chain-split-dots --max-len=80 --reformat-tags .

test: mod-tidy
	go test -v -race ./...

bench: mod-tidy
	go test -run=^$$ -bench=. -benchmem ./...

test-load:
	rm -rf .dingo
	go run ./cmd/dingo load database/immutable/testdata

test-load-profile:
	rm -rf .dingo dingo
	go build -o dingo ./cmd/dingo
	./dingo --cpuprofile=cpu.prof --memprofile=mem.prof load database/immutable/testdata
	@echo "Profiling complete. Run 'go tool pprof cpu.prof' or 'go tool pprof mem.prof' to analyze"

# Build our program binaries
# Depends on GO_FILES to determine when rebuild is needed
$(BINARIES): mod-tidy $(GO_FILES)
	CGO_ENABLED=0 \
	go build \
		$(GO_LDFLAGS) \
		-o $(@) \
		./cmd/$(@)
