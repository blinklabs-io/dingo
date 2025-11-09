# Determine root directory
ROOT_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# Gather all .go files for use in dependencies below
GO_FILES=$(shell find $(ROOT_DIR) -name '*.go')

# Gather list of expected binaries
BINARIES=$(shell cd $(ROOT_DIR)/cmd && ls -1 | grep -v ^common)

# Extract Go module name from go.mod
GOMODULE=$(shell grep ^module $(ROOT_DIR)/go.mod | awk '{ print $$2 }')

# Set version strings based on git tag and current ref
GO_LDFLAGS=-ldflags "-s -w -X '$(GOMODULE)/internal/version.Version=$(shell git describe --tags --exact-match 2>/dev/null)' -X '$(GOMODULE)/internal/version.CommitHash=$(shell git rev-parse --short HEAD)'"

.PHONY: build mod-tidy clean format golines test

# Alias for building program binary
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
	golines -w --ignore-generated --chain-split-dots --max-len=80 --reformat-tags --shorten-comments .

test: mod-tidy
	go test -v -race ./...

test-load:
	rm -rf .dingo
	go run ./cmd/dingo load database/immutable/testdata

# Build our program binaries
# Depends on GO_FILES to determine when rebuild is needed
$(BINARIES): mod-tidy $(GO_FILES)
	CGO_ENABLED=0 \
	go build \
		$(GO_LDFLAGS) \
		-o $(@) \
		./cmd/$(@)
