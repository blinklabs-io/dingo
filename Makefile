# Determine root directory
ROOT_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# Gather all .go files for use in dependencies below. Exclude local git
# worktrees so sibling checkouts do not affect formatting or rebuild inputs.
GO_FILES=$(shell find $(ROOT_DIR) -path '$(ROOT_DIR)/.worktrees' -prune -o -name '*.go' -print)

# Gather list of expected binaries
BINARIES=$(shell cd $(ROOT_DIR)/cmd && ls -1 | grep -v ^common)

# Extract Go module name from go.mod
GOMODULE=$(shell grep ^module $(ROOT_DIR)/go.mod | awk '{ print $$2 }')
TOOLS_BIN=$(ROOT_DIR)/.tools/bin
HOST_OS=$(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH=$(shell uname -m)
PROTOC_VERSION=32.1
PROTOC_OS=$(if $(filter darwin,$(HOST_OS)),osx,$(HOST_OS))
PROTOC_ARCH=$(if $(filter arm64 aarch64,$(HOST_ARCH)),aarch_64,$(if $(filter x86_64 amd64,$(HOST_ARCH)),x86_64,$(error unsupported HOST_ARCH $(HOST_ARCH); supported: x86_64/amd64, arm64/aarch64)))
PROTOC_DIR=$(ROOT_DIR)/.tools/protoc-$(PROTOC_VERSION)-$(PROTOC_OS)-$(PROTOC_ARCH)
PROTOC_ZIP=$(ROOT_DIR)/.tools/protoc-$(PROTOC_VERSION)-$(PROTOC_OS)-$(PROTOC_ARCH).zip
PROTOC=$(PROTOC_DIR)/bin/protoc
PROTOC_SHA256_osx_aarch_64=a7b51b2113862690fa52c62f8891a6037bafb9db88d4f9924c486de9d9bb89d5
PROTOC_SHA256_osx_x86_64=f9caa5b4d0b537acffb0ffd7d53225511a5574ef903fca550ea9e7600987f13b
PROTOC_SHA256_linux_aarch_64=4a802ed23d70f7bad7eb19e5a3e724b3aa967250d572cadfd537c1ba939aee6a
PROTOC_SHA256_linux_x86_64=e9c129c176bb7df02546c4cd6185126ca53c89e7d2f09511e209319704b5dd7e
PROTOC_SHA256=$(PROTOC_SHA256_$(PROTOC_OS)_$(PROTOC_ARCH))

# Set version strings: use env vars if set, else git
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null)
COMMIT_HASH ?= $(shell git rev-parse --short HEAD)
GO_LDFLAGS=-ldflags "-s -w -X '$(GOMODULE)/internal/version.Version=$(VERSION)' -X '$(GOMODULE)/internal/version.CommitHash=$(COMMIT_HASH)'"

.PHONY: all build help mod-tidy clean format golines lint proto test bench test-load test-load-log test-load-profile test-devnet

# Default target
all: format build ## Format and build (default)

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build target
build: $(BINARIES) ## Build the dingo binary

# Builds and installs binary in ~/.local/bin
install: build ## Install binary to ~/.local/bin
	mkdir -p $(HOME)/.local/bin
	mv $(BINARIES) $(HOME)/.local/bin

uninstall: ## Remove installed binary from ~/.local/bin
	rm -f $(HOME)/.local/bin/$(BINARIES)

mod-tidy: ## Run go mod tidy
	# Needed to fetch new dependencies and add them to go.mod
	go mod tidy

clean: ## Remove compiled binaries
	rm -f $(BINARIES)

format: mod-tidy ## Format code and tidy go.mod
	go fmt ./...
	gofmt -s -w $(GO_FILES)

golines: ## Enforce 80-character line limit
	golines -w --ignore-generated --chain-split-dots --max-len=80 --reformat-tags .

lint: ## Run linters (golangci-lint + nilaway + modernize)
	golangci-lint run ./...
	nilaway ./...
	modernize ./...

proto: $(PROTOC) ## Generate Go code from protobuf definitions
	go build -o $(TOOLS_BIN)/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go
	go build -o $(TOOLS_BIN)/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc
	PATH="$(TOOLS_BIN):$$PATH" $(PROTOC) \
		-I $(ROOT_DIR) \
		--go_out=$(ROOT_DIR) \
		--go_opt=module=$(GOMODULE) \
		--go_opt=Mmidnight/proto/midnight_state.proto=$(GOMODULE)/midnight \
		--go-grpc_out=$(ROOT_DIR) \
		--go-grpc_opt=module=$(GOMODULE) \
		--go-grpc_opt=Mmidnight/proto/midnight_state.proto=$(GOMODULE)/midnight \
		$(ROOT_DIR)/midnight/proto/midnight_state.proto

$(PROTOC):
	mkdir -p $(TOOLS_BIN) $(PROTOC_DIR)
	test -n "$(PROTOC_SHA256)"
	curl -fL -o $(PROTOC_ZIP) https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_OS)-$(PROTOC_ARCH).zip
	if command -v sha256sum >/dev/null 2>&1; then \
		printf '%s  %s\n' "$(PROTOC_SHA256)" "$(PROTOC_ZIP)" | sha256sum -c -; \
	else \
		printf '%s  %s\n' "$(PROTOC_SHA256)" "$(PROTOC_ZIP)" | shasum -a 256 -c -; \
	fi
	unzip -q -o $(PROTOC_ZIP) -d $(PROTOC_DIR)

test: mod-tidy ## Run tests with race detection
	go test -v -race ./...

bench: mod-tidy ## Run benchmarks
	go test -run=^$$ -bench=. -benchmem ./...

test-load: build ## Load test data into a fresh database
	rm -rf .dingo
	./dingo load database/immutable/testdata

test-load-log: build ## Load test data and capture log output
	rm -rf .dingo dingo.log
	./dingo load database/immutable/testdata 2>&1 | tee dingo.log

test-load-profile: build ## Load test data with CPU/memory profiling
	rm -rf .dingo
	./dingo --cpuprofile=cpu.prof --memprofile=mem.prof load database/immutable/testdata
	@echo "Profiling complete. Run 'go tool pprof cpu.prof' or 'go tool pprof mem.prof' to analyze"

test-devnet: ## Run devnet integration tests
	./internal/test/devnet/run-tests.sh

# Build our program binaries
# Depends on GO_FILES to determine when rebuild is needed
$(BINARIES): mod-tidy $(GO_FILES)
	CGO_ENABLED=0 \
	go build \
		$(GO_LDFLAGS) \
		-o $(@)$(if $(filter windows,$(GOOS)),.exe,)  \
		./cmd/$(@)
