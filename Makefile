# Determine root directory
ROOT_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# Gather all .go files for use in dependencies below. Exclude local git
# worktrees so sibling checkouts do not affect formatting or rebuild inputs.
GO_FILES=$(shell find $(ROOT_DIR) -path '$(ROOT_DIR)/.worktrees' -prune -o -name '*.go' -print)

# Gather every Go module directory. Nested modules have their own go.mod and
# are therefore outside the root module's ./..., so they need their own run.
GO_MODULE_DIRS=$(shell find $(ROOT_DIR) -path '$(ROOT_DIR)/.worktrees' -prune -o -path '$(ROOT_DIR)/.tools' -prune -o -name go.mod -print | xargs -n1 dirname)

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
SQLC_VERSION=v1.31.1
SQLC=go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
# The scanner floats along with the advisory database it reads; a pin parks a
# new advisory behind a stale version instead of forcing it to be fixed.
GOVULNCHECK=go run golang.org/x/vuln/cmd/govulncheck@latest
PROTOC_SHA256_osx_aarch_64=a7b51b2113862690fa52c62f8891a6037bafb9db88d4f9924c486de9d9bb89d5
PROTOC_SHA256_osx_x86_64=f9caa5b4d0b537acffb0ffd7d53225511a5574ef903fca550ea9e7600987f13b
PROTOC_SHA256_linux_aarch_64=4a802ed23d70f7bad7eb19e5a3e724b3aa967250d572cadfd537c1ba939aee6a
PROTOC_SHA256_linux_x86_64=e9c129c176bb7df02546c4cd6185126ca53c89e7d2f09511e209319704b5dd7e
PROTOC_SHA256=$(PROTOC_SHA256_$(PROTOC_OS)_$(PROTOC_ARCH))

# Set version strings: use env vars if set, else git
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null)
# Pin the abbreviation to 7 chars so the stamped CommitHash is deterministic and
# matches the Homebrew formula bump (which slices the first 7 of the full SHA).
COMMIT_HASH ?= $(shell git rev-parse --short=7 HEAD)
GO_LDFLAGS=-ldflags "-s -w -X '$(GOMODULE)/internal/version.Version=$(VERSION)' -X '$(GOMODULE)/internal/version.CommitHash=$(COMMIT_HASH)'"
BUILD_TAGS ?= dingo_extra_plugins
GO_TAG_FLAGS=$(if $(strip $(BUILD_TAGS)),-tags "$(BUILD_TAGS)",)
# Generated sqlc and protobuf packages are validated by their generators;
# run modernize only against hand-written packages to avoid generator drift.
MODERNIZE_PACKAGES=$(shell go list $(GO_TAG_FLAGS) -f '{{if .GoFiles}}{{.ImportPath}}{{end}}' ./... | grep -Ev '/database/plugin/(blob/(aws|gcs)|metadata/(mysql|postgres)|metadata/sqlstore/internal/query/(mysql|postgres|sqlite))$$|/midnight$$')

.PHONY: all build help install uninstall mod-tidy clean format golines lint import-boundaries docs-parity proto sql sql-check govulncheck test test-live-lifecycle bench bench-ci bench-mempool bench-mempool-normal bench-mempool-degenerate bench-mempool-revalidation test-load test-load-log test-load-profile test-devnet

# Default target
all: format build ## Format and build (default)

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build target
build: $(BINARIES) ## Run mod-tidy, then build every command binary

# Builds and installs binary in ~/.local/bin
install: build ## Run build, then install the binaries to ~/.local/bin
	mkdir -p $(HOME)/.local/bin
	mv $(BINARIES) $(HOME)/.local/bin

uninstall: ## Remove installed binaries from ~/.local/bin
	rm -f $(addprefix $(HOME)/.local/bin/,$(BINARIES))

mod-tidy: ## Run go mod tidy
	# Needed to fetch new dependencies and add them to go.mod
	go mod tidy

clean: ## Remove compiled binaries
	rm -f $(BINARIES)

format: mod-tidy ## Run mod-tidy, then format code
	go fmt ./...
	gofmt -s -w $(GO_FILES)

golines: ## Enforce 80-character line limit
	golines -w --ignore-generated --chain-split-dots --max-len=80 --reformat-tags .

# golangci-lint covers one module for one GOOS per run. The loop reaches every
# nested module, and the GOOS=windows run reaches files behind
# `//go:build windows`, which the host build excludes. CI runs the same scopes
# in .github/workflows/golangci-lint.yml.
lint: import-boundaries ## Run import-boundaries, golangci-lint, nilaway, and modernize
	@for dir in $(GO_MODULE_DIRS); do \
		echo "golangci-lint run ./... ($$dir)"; \
		(cd $$dir && golangci-lint run ./...) || exit 1; \
	done
	GOOS=windows golangci-lint run ./...
	nilaway $(GO_TAG_FLAGS) ./...
	modernize $(GO_TAG_FLAGS) $(MODERNIZE_PACKAGES)

import-boundaries: ## Check reviewed package import boundaries
	go test ./internal/architecture

docs-parity: ## Check docs against go.mod, the Makefile, and the DevNet compose file
	go test ./internal/docsparity

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

sql: ## Generate typed database/sql queries with pinned sqlc
	$(SQLC) generate

sql-check: sql ## Run sql, then fail when checked-in sqlc output is stale
	git diff --exit-code -- database/plugin/metadata/sqlstore/internal/query

govulncheck: ## Fail on known vulnerabilities reachable from source, including the Go toolchain/stdlib
	$(GOVULNCHECK) $(GO_TAG_FLAGS) ./...

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

test: mod-tidy ## Run mod-tidy, then all tests with race detection
	go test $(GO_TAG_FLAGS) -v -race -timeout 20m ./...

test-live-lifecycle: ## Run the live two-node lifecycle integration tests with race detection
	go test -tags "$(BUILD_TAGS) dingo_db_integration" -v -race -timeout 20m -count=1 -run '^TestLive.*UnderRealForgingAndNetworking$$' .

bench: mod-tidy ## Run mod-tidy, then benchmarks
	go test $(GO_TAG_FLAGS) -run=^$$ -bench=. -benchmem ./...

bench-ci: mod-tidy ## Run mod-tidy, then the curated CI benchmark suite (count=10) plus a GOMAXPROCS lock-contention sweep
	go test $(GO_TAG_FLAGS) -run=^$$ -bench='^Benchmark(BlockProcessingThroughput|BlockProcessingThroughputPredecoded|BlockBatchProcessingThroughput|RawBlockBatchProcessingThroughput|VerifyBlockHeader|TransactionValidation|ChainSyncFromGenesis|RealBlockProcessing|EraTransitionPerformanceRealData|TestLoad|BlockfetchNearTipThroughput|BlockfetchNearTipThroughputPredecoded|BlockfetchNearTipFlushOnlyPredecoded|BlockfetchNearTipQueuedHeaderPredecoded|BlockfetchVerifiedHeaderDispatch|BlockfetchClientBlockMetrics|UpdateConnectionMetrics|HasInboundPeerAddress|Reconcile|PublishSubscribers|BlockMemoryUsage|HotCacheGet|HotCachePut|HotCacheGetMiss|BlockLRUCacheGet|BlockLRUCachePut|TieredCacheHotHit|CachedBlockExtract|CborOffsetEncode|CborOffsetDecode|StorageModeIngest|StorageModeIngestSteadyState)$$' -benchmem -count=10 -timeout=90m ./...
	go test $(GO_TAG_FLAGS) -run=^$$ -bench='^Benchmark(BlockLRUParallelReadHeavy|BlockLRUParallelBalanced|BlockLRUParallelReadOnly|HotCacheParallelGet|TryReserveInboundSlotParallel|ConcurrentQueries|TipSnapshotReadOnly|TipSnapshotReadUnderWriter)$$' -benchmem -count=10 -cpu=1,4,8,16 -timeout=30m ./...

bench-mempool-revalidation: ## Benchmark FIFO admission during normal and degenerate rebuilds
	go test $(GO_TAG_FLAGS) -run=^$$ -bench='^BenchmarkFIFO(AdmissionNoRevalidation|Revalidation)$$' -benchmem ./mempool

bench-mempool: ## Compare FIFO and DAG mempool providers under concurrent load
	go test $(GO_TAG_FLAGS) -run=^$$ -bench=^BenchmarkMempoolPlugins -benchmem ./mempool

bench-mempool-normal: ## Compare FIFO and DAG under the normal load matrix
	go test $(GO_TAG_FLAGS) -run=^$$ -bench=^BenchmarkMempoolPlugins$$ -benchmem ./mempool

bench-mempool-degenerate: ## Compare FIFO and DAG under degenerate workloads
	go test $(GO_TAG_FLAGS) -run=^$$ -bench=^BenchmarkMempoolPluginsDegenerate$$ -benchmem ./mempool

test-load: build ## Run build, then load test data into a fresh database
	rm -rf .dingo
	./internal/test/load/run-tests.sh

test-load-log: build ## Run build, then load test data and capture log output
	rm -rf .dingo dingo.log
	./dingo load database/immutable/testdata 2>&1 | tee dingo.log

test-load-profile: build ## Run build, then load test data with CPU/memory profiling
	rm -rf .dingo
	./dingo --cpuprofile=cpu.prof --memprofile=mem.prof load database/immutable/testdata
	@echo "Profiling complete. Run 'go tool pprof cpu.prof' or 'go tool pprof mem.prof' to analyze"

test-devnet: ## Run the default all-Dingo DevNet integration tests
	./internal/test/devnet/run-tests.sh

# Build our program binaries
# Depends on GO_FILES to determine when rebuild is needed
$(BINARIES): mod-tidy $(GO_FILES)
	CGO_ENABLED=0 \
	go build \
		$(GO_TAG_FLAGS) \
		$(GO_LDFLAGS) \
		-o $(@)$(if $(filter windows,$(GOOS)),.exe,)  \
		./cmd/$(@)
