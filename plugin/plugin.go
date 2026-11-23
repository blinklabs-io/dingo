// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package plugin provides the instance-owned host for compiled-in Dingo
// plugins. Subsystem service contracts remain in their owning packages; this
// package only coordinates provider selection, typed construction, and
// lifecycle.
package plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Capability identifies a pluggable subsystem together with the version of
// the service contract its providers implement. It is written
// "<name>/v<N>", for example "storage.blob/v0". ParseCapability documents the
// accepted form. Selections and environment variables are keyed by the name
// alone, so a contract version change never renames configuration.
type Capability string

const (
	CapabilityStorageBlob      Capability = "storage.blob/v0"
	CapabilityStorageMetadata  Capability = "storage.metadata/v0"
	CapabilityStorageImmutable Capability = "storage.immutable/v0"
	CapabilityConsensus        Capability = "consensus/v0"
	CapabilityMempool          Capability = "mempool/v0"
	CapabilityAPIBlockfrost    Capability = "api.blockfrost/v0"
	CapabilityAPIKupo          Capability = "api.kupo/v0"
	CapabilityAPIMesh          Capability = "api.mesh/v0"
	CapabilityAPIUtxorpc       Capability = "api.utxorpc/v0"
	CapabilityAPIMcp           Capability = "api.mcp/v0"
)

// allCapabilities is the set of contracts every Host speaks from NewHost.
var allCapabilities = []Capability{
	CapabilityStorageBlob,
	CapabilityStorageMetadata,
	CapabilityStorageImmutable,
	CapabilityConsensus,
	CapabilityMempool,
	CapabilityAPIBlockfrost,
	CapabilityAPIKupo,
	CapabilityAPIMesh,
	CapabilityAPIUtxorpc,
	CapabilityAPIMcp,
}

// AllCapabilities returns the built-in contracts, in a stable order. A Host
// speaks these from NewHost; Host.Declare adds others.
func AllCapabilities() []Capability {
	return slices.Clone(allCapabilities)
}

// Descriptor describes a compiled-in provider.
type Descriptor struct {
	Capability  Capability
	Name        string
	Description string
	// DurableClose marks a provider whose Stop persists state and must run to
	// completion. Host stops such an instance with a context that ignores
	// cancellation and deadline, waits for it even after the Stop context
	// expires, and logs progress while it runs. Everything else keeps the
	// Stop context's deadline.
	DurableClose bool
}

// Instance is the lifecycle owned by a Host. Start and Stop must honor the
// supplied context. Stop implementations should be safe to call more than
// once; Host also guarantees that it invokes each successful instance once.
type Instance interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// Lifecycle adapts functions to Instance.
type Lifecycle struct {
	StartFunc func(context.Context) error
	StopFunc  func(context.Context) error
}

func (l Lifecycle) Start(ctx context.Context) error {
	if l.StartFunc == nil {
		return nil
	}
	return l.StartFunc(ctx)
}

func (l Lifecycle) Stop(ctx context.Context) error {
	if l.StopFunc == nil {
		return nil
	}
	return l.StopFunc(ctx)
}

// Factory constructs a typed service and its lifecycle from typed
// configuration and dependencies.
type Factory[S, C, D any] func(context.Context, C, D) (S, Instance, error)

type providerKey struct {
	capability Capability
	name       string
}

type provider struct {
	descriptor Descriptor
	decode     func(map[string]any) (any, error)
	construct  func(context.Context, any, any) (any, Instance, error)
}

type startedInstance struct {
	descriptor Descriptor
	instance   Instance
}

// Host is an application-owned provider registry and lifecycle coordinator.
// A Host is safe for concurrent listing and resolution.
type Host struct {
	mu        sync.Mutex
	providers map[providerKey]provider
	// contracts maps a capability name to the one contract version this host
	// speaks for it.
	contracts map[string]Capability
	started   []startedInstance
	stopped   bool
	stopDone  chan struct{}
	stopErr   error
	// stopping counts in-flight StopCapability calls per capability so a
	// concurrent Resolve can detect that the capability it just started is
	// being torn down and unwind its own instance instead of leaking it.
	stopping map[Capability]int
	// stopCapabilityWG tracks StopCapability teardown that has already removed
	// its providers from started, so Stop can wait for it. Add happens under mu
	// while stopped is false; Stop sets stopped under mu before it Waits.
	stopCapabilityWG  sync.WaitGroup
	capabilityStopErr error
	// stopDurable records that Stop found a DurableClose instance to stop, so
	// waitForStop keeps waiting for teardown after its context expires.
	stopDurable bool
	// durableCapabilityStops counts in-flight StopCapability calls that took a
	// DurableClose instance out of started. Stop must still wait for those
	// even though started no longer lists them.
	durableCapabilityStops int
	logger                 *slog.Logger
	// failureHandler receives failures of started external plugins.
	failureHandler func(Failure)
	// progressInterval overrides defaultDurableCloseProgressInterval when
	// positive.
	progressInterval time.Duration
}

// defaultDurableCloseProgressInterval is how often a durable close, or a
// Host.Stop waiting on one past its deadline, logs that it is still running.
const defaultDurableCloseProgressInterval = 10 * time.Second

// SetLogger sets the logger used for durable-close progress. A nil logger
// selects slog.Default.
func (h *Host) SetLogger(logger *slog.Logger) {
	h.mu.Lock()
	h.logger = logger
	h.mu.Unlock()
}

func (h *Host) log() *slog.Logger {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.logger == nil {
		return slog.Default()
	}
	return h.logger
}

// NewHost returns a plugin host with no providers that speaks the built-in
// contracts listed by AllCapabilities.
func NewHost() *Host {
	h := &Host{
		providers: make(map[providerKey]provider),
		contracts: make(map[string]Capability, len(allCapabilities)),
		stopping:  make(map[Capability]int),
	}
	for _, c := range allCapabilities {
		h.contracts[c.Name()] = c
	}
	return h
}

// Declare adds contracts the host speaks beyond the built-in set. Declaring a
// contract the host already speaks is a no-op. A host speaks one version per
// name, so declaring a different version of a spoken name is an error that
// names both versions. A name may not nest under or contain another spoken
// name: the two would share a configuration subtree under plugins and an
// environment prefix. Declare is all-or-nothing.
func (h *Host) Declare(capabilities ...Capability) error {
	if h == nil {
		return errors.New("plugin host is nil")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	pending := make(map[string]Capability, len(capabilities))
	for _, c := range capabilities {
		if err := c.validate(); err != nil {
			return err
		}
		name := c.Name()
		for _, spoken := range []Capability{h.contracts[name], pending[name]} {
			if spoken != "" && spoken != c {
				return fmt.Errorf(
					"plugin capability %q conflicts with %q: a host speaks one version per capability",
					c,
					spoken,
				)
			}
		}
		for _, spoken := range []map[string]Capability{h.contracts, pending} {
			for other, contract := range spoken {
				if strings.HasPrefix(name, other+".") ||
					strings.HasPrefix(other, name+".") {
					return fmt.Errorf(
						"plugin capability %q nests with %q: their configuration and environment prefixes overlap",
						c,
						contract,
					)
				}
			}
		}
		pending[name] = c
	}
	maps.Copy(h.contracts, pending)
	return nil
}

// Speaks reports whether the host speaks exactly the contract c, name and
// version.
func (h *Host) Speaks(c Capability) bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	spoken, ok := h.contracts[c.Name()]
	return ok && spoken == c
}

// Contract returns the contract the host speaks for a capability name.
func (h *Host) Contract(name string) (Capability, bool) {
	if h == nil {
		return "", false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.contracts[name]
	return c, ok
}

// checkSpoken returns an error unless the host speaks c. A name spoken at a
// different version is reported with both versions.
func (h *Host) checkSpoken(c Capability) error {
	if err := c.validate(); err != nil {
		return err
	}
	spoken, ok := h.Contract(c.Name())
	switch {
	case !ok:
		return fmt.Errorf(
			"unknown plugin capability %q: the host does not speak %q",
			c,
			c.Name(),
		)
	case spoken != c:
		return fmt.Errorf(
			"plugin capability version mismatch for %q: requested %q, host speaks %q",
			c.Name(),
			c,
			spoken,
		)
	}
	return nil
}

// Register adds a typed provider to host. defaults is called before strict
// decoding and may be nil when the zero value is the provider default.
func Register[S, C, D any](
	host *Host,
	descriptor Descriptor,
	defaults func() C,
	factory Factory[S, C, D],
) error {
	if host == nil {
		return errors.New("plugin host is nil")
	}
	if descriptor.Capability == "" {
		return errors.New("plugin capability is empty")
	}
	if err := host.checkSpoken(descriptor.Capability); err != nil {
		return err
	}
	if descriptor.Name == "" {
		return fmt.Errorf(
			"plugin provider name is empty for capability %q",
			descriptor.Capability,
		)
	}
	if descriptor.Name != strings.ToLower(descriptor.Name) {
		return fmt.Errorf(
			"plugin provider name must be lowercase: %q",
			descriptor.Name,
		)
	}
	if factory == nil {
		return fmt.Errorf(
			"plugin factory is nil for %s/%s",
			descriptor.Capability.Name(),
			descriptor.Name,
		)
	}

	p := provider{
		descriptor: descriptor,
		decode: func(raw map[string]any) (any, error) {
			var cfg C
			if defaults != nil {
				cfg = defaults()
			}
			if err := decodeStrict(raw, &cfg); err != nil {
				return nil, fmt.Errorf(
					"decode configuration for %s/%s: %w",
					descriptor.Capability.Name(),
					descriptor.Name,
					err,
				)
			}
			return cfg, nil
		},
		construct: func(ctx context.Context, cfg, deps any) (any, Instance, error) {
			typedCfg, ok := cfg.(C)
			if !ok {
				return nil, nil, fmt.Errorf(
					"internal configuration type mismatch for %s/%s",
					descriptor.Capability.Name(),
					descriptor.Name,
				)
			}
			typedDeps, ok := deps.(D)
			if !ok {
				return nil, nil, fmt.Errorf(
					"dependency type mismatch for %s/%s: got %T",
					descriptor.Capability.Name(),
					descriptor.Name,
					deps,
				)
			}
			service, instance, err := factory(ctx, typedCfg, typedDeps)
			return service, instance, err
		},
	}

	host.mu.Lock()
	defer host.mu.Unlock()
	key := providerKey{capability: descriptor.Capability, name: descriptor.Name}
	if _, exists := host.providers[key]; exists {
		return fmt.Errorf(
			"plugin provider already registered: %s/%s",
			descriptor.Capability.Name(),
			descriptor.Name,
		)
	}
	host.providers[key] = p
	return nil
}

// Resolve constructs and starts one selected provider, returning its typed
// service. A provider that fails during startup is stopped before the error is
// returned. Previously started providers remain active so composition code can
// unwind them around non-plugin dependents in the correct order.
func Resolve[S, D any](
	ctx context.Context,
	host *Host,
	capability Capability,
	name string,
	rawConfig map[string]any,
	deps D,
) (S, error) {
	var zero S
	if host == nil {
		return zero, errors.New("plugin host is nil")
	}

	host.mu.Lock()
	if host.stopped {
		host.mu.Unlock()
		return zero, errors.New("plugin host is stopped")
	}
	p, ok := host.providers[providerKey{capability: capability, name: name}]
	host.mu.Unlock()
	if !ok {
		return zero, MissingProviderError(capability, name)
	}

	cfg, err := p.decode(rawConfig)
	if err != nil {
		return zero, err
	}
	service, instance, err := p.construct(ctx, cfg, deps)
	if err != nil {
		return zero, fmt.Errorf(
			"construct plugin %s/%s: %w",
			capability.Name(),
			name,
			err,
		)
	}
	if instance == nil || isNilInstance(instance) {
		return zero, fmt.Errorf(
			"plugin %s/%s returned a nil lifecycle",
			capability.Name(),
			name,
		)
	}
	if err := instance.Start(ctx); err != nil {
		stopErr := instance.Stop(ctx)
		return zero, errors.Join(
			fmt.Errorf("start plugin %s/%s: %w", capability.Name(), name, err),
			stopErr,
		)
	}
	typedService, ok := service.(S)
	if !ok {
		stopErr := instance.Stop(ctx)
		return zero, errors.Join(
			fmt.Errorf(
				"service type mismatch for %s/%s: got %T",
				capability.Name(),
				name,
				service,
			),
			stopErr,
		)
	}

	host.mu.Lock()
	if host.stopped {
		host.mu.Unlock()
		return zero, errors.Join(
			errors.New("plugin host stopped during resolution"),
			instance.Stop(ctx),
		)
	}
	if host.stopping[capability] > 0 {
		// A StopCapability for this capability is tearing down its
		// providers concurrently. Appending here would leave this
		// instance active but omitted from that stop, so unwind it now
		// rather than recording an orphan the caller believes is stopped.
		host.mu.Unlock()
		return zero, errors.Join(
			fmt.Errorf(
				"plugin capability %s stopped during resolution",
				capability,
			),
			instance.Stop(ctx),
		)
	}
	host.started = append(
		host.started,
		startedInstance{descriptor: p.descriptor, instance: instance},
	)
	host.mu.Unlock()
	return typedService, nil
}

// isNilInstance reports whether instance is a nil interface or an interface
// wrapping a typed nil value. An interface holding a nil-able typed value is not
// equal to nil, so a factory that returns one would otherwise slip past the
// plain == nil check and may panic when Start is called.
func isNilInstance(instance Instance) bool {
	if instance == nil {
		return true
	}
	v := reflect.ValueOf(instance)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return v.IsNil()
	case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8,
		reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Complex64,
		reflect.Complex128, reflect.Array, reflect.String, reflect.Struct:
		return false
	}
	return false
}

// ResolveProvider constructs and starts a provider whose service has no
// in-process consumers. The host still owns its lifecycle, while the provider
// remains free to use a package-specific service type internally. This is
// useful for endpoint capabilities that only need to be started and stopped.
func ResolveProvider[D any](
	ctx context.Context,
	host *Host,
	capability Capability,
	name string,
	rawConfig map[string]any,
	deps D,
) error {
	_, err := Resolve[any](ctx, host, capability, name, rawConfig, deps)
	return err
}

// Providers returns all registered providers in capability/name order.
func (h *Host) Providers() []Descriptor {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	ret := make([]Descriptor, 0, len(h.providers))
	for _, p := range h.providers {
		ret = append(ret, p.descriptor)
	}
	h.mu.Unlock()
	sort.Slice(ret, func(i, j int) bool {
		if ret[i].Capability == ret[j].Capability {
			return ret[i].Name < ret[j].Name
		}
		return ret[i].Capability < ret[j].Capability
	})
	return ret
}

// ValidateSelection verifies that a provider exists and its configuration
// decodes strictly, without constructing or starting it.
func (h *Host) ValidateSelection(
	capability Capability,
	name string,
	rawConfig map[string]any,
) error {
	if h == nil {
		return errors.New("plugin host is nil")
	}
	if err := h.checkSpoken(capability); err != nil {
		return err
	}
	h.mu.Lock()
	p, ok := h.providers[providerKey{capability: capability, name: name}]
	h.mu.Unlock()
	if !ok {
		return MissingProviderError(capability, name)
	}
	_, err := p.decode(rawConfig)
	return err
}

// Stop stops all successfully started providers in reverse order. It is
// idempotent. Without durable providers, cancellation returns ctx.Err while
// teardown continues in the background. With durable providers, Stop waits for
// closure to finish even after cancellation. Provider errors and cancellation
// during ordinary teardown remain errors; a durable close exceeding the deadline
// alone is not an error. A later call waits for the accumulated result.
func (h *Host) Stop(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.stopped {
		done := h.stopDone
		durable := h.stopDurable
		h.mu.Unlock()
		return h.waitForStop(ctx, done, durable)
	}
	h.stopped = true
	h.stopDone = make(chan struct{})
	done := h.stopDone
	durable := h.durableCapabilityStops > 0 ||
		slices.ContainsFunc(h.started, func(i startedInstance) bool {
			return i.descriptor.DurableClose
		})
	h.stopDurable = durable
	if len(h.stopping) == 0 && len(h.started) == 0 {
		h.stopErr = h.capabilityStopErr
		close(done)
		err := h.stopErr
		h.mu.Unlock()
		return err
	}
	h.mu.Unlock()
	go h.finishStop(ctx)
	return h.waitForStop(ctx, done, durable)
}

// waitForStop waits for teardown to finish. When the host has a durable
// instance to close it keeps waiting after ctx expires, logging periodically,
// because abandoning that close would let the process exit mid-write.
func (h *Host) waitForStop(
	ctx context.Context,
	done <-chan struct{},
	durable bool,
) error {
	result := func() error {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.stopErr
	}
	select {
	case <-done:
		return result()
	case <-ctx.Done():
	}
	select {
	case <-done:
		return result()
	default:
	}
	if !durable {
		return ctx.Err()
	}
	start := time.Now()
	ticker := time.NewTicker(h.progressEvery())
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return result()
		case <-ticker.C:
			h.log().Warn(
				"plugin host stop is past its deadline, waiting for durable close",
				"elapsed", time.Since(start).Round(time.Millisecond),
			)
		}
	}
}

func (h *Host) progressEvery() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.progressInterval > 0 {
		return h.progressInterval
	}
	return defaultDurableCloseProgressInterval
}

func (h *Host) finishStop(ctx context.Context) {
	// StopCapability removes its providers from started before stopping them.
	// Wait for those consumers before taking the remaining dependencies out of
	// the host, and retain their references here until their stops complete.
	h.stopCapabilityWG.Wait()
	h.mu.Lock()
	started := h.started
	h.started = nil
	err := h.capabilityStopErr
	durable := h.stopDurable
	h.mu.Unlock()
	stopErr, deadlineErr := h.stopReverse(ctx, started, durable)
	err = errors.Join(err, stopErr, deadlineErr)
	h.mu.Lock()
	h.stopErr = err
	close(h.stopDone)
	h.mu.Unlock()
}

// StopCapability stops successfully started providers for one capability in
// reverse start order. It is idempotent and leaves other capabilities active,
// allowing composition code to preserve dependencies on non-plugin services.
func (h *Host) StopCapability(
	ctx context.Context,
	capability Capability,
) error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return nil
	}
	// Mark the capability as stopping before releasing the lock so a Resolve
	// that completes while stopReverse runs unwinds its own instance instead
	// of appending it behind our back.
	h.stopping[capability]++
	h.stopCapabilityWG.Add(1)
	selected := make([]startedInstance, 0, 1)
	remaining := make([]startedInstance, 0, len(h.started))
	for _, item := range h.started {
		if item.descriptor.Capability == capability {
			selected = append(selected, item)
		} else {
			remaining = append(remaining, item)
		}
	}
	h.started = remaining
	durable := slices.ContainsFunc(selected, func(i startedInstance) bool {
		return i.descriptor.DurableClose
	})
	if durable {
		h.durableCapabilityStops++
	}
	h.mu.Unlock()
	err, _ := h.stopReverse(ctx, selected, false)
	h.mu.Lock()
	if durable {
		h.durableCapabilityStops--
	}
	h.capabilityStopErr = errors.Join(h.capabilityStopErr, err)
	h.stopping[capability]--
	if h.stopping[capability] <= 0 {
		delete(h.stopping, capability)
	}
	h.stopCapabilityWG.Done()
	h.mu.Unlock()
	return err
}

// stopReverse stops started in reverse order. A DurableClose instance gets a
// context detached from ctx's cancellation and is waited for, with progress
// logged. The second result records cancellation during a non-durable stop,
// or cancellation of a teardown without durable work. A successful stop that
// begins after durable work exhausted the deadline does not add a context error.
// durableElsewhere reports durable work outside this list.
func (h *Host) stopReverse(
	ctx context.Context,
	started []startedInstance,
	durableElsewhere bool,
) (ret, deadlineErr error) {
	durableSeen := durableElsewhere
	for _, item := range slices.Backward(started) {
		var err error
		if item.descriptor.DurableClose {
			durableSeen = true
			err = h.closeDurable(ctx, item)
		} else {
			wasCanceled := ctx.Err() != nil
			err = item.instance.Stop(ctx)
			if !wasCanceled || !durableSeen {
				deadlineErr = ctx.Err()
			}
		}
		if err != nil {
			ret = errors.Join(ret, fmt.Errorf(
				"stop plugin %s/%s: %w",
				item.descriptor.Capability.Name(),
				item.descriptor.Name,
				err,
			))
		}
	}
	if !durableSeen {
		deadlineErr = ctx.Err()
	}
	return ret, deadlineErr
}

func (h *Host) closeDurable(ctx context.Context, item startedInstance) error {
	logger := h.log().With(
		"capability", item.descriptor.Capability,
		"plugin", item.descriptor.Name,
	)
	start := time.Now()
	logger.Info("closing durable plugin store")
	finished := make(chan struct{})
	go func() {
		ticker := time.NewTicker(h.progressEvery())
		defer ticker.Stop()
		for {
			select {
			case <-finished:
				return
			case <-ticker.C:
				logger.Warn(
					"durable plugin store close still in progress",
					"elapsed", time.Since(start).Round(time.Millisecond),
				)
			}
		}
	}()
	err := item.instance.Stop(context.WithoutCancel(ctx))
	close(finished)
	elapsed := time.Since(start).Round(time.Millisecond)
	if err != nil {
		logger.Error(
			"durable plugin store close failed",
			"elapsed", elapsed, "error", err,
		)
		return err
	}
	logger.Info("durable plugin store closed", "elapsed", elapsed)
	return nil
}

func decodeStrict(raw map[string]any, dst any) error {
	if raw == nil {
		raw = map[string]any{}
	}
	data, err := yaml.Marshal(raw)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	return decoder.Decode(dst)
}

var knownOptionalProviders = map[Capability]map[string]struct{}{
	CapabilityStorageBlob: {
		"gcs": {},
		"s3":  {},
	},
	CapabilityStorageMetadata: {
		"mysql":    {},
		"postgres": {},
	},
	CapabilityStorageImmutable: {
		"gcs": {},
		"s3":  {},
	},
}

// MissingProviderError returns an actionable error for known providers that
// require the optional-provider build tag.
func MissingProviderError(capability Capability, name string) error {
	if providers, ok := knownOptionalProviders[capability]; ok {
		if _, ok := providers[name]; ok {
			return fmt.Errorf(
				"plugin provider %s/%s is not included in this build; rebuild with -tags dingo_extra_plugins or use an official release binary",
				capability.Name(),
				name,
			)
		}
	}
	return fmt.Errorf(
		"plugin provider not found: %s/%s",
		capability.Name(),
		name,
	)
}
