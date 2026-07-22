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
	"reflect"
	"slices"
	"sort"
	"sync"

	"gopkg.in/yaml.v3"
)

// Capability is a stable identifier for a pluggable subsystem.
type Capability string

const (
	CapabilityStorageBlob     Capability = "storage.blob"
	CapabilityStorageMetadata Capability = "storage.metadata"
	CapabilityMempool         Capability = "mempool"
	CapabilityAPIBlockfrost   Capability = "api.blockfrost"
	CapabilityAPIMesh         Capability = "api.mesh"
	CapabilityAPIUtxorpc      Capability = "api.utxorpc"
)

// Valid reports whether c is a capability supported by this platform.
func (c Capability) Valid() bool {
	switch c {
	case CapabilityStorageBlob, CapabilityStorageMetadata, CapabilityMempool,
		CapabilityAPIBlockfrost, CapabilityAPIMesh, CapabilityAPIUtxorpc:
		return true
	default:
		return false
	}
}

// Descriptor describes a compiled-in provider.
type Descriptor struct {
	Capability  Capability
	Name        string
	Description string
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
	started   []startedInstance
	stopped   bool
	stopDone  chan struct{}
	stopErr   error
	// stopping counts in-flight StopCapability calls per capability so a
	// concurrent Resolve can detect that the capability it just started is
	// being torn down and unwind its own instance instead of leaking it.
	stopping map[Capability]int
}

// NewHost returns an empty plugin host.
func NewHost() *Host {
	return &Host{
		providers: make(map[providerKey]provider),
		stopping:  make(map[Capability]int),
	}
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
	if !descriptor.Capability.Valid() {
		return fmt.Errorf("unknown plugin capability %q", descriptor.Capability)
	}
	if descriptor.Name == "" {
		return fmt.Errorf(
			"plugin provider name is empty for capability %q",
			descriptor.Capability,
		)
	}
	if factory == nil {
		return fmt.Errorf(
			"plugin factory is nil for %s/%s",
			descriptor.Capability,
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
					descriptor.Capability,
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
					descriptor.Capability,
					descriptor.Name,
				)
			}
			typedDeps, ok := deps.(D)
			if !ok {
				return nil, nil, fmt.Errorf(
					"dependency type mismatch for %s/%s: got %T",
					descriptor.Capability,
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
			descriptor.Capability,
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
			capability,
			name,
			err,
		)
	}
	if instance == nil || isNilInstance(instance) {
		return zero, fmt.Errorf(
			"plugin %s/%s returned a nil lifecycle",
			capability,
			name,
		)
	}
	if err := instance.Start(ctx); err != nil {
		stopErr := instance.Stop(ctx)
		return zero, errors.Join(
			fmt.Errorf("start plugin %s/%s: %w", capability, name, err),
			stopErr,
		)
	}
	typedService, ok := service.(S)
	if !ok {
		stopErr := instance.Stop(ctx)
		return zero, errors.Join(
			fmt.Errorf(
				"service type mismatch for %s/%s: got %T",
				capability,
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
// wrapping a typed nil pointer. An interface holding a nil pointer is not equal
// to nil, so a factory that returns a typed nil would otherwise slip past the
// plain == nil check and panic when Start is called.
func isNilInstance(instance Instance) bool {
	if instance == nil {
		return true
	}
	v := reflect.ValueOf(instance)
	return v.Kind() == reflect.Pointer && v.IsNil()
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
	if !capability.Valid() {
		return fmt.Errorf("unknown plugin capability %q", capability)
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
// idempotent; subsequent calls return the first call's result.
func (h *Host) Stop(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.stopped {
		done := h.stopDone
		h.mu.Unlock()
		select {
		case <-done:
			h.mu.Lock()
			err := h.stopErr
			h.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h.stopped = true
	h.stopDone = make(chan struct{})
	started := h.started
	h.started = nil
	h.mu.Unlock()
	err := stopReverse(ctx, started)
	h.mu.Lock()
	h.stopErr = err
	close(h.stopDone)
	h.mu.Unlock()
	return err
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
	h.mu.Unlock()
	err := stopReverse(ctx, selected)
	h.mu.Lock()
	h.stopping[capability]--
	if h.stopping[capability] <= 0 {
		delete(h.stopping, capability)
	}
	h.mu.Unlock()
	return err
}

func stopReverse(ctx context.Context, started []startedInstance) error {
	var ret error
	for _, item := range slices.Backward(started) {
		if err := item.instance.Stop(ctx); err != nil {
			ret = errors.Join(ret, fmt.Errorf(
				"stop plugin %s/%s: %w",
				item.descriptor.Capability,
				item.descriptor.Name,
				err,
			))
		}
	}
	return ret
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
}

// MissingProviderError returns an actionable error for known providers that
// require the optional-provider build tag.
func MissingProviderError(capability Capability, name string) error {
	if providers, ok := knownOptionalProviders[capability]; ok {
		if _, ok := providers[name]; ok {
			return fmt.Errorf(
				"plugin provider %s/%s is not included in this build; rebuild with -tags dingo_extra_plugins or use an official release binary",
				capability,
				name,
			)
		}
	}
	return fmt.Errorf("plugin provider not found: %s/%s", capability, name)
}
