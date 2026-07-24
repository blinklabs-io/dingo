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

package plugin

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type testConfig struct {
	Value string `yaml:"value"`
}

type testDeps struct {
	Suffix string
}

type testInstance struct {
	name      string
	events    *[]string
	startErr  error
	stopCount int
}

type nilMapInstance map[string]struct{}

func (i nilMapInstance) Start(context.Context) error {
	i["started"] = struct{}{}
	return nil
}

func (nilMapInstance) Stop(context.Context) error {
	return nil
}

func (i *testInstance) Start(context.Context) error {
	*i.events = append(*i.events, "start:"+i.name)
	return i.startErr
}

func (i *testInstance) Stop(context.Context) error {
	i.stopCount++
	*i.events = append(*i.events, "stop:"+i.name)
	return nil
}

func registerTestProvider(
	t *testing.T,
	host *Host,
	cap Capability,
	name string,
	events *[]string,
	startErr error,
) {
	t.Helper()
	err := Register(
		host,
		Descriptor{Capability: cap, Name: name, Description: name},
		func() testConfig { return testConfig{Value: "default"} },
		func(_ context.Context, cfg testConfig, deps testDeps) (string, Instance, error) {
			return cfg.Value + deps.Suffix, &testInstance{
				name:     name,
				events:   events,
				startErr: startErr,
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegisterResolveAndStop(t *testing.T) {
	host := NewHost()
	var events []string
	registerTestProvider(t, host, CapabilityMempool, "default", &events, nil)

	service, err := Resolve[string](
		context.Background(),
		host,
		CapabilityMempool,
		"default",
		map[string]any{"value": "configured"},
		testDeps{Suffix: "!"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if service != "configured!" {
		t.Fatalf("service = %q", service)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:default", "stop:default"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestResolveProviderDoesNotConstrainServiceType(t *testing.T) {
	host := NewHost()
	var events []string
	registerTestProvider(
		t,
		host,
		CapabilityAPIBlockfrost,
		"custom",
		&events,
		nil,
	)

	err := ResolveProvider(
		context.Background(),
		host,
		CapabilityAPIBlockfrost,
		"custom",
		nil,
		testDeps{Suffix: "!"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:custom", "stop:custom"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestDuplicateAndUnknownConfigRejected(t *testing.T) {
	host := NewHost()
	var events []string
	registerTestProvider(t, host, CapabilityMempool, "default", &events, nil)
	err := Register(
		host,
		Descriptor{Capability: CapabilityMempool, Name: "default"},
		func() testConfig { return testConfig{} },
		func(context.Context, testConfig, testDeps) (string, Instance, error) { return "", Lifecycle{}, nil },
	)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("unexpected duplicate error: %v", err)
	}
	_, err = Resolve[string](
		context.Background(),
		host,
		CapabilityMempool,
		"default",
		map[string]any{"unknown": true},
		testDeps{},
	)
	if err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("unexpected decode error: %v", err)
	}
}

func TestUnknownCapabilityRejected(t *testing.T) {
	host := NewHost()
	err := Register(host, Descriptor{Capability: "unknown", Name: "provider"},
		func() testConfig { return testConfig{} },
		func(context.Context, testConfig, testDeps) (string, Instance, error) {
			return "", Lifecycle{}, nil
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "unknown plugin capability") {
		t.Fatalf("unexpected registration error: %v", err)
	}
	selection := Selection{}
	err = ApplyEnvironment("unknown", &selection, nil)
	if err == nil ||
		!strings.Contains(err.Error(), "unknown plugin capability") {
		t.Fatalf("unexpected environment error: %v", err)
	}
}

func TestMixedCaseProviderNameRejected(t *testing.T) {
	err := Register(
		NewHost(),
		Descriptor{Capability: CapabilityMempool, Name: "mixedCase"},
		func() testConfig { return testConfig{} },
		func(context.Context, testConfig, testDeps) (string, Instance, error) {
			return "", Lifecycle{}, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "must be lowercase") {
		t.Fatalf("unexpected registration error: %v", err)
	}
}

func TestDeterministicListingAndStartupCleanup(t *testing.T) {
	host := NewHost()
	var events []string
	registerTestProvider(
		t,
		host,
		CapabilityStorageMetadata,
		"sqlite",
		&events,
		nil,
	)
	registerTestProvider(t, host, CapabilityStorageBlob, "memory", &events, nil)
	registerTestProvider(t, host, CapabilityStorageBlob, "badger", &events, nil)
	registerTestProvider(
		t,
		host,
		CapabilityMempool,
		"default",
		&events,
		errors.New("boom"),
	)

	providers := host.Providers()
	wantProviders := []Descriptor{
		{
			Capability:  CapabilityMempool,
			Name:        "default",
			Description: "default",
		},
		{
			Capability:  CapabilityStorageBlob,
			Name:        "badger",
			Description: "badger",
		},
		{
			Capability:  CapabilityStorageBlob,
			Name:        "memory",
			Description: "memory",
		},
		{
			Capability:  CapabilityStorageMetadata,
			Name:        "sqlite",
			Description: "sqlite",
		},
	}
	if !reflect.DeepEqual(providers, wantProviders) {
		t.Fatalf("providers = %#v, want %#v", providers, wantProviders)
	}
	if _, err := Resolve[string](context.Background(), host, CapabilityStorageBlob, "badger", nil, testDeps{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve[string](context.Background(), host, CapabilityStorageMetadata, "sqlite", nil, testDeps{}); err != nil {
		t.Fatal(err)
	}
	_, err := Resolve[string](
		context.Background(),
		host,
		CapabilityMempool,
		"default",
		nil,
		testDeps{},
	)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("unexpected start error: %v", err)
	}
	wantEvents := []string{
		"start:badger",
		"start:sqlite",
		"start:default",
		"stop:default",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantEvents = append(
		wantEvents,
		"stop:sqlite",
		"stop:badger",
	)
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}

func TestMissingOptionalProviderError(t *testing.T) {
	err := MissingProviderError(CapabilityStorageBlob, "s3")
	if !strings.Contains(err.Error(), "dingo_extra_plugins") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyEnvironment(t *testing.T) {
	selection := Selection{
		Provider: "yaml",
		Config:   map[string]any{"capacity": 1},
	}
	err := ApplyEnvironment(CapabilityMempool, &selection, []string{
		"IGNORED=value",
		"DINGO_PLUGINS_MEMPOOL_PROVIDER=default",
		"DINGO_PLUGINS_MEMPOOL_CONFIG_CAPACITY=1048576",
		"DINGO_PLUGINS_MEMPOOL_CONFIG_EVICTION_WATERMARK=0.90",
	})
	if err != nil {
		t.Fatal(err)
	}
	if selection.Provider != "default" {
		t.Fatalf("provider = %q", selection.Provider)
	}
	if selection.Config["capacity"] != 1048576 {
		t.Fatalf("capacity = %#v", selection.Config["capacity"])
	}
	if selection.Config["evictionWatermark"] != 0.9 {
		t.Fatalf(
			"evictionWatermark = %#v",
			selection.Config["evictionWatermark"],
		)
	}
}

func TestApplyEnvironmentRejectsEmptyPathComponent(t *testing.T) {
	for _, entry := range []string{
		"DINGO_PLUGINS_MEMPOOL_CONFIG_DATA__DIR=x",
		"DINGO_PLUGINS_MEMPOOL_CONFIG_DATA_DIR_=x",
		"DINGO_PLUGINS_MEMPOOL_CONFIG__DATA_DIR=x",
	} {
		selection := Selection{}
		err := ApplyEnvironment(CapabilityMempool, &selection, []string{entry})
		if err == nil {
			t.Fatalf("%s: expected error for malformed path, got nil", entry)
		}
		if !strings.Contains(err.Error(), "empty path component") {
			t.Fatalf("%s: error = %v", entry, err)
		}
	}
}

func TestResolveRejectsTypedNilInstance(t *testing.T) {
	host := NewHost()
	err := Register(
		host,
		Descriptor{
			Capability:  CapabilityMempool,
			Name:        "typednil",
			Description: "typednil",
		},
		func() testConfig { return testConfig{} },
		func(_ context.Context, _ testConfig, _ testDeps) (string, Instance, error) {
			// Return a typed nil pointer as the Instance: the interface is
			// non-nil but wraps a nil *testInstance, which would panic in
			// Start if the nil-lifecycle guard only checked == nil.
			var inst *testInstance
			return "svc", inst, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Resolve[string](
		context.Background(),
		host,
		CapabilityMempool,
		"typednil",
		nil,
		testDeps{},
	)
	if err == nil {
		t.Fatal("expected error for typed-nil instance, got nil")
	}
	if !strings.Contains(err.Error(), "nil lifecycle") {
		t.Fatalf("error = %v, want nil lifecycle", err)
	}
}

func TestResolveRejectsTypedNilMapInstance(t *testing.T) {
	host := NewHost()
	err := Register(
		host,
		Descriptor{
			Capability:  CapabilityMempool,
			Name:        "typednilmap",
			Description: "typednilmap",
		},
		func() testConfig { return testConfig{} },
		func(_ context.Context, _ testConfig, _ testDeps) (string, Instance, error) {
			var inst nilMapInstance
			return "svc", inst, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Resolve[string](
		context.Background(),
		host,
		CapabilityMempool,
		"typednilmap",
		nil,
		testDeps{},
	)
	if err == nil {
		t.Fatal("expected error for typed-nil map instance, got nil")
	}
	if !strings.Contains(err.Error(), "nil lifecycle") {
		t.Fatalf("error = %v, want nil lifecycle", err)
	}
}
