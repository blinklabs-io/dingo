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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
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

func TestApplyEnvironmentReadsFileBackedConfig(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	require.NoError(
		t,
		os.WriteFile(tokenPath, []byte("00123:true secret\n"), 0o600),
	)
	passwordPath := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(passwordPath, []byte("from-file"), 0o600))
	selection := Selection{Config: map[string]any{"password": "from-yaml"}}
	err := ApplyEnvironment(CapabilityAPIMcp, &selection, []string{
		"DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN_FILE=" + tokenPath,
		"DINGO_PLUGINS_API_MCP_CONFIG_PASSWORD_FILE=" + passwordPath,
	})
	require.NoError(t, err)
	// File contents are taken verbatim as a string, never parsed as YAML.
	require.Equal(t, "00123:true secret", selection.Config["authToken"])
	require.Equal(t, "from-file", selection.Config["password"])
	require.NotContains(t, selection.Config, "authTokenFile")
}

func TestApplyEnvironmentRejectsLiteralAndFileForSameField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	require.NoError(t, os.WriteFile(path, []byte("from-file"), 0o600))
	literal := "DINGO_PLUGINS_STORAGE_METADATA_CONFIG_PASSWORD=literal"
	file := "DINGO_PLUGINS_STORAGE_METADATA_CONFIG_PASSWORD_FILE=" + path
	for _, environ := range [][]string{{literal, file}, {file, literal}} {
		selection := Selection{}
		err := ApplyEnvironment(
			CapabilityStorageMetadata,
			&selection,
			environ,
		)
		require.ErrorContains(
			t,
			err,
			"DINGO_PLUGINS_STORAGE_METADATA_CONFIG_PASSWORD",
		)
		require.ErrorContains(t, err, "_FILE")
	}
}

func TestApplyEnvironmentFileErrorNamesVariable(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	selection := Selection{}
	err := ApplyEnvironment(CapabilityStorageMetadata, &selection, []string{
		"DINGO_PLUGINS_STORAGE_METADATA_CONFIG_DSN_FILE=" + missing,
	})
	require.ErrorContains(
		t,
		err,
		"DINGO_PLUGINS_STORAGE_METADATA_CONFIG_DSN_FILE",
	)
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

func TestStopCapabilityRacingWithResolveUnwindsNewInstance(t *testing.T) {
	host := NewHost()
	existingStopStarted := make(chan struct{})
	releaseExistingStop := make(chan struct{})
	racingStartFinished := make(chan struct{})
	releaseRacingStart := make(chan struct{})
	var releaseExistingOnce sync.Once
	var releaseRacingOnce sync.Once
	var existingStopStartedOnce sync.Once
	var racingStartFinishedOnce sync.Once
	releaseExisting := func() {
		releaseExistingOnce.Do(func() { close(releaseExistingStop) })
	}
	releaseRacing := func() {
		releaseRacingOnce.Do(func() { close(releaseRacingStart) })
	}
	t.Cleanup(releaseExisting)
	t.Cleanup(releaseRacing)

	var existingStops atomic.Int32
	var racingStops atomic.Int32
	register := func(
		name string,
		start func(context.Context) error,
		stop func(context.Context) error,
	) {
		t.Helper()
		err := Register(
			host,
			Descriptor{Capability: CapabilityMempool, Name: name},
			func() testConfig { return testConfig{} },
			func(
				context.Context,
				testConfig,
				testDeps,
			) (string, Instance, error) {
				return name, Lifecycle{
					StartFunc: start,
					StopFunc:  stop,
				}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	register(
		"existing",
		nil,
		func(context.Context) error {
			existingStops.Add(1)
			existingStopStartedOnce.Do(func() { close(existingStopStarted) })
			<-releaseExistingStop
			return nil
		},
	)
	register(
		"racing",
		func(context.Context) error {
			racingStartFinishedOnce.Do(func() { close(racingStartFinished) })
			<-releaseRacingStart
			return nil
		},
		func(context.Context) error {
			racingStops.Add(1)
			return nil
		},
	)

	_, err := Resolve[string](
		context.Background(),
		host,
		CapabilityMempool,
		"existing",
		nil,
		testDeps{},
	)
	if err != nil {
		t.Fatal(err)
	}

	resolveDone := make(chan error, 1)
	go func() {
		_, resolveErr := Resolve[string](
			context.Background(),
			host,
			CapabilityMempool,
			"racing",
			nil,
			testDeps{},
		)
		resolveDone <- resolveErr
	}()
	testutil.RequireReceive(
		t,
		racingStartFinished,
		3*time.Second,
		"racing provider start",
	)

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- host.StopCapability(
			context.Background(),
			CapabilityMempool,
		)
	}()
	testutil.RequireReceive(
		t,
		existingStopStarted,
		3*time.Second,
		"existing provider stop",
	)

	releaseRacing()
	err = testutil.RequireReceive(
		t,
		resolveDone,
		3*time.Second,
		"racing provider resolution",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "stopped during resolution") {
		t.Fatalf("resolve error = %v, want capability stopped error", err)
	}
	if got := racingStops.Load(); got != 1 {
		t.Fatalf("racing provider stop count = %d, want 1", got)
	}

	releaseExisting()
	if err := testutil.RequireReceive(
		t,
		stopDone,
		3*time.Second,
		"capability stop",
	); err != nil {
		t.Fatal(err)
	}
	if got := existingStops.Load(); got != 1 {
		t.Fatalf("existing provider stop count = %d, want 1", got)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := racingStops.Load(); got != 1 {
		t.Fatalf("racing provider stop count after host stop = %d, want 1", got)
	}
}

func TestHostStopWaitsForInFlightStopCapability(t *testing.T) {
	t.Parallel()

	host := NewHost()
	stopStarted := make(chan struct{})
	releaseStop := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseStop) }) }
	defer release()
	var stopFinished atomic.Bool
	stopFailure := errors.New("capability drain failed")
	if err := Register(host, Descriptor{Capability: CapabilityStorageBlob, Name: "dependency"},
		func() testConfig { return testConfig{} },
		func(context.Context, testConfig, testDeps) (string, Instance, error) {
			return "dependency", Lifecycle{StopFunc: func(context.Context) error {
				if !stopFinished.Load() {
					return errors.New("dependency stopped before consumer")
				}
				return nil
			}}, nil
		}); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve[string](context.Background(), host, CapabilityStorageBlob, "dependency", nil, testDeps{}); err != nil {
		t.Fatal(err)
	}

	err := Register(
		host,
		Descriptor{Capability: CapabilityMempool, Name: "blocked"},
		func() testConfig { return testConfig{} },
		func(context.Context, testConfig, testDeps) (string, Instance, error) {
			return "blocked", Lifecycle{
				StopFunc: func(context.Context) error {
					close(stopStarted)
					<-releaseStop
					stopFinished.Store(true)
					return stopFailure
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve[string](
		context.Background(), host, CapabilityMempool, "blocked", nil, testDeps{},
	); err != nil {
		t.Fatal(err)
	}

	capStopDone := make(chan error, 1)
	go func() {
		capStopDone <- host.StopCapability(context.Background(), CapabilityMempool)
	}()
	testutil.RequireReceive(t, stopStarted, 3*time.Second, "provider stop")

	hostStopDone := make(chan error, 1)
	go func() { hostStopDone <- host.Stop(context.Background()) }()

	select {
	case <-hostStopDone:
		t.Fatal("Host.Stop returned while a provider was still being stopped")
	case <-time.After(200 * time.Millisecond):
	}

	release()
	if err := testutil.RequireReceive(t, hostStopDone, 3*time.Second, "host stop"); !errors.Is(
		err,
		stopFailure,
	) ||
		strings.Contains(err.Error(), "dependency stopped") {
		t.Fatalf("Host.Stop error = %v", err)
	}
	if !stopFinished.Load() {
		t.Fatal("Host.Stop returned before provider teardown completed")
	}
	if err := testutil.RequireReceive(t, capStopDone, 3*time.Second, "capability stop"); !errors.Is(
		err,
		stopFailure,
	) {
		t.Fatal(err)
	}
}

func TestHostStopHonorsContextWhileWaitingForStopCapability(t *testing.T) {
	t.Parallel()

	host := NewHost()
	dependencyStopStarted := make(chan struct{}, 1)
	consumerStopStarted := make(chan struct{})
	consumerFinished := make(chan struct{})
	releaseConsumerStop := make(chan struct{})
	var releaseOnce sync.Once
	releaseConsumer := func() {
		releaseOnce.Do(func() { close(releaseConsumerStop) })
	}
	defer releaseConsumer()
	var dependencyStoppedAfterConsumer atomic.Bool
	consumerStopErr := errors.New("consumer stop failed")
	dependencyStopErr := errors.New("dependency stop failed")
	err := Register(
		host,
		Descriptor{Capability: CapabilityStorageBlob, Name: "dependency"},
		func() testConfig { return testConfig{} },
		func(context.Context, testConfig, testDeps) (string, Instance, error) {
			return "dependency", Lifecycle{StopFunc: func(context.Context) error {
				select {
				case <-consumerFinished:
					dependencyStoppedAfterConsumer.Store(true)
				default:
				}
				dependencyStopStarted <- struct{}{}
				return dependencyStopErr
			}}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve[string](
		context.Background(), host, CapabilityStorageBlob, "dependency", nil, testDeps{},
	); err != nil {
		t.Fatal(err)
	}

	err = Register(
		host,
		Descriptor{Capability: CapabilityMempool, Name: "consumer"},
		func() testConfig { return testConfig{} },
		func(context.Context, testConfig, testDeps) (string, Instance, error) {
			return "consumer", Lifecycle{
				StopFunc: func(context.Context) error {
					close(consumerStopStarted)
					<-releaseConsumerStop
					close(consumerFinished)
					return consumerStopErr
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve[string](
		context.Background(), host, CapabilityMempool, "consumer", nil, testDeps{},
	); err != nil {
		t.Fatal(err)
	}
	capStopDone := make(chan error, 1)
	go func() {
		capStopDone <- host.StopCapability(
			context.Background(),
			CapabilityMempool,
		)
	}()
	testutil.RequireReceive(
		t,
		consumerStopStarted,
		3*time.Second,
		"consumer provider stop",
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()
	if err := host.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Host.Stop error = %v, want deadline exceeded", err)
	}
	select {
	case <-dependencyStopStarted:
		t.Fatal("Host.Stop stopped a dependency before consumer teardown finished")
	default:
	}

	releaseConsumer()
	if err := testutil.RequireReceive(
		t,
		capStopDone,
		3*time.Second,
		"capability stop",
	); !errors.Is(err, consumerStopErr) {
		t.Fatalf("StopCapability error = %v, want consumer stop error", err)
	}
	testutil.RequireReceive(
		t,
		dependencyStopStarted,
		3*time.Second,
		"dependency stop after consumer teardown",
	)
	if !dependencyStoppedAfterConsumer.Load() {
		t.Fatal("dependency stop started before consumer teardown finished")
	}
	if err := host.Stop(context.Background()); !errors.Is(err, context.DeadlineExceeded) ||
		!errors.Is(err, consumerStopErr) || !errors.Is(err, dependencyStopErr) {
		t.Fatalf(
			"completed Host.Stop error = %v, want deadline and both provider stop errors",
			err,
		)
	}
}

func TestHostStopWithExpiredContextAndNoStopCapabilityInFlight(t *testing.T) {
	t.Parallel()

	host := NewHost()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := host.Stop(ctx); err != nil {
		t.Fatalf("Host.Stop error = %v, want nil with nothing in flight", err)
	}
}
