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
	"testing"

	"github.com/stretchr/testify/require"
)

// widgetService is a service contract the plugin package has never heard of.
type widgetService interface {
	Greeting() string
}

type widgetImpl struct{ greeting string }

func (w widgetImpl) Greeting() string { return w.greeting }

type widgetConfig struct {
	Greeting string `yaml:"greeting"`
	Suffix   string `yaml:"suffix"`
}

func registerWidget(t *testing.T, host *Host, capability Capability) error {
	t.Helper()
	return Register(
		host,
		Descriptor{Capability: capability, Name: "basic"},
		func() widgetConfig { return widgetConfig{Greeting: "hello"} },
		func(
			_ context.Context,
			cfg widgetConfig,
			_ struct{},
		) (widgetService, Instance, error) {
			return widgetImpl{greeting: cfg.Greeting + cfg.Suffix},
				Lifecycle{},
				nil
		},
	)
}

func TestParseCapability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		name    string
		wantErr string
	}{
		{input: "storage.blob/v0", name: "storage.blob"},
		{input: "consensus/v0", name: "consensus"},
		{input: "cardano.query/v12", name: "cardano.query"},
		{input: "a1.b2.c3/v7", name: "a1.b2.c3"},
		{input: "", wantErr: "empty"},
		{input: "Storage.Blob/v0", wantErr: "lowercase"},
		{input: "storage.blob", wantErr: "missing contract version"},
		{input: "storage.blob/", wantErr: "v<N>"},
		{input: "storage.blob/0", wantErr: "v<N>"},
		{input: "storage.blob/v", wantErr: "v<N>"},
		{input: "storage.blob/v01", wantErr: "leading zeros"},
		{input: "storage.blob/v-1", wantErr: "v<N>"},
		{input: "storage.blob/v1x", wantErr: "v<N>"},
		{input: "storage.blob/v4294967296", wantErr: "out of range"},
		{input: "storage..blob/v0", wantErr: "empty name segment"},
		{input: ".blob/v0", wantErr: "empty name segment"},
		{input: "blob./v0", wantErr: "empty name segment"},
		{input: "/v0", wantErr: "empty name"},
		{input: "storage_blob/v0", wantErr: "lowercase"},
		{input: "a/b/v0", wantErr: "v<N>"},
		{input: "example.config/v0", wantErr: "reserved"},
		{input: "provider/v0", wantErr: "reserved"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got, err := ParseCapability(tt.input)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.False(t, Capability(tt.input).Valid())
				return
			}
			require.NoError(t, err)
			require.Equal(t, Capability(tt.input), got)
			require.True(t, got.Valid())
			require.Equal(t, tt.name, got.Name())
		})
	}
}

func TestEnvironmentPrefixesUnchangedForBuiltIns(t *testing.T) {
	t.Parallel()

	want := map[Capability]string{
		CapabilityStorageBlob:      "DINGO_PLUGINS_STORAGE_BLOB_",
		CapabilityStorageMetadata:  "DINGO_PLUGINS_STORAGE_METADATA_",
		CapabilityStorageImmutable: "DINGO_PLUGINS_STORAGE_IMMUTABLE_",
		CapabilityConsensus:        "DINGO_PLUGINS_CONSENSUS_",
		CapabilityMempool:          "DINGO_PLUGINS_MEMPOOL_",
		CapabilityAPIBlockfrost:    "DINGO_PLUGINS_API_BLOCKFROST_",
		CapabilityAPIKupo:          "DINGO_PLUGINS_API_KUPO_",
		CapabilityAPIMesh:          "DINGO_PLUGINS_API_MESH_",
		CapabilityAPIUtxorpc:       "DINGO_PLUGINS_API_UTXORPC_",
		CapabilityAPIMcp:           "DINGO_PLUGINS_API_MCP_",
	}
	require.Len(t, want, len(AllCapabilities()))
	for _, capability := range AllCapabilities() {
		require.Equal(t, want[capability], EnvironmentPrefix(capability))
	}
}

func TestNewHostSpeaksBuiltIns(t *testing.T) {
	t.Parallel()

	host := NewHost()
	for _, capability := range AllCapabilities() {
		require.True(t, host.Speaks(capability), capability)
	}
	require.False(t, host.Speaks("storage.blob/v1"))
	require.False(t, host.Speaks("example.widget/v0"))
}

// TestUnknownCapabilityEndToEnd uses a capability name no part of the tree
// mentions: declaration, registration, environment overlay, and typed
// resolution all work without editing the host.
func TestUnknownCapabilityEndToEnd(t *testing.T) {
	t.Parallel()

	const widget Capability = "example.widget/v0"
	host := NewHost()
	require.Error(t, registerWidget(t, host, widget), "undeclared capability")
	require.NoError(t, host.Declare(widget))
	require.True(t, host.Speaks(widget))
	require.NoError(t, registerWidget(t, host, widget))

	selection := Selection{Provider: "basic"}
	require.NoError(t, ApplyEnvironment(widget, &selection, []string{
		"DINGO_PLUGINS_EXAMPLE_WIDGET_CONFIG_SUFFIX=world",
		"DINGO_PLUGINS_STORAGE_BLOB_PROVIDER=ignored",
	}))
	require.Equal(t, "world", selection.Config["suffix"])
	require.NoError(t, host.ValidateSelection(
		widget, selection.Provider, selection.Config,
	))

	service, err := Resolve[widgetService](
		t.Context(), host, widget, selection.Provider, selection.Config,
		struct{}{},
	)
	require.NoError(t, err)
	require.Equal(t, "helloworld", service.Greeting())
	require.NoError(t, host.Stop(t.Context()))
}

func TestRegisterRejectsContractVersionMismatch(t *testing.T) {
	t.Parallel()

	host := NewHost()
	require.NoError(t, host.Declare("x.y/v0"))
	err := registerWidget(t, host, "x.y/v1")
	require.ErrorContains(t, err, "x.y/v0")
	require.ErrorContains(t, err, "x.y/v1")

	err = host.ValidateSelection("x.y/v1", "basic", nil)
	require.ErrorContains(t, err, "x.y/v0")
	require.ErrorContains(t, err, "x.y/v1")

	err = registerWidget(t, host, CapabilityStorageBlob+"1")
	require.Error(t, err, "built-in name at another version")
	require.ErrorContains(t, err, string(CapabilityStorageBlob))
}

func TestDeclareRules(t *testing.T) {
	t.Parallel()

	host := NewHost()
	require.NoError(t, host.Declare("x.y/v0"))
	require.NoError(t, host.Declare("x.y/v0"), "redeclaring is a no-op")
	require.NoError(t, host.Declare(CapabilityMempool))

	err := host.Declare("x.y/v1")
	require.ErrorContains(t, err, "x.y/v0")
	require.ErrorContains(t, err, "x.y/v1")
	err = host.Declare(CapabilityMempool + "1")
	require.Error(t, err)
	require.ErrorContains(t, host.Declare("bad"), "missing contract version")

	err = host.Declare("p.q/v0", "p.q/v1")
	require.Error(t, err)
	require.False(t, host.Speaks("p.q/v0"), "Declare is all or nothing")

	got, ok := host.Contract("x.y")
	require.True(t, ok)
	require.Equal(t, Capability("x.y/v0"), got)
	_, ok = host.Contract("nope")
	require.False(t, ok)
}
