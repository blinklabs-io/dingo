// Copyright 2025 Blink Labs Software
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

package dingo

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func baseTestConfigOpts() []ConfigOptionFunc {
	return []ConfigOptionFunc{
		WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
		WithNetworkMagic(764824073),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
	}
}

func TestConfigValidate_BlockProducerDisabled(t *testing.T) {
	cfg := NewConfig(baseTestConfigOpts()...)
	if _, err := New(cfg); err != nil {
		t.Fatalf("New: %v", err)
	}
}

func TestConfigValidate_BlockProducerNoCardanoConfig(t *testing.T) {
	// Block producer mode without a cardanoNodeConfig must be rejected
	// because we need the Shelley genesis for KES period math.
	opts := append(baseTestConfigOpts(), WithBlockProducer(
		true, "vrf.skey", "kes.skey", "op.cert",
	))
	_, err := New(NewConfig(opts...))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "Cardano node config") {
		t.Errorf("expected error to mention Cardano node config, got: %v", err)
	}
}

func TestConfigValidate_BlockProducerEmptyPaths(t *testing.T) {
	opts := append(baseTestConfigOpts(), WithBlockProducer(true, "", "", ""))
	_, err := New(NewConfig(opts...))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "vrfKeyFile") {
		t.Errorf("expected error to name missing fields, got: %v", err)
	}
}

func TestConfigValidate_BlockProducerMissingFile(t *testing.T) {
	tmp := t.TempDir()
	opts := append(baseTestConfigOpts(), WithBlockProducer(
		true,
		filepath.Join(tmp, "missing.skey"),
		filepath.Join(tmp, "missing.skey"),
		filepath.Join(tmp, "missing.cert"),
	))
	_, err := New(NewConfig(opts...))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "block producer") {
		t.Errorf("expected error to mention block producer, got: %v", err)
	}
}
