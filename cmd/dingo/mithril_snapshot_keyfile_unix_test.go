//go:build unix

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

package main

import (
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/keystore"
	"github.com/stretchr/testify/require"
)

func TestMithrilSecretFilesRejectInsecureMode(t *testing.T) {
	tests := []struct {
		name string
		path func(*config.Config) string
		run  func(*testing.T, *config.Config) error
	}{
		{
			name: "ancillary signing key",
			path: func(cfg *config.Config) string {
				return cfg.Mithril.Server.AncillarySigningKeyFile
			},
			run: func(t *testing.T, cfg *config.Config) error {
				_, err := runMithrilSnapshotCreate(
					t.Context(), cfg, "", discardLogger,
				)
				return err
			},
		},
		{
			name: "genesis signing key",
			path: func(cfg *config.Config) string {
				return cfg.Mithril.Server.Aggregator.GenesisSigningKeyFile
			},
			run: func(t *testing.T, cfg *config.Config) error {
				_, err := newMithrilServer(t.Context(), cfg, discardLogger)
				return err
			},
		},
		{
			name: "operator token",
			path: func(cfg *config.Config) string {
				return cfg.Mithril.Server.Aggregator.OperatorTokenFile
			},
			run: func(t *testing.T, cfg *config.Config) error {
				_, err := newMithrilServer(t.Context(), cfg, discardLogger)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := aggregatorTestConfig(t)
			require.NoError(t, os.Chmod(test.path(cfg), 0o644))

			err := test.run(t, cfg)
			require.ErrorIs(t, err, keystore.ErrInsecureFileMode)
		})
	}
}
