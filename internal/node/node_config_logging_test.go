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

package node

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/config"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// sentinelConfig builds a config whose every secret-bearing field carries a
// distinctive sentinel value, alongside non-secret values that must survive
// redaction.
func sentinelConfig() (*config.Config, []string) {
	token := "SENTINEL-API-AUTH-TOKEN"
	cfg := &config.Config{
		Network:      "preview",
		DatabasePath: "/var/lib/dingo",
		API: config.APIConfig{
			Auth: apiconfig.AuthPolicy{Token: &token},
		},
		KoiosParity: config.KoiosParityConfig{
			Enabled: true,
			APIKey:  "SENTINEL-KOIOS-API-KEY",
		},
		BarkBaseUrl: "https://bark:SENTINEL-BARK-PASSWORD@bark.example/api",
		Mithril: config.MithrilConfig{
			AggregatorURL: "https://aggregator.example/aggregator" +
				"?apiKey=SENTINEL-MITHRIL-KEY",
		},
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Metadata: hostplugin.Selection{
					Provider: "postgres",
					Config: map[string]any{
						"host":     "db.example",
						"user":     "dingo",
						"password": "SENTINEL-PG-PASSWORD",
						"dsn": "postgres://dingo:SENTINEL-DSN-PASSWORD" +
							"@db.example:5432/dingo?sslmode=require",
						"futureKey": "SENTINEL-UNKNOWN-PROVIDER-KEY",
					},
				},
			},
			API: config.APIPluginsConfig{
				Blockfrost: hostplugin.Selection{
					Provider: "blockfrost",
					Config: map[string]any{
						"auth": map[string]any{
							"mode":  "token",
							"token": "SENTINEL-BLOCKFROST-TOKEN",
						},
					},
				},
			},
		},
	}
	return cfg, []string{
		"SENTINEL-API-AUTH-TOKEN",
		"SENTINEL-KOIOS-API-KEY",
		"SENTINEL-BARK-PASSWORD",
		"SENTINEL-MITHRIL-KEY",
		"SENTINEL-PG-PASSWORD",
		"SENTINEL-DSN-PASSWORD",
		"SENTINEL-UNKNOWN-PROVIDER-KEY",
		"SENTINEL-BLOCKFROST-TOKEN",
	}
}

// TestLogStartupConfigRedactsSecrets covers the startup debug log that
// records the effective configuration: no secret-bearing value may reach it.
func TestLogStartupConfigRedactsSecrets(t *testing.T) {
	t.Parallel()

	cfg, sentinels := sentinelConfig()
	for _, handler := range []struct {
		name string
		make func(*bytes.Buffer) slog.Handler
	}{
		{"text", func(b *bytes.Buffer) slog.Handler {
			return slog.NewTextHandler(b, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})
		}},
		{"json", func(b *bytes.Buffer) slog.Handler {
			return slog.NewJSONHandler(b, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})
		}},
	} {
		t.Run(handler.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logStartupConfig(slog.New(handler.make(&buf)), cfg)
			rendered := buf.String()
			if rendered == "" {
				t.Fatal("startup config log produced no output")
			}
			for _, sentinel := range sentinels {
				if strings.Contains(rendered, sentinel) {
					t.Errorf(
						"rendered log leaks %s: %s",
						sentinel,
						rendered,
					)
				}
			}
			// Negative case: redaction must not blank the whole record.
			for _, want := range []string{
				"preview",
				"/var/lib/dingo",
				"db.example",
				"postgres",
				"sslmode=require",
			} {
				if !strings.Contains(rendered, want) {
					t.Errorf(
						"rendered log dropped non-secret %q: %s",
						want,
						rendered,
					)
				}
			}
		})
	}
}
