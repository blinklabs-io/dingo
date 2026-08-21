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

package config

import (
	"bytes"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// configLeafFieldPaths returns the dotted Go field path of every exported
// leaf (non-struct) field reachable from Config, which is exactly the set
// configLogClasses has to classify.
func configLeafFieldPaths(t *testing.T) []string {
	t.Helper()

	var paths []string
	var walk func(rt reflect.Type, prefix string)
	walk = func(rt reflect.Type, prefix string) {
		for field := range rt.Fields() {
			if !field.IsExported() {
				continue
			}
			ft := field.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			path := prefix + field.Name
			if ft.Kind() == reflect.Struct {
				walk(ft, path+".")
				continue
			}
			paths = append(paths, path)
		}
	}
	walk(reflect.TypeFor[Config](), "")
	slices.Sort(paths)
	return paths
}

// TestConfigLogClassesCoverEveryConfigField is the fail-safe gate on the
// redacted representation. An unclassified field resolves to logSecret, so
// adding one cannot leak a secret -- but it would silently reduce a benign
// new field to ***redacted*** in every operator's debug log. Failing here
// forces an explicit classification decision instead.
func TestConfigLogClassesCoverEveryConfigField(t *testing.T) {
	t.Parallel()

	classes := configLogClasses()
	paths := configLeafFieldPaths(t)
	for _, path := range paths {
		if _, ok := classes[path]; !ok {
			t.Errorf(
				"Config field %s has no logClass: add it to one of "+
					"the logPlain/logSecret/logURI/logProviderConfig "+
					"field lists in redact.go",
				path,
			)
		}
	}
	// A stale entry means a field was renamed or removed and its
	// classification no longer applies to anything.
	for path := range classes {
		if !slices.Contains(paths, path) {
			t.Errorf(
				"redact.go classifies %s, which is not a Config field",
				path,
			)
		}
	}
}

// TestConfigLogClassesAreUnambiguous catches a field path listed under two
// different classes, where the effective class would depend on map
// iteration order.
func TestConfigLogClassesAreUnambiguous(t *testing.T) {
	t.Parallel()

	seen := make(map[string]int)
	for _, paths := range [][]string{
		logPlainConfigFields,
		logSecretConfigFields,
		logURIConfigFields,
		logProviderConfigFields,
	} {
		for _, path := range paths {
			seen[path]++
		}
	}
	for path, count := range seen {
		if count > 1 {
			t.Errorf("Config field %s is classified %d times", path, count)
		}
	}
}

// sentinelSecrets are the distinctive values planted in the configuration
// below. None of them may appear in a rendered log line.
var sentinelSecrets = []string{
	"SENTINEL-API-AUTH-TOKEN",
	"SENTINEL-KOIOS-API-KEY",
	"SENTINEL-BARK-PASSWORD",
	"SENTINEL-BARK-HOST-PASSWORD",
	"SENTINEL-MITHRIL-KEY",
	"SENTINEL-S3-SECRET-KEY",
	"SENTINEL-TOKEN-REGISTRY-PASSWORD",
	"SENTINEL-IPFS-PASSWORD",
	"SENTINEL-PG-PASSWORD",
	"SENTINEL-DSN-PASSWORD",
	"SENTINEL-DSN-KEYWORD-PASSWORD",
	"SENTINEL-UNKNOWN-PROVIDER-KEY",
	"SENTINEL-BLOCKFROST-TOKEN",
	"SENTINEL-NESTED-PROVIDER-SECRET",
	"SENTINEL-MEMPOOL-LIST-SECRET",
}

// sentinelSecretConfig plants a sentinel in every secret-bearing field and
// provider-map key, alongside non-secret values that must survive.
func sentinelSecretConfig() *Config {
	authToken := "SENTINEL-API-AUTH-TOKEN"
	certPath := "/etc/dingo/api.crt"
	return &Config{
		Network:      "preview",
		DatabasePath: "/var/lib/dingo",
		StorageMode:  "api",
		Logging:      LoggingConfig{Format: "json", Level: "debug"},
		API: APIConfig{
			TLS:  apiconfig.TLSPolicy{CertFilePath: &certPath},
			Auth: apiconfig.AuthPolicy{Token: &authToken},
		},
		KoiosParity: KoiosParityConfig{
			Enabled: true,
			APIKey:  "SENTINEL-KOIOS-API-KEY",
		},
		BarkBaseUrl: "https://bark:SENTINEL-BARK-PASSWORD@bark.example/api",
		BarkBlockDownloadHosts: []string{
			"https://blocks:SENTINEL-BARK-HOST-PASSWORD@blocks.example",
			"https://plain.example",
		},
		Mithril: MithrilConfig{
			AggregatorURL: "https://aggregator.example/aggregator" +
				"?apiKey=SENTINEL-MITHRIL-KEY&network=preview",
		},
		TokenRegistry: TokenRegistryConfig{
			SourceURL: "https://reg:SENTINEL-TOKEN-REGISTRY-PASSWORD" +
				"@registry.example/registry.tar.gz",
		},
		OffchainMetadata: OffchainMetadataConfig{
			IPFSGatewayURL: "https://ipfs:SENTINEL-IPFS-PASSWORD" +
				"@ipfs.example/ipfs/",
		},
		DatabaseLifecycle: DatabaseLifecycleConfig{
			SnapshotCloudDestination: "s3://snapshots/dingo" +
				"?accessKeyId=AKIAEXAMPLE" +
				"&secretKey=SENTINEL-S3-SECRET-KEY",
		},
		ShelleyVRFKey: "/etc/dingo/vrf.skey",
		Plugins: PluginsConfig{
			Storage: StoragePluginsConfig{
				Blob: hostplugin.Selection{
					Provider: "badger",
					Config:   map[string]any{"dataDir": "/var/lib/dingo/blob"},
				},
				Metadata: hostplugin.Selection{
					Provider: "postgres",
					Config: map[string]any{
						"host":     "db.example",
						"port":     5432,
						"user":     "dingo",
						"database": "dingo",
						"sslMode":  "require",
						"password": "SENTINEL-PG-PASSWORD",
						"dsn": "postgres://dingo:SENTINEL-DSN-PASSWORD" +
							"@db.example:5432/dingo?sslmode=require",
						"futureKey": "SENTINEL-UNKNOWN-PROVIDER-KEY",
					},
				},
			},
			Mempool: hostplugin.Selection{
				Provider: "dag",
				Config: map[string]any{
					"capacity": 1024,
					"futureList": []any{
						"SENTINEL-MEMPOOL-LIST-SECRET",
					},
				},
			},
			API: APIPluginsConfig{
				Blockfrost: hostplugin.Selection{
					Provider: "blockfrost",
					Config: map[string]any{
						"port": 3000,
						"auth": map[string]any{
							"mode":  "token",
							"token": "SENTINEL-BLOCKFROST-TOKEN",
						},
						"futureSection": map[string]any{
							"deeper": map[string]any{
								"anything": "SENTINEL-NESTED-" +
									"PROVIDER-SECRET",
							},
						},
					},
				},
				Mesh: hostplugin.Selection{
					Provider: "mesh",
					Config: map[string]any{
						"dsn": "mysql://mesh:" +
							"SENTINEL-DSN-KEYWORD-PASSWORD@tcp" +
							"(mesh.example:3306)/mesh",
					},
				},
			},
		},
	}
}

// TestConfigLogValueRedactsSentinelSecrets renders the configuration
// through both real slog handlers and asserts that no sentinel secret
// survives, and that non-secret values still do.
func TestConfigLogValueRedactsSentinelSecrets(t *testing.T) {
	t.Parallel()

	cfg := sentinelSecretConfig()
	handlers := map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler {
			return slog.NewTextHandler(b, nil)
		},
		"json": func(b *bytes.Buffer) slog.Handler {
			return slog.NewJSONHandler(b, nil)
		},
	}
	for name, newHandler := range handlers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			slog.New(newHandler(&buf)).Info("config", "config", cfg)
			rendered := buf.String()
			for _, sentinel := range sentinelSecrets {
				if strings.Contains(rendered, sentinel) {
					t.Errorf("rendered log leaks %s", sentinel)
				}
			}
			// Negative case: redaction must not blank the record.
			for _, want := range []string{
				"preview",
				"/var/lib/dingo",
				"/etc/dingo/api.crt",
				"/etc/dingo/vrf.skey",
				"badger",
				"postgres",
				"db.example",
				"5432",
				"dingo",
				"require",
				"aggregator.example",
				"registry.example",
				"blocks.example",
				"plain.example",
				"snapshots/dingo",
				"AKIAEXAMPLE",
				"mesh.example:3306",
				"1024",
			} {
				if !strings.Contains(rendered, want) {
					t.Errorf(
						"rendered log dropped non-secret %q:\n%s",
						want,
						rendered,
					)
				}
			}
			if !strings.Contains(rendered, redactedPlaceholder) {
				t.Errorf("rendered log has no redaction marker:\n%s",
					rendered)
			}
		})
	}
}

// TestConfigLogValueNil covers a nil receiver, which slog resolves the same
// way as any other LogValuer.
func TestConfigLogValueNil(t *testing.T) {
	t.Parallel()

	var cfg *Config
	if got := cfg.LogValue().String(); got != "<nil>" {
		t.Errorf("nil Config LogValue = %q, want <nil>", got)
	}
}

func TestRedactURICredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "postgres url keeps host database and sslmode",
			in: "postgres://dingo:hunter2@db.example:5432/dingo" +
				"?sslmode=require",
			want: "postgres://dingo:" + redactedPlaceholder +
				"@db.example:5432/dingo?sslmode=require",
		},
		{
			name: "mysql dsn without scheme",
			in:   "dingo:hunter2@tcp(db.example:3306)/dingo",
			want: "dingo:" + redactedPlaceholder +
				"@tcp(db.example:3306)/dingo",
		},
		{
			name: "keyword form dsn",
			in:   "host=db.example user=dingo password=hunter2 dbname=dingo",
			want: "host=db.example user=dingo password=" +
				redactedPlaceholder + " dbname=dingo",
		},
		{
			name: "credential query parameter",
			in:   "https://aggregator.example/x?apiKey=abc123&network=preview",
			want: "https://aggregator.example/x?apiKey=" +
				redactedPlaceholder + "&network=preview",
		},
		{
			name: "no credentials is unchanged",
			in:   "https://aggregator.example/aggregator",
			want: "https://aggregator.example/aggregator",
		},
		{
			name: "username without password is not a credential",
			in:   "https://dingo@bark.example/api",
			want: "https://dingo@bark.example/api",
		},
		{
			name: "ipv6 host without userinfo is unchanged",
			in:   "http://[::1]:3000/metrics",
			want: "http://[::1]:3000/metrics",
		},
		{
			name: "bucket uri is unchanged",
			in:   "s3://snapshots/dingo",
			want: "s3://snapshots/dingo",
		},
		{
			name: "at sign in path is not userinfo",
			in:   "https://registry.example/a@b/registry.tar.gz",
			want: "https://registry.example/a@b/registry.tar.gz",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := redactURICredentials(tc.in); got != tc.want {
				t.Errorf(
					"redactURICredentials(%q) = %q, want %q",
					tc.in,
					got,
					tc.want,
				)
			}
		})
	}
}

// TestProviderConfigUnknownKeyIsRedacted pins the fail-safe default for a
// provider configuration map: a key nobody has classified is redacted, at
// any nesting depth, rather than logged on the chance that it is benign.
func TestProviderConfigUnknownKeyIsRedacted(t *testing.T) {
	t.Parallel()

	value := providerConfigValue(reflect.ValueOf(map[string]any{
		"host":       "db.example",
		"futureKey":  "unclassified-secret",
		"futureTree": map[string]any{"deeper": "unclassified-nested-secret"},
	}))
	rendered := value.String()
	for _, unwanted := range []string{
		"unclassified-secret",
		"unclassified-nested-secret",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Errorf("provider config leaks %q: %s", unwanted, rendered)
		}
	}
	if !strings.Contains(rendered, "db.example") {
		t.Errorf("provider config dropped classified host: %s", rendered)
	}
}
