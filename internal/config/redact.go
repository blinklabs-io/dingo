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
	"log/slog"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
)

// redactedPlaceholder replaces a secret-bearing value in a rendered log. It
// matches internal/apiconfig's own AuthPolicy.LogValue placeholder so an
// operator sees one spelling everywhere.
const redactedPlaceholder = "***redacted***"

// logClass says how one configuration value is rendered for logging.
//
// logSecret is deliberately the zero value: an unclassified field path or an
// unrecognized provider-config map key looks up as logSecret, so a value
// nobody has classified is redacted rather than logged. That makes omission
// fail safe instead of leaking, and
// TestConfigLogClassesCoverEveryConfigField turns the resulting silent
// over-redaction of a newly added benign field into a test failure, forcing
// an explicit decision.
type logClass uint8

const (
	// logSecret replaces a non-zero value with redactedPlaceholder.
	logSecret logClass = iota
	// logPlain renders the value as-is.
	logPlain
	// logURI renders a URI or database DSN with only its credential
	// components removed, keeping scheme, host, port, path, and
	// non-credential parameters so the value stays diagnosable.
	logURI
	// logProviderConfig recursively renders a plugin provider's
	// free-form configuration map, classifying each key by name.
	logProviderConfig
)

// logSecretConfigFields are the Config field paths (dotted Go field names)
// whose values are secrets in themselves and are never rendered.
var logSecretConfigFields = []string{
	// Inline shared secret for API token authentication.
	"API.Auth.Token",
	// Koios Bearer token.
	"KoiosParity.APIKey",
}

// logURIConfigFields are Config field paths holding a URI that an operator
// may have embedded credentials in, either as userinfo
// ("scheme://user:password@host") or as a credential-shaped query
// parameter.
var logURIConfigFields = []string{
	"BarkBaseUrl",
	"BarkBlockDownloadHosts",
	"DatabaseLifecycle.SnapshotCloudDestination",
	"Mithril.AggregatorURL",
	"OffchainMetadata.IPFSGatewayURL",
	"TokenRegistry.SourceURL",
}

// logProviderConfigFields are Config field paths holding a plugin
// provider's free-form configuration map. Their contents are provider
// defined -- a Postgres or MySQL provider accepts "password" and "dsn", an
// API provider accepts a nested "auth.token" -- so they are walked
// recursively and classified per key rather than as a whole.
var logProviderConfigFields = []string{
	"Plugins.API.Blockfrost.Config",
	"Plugins.API.Mesh.Config",
	"Plugins.API.Utxorpc.Config",
	"Plugins.Mempool.Config",
	"Plugins.Storage.Blob.Config",
	"Plugins.Storage.Metadata.Config",
}

// logPlainConfigFields are the Config field paths carrying no secret. Every
// entry is an explicit statement that the value is safe to persist in a
// debug log. Filesystem paths (including key and certificate file paths),
// on-chain policy IDs and addresses, network and peer tuning, and public
// key registries all belong here; the key material behind a path does not
// pass through Config at all.
var logPlainConfigFields = []string{
	"API.Auth.Mode",
	"API.Auth.TokenFilePath",
	"API.TLS.CertFilePath",
	"API.TLS.KeyFilePath",
	"API.TLS.Mode",
	"ActivePeersGossipQuota",
	"ActivePeersLedgerQuota",
	"ActivePeersTopologyQuota",
	"BackfillBatchSize",
	"BarkClientCAFilePath",
	"BarkHost",
	"BarkPort",
	"BindAddr",
	"BlockPipelineEnabled",
	"BlockPipelineValidateEnabled",
	"BlockProducer",
	"CORSAllowedOrigins",
	"Cache.BlockLRUEntries",
	"Cache.HotTxEntries",
	"Cache.HotTxMaxBytes",
	"Cache.HotUtxoEntries",
	"Cache.WarmupBlocks",
	"Cache.WarmupSync",
	"CardanoConfig",
	"Chainsync.MaxClients",
	"Chainsync.StallTimeout",
	"Chainsync.Strategy",
	"DatabaseLifecycle.SnapshotCloudDestinationPrefix",
	"DatabaseLifecycle.SnapshotDir",
	"DatabaseLifecycle.SnapshotEnabled",
	"DatabaseLifecycle.SnapshotEveryNEpochs",
	"DatabaseLifecycle.SnapshotRetention",
	"DatabasePath",
	"DatabaseQueueSize",
	"DatabaseWorkers",
	"DebugPort",
	"DelegatorInactivity",
	"DelegatorInactivityEnabled",
	"ForgeStaleGapThresholdSlots",
	"ForgeSyncToleranceSlots",
	"FullPotRewardsEnabled",
	"GenesisBootstrap.CorroborationPeers",
	"GenesisBootstrap.Enabled",
	"GenesisBootstrap.PromotionMinDiversityGroups",
	"GenesisBootstrap.WindowSlots",
	"HistoryExpiry.Enabled",
	"HistoryExpiry.Frequency",
	"ImmutableDbPath",
	"InactivityTimeout",
	"InboundCooldown",
	"InboundDuplexOnlyForHot",
	"InboundHotQuota",
	"InboundHotScoreThreshold",
	"InboundMinTenure",
	"InboundPruneAfter",
	"InboundWarmTarget",
	"IntersectTip",
	"KoiosParity.AccountChunkMaxBytes",
	"KoiosParity.AccountChunkSize",
	"KoiosParity.Accounts",
	"KoiosParity.CachePath",
	"KoiosParity.Enabled",
	"KoiosParity.GraceHours",
	"KoiosParity.Network",
	"KoiosParity.Strict",
	"LedgerCatchupTimeout",
	"LeiosVoteSigningKeyFile",
	"LeiosVoterPublicKeys",
	"Logging.Format",
	"Logging.Level",
	"MaxConnectionsPerIP",
	"MaxInboundConns",
	"MaxKESEvolutions",
	"MetricsPort",
	"Midnight.AuthTokenAssetName",
	"Midnight.AuthTokenPolicyID",
	"Midnight.CNightAssetName",
	"Midnight.CNightPolicyID",
	"Midnight.CommitteeCandidateAddress",
	"Midnight.CouncilAddress",
	"Midnight.CouncilPolicyID",
	"Midnight.Enabled",
	"Midnight.Host",
	"Midnight.MappingValidatorAddress",
	"Midnight.PermissionedCandidatePolicy",
	"Midnight.Port",
	"Midnight.TechnicalCommitteeAddress",
	"Midnight.TechnicalCommitteePolicyID",
	"MinHotPeers",
	"MinPoolMargin",
	"Mithril.AllowInsecureHTTP",
	"Mithril.Backend",
	"Mithril.CleanupAfterLoad",
	"Mithril.DownloadDir",
	"Mithril.DownloadIdleTimeout",
	"Mithril.DownloadMaxIdleRetries",
	"Mithril.Enabled",
	"Mithril.VerifyCertificates",
	"Network",
	"NetworkMagic",
	"OffchainMetadata.AllowPrivateAddresses",
	"OffchainMetadata.BatchSize",
	"OffchainMetadata.Interval",
	"OffchainMetadata.MaxBytes",
	"OffchainMetadata.RequestTimeout",
	"OffchainMetadata.UserAgent",
	"PeerSharing",
	"PledgeLeverage",
	"PledgeLeverageEnabled",
	"Plugins.API.Blockfrost.Provider",
	"Plugins.API.Mesh.Provider",
	"Plugins.API.Utxorpc.Provider",
	"Plugins.Mempool.Provider",
	"Plugins.Storage.Blob.Provider",
	"Plugins.Storage.Metadata.Provider",
	"PrivateBindAddr",
	"PrivatePort",
	"ReconcileInterval",
	"RelayPort",
	"RunMode",
	"ShelleyKESKey",
	"ShelleyOperationalCertificate",
	"ShelleyVRFKey",
	"ShutdownTimeout",
	"SlotsPerKESPeriod",
	"SocketPath",
	"StartEra",
	"StorageMode",
	"StrictUtxoValidation",
	"TargetNumberOfActivePeers",
	"TargetNumberOfEstablishedPeers",
	"TargetNumberOfKnownPeers",
	"TlsCertFilePath",
	"TlsKeyFilePath",
	"TokenRegistry.AllowPrivateAddresses",
	"TokenRegistry.Enabled",
	"TokenRegistry.Interval",
	"TokenRegistry.MaxBytes",
	"TokenRegistry.MaxEntryBytes",
	"TokenRegistry.RequestTimeout",
	"TokenRegistry.StoreLogos",
	"TokenRegistry.UserAgent",
	"Topology",
	"Tracing",
	"TracingStdout",
	"UnsafeFullPotRewardsOnStandardNetworks",
	"ValidateForgedBlock",
	"ValidateHistorical",
}

// configLogClasses maps a dotted Config field path to its logClass. A path
// absent from it resolves to logSecret; see logClass.
var configLogClasses = sync.OnceValue(func() map[string]logClass {
	classes := make(map[string]logClass)
	for class, paths := range map[logClass][]string{
		logPlain:          logPlainConfigFields,
		logSecret:         logSecretConfigFields,
		logURI:            logURIConfigFields,
		logProviderConfig: logProviderConfigFields,
	} {
		for _, path := range paths {
			classes[path] = class
		}
	}
	return classes
})

// providerConfigKeyClasses classifies the keys a compiled-in plugin
// provider accepts in its free-form configuration map. A key absent from it
// resolves to logSecret, so an out-of-tree or newly added provider key is
// redacted until it is classified here.
var providerConfigKeyClasses = sync.OnceValue(func() map[string]logClass {
	classes := make(map[string]logClass)
	plain := []string{
		// database/plugin/metadata/sqlite
		"datadir", "maxconnections",
		// database/plugin/metadata/{mysql,postgres}
		"host", "port", "user", "database", "sslmode", "timezone",
		"poolmaxopenconns", "poolmaxidleconns", "poolconnmaxlifetime",
		// database/plugin/blob/{aws,gcs}
		"bucket", "region", "prefix", "timeout",
		// database/plugin/blob/badger
		"blockcachesize", "indexcachesize", "valuelogfilesize",
		"memtablesize", "valuethreshold", "gc", "compression",
		"compressionlevel",
		// mempool
		"capacity", "evictionwatermark", "rejectionwatermark",
		"revalidationdeltacap",
		// api/{blockfrost,mesh,utxorpc} tls and auth policies
		"mode", "certfilepath", "keyfilepath", "tokenfilepath",
	}
	uri := []string{"dsn", "endpoint", "url"}
	secret := []string{"password", "token"}
	for class, keys := range map[logClass][]string{
		logPlain:  plain,
		logURI:    uri,
		logSecret: secret,
	} {
		for _, key := range keys {
			classes[key] = class
		}
	}
	return classes
})

// LogValue renders c for structured logging with every secret-bearing
// value replaced by redactedPlaceholder, so `slog` never persists a Koios
// API key, an inline API auth token, a provider password, or a DSN
// credential. Unexported fields are not rendered at all.
//
// The walk is uniform and does not defer to a nested type's own
// slog.LogValuer implementation: one classification table with one
// exhaustiveness test is the only thing that decides what is logged, so a
// nested LogValue cannot become a second, untested source of truth.
func (c *Config) LogValue() slog.Value {
	if c == nil {
		return slog.StringValue("<nil>")
	}
	return slog.GroupValue(
		configLogAttrs(reflect.ValueOf(c).Elem(), "")...,
	)
}

// configLogAttrs renders the exported fields of struct value v, whose
// dotted Config field path is prefix.
func configLogAttrs(v reflect.Value, prefix string) []slog.Attr {
	rt := v.Type()
	attrs := make([]slog.Attr, 0, rt.NumField())
	for i := range rt.NumField() {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		name := logFieldName(field)
		path := prefix + field.Name
		value := v.Field(i)
		for value.Kind() == reflect.Pointer {
			if value.IsNil() {
				attrs = append(attrs, slog.Any(name, nil))
				value = reflect.Value{}
				break
			}
			value = value.Elem()
		}
		if !value.IsValid() {
			continue
		}
		if value.Kind() == reflect.Struct {
			attrs = append(attrs, slog.Attr{
				Key: name,
				Value: slog.GroupValue(
					configLogAttrs(value, path+".")...,
				),
			})
			continue
		}
		attrs = append(attrs, slog.Attr{
			Key:   name,
			Value: classedValue(value, configLogClasses()[path]),
		})
	}
	return attrs
}

// logFieldName is the attribute key for a Config field: its yaml key when
// it has one, matching what an operator writes in dingo.yaml, otherwise the
// Go field name.
func logFieldName(field reflect.StructField) string {
	tag, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
	if tag == "" || tag == "-" {
		return field.Name
	}
	return tag
}

// classedValue renders a non-struct configuration value according to class.
func classedValue(v reflect.Value, class logClass) slog.Value {
	switch class {
	case logProviderConfig:
		return providerConfigValue(v)
	case logURI:
		return mappedValue(v, redactURICredentials)
	case logPlain:
		return mappedValue(v, func(s string) string { return s })
	case logSecret:
		return redactedValue(v)
	default:
		return redactedValue(v)
	}
}

// redactedValue replaces a secret-bearing value with redactedPlaceholder. A
// zero value is rendered as itself: "the token is unset" is what an
// operator needs to see, and it discloses nothing.
func redactedValue(v reflect.Value) slog.Value {
	if v.IsZero() {
		return slog.AnyValue(v.Interface())
	}
	return slog.StringValue(redactedPlaceholder)
}

// mappedValue renders v, applying transform to every string it contains so
// a slice or map of URIs is handled element by element rather than as one
// opaque value.
func mappedValue(v reflect.Value, transform func(string) string) slog.Value {
	switch v.Kind() { //nolint:exhaustive // reflect.Kind default is intended
	case reflect.String:
		return slog.StringValue(transform(v.String()))
	case reflect.Slice, reflect.Array:
		items := make([]string, 0, v.Len())
		for i := range v.Len() {
			item := v.Index(i)
			if item.Kind() != reflect.String {
				return slog.AnyValue(v.Interface())
			}
			items = append(items, transform(item.String()))
		}
		return slog.AnyValue(items)
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String ||
			v.Type().Elem().Kind() != reflect.String {
			return slog.AnyValue(v.Interface())
		}
		attrs := make([]slog.Attr, 0, v.Len())
		for _, key := range sortedStringKeys(v) {
			attrs = append(attrs, slog.String(
				key,
				transform(v.MapIndex(reflect.ValueOf(key)).String()),
			))
		}
		return slog.GroupValue(attrs...)
	default:
		return slog.AnyValue(v.Interface())
	}
}

// providerConfigValue renders a plugin provider's free-form configuration
// map, classifying each key by name and recursing into nested maps so a
// secret nested under any depth of provider sections is still redacted.
func providerConfigValue(v reflect.Value) slog.Value {
	if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
		return slog.StringValue(redactedPlaceholder)
	}
	attrs := make([]slog.Attr, 0, v.Len())
	for _, key := range sortedStringKeys(v) {
		entry := unwrapInterface(v.MapIndex(reflect.ValueOf(key)))
		class := providerConfigKeyClasses()[strings.ToLower(key)]
		attrs = append(attrs, slog.Attr{
			Key:   key,
			Value: providerConfigEntry(entry, class),
		})
	}
	return slog.GroupValue(attrs...)
}

// providerConfigEntry renders one provider configuration entry. A nested
// map is recursed into so its own keys are classified by name; anything
// else is rendered according to its key's class.
func providerConfigEntry(v reflect.Value, class logClass) slog.Value {
	if !v.IsValid() {
		return slog.AnyValue(nil)
	}
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
		return providerConfigValue(v)
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		items := make([]any, 0, v.Len())
		for i := range v.Len() {
			item := providerConfigEntry(
				unwrapInterface(v.Index(i)),
				class,
			)
			items = append(items, item.Any())
		}
		return slog.AnyValue(items)
	}
	return classedValue(v, class)
}

// unwrapInterface resolves an interface-typed reflect.Value to the value it
// holds, which is what a map[string]any's entries always are.
func unwrapInterface(v reflect.Value) reflect.Value {
	for v.IsValid() && v.Kind() == reflect.Interface {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

// sortedStringKeys returns v's string keys in sorted order, so a rendered
// map is stable across log lines.
func sortedStringKeys(v reflect.Value) []string {
	keys := make([]string, 0, v.Len())
	for _, key := range v.MapKeys() {
		keys = append(keys, key.String())
	}
	slices.Sort(keys)
	return keys
}

// uriCredentialParam matches a credential-shaped parameter in a URI query
// string or in a keyword-form database DSN
// ("host=db user=dingo password=hunter2").
var uriCredentialParam = regexp.MustCompile(
	`(?i)\b(password|passwd|pwd|secret|token|api[-_]?key|` +
		`access[-_]?key|secret[-_]?key|sig|signature)\s*=\s*[^\s;&]*`,
)

// redactURICredentials removes the credential components of a URI or
// database DSN -- the userinfo password and any credential-shaped
// parameter -- while keeping scheme, host, port, path, database name, and
// every other parameter. A DSN redacted whole loses the operational value
// of knowing which host and database the node was pointed at, which is
// most of the reason the configuration is logged at all.
func redactURICredentials(s string) string {
	if s == "" {
		return s
	}
	s = uriCredentialParam.ReplaceAllString(
		s,
		"${1}="+redactedPlaceholder,
	)
	return redactURIUserinfo(s)
}

// redactURIUserinfo replaces the password half of a "user:password@"
// userinfo component. It handles both "scheme://user:password@host/path"
// and the schemeless MySQL DSN form "user:password@tcp(host:3306)/db".
func redactURIUserinfo(s string) string {
	start := 0
	if i := strings.Index(s, "://"); i >= 0 {
		start = i + len("://")
	}
	end := len(s)
	if i := strings.IndexAny(s[start:], "/?#"); i >= 0 {
		end = start + i
	}
	at := strings.LastIndex(s[start:end], "@")
	if at < 0 {
		return s
	}
	userinfo := s[start : start+at]
	colon := strings.Index(userinfo, ":")
	if colon < 0 {
		// A username with no password is not a credential.
		return s
	}
	return s[:start+colon+1] + redactedPlaceholder + s[start+at:]
}
