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
	"net/url"
	"reflect"
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
	// logProviderSection is the class of a provider configuration key
	// that names a nested section rather than a value of its own. The
	// section is walked so its own keys are classified by name, and a
	// value of any other shape at the same key is redacted: the key says
	// only that a container belongs there, so it classifies nothing
	// about a scalar, a slice, or a map this walk cannot key into.
	logProviderSection
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
	"BarkOperatorCertificateFingerprints",
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
	"DebugBindAddr",
	"DebugPort",
	"DelegatorInactivity",
	"DelegatorInactivityEnabled",
	"ForgeHeaderFrontierToleranceSlots",
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
	"Logging.Format",
	"Logging.Level",
	"MaxConnectionsPerIP",
	"MaxInboundConns",
	"MaxKESEvolutions",
	"MetricsPort",
	"Midnight.AuthTokenAssetName",
	"Midnight.AuthTokenPolicyID",
	"Midnight.AllowInsecureRemote",
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
	"Midnight.ReflectionEnabled",
	"Midnight.ServerEnabled",
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
	"TargetNumberOfRootPeers",
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

// providerConfigPlainKeys are the lower-cased keys a compiled-in plugin
// provider accepts in its free-form configuration map that carry no
// secret.
var providerConfigPlainKeys = []string{
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
	// api/{blockfrost,mesh,utxorpc} policy keys inside the tls and auth
	// sections
	"mode", "certfilepath", "keyfilepath", "tokenfilepath",
}

// providerConfigSectionKeys are provider configuration keys whose value is
// a nested section of further keys. The API providers nest their tls and
// auth policies there, so the section has to stay walkable or the whole
// policy disappears from a startup log -- but only a section is walkable,
// and classifying these keys separately from the values that carry no
// secret is what keeps a non-section value at the same key from being
// rendered as one.
var providerConfigSectionKeys = []string{"auth", "tls"}

// providerConfigURIKeys are provider configuration keys holding a URI or
// database DSN, rendered with only their credential components removed.
var providerConfigURIKeys = []string{"dsn", "endpoint", "url"}

// providerConfigSecretKeys are provider configuration keys whose value is
// a secret in itself. isCredentialKeyName reaches the same verdict for
// each of them, and TestProviderConfigKeyTableAgreesWithClassifier holds
// the two together.
var providerConfigSecretKeys = []string{"password", "token"}

// providerConfigKeyClasses classifies the keys a compiled-in plugin
// provider accepts in its free-form configuration map. A key absent from it
// resolves to logSecret, so an out-of-tree or newly added provider key is
// redacted until it is classified here.
var providerConfigKeyClasses = sync.OnceValue(func() map[string]logClass {
	classes := make(map[string]logClass)
	for class, keys := range map[logClass][]string{
		logPlain:           providerConfigPlainKeys,
		logProviderSection: providerConfigSectionKeys,
		logURI:             providerConfigURIKeys,
		logSecret:          providerConfigSecretKeys,
	} {
		for _, key := range keys {
			classes[key] = class
		}
	}
	return classes
})

// providerConfigKeyClass classifies one key of a provider configuration
// map. isCredentialKeyName decides first, so a credential-shaped key can
// never be rendered even if it were added to providerConfigPlainKeys by
// mistake; the provider key set is open-world, which is why this path
// carries a name-shape guard while the exhaustively enumerated Config
// field paths do not.
func providerConfigKeyClass(key string) logClass {
	if isCredentialKeyName(key) {
		return logSecret
	}
	return providerConfigKeyClasses()[strings.ToLower(key)]
}

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

// classedValue renders a non-struct configuration value according to
// class. Every class that renders anything states which shapes it can
// render, and a value of any other shape is redacted rather than rendered
// through a rule that does not apply to it.
func classedValue(v reflect.Value, class logClass) slog.Value {
	switch class {
	case logProviderConfig:
		return providerConfigValue(v)
	case logURI:
		return mappedValue(v, redactURICredentials, redactedValue)
	case logPlain:
		return mappedValue(
			v,
			func(s string) string { return s },
			plainValue,
		)
	case logProviderSection:
		// A section is rendered by providerConfigEntry, which reaches
		// here only for a value that is not one.
		return redactedValue(v)
	case logSecret:
		return redactedValue(v)
	default:
		return redactedValue(v)
	}
}

// plainValue renders v as itself.
func plainValue(v reflect.Value) slog.Value {
	return slog.AnyValue(v.Interface())
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
//
// A shape holding no strings to transform -- a map of slices, a slice of
// structs -- cannot be transformed at all, so fallback decides it.
// Rendering such a value whole is right for a plain field and would leak
// an untransformed credential under a URI one.
func mappedValue(
	v reflect.Value,
	transform func(string) string,
	fallback func(reflect.Value) slog.Value,
) slog.Value {
	switch v.Kind() { //nolint:exhaustive // reflect.Kind default is intended
	case reflect.String:
		return slog.StringValue(transform(v.String()))
	case reflect.Slice, reflect.Array:
		items := make([]string, 0, v.Len())
		for i := range v.Len() {
			item := v.Index(i)
			if item.Kind() != reflect.String {
				return fallback(v)
			}
			items = append(items, transform(item.String()))
		}
		return slog.AnyValue(items)
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String ||
			v.Type().Elem().Kind() != reflect.String {
			return fallback(v)
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
		return fallback(v)
	}
}

// providerConfigValue renders a plugin provider's free-form configuration
// map, classifying each key by name and recursing into nested maps so a
// secret nested under any depth of provider sections is still redacted.
func providerConfigValue(v reflect.Value) slog.Value {
	if !isProviderConfigSection(v) {
		return slog.StringValue(redactedPlaceholder)
	}
	attrs := make([]slog.Attr, 0, v.Len())
	for _, key := range sortedStringKeys(v) {
		entry := unwrapInterface(v.MapIndex(reflect.ValueOf(key)))
		class := providerConfigKeyClass(key)
		attrs = append(attrs, slog.Attr{
			Key:   key,
			Value: providerConfigEntry(entry, class),
		})
	}
	return slog.GroupValue(attrs...)
}

// providerConfigEntry renders one provider configuration entry. A nested
// section or slice under a non-secret key is walked so its own keys are
// classified by name; anything else is rendered according to its key's
// class.
//
// A logSecret class -- which is also the class of an unrecognized key --
// covers the whole subtree and is applied before any recursion. Walking
// into it would reclassify the inner keys by their own names, and an
// inner key that happens to be classified plain ("host", "mode") would
// then render part of a value whose enclosing key is a secret.
//
// A logProviderSection class is the one place where the key's class and
// the value's shape have to agree: the key says a container belongs
// there, so a section is walked and anything else is redacted. Deciding
// that here, once, is what keeps every container key from needing its own
// shape check.
func providerConfigEntry(v reflect.Value, class logClass) slog.Value {
	if !v.IsValid() {
		return slog.AnyValue(nil)
	}
	if class == logSecret {
		return redactedValue(v)
	}
	section := isProviderConfigSection(v)
	if class == logProviderSection {
		if !section {
			return redactedValue(v)
		}
		return providerConfigValue(v)
	}
	if section {
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

// isProviderConfigSection reports whether v is a nested provider
// configuration section: a map this walk can key into and classify entry
// by entry.
func isProviderConfigSection(v reflect.Value) bool {
	return v.Kind() == reflect.Map &&
		v.Type().Key().Kind() == reflect.String
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

// redactURICredentials removes the credential components of a URI or
// database DSN -- the userinfo password and the value of every
// credential-named parameter -- while keeping scheme, host, port, path,
// database name, and every other parameter. A DSN redacted whole loses the
// operational value of knowing which host and database the node was
// pointed at, which is most of the reason the configuration is logged at
// all.
func redactURICredentials(s string) string {
	if s == "" {
		return s
	}
	return redactURIUserinfo(redactCredentialParams(s))
}

// redactCredentialParams redacts the value of every credential-named
// "name=value" parameter in s. The decision is made per parameter name
// through isCredentialKeyName, not by matching credential shapes against
// the whole string.
//
// A URI's parameters live in its query string, where net/url decides
// where the query begins and '&' or ';' separates the pairs. A
// keyword-form database DSN ("host=db user=dingo password='hunter 2'")
// has no query string: its pairs are whitespace separated, whitespace
// around '=' is optional, and a value containing whitespace is quoted.
func redactCredentialParams(s string) string {
	if start, end, ok := uriQuerySpan(s); ok {
		return s[:start] +
			redactParams(s[start:end], uriQuerySyntax) +
			s[end:]
	}
	return redactParams(s, keywordDSNSyntax)
}

// paramSyntax describes how one parameter form spells its "name=value"
// pairs, so one scanner serves both forms and neither can be given the
// other's rules by accident.
type paramSyntax struct {
	// delims are the bytes that separate one pair from the next.
	delims string
	// encoded reports whether a name carries percent- and plus-escapes,
	// as a URI query's names do. Those escape bytes belong to the name:
	// without them "api%5Fkey" scans as the two fragments "api" and
	// "5Fkey", and neither of those reads as a credential.
	encoded bool
}

var (
	// uriQuerySyntax is a URI query string: '&' or ';' separates the
	// pairs and the names are percent-encoded.
	uriQuerySyntax = paramSyntax{delims: "&;", encoded: true}
	// keywordDSNSyntax is the keyword-form database DSN: whitespace
	// separates the pairs and the keywords are literal, because no DSN
	// parser percent-decodes them.
	keywordDSNSyntax = paramSyntax{delims: " \t\r\n"}
)

// isNameByte reports whether b belongs to a parameter name written in this
// syntax.
func (syntax paramSyntax) isNameByte(b byte) bool {
	if syntax.encoded && (b == '%' || b == '+') {
		return true
	}
	return isKeyNameByte(b)
}

// uriQuerySpan returns the bounds of s's URI query string, if it has one.
// net/url does the parsing, so a keyword DSN -- which is not a URI -- does
// not get its whitespace-separated keywords treated as query parameters.
// The span is verified against RawQuery before it is used, so a URI whose
// '?' net/url located differently is left to the keyword scanner instead
// of being spliced at the wrong offset.
func uriQuerySpan(s string) (int, int, bool) {
	parsed, err := url.Parse(s)
	if err != nil || parsed.RawQuery == "" {
		return 0, 0, false
	}
	mark := strings.IndexByte(s, '?')
	if mark < 0 {
		return 0, 0, false
	}
	start := mark + 1
	end := start + len(parsed.RawQuery)
	if end > len(s) || s[start:end] != parsed.RawQuery {
		return 0, 0, false
	}
	return start, end, true
}

// redactParams replaces the value of every credential-named parameter in
// s, whose parameters are spelled in syntax. Separators, ordering,
// quoting, name encoding, and every non-credential value are copied
// through byte for byte, because a redacted DSN is only useful if the
// host, database, and options it names survive. Only the classification
// of a name decodes it; the output keeps the operator's own bytes.
func redactParams(s string, syntax paramSyntax) string {
	var out strings.Builder
	out.Grow(len(s))
	for i := 0; i < len(s); {
		skipped := i
		for i < len(s) && !syntax.isNameByte(s[i]) {
			i++
		}
		out.WriteString(s[skipped:i])
		nameEnd := i
		for nameEnd < len(s) && syntax.isNameByte(s[nameEnd]) {
			nameEnd++
		}
		name := s[i:nameEnd]
		if name == "" {
			// i is at the end of s; the loop condition ends the walk.
			continue
		}
		out.WriteString(name)
		i = nameEnd
		valueStart, ok := paramValueStart(s, nameEnd, syntax)
		if !ok {
			continue
		}
		valueEnd := paramValueEnd(s, valueStart, syntax)
		if valueEnd > valueStart && isCredentialKeyName(name) {
			out.WriteString(s[nameEnd:valueStart])
			out.WriteString(redactedPlaceholder)
		} else {
			out.WriteString(s[nameEnd:valueEnd])
		}
		i = valueEnd
	}
	return out.String()
}

// paramValueStart returns the index at which the value of the parameter
// whose name ends at nameEnd begins, and whether the parameter has a
// value at all.
//
// The keyword DSN form permits whitespace on both sides of '='. Trailing
// whitespace followed by another "name=" pair instead means this
// parameter's value is empty; consuming the next pair as the value would
// hide a non-credential option behind the placeholder.
func paramValueStart(s string, nameEnd int, syntax paramSyntax) (int, bool) {
	i := skipParamSpace(s, nameEnd)
	if i >= len(s) || s[i] != '=' {
		return 0, false
	}
	i++
	if spaced := skipParamSpace(s, i); spaced != i &&
		!startsParam(s, spaced, syntax) {
		i = spaced
	}
	return i, true
}

// paramValueEnd returns the index one past the value beginning at i. A
// quoted value ends at its closing quote, so a keyword DSN password
// containing whitespace is redacted whole rather than up to its first
// space; an unquoted value ends at the first byte in delims.
func paramValueEnd(s string, i int, syntax paramSyntax) int {
	if i < len(s) && (s[i] == '\'' || s[i] == '"') {
		quote := s[i]
		for j := i + 1; j < len(s); {
			switch s[j] {
			case '\\':
				j += 2
			case quote:
				return j + 1
			default:
				j++
			}
		}
		return len(s)
	}
	for ; i < len(s); i++ {
		if strings.IndexByte(syntax.delims, s[i]) >= 0 {
			return i
		}
	}
	return len(s)
}

// startsParam reports whether a new "name=" pair begins at i.
func startsParam(s string, i int, syntax paramSyntax) bool {
	end := i
	for end < len(s) && syntax.isNameByte(s[end]) {
		end++
	}
	if end == i {
		return false
	}
	end = skipParamSpace(s, end)
	return end < len(s) && s[end] == '='
}

// skipParamSpace returns the index of the first byte at or after i that is
// not whitespace.
func skipParamSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' ||
		s[i] == '\r' || s[i] == '\n') {
		i++
	}
	return i
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

// A key name is classified per whole word, never per substring. A
// \b-anchored regular expression over the raw name is the wrong shape for
// this job twice over: '_' is a word character, so `\bsecret` cannot match
// "client_secret" and `\btoken` cannot match "api_token"; and
// `access[-_]?key` cannot reach the '=' of "accessKeyId=" past the "Id"
// suffix. Both misses leak. Enumerating spellings instead --
// accessKeyId, client_secret, api_token, x-api-key, authToken,
// refreshToken, privateKey, sasToken, SharedAccessSignature -- only moves
// the next miss one spelling further out.
//
// So a name is decoded and then decomposed into words: at separators
// ("client_secret", "x-api-key", "auth.token"), at camelCase and acronym
// boundaries ("accessKeyId", "IPFSGatewayURL"), and, for a run-together
// spelling such as "apikey" or "accesskeyid", by segmenting the word
// against the vocabulary below -- which succeeds only when known words
// cover the word completely, so "monkey" and "keyspace" do not become
// credentials. The decision is then set membership over those words, and
// no prefix, suffix, separator, or percent-escape can move a term out of
// reach.

// credentialWords name a credential themselves: one of them anywhere in a
// key name means the value is a credential.
var credentialWords = []string{
	"credential", "credentials",
	"pass", "passphrase", "passwd", "password", "passwords", "pwd",
	// Azure shared access signature, spelled "sas" in its key names.
	"sas",
	"secret", "secrets",
	"sig", "signature", "signatures",
	"token", "tokens",
}

// keyWords name a key, which is a credential only when a qualifier says
// which key it is: "privateKey" and "accessKeyId" are credentials,
// "publicKeys" and "shelleyVrfKey" are not.
var keyWords = []string{"key", "keys"}

// credentialQualifierWords turn an accompanying key word into a
// credential.
var credentialQualifierWords = []string{
	"access", "account", "api", "app", "application", "auth", "bearer",
	"client", "consumer", "encryption", "master", "private", "refresh",
	"secret", "service", "session", "shared", "sign", "signing",
	"subscription",
}

// locationWords name where a credential is kept rather than the
// credential itself: "tokenFilePath", "signingKeyFile", and "dataDir"
// hold a path, and which path was configured is exactly what an operator
// needs from a startup log. A name containing one of these is therefore
// not a credential. The trade is that a secret written inline under a
// "...File" name would be rendered; the alternative redacts every
// configured path, including the ones this logging exists to show.
var locationWords = []string{
	"dir", "directory", "file", "folder", "path",
}

// keyNameFillerWords carry no classification of their own. They exist so
// that a run-together spelling segments: "accesskeyid" is access+key+id.
var keyNameFillerWords = []string{"id", "ids", "name", "names"}

// keyNameVocabulary is every word the classifier knows, used to segment a
// run-together key name.
var keyNameVocabulary = sync.OnceValue(func() []string {
	var vocabulary []string
	for _, words := range [][]string{
		credentialWords,
		credentialQualifierWords,
		keyNameFillerWords,
		keyWords,
		locationWords,
	} {
		vocabulary = append(vocabulary, words...)
	}
	slices.Sort(vocabulary)
	return slices.Compact(vocabulary)
})

// isCredentialKeyName reports whether a value stored under the
// configuration key or URI parameter name is a credential.
//
// The name is decoded before it is split, so an encoded spelling
// classifies as the name it decodes to. Only the classification decodes:
// every caller renders the operator's original bytes.
func isCredentialKeyName(name string) bool {
	decoded, ok := decodeKeyNameEscapes(name)
	if !ok {
		// An escape that does not decode leaves the name unreadable
		// here and in whatever else would consume it, so what it names
		// is unknown and its value fails closed.
		return true
	}
	words := keyNameWords(decoded)
	switch {
	case containsWord(words, locationWords):
		return false
	case containsWord(words, credentialWords):
		return true
	default:
		return containsWord(words, keyWords) &&
			containsWord(words, credentialQualifierWords)
	}
}

// decodeKeyNameEscapes decodes the percent- and plus-escapes of a key
// name, reporting whether every escape in it was well formed. A name
// carrying no escape is its own decoding, so the common case neither
// allocates nor changes classification.
func decodeKeyNameEscapes(name string) (string, bool) {
	if !strings.ContainsAny(name, "%+") {
		return name, true
	}
	decoded, err := url.QueryUnescape(name)
	if err != nil {
		return name, false
	}
	return decoded, true
}

// containsWord reports whether any of words appears in set.
func containsWord(words, set []string) bool {
	return slices.ContainsFunc(words, func(word string) bool {
		return slices.Contains(set, word)
	})
}

// keyNameWords is name split into lower-cased words, each then segmented
// against the vocabulary.
func keyNameWords(name string) []string {
	parts := splitKeyName(name)
	words := make([]string, 0, len(parts))
	for _, part := range parts {
		words = append(words, segmentKeyWord(part)...)
	}
	return words
}

// splitKeyName splits name into lower-cased words at every
// non-alphanumeric byte and at every camelCase or acronym boundary, so
// "accessKeyId", "access_key_id", "access-key-id", "ACCESS_KEY_ID", and
// "access.key.id" all yield access, key, id.
func splitKeyName(name string) []string {
	var words []string
	start := -1
	for i := range len(name) {
		if !isKeyWordByte(name[i]) {
			if start >= 0 {
				words = append(words, strings.ToLower(name[start:i]))
				start = -1
			}
			continue
		}
		if start < 0 {
			start = i
			continue
		}
		if isKeyWordBoundary(name, i) {
			words = append(words, strings.ToLower(name[start:i]))
			start = i
		}
	}
	if start >= 0 {
		words = append(words, strings.ToLower(name[start:]))
	}
	return words
}

// isKeyWordBoundary reports whether the byte at i starts a new camelCase
// or acronym word: an upper-case byte following a lower-case byte or a
// digit ("accessKey", "sha256Key"), or the last upper-case byte of an
// acronym run when a lower-case byte follows it ("APIKey" is api, key).
func isKeyWordBoundary(name string, i int) bool {
	if !isUpperByte(name[i]) {
		return false
	}
	prev := name[i-1]
	if isLowerByte(prev) || isDigitByte(prev) {
		return true
	}
	return isUpperByte(prev) && i+1 < len(name) && isLowerByte(name[i+1])
}

// segmentKeyWord splits a run-together word into vocabulary words, and
// only when those words cover it completely: "apikey" is api+key, while
// "monkey", "keyspace", and "sslmode" do not segment and stay whole.
// Requiring full coverage is what keeps a benign name from classifying on
// an embedded substring.
func segmentKeyWord(word string) []string {
	if word == "" {
		return nil
	}
	vocabulary := keyNameVocabulary()
	if slices.Contains(vocabulary, word) {
		return []string{word}
	}
	reachable := make([]bool, len(word)+1)
	from := make([]int, len(word)+1)
	reachable[0] = true
	for end := 1; end <= len(word); end++ {
		for start := range end {
			if !reachable[start] {
				continue
			}
			if slices.Contains(vocabulary, word[start:end]) {
				reachable[end] = true
				from[end] = start
				break
			}
		}
	}
	if !reachable[len(word)] {
		return []string{word}
	}
	var words []string
	for end := len(word); end > 0; end = from[end] {
		words = append(words, word[from[end]:end])
	}
	slices.Reverse(words)
	return words
}

// isKeyNameByte reports whether b can appear in a configuration key or
// URI parameter name: a word byte, or one of the separators an operator
// spells a multi-word name with.
func isKeyNameByte(b byte) bool {
	return b == '-' || b == '.' || b == '_' || isKeyWordByte(b)
}

// isKeyWordByte reports whether b belongs to a single word within a key
// name. Every other byte, including any non-ASCII byte, separates words.
func isKeyWordByte(b byte) bool {
	return isDigitByte(b) || isLowerByte(b) || isUpperByte(b)
}

func isDigitByte(b byte) bool { return b >= '0' && b <= '9' }

func isLowerByte(b byte) bool { return b >= 'a' && b <= 'z' }

func isUpperByte(b byte) bool { return b >= 'A' && b <= 'Z' }
