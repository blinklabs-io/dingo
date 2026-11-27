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
	"archive/tar"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/api/blockfrost"
	"github.com/blinklabs-io/dingo/api/kupo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/settingsresolve"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// legacyParityChunks is the number of preview chunks the released version
// loaded into the database under legacyParityArchive: all of epoch 0 and the
// first chunk of epoch 1.
const legacyParityChunks = 21

// legacyParityArchive is a data directory `dingo load` of the v0.94.2 release
// wrote in API storage mode; the README beside it states how.
var legacyParityArchive = filepath.Join(
	"testdata", "v0.94.2-api-preview", "database.tar.zst",
)

// parityNode is a started node and the base URLs of its API servers.
type parityNode struct {
	name       string
	blockfrost string
	kupo       string
	mesh       string
	utxorpc    string
	// ledgerSlot is the slot of the ledger's applied tip.
	ledgerSlot func() uint64
	// stop stops the node; the test's cleanup calls it if the test has not.
	stop func()
}

func parityListeners(t *testing.T) (
	map[string]uint,
	func(string, string) (net.Listener, error),
) {
	t.Helper()
	ports := make(map[string]uint)
	listeners := make(map[string]net.Listener)
	for _, name := range []string{"blockfrost", "mesh", "utxorpc", "kupo"} {
		listener, port := reservedTCPListener(t, "127.0.0.1")
		ports[name] = port
		listeners[listener.Addr().String()] = listener
	}
	var mu sync.Mutex
	return ports, func(network, address string) (net.Listener, error) {
		mu.Lock()
		defer mu.Unlock()
		listener, ok := listeners[address]
		if network != "tcp" || !ok {
			return nil, fmt.Errorf(
				"no reserved parity listener for %s %s",
				network,
				address,
			)
		}
		delete(listeners, address)
		return listener, nil
	}
}

// parityConfig is the configuration of a node in API storage mode over dir.
func parityConfig(dir string) *config.Config {
	cfg := &config.Config{
		Network:      "preview",
		DatabasePath: dir,
		StorageMode:  "api",
	}
	cfg.Plugins.Storage.Blob = plugin.Selection{Provider: "badger"}
	cfg.Plugins.Storage.Metadata = plugin.Selection{Provider: "sqlite"}
	return cfg
}

// loadParityChain loads real preview blocks into a new data directory, as
// `dingo load` does.
func loadParityChain(t *testing.T, immutableDir string) string {
	t.Helper()
	dir := testutil.OwnerOnlyTempDir(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(
		t,
		Load(t.Context(), parityConfig(dir), logger, immutableDir),
	)
	return dir
}

// unpackLegacyDatabase unpacks the released version's data directory from
// archive into a new directory.
func unpackLegacyDatabase(t *testing.T, archive string) string {
	t.Helper()
	dir := testutil.OwnerOnlyTempDir(t)
	f, err := os.Open(archive)
	require.NoError(t, err)
	defer f.Close()
	zr, err := zstd.NewReader(f)
	require.NoError(t, err)
	defer zr.Close()
	// The root refuses an entry that names a path outside dir.
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch hdr.Typeflag {
		case tar.TypeDir:
			require.NoError(t, root.MkdirAll(hdr.Name, 0o700))
		case tar.TypeReg:
			out, err := root.OpenFile(
				hdr.Name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600,
			)
			require.NoError(t, err)
			_, err = io.CopyN(out, tr, hdr.Size)
			require.NoError(t, errors.Join(err, out.Close()), hdr.Name)
		default:
			t.Fatalf("unexpected archive entry %q", hdr.Name)
		}
	}
	return dir
}

// startParityNode runs a node over dir with every API enabled. Both nodes
// gets the real IndexerDB writer.
func startParityNode(
	t *testing.T,
	name, dir string,
	cardanoCfg *cardano.CardanoNodeConfig,
	extra ...dingo.ConfigOptionFunc,
) parityNode {
	t.Helper()
	pn := parityNode{name: name}
	ports, listen := parityListeners(t)
	blockfrostPort := ports["blockfrost"]
	meshPort := ports["mesh"]
	utxorpcPort := ports["utxorpc"]
	kupoPort := ports["kupo"]
	base := func(port uint) string {
		return "http://127.0.0.1:" + strconv.FormatUint(uint64(port), 10)
	}
	pn.blockfrost = base(blockfrostPort)
	pn.mesh = base(meshPort)
	pn.utxorpc = base(utxorpcPort)
	pn.kupo = base(kupoPort)

	opts := []dingo.ConfigOptionFunc{
		dingo.WithAPIListenerFactory(listen),
		dingo.WithDatabasePath(dir),
		dingo.WithCardanoNodeConfig(cardanoCfg),
		dingo.WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		dingo.WithPrometheusRegistry(prometheus.NewRegistry()),
		dingo.WithStorageMode(dingo.StorageModeAPI),
		dingo.WithListeners(dingo.ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
		dingo.WithBindAddr("127.0.0.1"),
		dingo.WithNetwork("preview"),
		dingo.WithUtxorpcPort(utxorpcPort),
		dingo.WithBlockfrostPort(blockfrostPort),
		dingo.WithMeshPort(meshPort),
		dingo.WithPluginSelection(
			plugin.CapabilityAPIKupo,
			plugin.Selection{
				Provider: "builtin",
				Config:   map[string]any{"port": kupoPort},
			},
		),
		// The shutdown deadline bounds Stop's phases, and a store close
		// outlives it; a package of parallel tests makes a close of a few
		// hundred milliseconds take longer than a short bound, which Stop
		// reports as an error.
		dingo.WithShutdownTimeout(time.Minute),
	}
	opts = append(
		opts,
		dingo.WithIndexerWriterFactory(NewIndexerService),
		dingo.WithIndexerMaxReadyLagBlocks(0),
	)
	opts = append(opts, extra...)
	n, err := dingo.New(dingo.NewConfig(opts...))
	require.NoError(t, err)
	pn.ledgerSlot = func() uint64 {
		return n.LifecycleStatus().GetSync().GetTip().GetSlot()
	}
	runErr := make(chan error, 1)
	go func() { runErr <- n.Run(context.Background()) }()
	// Failures are reported without FailNow: the cleanup calls stop again,
	// and a sync.OnceFunc whose function ended through Goexit panics the
	// second call, which ends the whole test binary.
	pn.stop = sync.OnceFunc(func() {
		if err := n.Stop(); err != nil {
			t.Errorf("stop the %s node: %v", name, err)
		}
		select {
		case <-runErr:
		case <-time.After(3 * time.Minute):
			t.Errorf("the %s node run did not return", name)
		}
	})
	t.Cleanup(pn.stop)
	require.Eventually(t, func() bool {
		select {
		case err := <-runErr:
			t.Fatalf("%s node run ended early: %v", name, err)
		default:
		}
		resp, err := http.Get(pn.blockfrost + "/api/v0/health") //nolint:noctx
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	}, 3*time.Minute, 50*time.Millisecond, name+" API listens")
	return pn
}

// apiResponse is one HTTP exchange, compared across the two nodes.
type apiResponse struct {
	status int
	body   string
}

func parityDo(
	t *testing.T,
	method, target, payload string,
) apiResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(
		t.Context(), method, target, strings.NewReader(payload),
	)
	require.NoError(t, err)
	if payload != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec
	require.NoError(t, err, target)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return apiResponse{status: resp.StatusCode, body: string(body)}
}

// The UTxO RPC search that names no address returns the oldest outputs of the
// chain first.
const (
	paritySearchUtxos      = "/utxorpc.v1alpha.query.QueryService/SearchUtxos"
	parityUnfilteredSearch = `{"maxItems":50}`
)

// normalizeParity removes what differs between a database the released
// version wrote and one the current code loads from the same chain, none of
// which is chain data.
//
// The clock route reports each node's wall clock when it answers, and the two
// requests are made one after the other, so its value is dropped.
//
// Pool metadata documents are fetched from the network by each node on its
// own schedule, so everything but the registered hash and URL is dropped.
//
// The search orders outputs by the slot, block index and output index of the
// transaction that made them and then by that transaction's row id. The
// genesis transactions share the first three, so their outputs come in the
// order the transactions were inserted in. The released version inserts them
// in an order that changes from one load to the next (three loads of these
// chunks gave three orders) and the current code inserts them sorted by hash.
// The outputs of the genesis transactions, whose hashes genesisTxs holds in
// base64, are therefore compared as a set, each kept at a position a genesis
// output holds; every other output is compared in place.
func normalizeParity(
	path, payload, body string,
	genesisTxs map[string]bool,
) string {
	var v any
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		return body
	}
	switch {
	case path == "/health/clock" || path == "/api/v0/health/clock":
		if m, ok := v.(map[string]any); ok {
			delete(m, "server_time")
		}
	case strings.SplitN(path, "?", 2)[0] == "/api/v0/pools/extended":
		if list, ok := v.([]any); ok {
			for _, row := range list {
				if m, ok := row.(map[string]any); ok {
					if meta, ok := m["metadata"].(map[string]any); ok {
						for k := range meta {
							if k != "hash" && k != "url" {
								delete(meta, k)
							}
						}
					}
				}
			}
		}
	case path == paritySearchUtxos && payload == parityUnfilteredSearch:
		m, _ := v.(map[string]any)
		items, _ := m["items"].([]any)
		var at []int
		var genesis []string
		for i, item := range items {
			entry, _ := item.(map[string]any)
			ref, _ := entry["txoRef"].(map[string]any)
			if hash, _ := ref["hash"].(string); genesisTxs[hash] {
				b, _ := json.Marshal(item)
				at = append(at, i)
				genesis = append(genesis, string(b))
			}
		}
		sort.Strings(genesis)
		for n, i := range at {
			items[i] = json.RawMessage(genesis[n])
		}
	}
	out, err := json.Marshal(v)
	if err != nil {
		return body
	}
	return string(out)
}

// parityGenesisTxs returns the hashes of the transactions the genesis files
// fund, in base64 as the UTxO RPC JSON mapping carries them.
func parityGenesisTxs(
	t *testing.T,
	cfg *cardano.CardanoNodeConfig,
) map[string]bool {
	t.Helper()
	var utxos []lcommon.Utxo
	if byron := cfg.ByronGenesis(); byron != nil {
		more, err := byron.GenesisUtxos()
		require.NoError(t, err)
		utxos = append(utxos, more...)
	}
	if shelley := cfg.ShelleyGenesis(); shelley != nil {
		more, err := shelley.GenesisUtxos()
		require.NoError(t, err)
		utxos = append(utxos, more...)
	}
	out := map[string]bool{}
	for _, u := range utxos {
		out[base64.StdEncoding.EncodeToString(u.Id.Id().Bytes())] = true
	}
	require.NotEmpty(t, out, "the genesis files fund outputs")
	return out
}

// getJSON decodes a successful GET from a node into v.
func (p parityNode) getJSON(t *testing.T, path string, v any) bool {
	t.Helper()
	r := parityDo(t, http.MethodGet, p.blockfrost+path, "")
	if r.status != http.StatusOK {
		return false
	}
	require.NoError(t, json.Unmarshal([]byte(r.body), v), path)
	return true
}

// waitParityTip blocks until both nodes serve one chain tip and answer the
// current-state routes, so the comparison starts from one chain point. A fresh
// node fills its IndexerDB from the ledger after it starts, while a migrated
// one already holds the rows it copied, so the tips meet when the fresh node
// has caught up.
func waitParityTip(t *testing.T, fresh, migrated parityNode) {
	t.Helper()
	tip := func(p parityNode) string {
		var latest struct {
			Hash string `json:"hash"`
		}
		if !p.getJSON(t, "/api/v0/blocks/latest", &latest) {
			return ""
		}
		return latest.Hash
	}
	require.Eventually(t, func() bool {
		want := tip(fresh)
		if want == "" || tip(migrated) != want {
			return false
		}
		// The IndexerDB of a migrated node is at the released tip while its
		// ledger applies the last blocks again.
		if fresh.ledgerSlot() != migrated.ledgerSlot() {
			return false
		}
		for _, p := range []parityNode{fresh, migrated} {
			r := parityDo(
				t,
				http.MethodGet,
				p.blockfrost+"/api/v0/epochs/latest",
				"",
			)
			if r.status != http.StatusOK {
				return false
			}
		}
		return true
	}, 10*time.Minute, 100*time.Millisecond,
		"the fresh and the migrated node serve one chain tip")
}

// parityIDs are path-parameter values found in the loaded chain.
type parityIDs struct {
	latestHash                 string
	latestHeight, latestSlot   uint64
	latestEpoch, earlyEpoch    uint64
	pools                      []string
	stakes                     []string
	addresses                  []string
	txs                        []string
	metadataTxs                []string
	blocks                     []string
	labels                     []string
	assets                     []string
	scripts                    []string
	datumHashes                []string
	assetAddressesByPolicyName []string
}

func decodeList(t *testing.T, raw string, key string) []string {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		var rowsStr []string
		if err2 := json.Unmarshal([]byte(raw), &rowsStr); err2 != nil {
			return nil
		}
		return rowsStr
	}
	var out []string
	for _, r := range rows {
		if v, ok := r[key]; ok {
			out = append(out, fmt.Sprint(v))
		}
	}
	return out
}

// discoverParityIDs reads identifiers from the fresh node, whose answers are
// the reference.
func discoverParityIDs(t *testing.T, ref parityNode) parityIDs {
	t.Helper()
	var ids parityIDs
	var latest struct {
		Hash   string `json:"hash"`
		Height uint64 `json:"height"`
		Slot   uint64 `json:"slot"`
		Epoch  uint64 `json:"epoch"`
	}
	require.True(t, ref.getJSON(t, "/api/v0/blocks/latest", &latest))
	ids.latestHash, ids.latestHeight = latest.Hash, latest.Height
	ids.latestSlot, ids.latestEpoch = latest.Slot, latest.Epoch
	ids.earlyEpoch = 1
	list := func(path, key string, limit int) []string {
		r := parityDo(t, http.MethodGet, ref.blockfrost+path, "")
		if r.status != http.StatusOK {
			return nil
		}
		out := decodeList(t, r.body, key)
		if len(out) > limit {
			out = out[:limit]
		}
		return out
	}
	ids.pools = list("/api/v0/pools?count=3", "", 3)
	for back := uint64(0); back <= 2 && back <= ids.latestEpoch; back++ {
		ep := strconv.FormatUint(ids.latestEpoch-back, 10)
		for _, row := range list("/api/v0/epochs/"+ep+"/stakes?count=20", "stake_address", 20) {
			ids.stakes = append(ids.stakes, row)
		}
		ids.blocks = append(
			ids.blocks, list("/api/v0/epochs/"+ep+"/blocks?count=3", "", 3)...,
		)
	}
	for _, p := range ids.pools {
		ids.stakes = append(
			ids.stakes,
			list("/api/v0/pools/"+p+"/delegators?count=5", "address", 5)...,
		)
	}
	ids.labels = list("/api/v0/metadata/txs/labels?count=3", "label", 3)
	for _, l := range ids.labels {
		ids.metadataTxs = append(
			ids.metadataTxs,
			list("/api/v0/metadata/txs/labels/"+l+"?count=3", "tx_hash", 3)...,
		)
	}
	ids.assets = list("/api/v0/assets?count=3", "asset", 3)
	ids.scripts = list("/api/v0/scripts?count=3", "script_hash", 3)
	// Transactions and addresses come from the blocks that hold them.
	seen := map[string]bool{}
	for _, b := range append([]string{ids.latestHash}, ids.blocks...) {
		for _, h := range list("/api/v0/blocks/"+b+"/txs?count=5", "", 5) {
			if !seen[h] {
				seen[h] = true
				ids.txs = append(ids.txs, h)
			}
		}
	}
	for h := ids.latestHeight; h > 0 && len(ids.txs) < 12 &&
		ids.latestHeight-h < 6000; h -= 41 {
		for _, tx := range list(
			"/api/v0/blocks/"+strconv.FormatUint(h, 10)+"/txs?count=3", "", 3,
		) {
			if !seen[tx] {
				seen[tx] = true
				ids.txs = append(ids.txs, tx)
			}
		}
	}
	// The chain's first transactions, which the stride above can miss.
	for h := uint64(1); h <= 32 && h <= ids.latestHeight &&
		len(ids.txs) < 12; h++ {
		for _, tx := range list(
			"/api/v0/blocks/"+strconv.FormatUint(h, 10)+"/txs?count=3", "", 3,
		) {
			if !seen[tx] {
				seen[tx] = true
				ids.txs = append(ids.txs, tx)
			}
		}
	}
	for _, h := range ids.txs {
		var utxos struct {
			Outputs []struct {
				Address string `json:"address"`
			} `json:"outputs"`
		}
		if ref.getJSON(t, "/api/v0/txs/"+h+"/utxos", &utxos) {
			for _, o := range utxos.Outputs {
				if !slices.Contains(ids.addresses, o.Address) {
					ids.addresses = append(ids.addresses, o.Address)
				}
			}
		}
	}
	return ids
}

func (ids parityIDs) first(list []string, def string) string {
	if len(list) > 0 {
		return list[0]
	}
	return def
}

const parityMissingHash = "00000000000000000000000000000000000000000000000000000000000000ff"

// expand turns a route pattern into concrete requests: each placeholder takes
// every candidate value (bounded), so an empty-list parameter still checks the
// route against the unknown-id answer.
func (ids parityIDs) expand(pattern string) []string {
	path := strings.TrimPrefix(
		pattern[strings.Index(pattern, " ")+1:], "",
	)
	candidates := map[string][]string{
		"stake_address": append(
			capList(
				ids.stakes,
				6,
			),
			"stake_test1uzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzs0m4k9d",
		),
		"address": capList(ids.addresses, 6),
		"hash":    append(capList(ids.txs, 6), parityMissingHash),
		"label":   append(capList(ids.labels, 3), "674"),
		"pool_id": capList(ids.pools, 3),
		"script_hash": append(
			capList(ids.scripts, 3), parityMissingHash[:56],
		),
		"asset": append(
			capList(ids.assets, 3), parityMissingHash[:56]+"00",
		),
		"datum_hash": {parityMissingHash},
		"hash_or_number": append(
			capList(ids.blocks, 3),
			ids.latestHash, strconv.FormatUint(ids.latestHeight-5, 10),
			"1", strconv.FormatUint(ids.latestHeight, 10),
		),
		"epoch_number": {
			strconv.FormatUint(ids.latestEpoch, 10),
			strconv.FormatUint(ids.latestEpoch-min(ids.latestEpoch, 1), 10),
			"1",
		},
		"number": {
			strconv.FormatUint(ids.latestEpoch, 10),
			strconv.FormatUint(ids.latestEpoch-min(ids.latestEpoch, 1), 10),
			"1",
		},
		"slot_number": {strconv.FormatUint(ids.latestSlot, 10)},
		"cc_id": {
			"cc_cold1zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzs8nxkd",
		},
		"drep_id": {
			"drep_always_abstain",
			"drep1zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		},
		"first":  {parityMissingHash},
		"second": {"0"},
		"third":  {"0"},
	}
	sections := map[string][]string{
		"/blocks/{hash_or_number}/{section}": {
			"next",
			"previous",
			"txs",
			"addresses",
		},
		"/assets/{asset}/{section}": {
			"history",
			"txs",
			"transactions",
			"addresses",
		},
		"/scripts/{script_hash}/{section}": {"json", "cbor", "redeemers"},
	}
	var secs []string
	for suffix, v := range sections {
		if strings.HasSuffix(path, suffix) {
			secs = v
		}
	}
	paths := []string{path}
	for _, ph := range []string{
		"stake_address", "address", "hash_or_number", "hash", "label",
		"pool_id", "script_hash", "asset", "datum_hash", "epoch_number",
		"number", "slot_number", "cc_id", "drep_id", "first", "second",
		"third", "section",
	} {
		token := "{" + ph + "}"
		var next []string
		for _, p := range paths {
			if !strings.Contains(p, token) {
				next = append(next, p)
				continue
			}
			vals := candidates[ph]
			if ph == "section" {
				vals = secs
			}
			for _, v := range vals {
				next = append(
					next,
					strings.Replace(p, token, url.PathEscape(v), 1),
				)
			}
		}
		paths = next
	}
	return paths
}

func capList(l []string, n int) []string {
	if len(l) > n {
		return slices.Clone(l[:n])
	}
	return slices.Clone(l)
}

func hexToBase64(t *testing.T, h string) string {
	t.Helper()
	b, err := hex.DecodeString(h)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(b)
}

// TestMigratedNodeServesTheSameAPIsAsAFreshOne takes a database the v0.94.2
// release wrote in API storage mode, moves it to the storage-role layout the
// way a node's startup does, loads the same chunks into a new database with
// the current code, and requires every Blockfrost GET route, the Kupo and Mesh
// endpoints and the UTxO RPC queries to answer the same on both nodes.
func TestMigratedNodeServesTheSameAPIsAsAFreshOne(t *testing.T) {
	t.Parallel()
	requireMigratedNodeMatchesFresh(
		t,
		unpackLegacyDatabase(t, legacyParityArchive),
		copyImmutableChunkPrefix(t, legacyParityChunks),
	)
}

// requireMigratedNodeMatchesFresh migrates the released version's data
// directory legacyDir, loads the chunks under immutableDir it was loaded from
// into a new data directory, and compares the APIs of a node over each.
func requireMigratedNodeMatchesFresh(
	t *testing.T,
	legacyDir, immutableDir string,
) {
	t.Helper()
	cardanoCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	freshDir := loadParityChain(t, immutableDir)
	// The startup sequence of cmd/dingo: the API rows move to the IndexerDB
	// before anything opens the consensus store, and the node then moves the
	// UTxO set and the ledger state into the LedgerDB and the blocks into the
	// ImmutableDB.
	require.NoError(t, MigrateToIndexerDB(
		t.Context(), parityConfig(legacyDir),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	// The IndexerDB holds the released version's rows before the node and its
	// writer start, so what the migrated node serves is what was copied and
	// not what a writer indexed from genesis.
	copied, err := sql.Open("sqlite", "file:"+filepath.Join(
		legacyDir, internalplugins.IndexerDirName, "metadata.sqlite",
	)+"?mode=ro")
	require.NoError(t, err)
	for _, table := range []string{
		"applied_transaction", "utxo", "epoch", "pool_registration",
	} {
		require.NotZero(t, rowCount(t, copied, table),
			"the migration copied no %s rows", table)
	}
	require.NoError(t, copied.Close())
	migrated, ids := requireNodesServeTheSame(
		t, freshDir, legacyDir, cardanoCfg,
	)

	// The migrated directory is left as a node writes one: the ledger and
	// the live UTxO set at the tip.
	tipHash, err := hex.DecodeString(ids.latestHash)
	require.NoError(t, err)
	migrated.stop()
	requireLoadedStorage(
		t, legacyDir, ocommon.NewPoint(ids.latestSlot, tipHash),
	)

	// The migrated node applied no block of its own, so the LedgerDB's
	// transaction index holds exactly the released version's applied
	// transactions, less the genesis transactions whose only output was a
	// zero-coin Byron output: the start removes those from both stores
	// before the index is built.
	repaired, err := sql.Open("sqlite", "file:"+filepath.Join(
		legacyDir, internalplugins.IndexerDirName, "metadata.sqlite",
	)+"?mode=ro")
	require.NoError(t, err)
	applied := rowCount(t, repaired, "applied_transaction")
	require.NoError(t, repaired.Close())
	require.NotZero(t, applied)
	set, err := utxoset.Open(utxoset.Config{
		Dir:    utxoset.DirFor(legacyDir),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, set.Close()) }()
	indexed := 0
	require.NoError(t, set.EachTxIndexEntry(func(ledger.TxIndexEntry) error {
		indexed++
		return nil
	}))
	require.Equal(t, applied, indexed)
}

// requireNodesServeTheSame starts a node over freshDir and one over
// migratedDir, waits until they are at one tip, and compares every API
// answer. It returns the node over migratedDir and the values the walk used.
func requireNodesServeTheSame(
	t *testing.T,
	freshDir, migratedDir string,
	cardanoCfg *cardano.CardanoNodeConfig,
) (parityNode, parityIDs) {
	t.Helper()
	started := time.Now()
	fresh := startParityNode(t, "fresh", freshDir, cardanoCfg)
	migrated := startParityNode(
		t, "migrated", migratedDir, cardanoCfg, warnLogger(t),
	)
	waitParityTip(t, fresh, migrated)
	t.Logf(
		"nodes up and the migrated node caught up after %s",
		time.Since(started),
	)
	ids := discoverParityIDs(t, fresh)
	require.NotEmpty(t, ids.pools, "the chain has pools")
	require.NotEmpty(t, ids.stakes, "the chain has staked accounts")
	require.NotEmpty(t, ids.txs, "the chain has transactions")
	require.NotEmpty(t, ids.addresses, "the chain has addresses")

	genesisTxs := parityGenesisTxs(t, cardanoCfg)
	var diffs, timings, serverErrors []string
	statuses := map[int]int{}
	walkStart := time.Now()
	compare := func(label, method, path, payload string, a, b parityNode, aBase, bBase string) {
		t.Helper()
		// An epoch the chain has not reached is an error both nodes answer,
		// compared like any other answer.
		if label == "blockfrost" && epochStakesPath.MatchString(path) &&
			parityDo(t, method, aBase+path, payload).status == http.StatusOK {
			// The listing pages by row id, so which rows a page holds
			// follows the order the rows were written in: the released
			// version wrote a snapshot's stake rows as it captured them,
			// and the IndexerDB writer copies them by key. Whole
			// distributions are compared.
			want := allPages(t, aBase+path)
			got := allPages(t, bBase+path)
			if !slices.Equal(want, got) {
				missing, extra := diffLines(want, got)
				diffs = append(diffs, fmt.Sprintf(
					"%s GET %s\n  fresh: %d stakes, only here: %v\n  migrated: %d stakes, only here: %v",
					label, path, len(want), firstN(missing, 2),
					len(got), firstN(extra, 2),
				))
			}
			return
		}
		began := time.Now()
		want := parityDo(t, method, aBase+path, payload)
		mid := time.Now()
		got := parityDo(t, method, bBase+path, payload)
		timings = append(timings, fmt.Sprintf(
			"%6dms fresh %6dms migrated %s %s", mid.Sub(began).Milliseconds(),
			time.Since(mid).Milliseconds(), method, truncate(path, 90),
		))
		statuses[want.status]++
		if want.status >= http.StatusInternalServerError &&
			want.status != http.StatusNotImplemented {
			serverErrors = append(serverErrors, fmt.Sprintf(
				"%s %s %s: %d %s", label, method, truncate(path, 90),
				want.status, truncate(want.body, 200),
			))
		}
		if want.status == got.status &&
			normalizeParity(path, payload, want.body, genesisTxs) ==
				normalizeParity(path, payload, got.body, genesisTxs) {
			return
		}
		wantBody := normalizeParity(path, payload, want.body, genesisTxs)
		gotBody := normalizeParity(path, payload, got.body, genesisTxs)
		at := 0
		for at < len(wantBody) && at < len(gotBody) &&
			wantBody[at] == gotBody[at] {
			at++
		}
		from := max(at-80, 0)
		diffs = append(diffs, fmt.Sprintf(
			"%s %s %s\n  fresh: %d ...%s\n  migrated:  %d ...%s",
			label, method, path,
			want.status, truncate(wantBody[from:], 240),
			got.status, truncate(gotBody[from:], 240),
		))
	}

	checked := 0
	for _, pattern := range blockfrost.RoutePatterns() {
		method, _, _ := strings.Cut(pattern, " ")
		if method != http.MethodGet {
			continue
		}
		paths := ids.expand(pattern)
		require.NotEmpty(t, paths, "no request for %s", pattern)
		for _, p := range paths {
			checked++
			compare("blockfrost", method, p, "", fresh, migrated,
				fresh.blockfrost, migrated.blockfrost)
		}
	}
	require.Greater(t, checked, 100, "the route walk covered the API")
	kupoChecked := 0
	for _, p := range ids.kupoPaths(t, kupo.RoutePatterns()) {
		kupoChecked++
		compare("kupo", http.MethodGet, p, "", fresh, migrated,
			fresh.kupo, migrated.kupo)
	}
	require.Greater(t, kupoChecked, 10, "the Kupo walk covered the API")

	meshChecked := 0
	for _, r := range ids.meshRequests(t, fresh) {
		meshChecked++
		compare("mesh", http.MethodPost, r.path, r.body, fresh, migrated,
			fresh.mesh, migrated.mesh)
	}
	require.Greater(t, meshChecked, 5, "the Mesh walk covered the API")

	rpcChecked := 0
	for _, r := range ids.utxorpcRequests(t, fresh) {
		rpcChecked++
		compare("utxorpc", http.MethodPost, r.path, r.body, fresh, migrated,
			fresh.utxorpc, migrated.utxorpc)
	}
	require.Greater(t, rpcChecked, 5, "the UTxO RPC walk covered the API")

	t.Logf("route walk took %s", time.Since(walkStart))
	sort.Slice(timings, func(i, j int) bool { return timings[i] > timings[j] })
	for _, line := range timings[:min(12, len(timings))] {
		t.Log(line)
	}
	t.Logf("%d requests, by the fresh node's status: %v", len(timings), statuses)
	// A route both nodes fail the same way compares equal without comparing
	// any data, so the ones that do are listed; a route the API does not
	// implement answers 501 on any node.
	for _, e := range serverErrors {
		t.Logf("answered with a server error by both nodes: %s", e)
	}
	for _, d := range diffs {
		t.Error(d)
	}
	return migrated, ids
}

// kupoPaths expands the Kupo GET routes over the discovered addresses, assets
// and slots.
func (ids parityIDs) kupoPaths(t *testing.T, patterns []string) []string {
	t.Helper()
	candidates := map[string][]string{
		"pattern": {
			"*", ids.first(ids.addresses, "*"),
			ids.first(ids.stakes, "*"),
			ids.first(ids.scripts, "*") + ".*",
		},
		"payment":    {"*"},
		"delegation": {"*"},
		"datum_hash": {parityMissingHash},
		"script_hash": append(
			capList(ids.scripts, 2), parityMissingHash[:56],
		),
		"slot_no": {
			strconv.FormatUint(ids.latestSlot, 10),
			strconv.FormatUint(ids.latestSlot/2, 10),
		},
	}
	// A wildcard pattern matches every output ever created, which takes
	// seconds per request; a window of recent slots keeps it bounded.
	window := "created_after=" + strconv.FormatUint(
		ids.latestSlot-ids.latestSlot/40, 10,
	)
	queries := []string{"", "?unspent", "?spent"}
	wildcardQueries := []string{"?" + window, "?unspent&" + window}
	var out []string
	for _, pattern := range patterns {
		method, path, _ := strings.Cut(pattern, " ")
		if method != http.MethodGet {
			continue
		}
		paths := []string{path}
		for ph, vals := range candidates {
			token := "{" + ph + "}"
			var next []string
			for _, p := range paths {
				if !strings.Contains(p, token) {
					next = append(next, p)
					continue
				}
				for _, v := range vals {
					next = append(
						next,
						strings.Replace(p, token, url.PathEscape(v), 1),
					)
				}
			}
			paths = next
		}
		require.NotEmpty(t, paths, "no request for %s", pattern)
		for _, p := range paths {
			if strings.HasPrefix(strings.TrimPrefix(p, "/v1"), "/matches") {
				qs := queries
				if strings.HasSuffix(p, "/matches") || strings.Contains(p, "%2A") &&
					!strings.Contains(p, ".%2A") {
					qs = wildcardQueries
				}
				for _, q := range qs {
					out = append(out, p+q)
				}
				continue
			}
			out = append(out, p)
		}
	}
	return out
}

type parityRequest struct{ path, body string }

// meshRequests builds the Mesh data API requests for the discovered chain,
// taking the network identifier the fresh node announces.
func (ids parityIDs) meshRequests(
	t *testing.T,
	ref parityNode,
) []parityRequest {
	t.Helper()
	list := parityDo(t, http.MethodPost, ref.mesh+"/network/list",
		`{"metadata":{}}`)
	require.Equal(t, http.StatusOK, list.status, list.body)
	var networks struct {
		NetworkIdentifiers []json.RawMessage `json:"network_identifiers"`
	}
	require.NoError(t, json.Unmarshal([]byte(list.body), &networks))
	require.NotEmpty(t, networks.NetworkIdentifiers)
	network := string(networks.NetworkIdentifiers[0])
	out := []parityRequest{
		{"/network/list", `{"metadata":{}}`},
		{"/network/options", `{"network_identifier":` + network + `}`},
		{"/network/status", `{"network_identifier":` + network + `}`},
		{"/mempool", `{"network_identifier":` + network + `}`},
	}
	for _, h := range []uint64{1, ids.latestHeight / 2, ids.latestHeight} {
		out = append(out, parityRequest{
			"/block",
			fmt.Sprintf(
				`{"network_identifier":%s,"block_identifier":{"index":%d}}`,
				network, h,
			),
		})
	}
	out = append(out, parityRequest{
		"/block",
		fmt.Sprintf(
			`{"network_identifier":%s,"block_identifier":{"hash":%q}}`,
			network, ids.latestHash,
		),
	})
	for _, addr := range capList(ids.addresses, 6) {
		for _, path := range []string{"/account/balance", "/account/coins"} {
			out = append(out, parityRequest{
				path,
				fmt.Sprintf(
					`{"network_identifier":%s,"account_identifier":{"address":%q}}`,
					network,
					addr,
				),
			})
		}
	}
	return out
}

// utxorpcRequests builds the UTxO RPC query requests, with UTxO references
// taken from what the fresh node's search returns.
func (ids parityIDs) utxorpcRequests(
	t *testing.T,
	ref parityNode,
) []parityRequest {
	t.Helper()
	const svc = "/utxorpc.v1alpha.query.QueryService/"
	search := func(addr string) string {
		return fmt.Sprintf(
			`{"predicate":{"match":{"cardano":{"address":{"exactAddress":%q}}}},"maxItems":50}`,
			bech32ToBase64(t, addr),
		)
	}
	out := []parityRequest{
		{svc + "ReadParams", `{}`},
		{svc + "ReadEraSummary", `{}`},
		{svc + "ReadGenesis", `{}`},
		{svc + "SearchUtxos", parityUnfilteredSearch},
	}
	for _, addr := range capList(ids.addresses, 4) {
		out = append(out, parityRequest{svc + "SearchUtxos", search(addr)})
	}
	found := parityDo(t, http.MethodPost, ref.utxorpc+svc+"SearchUtxos",
		parityUnfilteredSearch)
	require.Equal(t, http.StatusOK, found.status, found.body)
	var items struct {
		Items []struct {
			TxoRef json.RawMessage `json:"txoRef"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(found.body), &items))
	var keys []string
	for _, it := range items.Items {
		if len(it.TxoRef) > 0 {
			keys = append(keys, `{"txoRef":`+string(it.TxoRef)+`}`)
		}
	}
	if len(keys) > 0 {
		out = append(out, parityRequest{
			svc + "ReadUtxos",
			`{"keys":[` + strings.Join(capList(keys, 10), ",") + `]}`,
		})
	}
	for _, h := range capList(ids.txs, 6) {
		out = append(out, parityRequest{
			svc + "ReadTx",
			fmt.Sprintf(`{"hash":%q}`, hexToBase64(t, h)),
		})
	}
	out = append(out, parityRequest{
		svc + "ReadData",
		fmt.Sprintf(`{"keys":[%q]}`, hexToBase64(t, parityMissingHash)),
	})
	return out
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// bech32ToBase64 returns the raw bytes of a bech32 address, as the UTxO RPC
// JSON mapping of a bytes field carries them.
func bech32ToBase64(t *testing.T, addr string) string {
	t.Helper()
	a, err := lcommon.NewAddress(addr)
	require.NoError(t, err)
	raw, err := a.Bytes()
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(raw)
}

var epochStakesPath = regexp.MustCompile(`^/api/v0/epochs/\d+/stakes$`)

// allPages lists every entry of a paged route, each as its JSON text, sorted.
func allPages(t *testing.T, target string) []string {
	t.Helper()
	var out []string
	for page := 1; ; page++ {
		r := parityDo(t, http.MethodGet,
			fmt.Sprintf("%s?count=100&page=%d", target, page), "")
		require.Equal(t, http.StatusOK, r.status, target)
		var entries []json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(r.body), &entries), target)
		for _, e := range entries {
			out = append(out, string(e))
		}
		if len(entries) < 100 {
			break
		}
	}
	slices.Sort(out)
	return out
}

// storeSchema is the schema of the SQLite store at path and the migrations
// recorded in it, as text to compare.
func storeSchema(t *testing.T, path string) string {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	require.NoError(t, err)
	defer raw.Close()
	var out strings.Builder
	for _, query := range []string{
		`SELECT type || ' ' || name || ' ' || COALESCE(sql, '') FROM sqlite_master ORDER BY type, name`,
		`SELECT version || ' ' || phase || ' ' || dirty FROM schema_migrations ORDER BY version`,
	} {
		rows, err := raw.Query(query)
		require.NoError(t, err)
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			out.WriteString(line + "\n")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return out.String()
}

// TestStartupRefusesAReleasedInterruptedBackfillUntouched takes the database
// the v0.94.2 release wrote and marks it as that release leaves an API-mode
// Mithril bootstrap that stopped during its metadata backfill. The first step
// of a start that opens the store refuses it, and the refusal names v0.94.2 as
// what finishes the backfill, so it must leave a store that release still
// opens: no schema migration applied, no IndexerDB, and the status in place.
func TestStartupRefusesAReleasedInterruptedBackfillUntouched(t *testing.T) {
	t.Parallel()
	dir := unpackLegacyDatabase(t, legacyParityArchive)
	store := filepath.Join(dir, "metadata.sqlite")
	raw, err := sql.Open("sqlite", store)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO sync_state(sync_key,value) VALUES('sync_status','backfill')`,
	)
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	before := storeSchema(t, store)
	require.NotEmpty(t, before)

	err = settingsresolve.Apply(parityConfig(dir))
	require.ErrorIs(t, err, settingsresolve.ErrInterruptedBackfill)
	require.ErrorContains(t, err, "v0.94.2")

	require.Equal(t, before, storeSchema(t, store),
		"the refusal changed the released store's schema")
	require.NoDirExists(t, filepath.Join(dir, internalplugins.IndexerDirName))
	raw, err = sql.Open("sqlite", "file:"+store+"?mode=ro")
	require.NoError(t, err)
	defer raw.Close()
	var status string
	require.NoError(t, raw.QueryRow(
		`SELECT value FROM sync_state WHERE sync_key='sync_status'`,
	).Scan(&status))
	require.Equal(t, "backfill", status)
}

func TestParityListenersKeepPortsReservedUntilHandoff(t *testing.T) {
	t.Parallel()
	ports, listen := parityListeners(t)
	for name, port := range ports {
		t.Run(name, func(t *testing.T) {
			address := net.JoinHostPort(
				"127.0.0.1",
				strconv.FormatUint(uint64(port), 10),
			)
			intruder, err := net.Listen("tcp", address)
			if intruder != nil {
				_ = intruder.Close()
			}
			if err == nil {
				t.Fatal("API socket was released before handoff")
			}
			listener, err := listen("tcp", address)
			require.NoError(t, err)
			require.Equal(t, address, listener.Addr().String())
			_, err = listen("tcp", address)
			require.Error(t, err, "the same socket cannot be transferred twice")
			require.NoError(t, listener.Close())
			replacement, err := net.Listen("tcp", address)
			require.NoError(t, err)
			require.NoError(t, replacement.Close())
		})
	}
}
