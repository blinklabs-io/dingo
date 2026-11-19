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
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
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
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// parityChunks is the number of preview chunks loaded, the whole fixture. The
// first transactions on the chain land in epoch 7 and the first rewards are
// paid in epoch 10, so the walk covers stake, pool history and reward epochs
// through epoch 14.
const parityChunks = 300

// parityNode is a started node and the base URLs of its API servers.
type parityNode struct {
	name       string
	blockfrost string
	kupo       string
	mesh       string
	utxorpc    string
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

// loadParityChain loads the same real preview blocks into a data directory
// whose layout is shared or split, as `dingo load` does.
func loadParityChain(t *testing.T, immutableDir string, split bool) string {
	t.Helper()
	dir := testutil.OwnerOnlyTempDir(t)
	cfg := &config.Config{
		Network:      "preview",
		DatabasePath: dir,
		StorageMode:  "api",
	}
	cfg.Plugins.Storage.Blob = plugin.Selection{Provider: "badger"}
	cfg.Plugins.Storage.Metadata = plugin.Selection{Provider: "sqlite"}
	if split {
		cfg.Indexer.Layout = config.IndexerLayoutSplit
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, Load(t.Context(), cfg, logger, immutableDir))
	return dir
}

// startParityNode runs a node over dir with every API enabled. The split node
// gets the real IndexerDB writer.
func startParityNode(
	t *testing.T,
	name, dir string,
	split bool,
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
		dingo.WithShutdownTimeout(5 * time.Second),
	}
	if split {
		opts = append(
			opts,
			dingo.WithStorageMode(dingo.StorageModeCore),
			dingo.WithIndexerLayout(config.IndexerLayoutSplit),
			dingo.WithIndexerWriterFactory(NewIndexerService),
			dingo.WithIndexerMaxReadyLagBlocks(0),
		)
	}
	opts = append(opts, extra...)
	n, err := dingo.New(dingo.NewConfig(opts...))
	require.NoError(t, err)
	runErr := make(chan error, 1)
	go func() { runErr <- n.Run(context.Background()) }()
	pn.stop = sync.OnceFunc(func() {
		require.NoError(t, n.Stop())
		testutil.RequireReceive(
			t, runErr, 3*time.Minute, name+" node run returned",
		)
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

// apiResponse is one HTTP exchange, compared across the two layouts.
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

// normalizeParity removes what legitimately differs between two nodes over
// one chain. Pool metadata documents are fetched from the network by
// each node on its own schedule, so everything but the registered hash and URL is dropped.
func normalizeParity(path, body string) string {
	var v any
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		return body
	}
	switch strings.SplitN(path, "?", 2)[0] {
	case "/api/v0/pools/extended":
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
	}
	canonicalAssetOrder(v)
	out, err := json.Marshal(v)
	if err != nil {
		return body
	}
	return string(out)
}

// canonicalAssetOrder sorts the multi-asset collections of a decoded
// response. The assets one transaction output holds are stored in the order
// of a Go map, so two loads of one chain may list them differently. This is
// needed only until the canonical asset order change lands on main, after
// which both layouts serve one order and the comparison can be exact.
func canonicalAssetOrder(v any) {
	switch x := v.(type) {
	case map[string]any:
		for _, child := range x {
			canonicalAssetOrder(child)
		}
	case []any:
		for _, child := range x {
			canonicalAssetOrder(child)
		}
		sortBy := func(key func(any) string) {
			sort.SliceStable(x, func(i, j int) bool {
				return key(x[i]) < key(x[j])
			})
		}
		field := func(name string) func(any) string {
			return func(e any) string {
				m, _ := e.(map[string]any)
				return fmt.Sprint(m[name])
			}
		}
		allHave := func(names ...string) bool {
			if len(x) == 0 {
				return false
			}
			for _, e := range x {
				m, ok := e.(map[string]any)
				if !ok {
					return false
				}
				for _, n := range names {
					if _, ok := m[n]; !ok {
						return false
					}
				}
			}
			return true
		}
		switch {
		case allHave("unit", "quantity"):
			sortBy(field("unit"))
		case allHave("policyId", "assets"):
			sortBy(field("policyId"))
		case allHave("name", "outputCoin"):
			sortBy(field("name"))
		case allHave("operation_identifier"):
			// Mesh numbers operations in output order, so the order of a
			// transaction's assets decides their indexes.
			for _, e := range x {
				m := e.(map[string]any)
				delete(m, "operation_identifier")
				delete(m, "related_operations")
			}
			sortBy(func(e any) string {
				b, _ := json.Marshal(e)
				return string(b)
			})
		}
	}
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

// waitParityTip blocks until the split node serves the chain the shared node
// holds, so the comparison starts from one chain point.
func waitParityTip(t *testing.T, shared, split parityNode) {
	t.Helper()
	var want struct {
		Hash string `json:"hash"`
	}
	require.True(t, shared.getJSON(t, "/api/v0/blocks/latest", &want))
	require.Eventually(t, func() bool {
		var got struct {
			Hash string `json:"hash"`
		}
		if !split.getJSON(t, "/api/v0/blocks/latest", &got) {
			return false
		}
		if got.Hash != want.Hash {
			return false
		}
		r := parityDo(t, http.MethodGet,
			split.blockfrost+"/api/v0/epochs/latest", "")
		return r.status == http.StatusOK
	}, 10*time.Minute, 100*time.Millisecond,
		"the split node serves the shared node's tip %s", want.Hash)
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

// discoverParityIDs reads identifiers from the shared node, the layout whose
// answers are the reference.
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
	for _, epoch := range []uint64{ids.latestEpoch - 1, ids.latestEpoch - 2} {
		ep := strconv.FormatUint(epoch, 10)
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
		"address":     capList(ids.addresses, 6),
		"hash":        capList(ids.txs, 6),
		"label":       capList(ids.labels, 3),
		"pool_id":     capList(ids.pools, 3),
		"script_hash": capList(ids.scripts, 3),
		"asset":       capList(ids.assets, 3),
		"datum_hash":  {parityMissingHash},
		"hash_or_number": append(
			capList(ids.blocks, 3),
			ids.latestHash, strconv.FormatUint(ids.latestHeight-5, 10),
			"1", strconv.FormatUint(ids.latestHeight, 10),
		),
		"epoch_number": {
			strconv.FormatUint(ids.latestEpoch, 10),
			strconv.FormatUint(ids.latestEpoch-1, 10),
			"1",
		},
		"number": {
			strconv.FormatUint(ids.latestEpoch, 10),
			strconv.FormatUint(ids.latestEpoch-1, 10),
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

// parityIgnored lists Blockfrost routes whose body legitimately differs: wall
// clock readings, process metrics, and the health route a split node answers
// from its IndexerDB readiness.
var parityIgnored = map[string]bool{
	"/api/v0/health":            true,
	"/health":                   true,
	"/health/clock":             true,
	"/api/v0/health/clock":      true,
	"/api/v0/metrics":           true,
	"/api/v0/metrics/endpoints": true,
}

func hexToBase64(t *testing.T, h string) string {
	t.Helper()
	b, err := hex.DecodeString(h)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(b)
}

// TestSplitLayoutServesTheSameAPIsAsShared loads one real preview chain into
// a shared-layout node and a split-layout node and requires every Blockfrost
// GET route, the Kupo and Mesh endpoints and the UTxO RPC queries to answer
// the same on both.
func TestSplitLayoutServesTheSameAPIsAsShared(t *testing.T) {
	t.Parallel()
	runSplitParity(t, false)
}

// The same walk with the split node's ledger holding its state only in the
// live UTxO set: every row the APIs read, pool history included, comes from
// the IndexerDB the writer filled from that set.
func TestSplitLayoutWithSetOnlyServesTheSameAPIsAsShared(t *testing.T) {
	t.Parallel()
	runSplitParity(t, true)
}

func runSplitParity(t *testing.T, setOnly bool) {
	testutil.SkipUnderRace(
		t,
		"it replays one real chain into two nodes and walks every API, "+
			"about 18 minutes instrumented against about 75 seconds "+
			"without; the unraced run covers the comparison, and the "+
			"IndexerDB writer under race is covered by "+
			"TestIndexerWriterMatchesLiveApplyOnRealBlocks and "+
			"TestSplitNodeServesEveryAPI",
	)
	immutableDir := copyImmutableChunkPrefix(t, parityChunks)
	cardanoCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	var sharedDir, splitDir string
	var loads sync.WaitGroup
	loads.Add(2)
	go func() { defer loads.Done(); sharedDir = loadParityChain(t, immutableDir, false) }()
	go func() { defer loads.Done(); splitDir = loadParityChain(t, immutableDir, true) }()
	loads.Wait()
	var extra []dingo.ConfigOptionFunc
	if setOnly {
		// The node moves the UTxO set out of the consensus store itself.
		extra = append(setOnlyOptions(), warnLogger(t))
	} else {
		drainConsensusUtxos(t, splitDir)
	}
	started := time.Now()
	shared := startParityNode(t, "shared", sharedDir, false, cardanoCfg)
	split := startParityNode(t, "split", splitDir, true, cardanoCfg, extra...)
	waitParityTip(t, shared, split)
	t.Logf("nodes up and split caught up after %s", time.Since(started))
	ids := discoverParityIDs(t, shared)
	require.NotEmpty(t, ids.pools, "the chain has pools")
	require.NotEmpty(t, ids.stakes, "the chain has staked accounts")
	require.NotEmpty(t, ids.txs, "the chain has transactions")
	require.NotEmpty(t, ids.addresses, "the chain has addresses")
	require.True(t, anyAccountHasRewards(t, shared, ids.stakes),
		"the walk reaches epochs in which accounts earned rewards")

	var diffs, timings []string
	walkStart := time.Now()
	compare := func(label, method, path, payload string, a, b parityNode, aBase, bBase string) {
		t.Helper()
		if label == "blockfrost" && epochStakesPath.MatchString(path) {
			// The listing pages by row id, so which rows a page holds
			// follows the order the rows were written in; whole
			// distributions are compared.
			want := allPages(t, aBase+path)
			got := allPages(t, bBase+path)
			if !slices.Equal(want, got) {
				diffs = append(diffs, fmt.Sprintf(
					"%s GET %s\n  shared: %d stakes\n  split:  %d stakes",
					label, path, len(want), len(got),
				))
			}
			return
		}
		began := time.Now()
		want := parityDo(t, method, aBase+path, payload)
		mid := time.Now()
		got := parityDo(t, method, bBase+path, payload)
		timings = append(timings, fmt.Sprintf(
			"%6dms shared %6dms split %s %s", mid.Sub(began).Milliseconds(),
			time.Since(mid).Milliseconds(), method, truncate(path, 90),
		))
		if want.status == got.status &&
			normalizeParity(path, want.body) ==
				normalizeParity(path, got.body) {
			return
		}
		wantBody := normalizeParity(path, want.body)
		gotBody := normalizeParity(path, got.body)
		at := 0
		for at < len(wantBody) && at < len(gotBody) &&
			wantBody[at] == gotBody[at] {
			at++
		}
		from := max(at-80, 0)
		diffs = append(diffs, fmt.Sprintf(
			"%s %s %s\n  shared: %d ...%s\n  split:  %d ...%s",
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
		for _, p := range ids.expand(pattern) {
			if parityIgnored[strings.SplitN(p, "?", 2)[0]] {
				continue
			}
			if p == "/api/v0/assets" {
				continue
			}
			checked++
			compare("blockfrost", method, p, "", shared, split,
				shared.blockfrost, split.blockfrost)
		}
	}
	require.Greater(t, checked, 100, "the route walk covered the API")
	// The assets list orders by the id of an asset's first mint row, and the
	// assets one transaction mints are inserted in the order of a Go map, so
	// the order within a transaction differs between any two loads and a
	// page can end inside such a group. Every page is compared as a set.
	require.Equal(t, allAssets(t, shared), allAssets(t, split),
		"the assets the two nodes list")

	kupoChecked := 0
	for _, p := range ids.kupoPaths(kupo.RoutePatterns()) {
		kupoChecked++
		compare("kupo", http.MethodGet, p, "", shared, split,
			shared.kupo, split.kupo)
	}
	require.Greater(t, kupoChecked, 10, "the Kupo walk covered the API")

	meshChecked := 0
	for _, r := range ids.meshRequests(t, shared) {
		meshChecked++
		compare("mesh", http.MethodPost, r.path, r.body, shared, split,
			shared.mesh, split.mesh)
	}
	require.Greater(t, meshChecked, 5, "the Mesh walk covered the API")

	rpcChecked := 0
	for _, r := range ids.utxorpcRequests(t, shared) {
		rpcChecked++
		compare("utxorpc", http.MethodPost, r.path, r.body, shared, split,
			shared.utxorpc, split.utxorpc)
	}
	require.Greater(t, rpcChecked, 5, "the UTxO RPC walk covered the API")

	t.Logf("route walk took %s", time.Since(walkStart))
	sort.Slice(timings, func(i, j int) bool { return timings[i] > timings[j] })
	for _, line := range timings[:min(12, len(timings))] {
		t.Log(line)
	}
	t.Logf("%d requests", len(timings))
	for _, d := range diffs {
		t.Error(d)
	}
}

// anyAccountHasRewards reports whether the node lists a reward for any of the
// stake addresses.
func anyAccountHasRewards(
	t *testing.T,
	p parityNode,
	stakes []string,
) bool {
	t.Helper()
	for _, stake := range stakes {
		var rewards []json.RawMessage
		if p.getJSON(
			t,
			"/api/v0/accounts/"+stake+"/rewards?count=1",
			&rewards,
		) &&
			len(rewards) > 0 {
			return true
		}
	}
	return false
}

// kupoPaths expands the Kupo GET routes, except the health and metrics
// routes, over the discovered addresses, assets and slots.
func (ids parityIDs) kupoPaths(patterns []string) []string {
	candidates := map[string][]string{
		"pattern": {
			"*", ids.first(ids.addresses, "*"),
			ids.first(ids.stakes, "*"),
			ids.first(ids.scripts, "*") + ".*",
		},
		"payment":     {"*"},
		"delegation":  {"*"},
		"datum_hash":  {parityMissingHash},
		"script_hash": capList(ids.scripts, 2),
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
		if method != http.MethodGet || strings.HasPrefix(path, "/v1") ||
			path == "/health" || path == "/metrics" {
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
		for _, p := range paths {
			if strings.HasPrefix(p, "/matches") {
				qs := queries
				if p == "/matches" || strings.Contains(p, "%2A") &&
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
// taking the network identifier the shared node announces.
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
// taken from what the shared node's search returns.
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
		{svc + "SearchUtxos", `{"maxItems":50}`},
	}
	for _, addr := range capList(ids.addresses, 4) {
		out = append(out, parityRequest{svc + "SearchUtxos", search(addr)})
	}
	found := parityDo(t, http.MethodPost, ref.utxorpc+svc+"SearchUtxos",
		`{"maxItems":50}`)
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

// allAssets returns every asset a node lists, sorted.
func allAssets(t *testing.T, p parityNode) []string {
	t.Helper()
	var out []string
	for page := 1; ; page++ {
		r := parityDo(t, http.MethodGet, fmt.Sprintf(
			"%s/api/v0/assets?count=100&page=%d", p.blockfrost, page,
		), "")
		require.Equal(t, http.StatusOK, r.status, r.body)
		var rows []struct {
			Asset    string `json:"asset"`
			Quantity string `json:"quantity"`
		}
		require.NoError(t, json.Unmarshal([]byte(r.body), &rows))
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			out = append(out, row.Asset+" "+row.Quantity)
		}
	}
	require.NotEmpty(t, out, p.name+" lists assets")
	sort.Strings(out)
	return out
}

// drainConsensusUtxos empties the UTxO table of a split data directory's
// consensus store. The split layout's API readers must find UTxOs in the
// IndexerDB alone: a consensus store that keeps the UTxO set outside SQL has
// none there, and a reader that still consults it answers as if no output
// existed.
func drainConsensusUtxos(t *testing.T, dataDir string) {
	t.Helper()
	raw := openRaw(t, filepath.Join(dataDir, "metadata.sqlite"))
	res, err := raw.ExecContext(t.Context(), "DELETE FROM utxo")
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.NotZero(t, n, "the consensus store held UTxOs to drain")
	require.NoError(t, raw.Close())
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
