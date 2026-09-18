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

package cardano

import (
	"bytes"
	"io"
	"os/exec"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The genesis hashes below are chain constants: a node that computes a
// different one for a network joins a different chain. They are pinned here,
// in test source, rather than being read from the network's own config.json,
// because config.json ships in the same commit as the genesis file it
// describes. Editing both together keeps every runtime check green and every
// existing test green while changing the constant, and an anchor that lives
// beside the thing it anchors cannot catch that.
//
// Provenance. The preview, preprod, and mainnet values are the ones published
// upstream at book.play.dev.cardano.org/environments/<network>/config.json,
// checked field by field against the values below. musashi is a Blink Labs
// network with no upstream publication, so its values are anchored only to the
// bytes committed here; they are still worth pinning, because that is enough
// to make an unintended edit to those bytes fail loudly.
//
// The checkpoints files are Blink Labs artifacts and have no upstream
// equivalent either. Their hashes guard the same property: the checkpoint set
// a node validates against is fixed by these bytes.
//
// Byron is hashed over a canonical re-serialization
// (canonicalizeByronGenesisJSON); every other era is hashed over the file
// bytes with line endings normalized (replaceGenesisLineEndings). Both are
// part of the constant, so both are covered by these values.
type embeddedGenesisHashes struct {
	byron       string
	shelley     string
	alonzo      string
	conway      string
	dijkstra    string
	checkpoints string
}

var pinnedEmbeddedGenesisHashes = map[string]embeddedGenesisHashes{
	"mainnet": {
		byron:       "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb",
		shelley:     "1a3be38bcbb7911969283716ad7aa550250226b76a61fc51cc9a9a35d9276d81",
		alonzo:      "7e94a15f55d1e82d10f09203fa1d40f8eede58fd8066542cf6566008068ed874",
		conway:      "15a199f895e461ec0ffc6dd4e4028af28a492ab4e806d39cb674c88f7643ef62",
		checkpoints: "3e6dee5bae7acc6d870187e72674b37c929be8c66e62a552cf6a876b1af31ade",
	},
	"preprod": {
		byron:   "d4b8de7a11d929a323373cbab6c1a9bdc931beffff11db111cf9d57356ee1937",
		shelley: "162d29c4e1cf6b8a84f2d692e67a3ac6bc7851bc3e6e4afe64d15778bed8bd86",
		alonzo:  "7e94a15f55d1e82d10f09203fa1d40f8eede58fd8066542cf6566008068ed874",
		conway:  "0eb6adaec3fcb1fe286c1b4ae0da2a117eafc3add51e17577d36dd39eddfc3db",
	},
	"preview": {
		byron:       "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761",
		shelley:     "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
		alonzo:      "7e94a15f55d1e82d10f09203fa1d40f8eede58fd8066542cf6566008068ed874",
		conway:      "9cc5084f02e27210eacba47af0872e3dba8946ad9460b6072d793e1d2f3987ef",
		checkpoints: "bb5056ff1ced9d68dd99720695789664f6bf6f0cb02a4010df09b813e225ac51",
	},
	"musashi": {
		byron:    "5809f031d8dc8ae8091f66a80ebe8f7d173e475436110c462a0533ed65954639",
		shelley:  "1944510a4fd91415444285231058f6f6ff0f6f3ff3d0356c76c00c5a77f29567",
		alonzo:   "387a7c4880477ce7b128566fa7f9f9ed99ee04476084e9f6332b6d42d907faab",
		conway:   "e2951aa7f08dcd89bb6ca7fcf9acae5c46bdefb5a9affbac769bbe1902e982eb",
		dijkstra: "aa1238f505479a9b104d2cc001b4bf951062cd527200bea9a1857bdd0dc41085",
	},
}

// unpinnableEmbeddedNetworks lists embedded networks that deliberately ship no
// genesis hash and cannot be pinned here.
//
// devnet is the only one. devmode.sh rewrites config/cardano/devnet's Byron
// startTime and Shelley systemStart in place on every start, so its genesis
// bytes are different on a developer's machine from one run to the next. A
// declared hash in devnet/config.json would refuse to start the bundled
// devnet, and a pinned hash here would fail as soon as anyone ran it. Devnet
// genesis is guarded by its parameter values instead -- see
// TestDevnetGenesisIsUsable and TestDevnetCostModelsCoverEveryPricedParameter.
var unpinnableEmbeddedNetworks = map[string]string{
	"devnet": "devmode.sh rewrites its Byron startTime and Shelley systemStart in place",
}

func (h embeddedGenesisHashes) byEra() map[string]string {
	return map[string]string{
		"byron":       h.byron,
		"shelley":     h.shelley,
		"alonzo":      h.alonzo,
		"conway":      h.conway,
		"dijkstra":    h.dijkstra,
		"checkpoints": h.checkpoints,
	}
}

func computedEmbeddedHashes(c *CardanoNodeConfig) map[string]string {
	return map[string]string{
		"byron":       c.ByronGenesisHash,
		"shelley":     c.ShelleyGenesisHash,
		"alonzo":      c.AlonzoGenesisHash,
		"conway":      c.ConwayGenesisHash,
		"dijkstra":    c.DijkstraGenesisHash,
		"checkpoints": c.CheckpointsFileHash,
	}
}

// TestEmbeddedGenesisHashesMatchPinnedValues loads each shipped network the
// way the node does and checks the hash it computes over the embedded bytes
// against the pinned constant.
//
// Nothing else does this. loadGenesisConfigsFromEmbed compares the computed
// hash against the one in the same directory's config.json, so the two move
// together; TestEmbedFS_AllNetworks and
// TestEmbeddedConfigsPassGenesisConsistency load the configs but assert
// nothing about the hashes. A change to the embedded bytes accompanied by a
// matching config.json edit therefore changed the chain constant with the
// whole suite green.
func TestEmbeddedGenesisHashesMatchPinnedValues(t *testing.T) {
	t.Parallel()

	for network, pinned := range pinnedEmbeddedGenesisHashes {
		t.Run(network, func(t *testing.T) {
			t.Parallel()

			cfg, err := NewCardanoNodeConfigFromEmbedFS(
				EmbeddedConfigFS,
				network+"/config.json",
			)
			require.NoError(t, err)

			computed := computedEmbeddedHashes(cfg)
			for era, want := range pinned.byEra() {
				assert.Equal(
					t,
					want,
					computed[era],
					"%s %s hash changed; the embedded bytes no longer produce the pinned chain constant",
					network,
					era,
				)
			}
		})
	}
}

// TestEmbeddedNetworksDeclareGenesisHashes checks that every genesis and
// checkpoints file a shipped network references also has a declared hash in
// that network's config.json.
//
// validateGenesisHash only compares when the expected hash is non-empty, so a
// file shipped without one is accepted unconditionally at runtime. That gate
// is correct for operator-supplied and generated genesis -- see
// TestValidateGenesisHashEmpty -- but a network Dingo itself embeds has no
// reason to omit the hash, and omitting it removes the runtime check
// entirely.
func TestEmbeddedNetworksDeclareGenesisHashes(t *testing.T) {
	t.Parallel()

	for network := range pinnedEmbeddedGenesisHashes {
		t.Run(network, func(t *testing.T) {
			t.Parallel()

			cfg, err := NewCardanoNodeConfigFromEmbedFS(
				EmbeddedConfigFS,
				network+"/config.json",
			)
			require.NoError(t, err)

			declared := computedEmbeddedHashes(cfg)
			for era, file := range map[string]string{
				"byron":       cfg.ByronGenesisFile,
				"shelley":     cfg.ShelleyGenesisFile,
				"alonzo":      cfg.AlonzoGenesisFile,
				"conway":      cfg.ConwayGenesisFile,
				"dijkstra":    cfg.DijkstraGenesisFile,
				"checkpoints": cfg.CheckpointsFile,
			} {
				if file == "" {
					continue
				}
				assert.NotEmpty(
					t,
					declared[era],
					"%s references %s with no declared hash, so nothing verifies it at runtime",
					network,
					file,
				)
			}
		})
	}
}

// TestEveryEmbeddedNetworkIsPinnedOrExempt makes adding a network to
// EmbeddedConfigFS without pinning its hashes a test failure rather than a
// silent omission.
func TestEveryEmbeddedNetworkIsPinnedOrExempt(t *testing.T) {
	t.Parallel()

	entries, err := EmbeddedConfigFS.ReadDir(".")
	require.NoError(t, err)

	seen := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		seen++
		network := entry.Name()
		_, pinned := pinnedEmbeddedGenesisHashes[network]
		reason, exempt := unpinnableEmbeddedNetworks[network]
		assert.True(
			t,
			pinned || exempt,
			"embedded network %s has no pinned genesis hashes and no recorded exemption",
			network,
		)
		if exempt {
			assert.NotEmpty(
				t,
				reason,
				"exemption for %s must record why it cannot be pinned",
				network,
			)
			assert.False(
				t,
				pinned,
				"%s is both pinned and exempt",
				network,
			)
		}
	}
	require.NotZero(t, seen, "no embedded networks found")
}

// TestEmbeddedGenesisHashIsLineEndingInvariant covers what made a line-ending
// change to a genesis file possible to miss: replaceGenesisLineEndings
// normalizes CRLF before hashing, so a checkout that converted the file would
// still compute the right constant. Nothing tested that, so removing the
// normalization would have reintroduced the exposure silently.
//
// Byron is covered by the same property through a different mechanism:
// canonicalizeByronGenesisJSON re-serializes the parsed document, which
// discards the original whitespace entirely.
//
// Both forms are derived here rather than read from the file as-is. The
// embedded bytes are whatever the working tree holds, and a Windows checkout
// converts text files to CRLF by default, so treating them as the LF form
// would build "\r\r\n" from an already-converted file and compare against a
// hash no platform computes.
func TestEmbeddedGenesisHashIsLineEndingInvariant(t *testing.T) {
	t.Parallel()

	for network, pinned := range pinnedEmbeddedGenesisHashes {
		t.Run(network, func(t *testing.T) {
			t.Parallel()

			f, err := EmbeddedConfigFS.Open(network + "/shelley-genesis.json")
			require.NoError(t, err)
			defer f.Close()
			raw, err := io.ReadAll(f)
			require.NoError(t, err)

			lf := replaceGenesisLineEndings(raw)
			crlf := bytes.ReplaceAll(lf, []byte("\n"), []byte("\r\n"))
			require.NotEqual(t, lf, crlf, "conversion produced no change")

			for name, in := range map[string][]byte{"lf": lf, "crlf": crlf} {
				hash, err := validateGenesisHash(
					"Shelley",
					"",
					replaceGenesisLineEndings(in),
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					pinned.shelley,
					hash,
					"%s shelley genesis hash differs for %s line endings",
					network,
					name,
				)
			}

			byronFile, err := EmbeddedConfigFS.Open(
				network + "/byron-genesis.json",
			)
			require.NoError(t, err)
			defer byronFile.Close()
			byronRaw, err := io.ReadAll(byronFile)
			require.NoError(t, err)
			byronLF := replaceGenesisLineEndings(byronRaw)
			byronCRLF := bytes.ReplaceAll(
				byronLF,
				[]byte("\n"),
				[]byte("\r\n"),
			)
			for name, in := range map[string][]byte{
				"lf":   byronLF,
				"crlf": byronCRLF,
			} {
				canonical, err := canonicalizeByronGenesisJSON(in)
				require.NoError(t, err)
				assert.Equal(
					t,
					pinned.byron,
					lcommon.Blake2b256Hash(canonical).String(),
					"%s byron genesis hash differs for %s line endings",
					network,
					name,
				)
			}
		})
	}
}

// committedRepoRoot returns the top level of the repository checkout, or skips
// when there is none to read. A module fetched from the proxy has no .git, and
// the property under test is about committed content, so there is nothing to
// assert in that case.
func committedRepoRoot(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf(
			"cannot read committed bytes: git rev-parse --show-toplevel failed: %v",
			err,
		)
	}
	return strings.TrimSpace(string(out))
}

// committedBytes returns the bytes of repoPath as committed at HEAD. git
// cat-file emits the blob itself, so the result is independent of the
// platform's checkout conversion and of any in-place edit to the working tree.
func committedBytes(t *testing.T, root, repoPath string) []byte {
	t.Helper()

	cmd := exec.Command("git", "-C", root, "cat-file", "blob", "HEAD:"+repoPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(
		t,
		err,
		"read committed %s: %s",
		repoPath,
		strings.TrimSpace(stderr.String()),
	)
	return out
}

// TestCommittedGenesisFilesUseLF checks that no genesis or checkpoints file is
// committed with carriage returns.
//
// This reads the blob out of git rather than the working tree on purpose. The
// working tree is a per-platform artifact: git converts text files to CRLF on
// a Windows checkout by default, so a no-carriage-return assertion over the
// embedded bytes asserts a property that platform does not have. What the
// repository can promise is about what is committed, and that is the same
// everywhere.
//
// The hash tests cannot stand in for this. replaceGenesisLineEndings
// normalizes before hashing, so a file committed with CRLF still produces the
// pinned constant and leaves every other test here green. This is the only
// check that fails on it.
func TestCommittedGenesisFilesUseLF(t *testing.T) {
	t.Parallel()

	root := committedRepoRoot(t)
	entries, err := EmbeddedConfigFS.ReadDir(".")
	require.NoError(t, err)

	checked := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		network := entry.Name()
		files, err := EmbeddedConfigFS.ReadDir(network)
		require.NoError(t, err)
		for _, file := range files {
			name := file.Name()
			if !strings.HasSuffix(name, "-genesis.json") &&
				name != "checkpoints.json" {
				continue
			}
			checked++
			// git addresses blobs with forward slashes on every platform.
			repoPath := "config/cardano/" + network + "/" + name
			assert.Zero(
				t,
				bytes.Count(committedBytes(t, root, repoPath), []byte("\r")),
				"%s is committed with carriage returns; genesis files must be committed with LF",
				repoPath,
			)
		}
	}
	require.NotZero(t, checked, "no genesis files found")
}
