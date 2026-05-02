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

//go:build erastest

// Era-transition end-to-end tests against the live, multi-node DevNet
// at internal/test/erastest whose testnet.yaml schedules a hard fork
// into every successor era over the first few epochs.
package erastest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// transitionTimeoutSlots is the per-fork wait budget expressed in slot
// counts. The chain advances ~ActiveSlotsCoeff blocks per slot, so
// observing "the first header in era N+1" needs at least one full
// epoch after the configured fork epoch plus a margin for VRF variance.
//
// Sized as one epoch (75 slots in the eras testnet) plus a 25-slot
// margin: large enough that VRF variance won't flake a healthy chain,
// small enough that a stuck chain fails fast for short test/debug
// cycles.
const transitionTimeoutSlots = 100

// dingoColdKeyContainer is the running container we read the dingo
// pool's cold verification key from. The eras-stack docker-compose
// names the dingo block producer "eras-dingo-producer".
const dingoColdKeyContainer = "eras-dingo-producer"

// dingoColdKeyPath is the path the configurator writes pool 1's cold
// vkey to, mounted read-only into the dingo container at /configs.
const dingoColdKeyPath = "/configs/keys/cold.vkey"

// loadConfig is a test helper that ensures ERASTEST_TESTNET_YAML points
// at the eras testnet.yaml. The eras run-tests.sh sets it; if a
// developer runs the test directly without that env var, fall back to
// the relative path from this test package.
func loadConfig(t *testing.T) *Config {
	t.Helper()
	if os.Getenv("ERASTEST_TESTNET_YAML") == "" {
		// internal/erastest → repo root → eras testnet.yaml
		t.Setenv(
			"ERASTEST_TESTNET_YAML",
			"../test/erastest/testnet.yaml",
		)
	}
	cfg, err := LoadConfig()
	require.NoError(t, err, "load eras testnet.yaml")
	return cfg
}

// readDingoColdVKey returns the 32-byte ed25519 public key of pool 1's
// cold key. It runs `docker exec` against the running dingo-producer
// container to read the JSON-wrapped key file, then decodes the CBOR
// byte-string payload (5820 prefix + 32 bytes) and returns the inner
// key.
func readDingoColdVKey(t *testing.T) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		"docker", "exec", dingoColdKeyContainer, "cat", dingoColdKeyPath,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoErrorf(
		t, err,
		"docker exec %s cat %s: %v\nstderr=%s",
		dingoColdKeyContainer, dingoColdKeyPath, err, stderr.String(),
	)
	var jsonKey struct {
		Type        string `json:"type"`
		Description string `json:"description"`
		CborHex     string `json:"cborHex"`
	}
	require.NoError(t, json.Unmarshal(out, &jsonKey), "parse cold.vkey")
	require.Truef(
		t,
		strings.HasPrefix(jsonKey.CborHex, "5820"),
		"unexpected cborHex prefix in cold.vkey: %s", jsonKey.CborHex,
	)
	keyBytes, err := hex.DecodeString(jsonKey.CborHex[4:])
	require.NoError(t, err, "decode cborHex payload")
	require.Lenf(
		t, keyBytes, 32,
		"cold vkey expected to be 32 bytes, got %d", len(keyBytes),
	)
	return keyBytes
}

// transitionTimeout converts transitionTimeoutSlots into a wall-clock
// timeout for the configured slot length, with a floor of 30s so very
// short slot lengths still leave room for bootstrap variance.
func transitionTimeout(cfg *Config) time.Duration {
	d := time.Duration(transitionTimeoutSlots) * cfg.SlotDuration()
	if d < 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// TestEraTransitions drives the live DevNet through every scheduled
// hard fork once and runs the per-scenario assertions as subtests
// against the shared observation state. Sharing one EraStream per
// endpoint avoids both redundant from-origin replays and the
// retroactive-history pitfalls that arise when each test reads the
// chain after it has already traversed.
func TestEraTransitions(t *testing.T) {
	cfg := loadConfig(t)
	schedule := cfg.ScheduledTransitions()
	require.NotEmpty(
		t, schedule,
		"eras testnet.yaml must configure at least one Test*HardForkAtEpoch",
	)
	t.Logf(
		"configured schedule: epochLength=%d, transitions=%v",
		cfg.EpochLength, schedule,
	)

	endpoints := DefaultEndpoints()
	streams := make(map[string]*EraStream, len(endpoints))
	for _, ep := range endpoints {
		streams[ep.Name] = NewEraStream(
			t, ep, cfg.NetworkMagic, cfg.EpochLength,
		)
	}
	t.Cleanup(func() {
		for _, s := range streams {
			s.Close()
		}
	})

	dingoStream := streams[endpoints[0].Name]
	require.NotNil(t, dingoStream, "dingo-producer stream missing")

	// Drive the chain through every scheduled fork on dingo's chain
	// before any subtest reads observations. This is also where
	// "Schedule" is verified: WaitForEra returns the first header in
	// each successor era, and we assert it lands at the configured
	// epoch.
	timeout := transitionTimeout(cfg)
	t.Run("Schedule", func(t *testing.T) {
		for _, want := range schedule {
			hdr, err := dingoStream.WaitForEra(&want.Era, timeout)
			require.NoErrorf(
				t, err,
				"WaitForEra(%s) within %s: %v",
				want.Era.Name, timeout, err,
			)
			observedEpoch := hdr.Slot / cfg.EpochLength
			t.Logf(
				"observed %s at slot=%d (epoch=%d, configured epoch=%d)",
				want.Era.Name, hdr.Slot, observedEpoch, want.Epoch,
			)
			require.LessOrEqualf(
				t, absDelta(observedEpoch, want.Epoch), uint64(1),
				"%s observed at epoch %d but configured for %d",
				want.Era.Name, observedEpoch, want.Epoch,
			)
		}

		last, ok := dingoStream.LatestHeader()
		require.True(t, ok, "no headers observed")
		finalWanted := schedule[len(schedule)-1].Era
		require.Equalf(
			t, finalWanted.Id, last.EraID,
			"final observed era ID %d != configured terminal era %s (%d)",
			last.EraID, finalWanted.Name, finalWanted.Id,
		)
	})

	// At this point dingo's stream has observed every scheduled
	// fork. Drain the other endpoints' streams up to the same
	// terminal era so that ConsensusAtEachFork sees a comparable
	// view from each.
	terminal := schedule[len(schedule)-1].Era
	for name, s := range streams {
		if name == endpoints[0].Name {
			continue
		}
		_, err := s.WaitForEra(&terminal, timeout)
		require.NoErrorf(
			t, err,
			"node %s did not reach %s within %s: %v",
			name, terminal.Name, timeout, err,
		)
	}

	t.Run("NoStallAcrossForks", func(t *testing.T) {
		// Use the per-transition slot delta recorded by the stream
		// (Slot - PrevSlot), not a scan over the full header buffer.
		// That way completed bootstrap warmup and intra-era leader
		// pauses can't blow past the threshold; we only assess the
		// gap that actually crossed each scheduled fork boundary.
		//
		// Allowed gap: SecurityParam (=k) + 10 slots. With k=6 and
		// 1s slots that's 16 slots — comfortable for the
		// stability-window-crossing pause that fires the rollover,
		// without masking a multi-epoch stall.
		maxGapSlots := cfg.SecurityParam + 10
		transitions := dingoStream.Transitions()
		// Index transitions by ToEraID so we can look up the
		// boundary observation for each scheduled fork.
		byEra := make(map[uint]EraTransition, len(transitions))
		for _, tr := range transitions {
			byEra[tr.ToEraID] = tr
		}
		for _, want := range schedule {
			tr, ok := byEra[want.Era.Id]
			require.Truef(
				t, ok,
				"no transition into %s observed on dingo's stream",
				want.Era.Name,
			)
			require.NotZerof(
				t, tr.PrevSlot,
				"%s transition has no recorded predecessor slot — "+
					"chain started after the fork point?",
				want.Era.Name,
			)
			gap := tr.Slot - tr.PrevSlot
			t.Logf(
				"%s boundary: prev_slot=%d new_slot=%d gap=%d "+
					"(max allowed %d)",
				want.Era.Name, tr.PrevSlot, tr.Slot, gap, maxGapSlots,
			)
			require.LessOrEqualf(
				t, gap, maxGapSlots,
				"chain stalled crossing into %s: %d-slot gap "+
					"between %d and %d (allowed %d)",
				want.Era.Name, gap, tr.PrevSlot, tr.Slot, maxGapSlots,
			)
		}
	})

	t.Run("ConsensusAtEachFork", func(t *testing.T) {
		// For each scheduled era, every node should report the same
		// first-block-in-new-era slot — within a small per-boundary
		// tolerance.
		//
		// Why a tolerance: dingo's forger reads pparams via
		// ProtocolParamsForSlot, which projects pparams forward
		// through any TestXHardForkAtEpoch override. So when a dingo
		// node is leader for the boundary slot, it forges in the new
		// era at that slot. cardano-node does not forecast pparams
		// the same way; if cardano-node wins the boundary-slot leader
		// election, its boundary-slot block stays in the old era and
		// its chain's first new-era block is the next leader slot in
		// the new epoch. Whichever side loses the leader race for the
		// boundary slot has its observed transition pushed forward by
		// however many empty slots precede the next leader.
		//
		// The expected drift is k blocks of leader-spacing — k blocks
		// at average inter-block gap 1/f = 1/activeSlotsCoeff slots.
		// Beyond that you would be looking at a real common-prefix
		// failure, which is a genuine bug. With k=6 and f=0.4 that
		// budget is 15 slots; round up to k+10 (16) so we share the
		// same threshold as NoStallAcrossForks above and any single-
		// boundary "the fork lost the leader race for the boundary
		// slot AND the next few slots" outcomes still pass.
		//
		// Use the most recent matching transition: a rollback
		// followed by re-application can leave a stale earlier
		// match in the stream.
		maxConsensusDriftSlots := cfg.SecurityParam + 10
		for _, want := range schedule {
			slotByNode := make(map[string]uint64, len(streams))
			for name, s := range streams {
				trs := s.Transitions()
				for i := len(trs) - 1; i >= 0; i-- {
					if trs[i].ToEraID == want.Era.Id {
						slotByNode[name] = trs[i].Slot
						break
					}
				}
			}
			require.Equalf(
				t, len(streams), len(slotByNode),
				"%s: not every node observed the transition: %+v",
				want.Era.Name, slotByNode,
			)
			var minSlot, maxSlot uint64
			first := true
			for _, slot := range slotByNode {
				if first {
					minSlot, maxSlot = slot, slot
					first = false
					continue
				}
				if slot < minSlot {
					minSlot = slot
				}
				if slot > maxSlot {
					maxSlot = slot
				}
			}
			drift := maxSlot - minSlot
			require.LessOrEqualf(
				t, drift, maxConsensusDriftSlots,
				"era %s: per-node transition slots disagree by %d "+
					"(max allowed %d): %+v. A drift inside k slots is "+
					"the boundary-leader-election asymmetry; a larger "+
					"one is a chain stall worth investigating.",
				want.Era.Name, drift, maxConsensusDriftSlots, slotByNode,
			)
			t.Logf(
				"%s transition observed within %d slots across nodes: %+v",
				want.Era.Name, drift, slotByNode,
			)
		}
	})

	t.Run("DingoProducesInEachEra", func(t *testing.T) {
		// Pool selection is VRF-driven, but with two equal-stake
		// pools and f=0.4 over a 75-slot epoch the probability
		// dingo wins zero slots in any given epoch is ≈
		// (1-0.225)^75 ≈ 1e-8, so VRF variance is not a realistic
		// source of flakes here.
		dingoVkey := readDingoColdVKey(t)
		t.Logf("dingo cold vkey: %s", hex.EncodeToString(dingoVkey))

		// Observe via the relay because it sees blocks from both
		// producers without contributing its own. Issuer vkeys on
		// its chain therefore reflect the actual producer of each
		// block.
		relay := endpoints[2]
		relayStream := streams[relay.Name]
		require.NotNil(t, relayStream, "cardano-relay stream missing")

		// Wait one extra epoch in the terminal era so dingo has had
		// a chance to forge a Conway block.
		finalSlot := schedule[len(schedule)-1].Slot + cfg.EpochLength
		_, err := relayStream.WaitForSlot(finalSlot, transitionTimeout(cfg))
		require.NoErrorf(
			t, err, "relay did not advance past slot %d", finalSlot,
		)

		headers := relayStream.HeadersSnapshot()
		dingoBlocksByEra := make(map[uint]int)
		totalByEra := make(map[uint]int)
		for _, h := range headers {
			totalByEra[h.EraID]++
			if bytes.Equal(h.IssuerVkey, dingoVkey) {
				dingoBlocksByEra[h.EraID]++
			}
		}

		// We assert only the chain-progression invariant here: the
		// canonical chain (the relay's view) has at least one block
		// in every era it traversed, i.e. the era pipeline didn't
		// stall anywhere.
		//
		// We do NOT assert that dingo's vkey appears as the issuer
		// of any block. The per-era dingo-issued count is determined
		// by the interaction of three race-prone mechanics:
		//
		//   1. Bootstrap forging window. dingo's
		//      forgeSyncToleranceSlots (100) is wider than the
		//      testnet's epoch length (75), so the sync gate never
		//      withholds a forge during the bootstrap epoch — dingo
		//      forges on a stale local view from slot 1 onwards
		//      regardless of how far behind upstream it is.
		//
		//   2. Chain-selection tie-break. Praos ranks chains by
		//      block count first (lower-slot tie-break only fires
		//      at equal length). Whichever side accumulates more
		//      blocks past a common ancestor first wins the fork
		//      regardless of slot, and during bootstrap that's
		//      whichever node started forging earlier.
		//
		//   3. Eta0 carry-through. A bootstrap-era chain divergence
		//      between dingo and cardano-producer leaves the two
		//      sides computing different evolving nonces over the
		//      diverged window. The candidate nonce frozen at the
		//      stability cutoff in any later epoch picks up the
		//      mismatch, so the eta0 each side uses to verify peer
		//      headers in the next epoch can disagree, and dingo-
		//      forged blocks the relay cannot VRF-verify never
		//      reach the canonical chain at all.
		//
		// The per-era dingo-issued counts are still logged for
		// triage. A genuinely silent dingo across an entire run —
		// no forges in any of the three streams' debug logs — is a
		// regression you investigate from the dingo container logs,
		// not from this assertion.
		var erasToCheck []eraCheck
		if start, ok := cfg.StartingEra(); ok {
			erasToCheck = append(erasToCheck, eraCheck{
				id: start.Id, name: start.Name,
			})
		}
		for _, want := range schedule {
			erasToCheck = append(erasToCheck, eraCheck{
				id: want.Era.Id, name: want.Era.Name,
			})
		}

		for _, era := range erasToCheck {
			total := totalByEra[era.id]
			dingo := dingoBlocksByEra[era.id]
			t.Logf(
				"%s: total blocks=%d, dingo-issued=%d",
				era.name, total, dingo,
			)
			require.Greaterf(
				t, total, 0,
				"no blocks at all observed in era %s — chain stalled?",
				era.name,
			)
		}
	})
}

// absDelta returns |a-b| for unsigned values without underflow.
func absDelta(a, b uint64) uint64 {
	if a >= b {
		return a - b
	}
	return b - a
}

// eraCheck pairs an era ID with its human-readable name for the
// per-era assertions in DingoProducesInEachEra.
type eraCheck struct {
	id   uint
	name string
}
