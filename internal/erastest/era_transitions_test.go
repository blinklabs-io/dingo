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
const transitionTimeoutSlots = 200

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
	cmd := exec.Command("docker", "exec", dingoColdKeyContainer, "cat", dingoColdKeyPath)
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
// timeout for the configured slot length, with a floor of 60s so very
// short slot lengths still leave room for bootstrap variance.
func transitionTimeout(cfg *Config) time.Duration {
	d := time.Duration(transitionTimeoutSlots) * cfg.SlotDuration()
	if d < 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

// TestEraSchedule asserts that every era transition configured in the
// testnet.yaml is observed on dingo's chain at the expected epoch. The
// final observed era must be the last successor in the schedule
// (Conway, for the eras testnet.yaml).
func TestEraSchedule(t *testing.T) {
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
	dingoEndpoint := endpoints[0]
	stream := NewEraStream(
		t, dingoEndpoint, cfg.NetworkMagic, cfg.EpochLength,
	)
	defer stream.Close()

	timeout := transitionTimeout(cfg)
	for _, want := range schedule {
		hdr, err := stream.WaitForEra(&want.Era, timeout)
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
		// A scheduled fork takes effect at the rollover into the
		// configured epoch. The first header in the new era is in that
		// epoch (within ±1 to absorb genesis-time skew + the "first
		// post-fork header may be a slot or two late" case).
		require.LessOrEqualf(
			t, absDelta(observedEpoch, want.Epoch), uint64(1),
			"%s observed at epoch %d but configured for %d",
			want.Era.Name, observedEpoch, want.Epoch,
		)
	}

	last, ok := stream.LatestHeader()
	require.True(t, ok, "no headers observed")
	finalWanted := schedule[len(schedule)-1].Era
	require.Equalf(
		t, finalWanted.Id, last.EraID,
		"final observed era ID %d != configured terminal era %s (%d)",
		last.EraID, finalWanted.Name, finalWanted.Id,
	)
}

// TestNoStallAcrossForks asserts that no era transition stalls forging
// for an unreasonable number of slots. Each gap between consecutive
// observed headers must stay under maxGapSlots; if a fork transition
// pauses the chain, the gap covering that boundary blows past the
// bound.
func TestNoStallAcrossForks(t *testing.T) {
	cfg := loadConfig(t)
	schedule := cfg.ScheduledTransitions()
	require.NotEmpty(t, schedule, "eras testnet.yaml must configure forks")

	endpoints := DefaultEndpoints()
	stream := NewEraStream(
		t, endpoints[0], cfg.NetworkMagic, cfg.EpochLength,
	)
	defer stream.Close()

	// Wait until the chain has reached the terminal era.
	terminal := schedule[len(schedule)-1].Era
	_, err := stream.WaitForEra(
		&terminal,
		transitionTimeout(cfg)*time.Duration(len(schedule)),
	)
	require.NoErrorf(
		t, err,
		"WaitForEra(%s): %v", terminal.Name, err,
	)
	// Give the chain one extra epoch in the terminal era so we have
	// post-final-fork data points for the stall analysis.
	finalSlot := schedule[len(schedule)-1].Slot + cfg.EpochLength
	_, err = stream.WaitForSlot(finalSlot, transitionTimeout(cfg))
	require.NoErrorf(
		t, err, "WaitForSlot(%d): %v", finalSlot, err,
	)

	headers := stream.HeadersSnapshot()
	require.GreaterOrEqual(
		t, len(headers), 2,
		"need ≥2 headers to measure gaps",
	)

	// Allowed gap: 4*expectedBlockTime (i.e. 4*slot/f). With f=0.4
	// and 1s slots that's 10 slots — comfortable for normal VRF
	// variance without masking a multi-epoch stall. We scale by k as
	// well so smaller-k testnets (which run shorter forks) still see
	// reasonable bounds.
	maxGapSlots := uint64(10) + cfg.SecurityParam
	var (
		biggestGap         uint64
		biggestGapPrevSlot uint64
		biggestGapNextSlot uint64
	)
	for i := 1; i < len(headers); i++ {
		gap := headers[i].Slot - headers[i-1].Slot
		if gap > biggestGap {
			biggestGap = gap
			biggestGapPrevSlot = headers[i-1].Slot
			biggestGapNextSlot = headers[i].Slot
		}
	}
	t.Logf(
		"largest inter-block gap: %d slots (between slot %d and slot %d) "+
			"— max allowed %d",
		biggestGap, biggestGapPrevSlot, biggestGapNextSlot, maxGapSlots,
	)
	require.LessOrEqualf(
		t, biggestGap, maxGapSlots,
		"chain stalled across at least one boundary: gap of %d slots "+
			"between %d and %d (allowed %d)",
		biggestGap, biggestGapPrevSlot, biggestGapNextSlot, maxGapSlots,
	)
}

// TestConsensusAtEachFork opens an EraStream against every endpoint
// and asserts that, by the time each node has reached the terminal
// era, all nodes agree on the slot at which each scheduled fork
// occurred.
func TestConsensusAtEachFork(t *testing.T) {
	cfg := loadConfig(t)
	schedule := cfg.ScheduledTransitions()
	require.NotEmpty(t, schedule, "eras testnet.yaml must configure forks")

	endpoints := DefaultEndpoints()
	streams := make(map[string]*EraStream, len(endpoints))
	for _, ep := range endpoints {
		streams[ep.Name] = NewEraStream(
			t, ep, cfg.NetworkMagic, cfg.EpochLength,
		)
	}
	defer func() {
		for _, s := range streams {
			s.Close()
		}
	}()

	terminal := schedule[len(schedule)-1].Era
	timeout := transitionTimeout(cfg) * time.Duration(len(schedule))
	for name, s := range streams {
		_, err := s.WaitForEra(&terminal, timeout)
		require.NoErrorf(
			t, err,
			"node %s did not reach %s within %s: %v",
			name, terminal.Name, timeout, err,
		)
	}

	// For each scheduled era, every node should report the same
	// transition slot (i.e. the same first-block-in-new-era slot).
	for _, want := range schedule {
		slotByNode := make(map[string]uint64, len(streams))
		for name, s := range streams {
			for _, tr := range s.Transitions() {
				if tr.ToEraID == want.Era.Id {
					slotByNode[name] = tr.Slot
					break
				}
			}
		}
		require.Equalf(
			t, len(streams), len(slotByNode),
			"%s: not every node observed the transition: %+v",
			want.Era.Name, slotByNode,
		)
		// All values must be identical.
		var canonical uint64
		var canonicalNode string
		for name, slot := range slotByNode {
			if canonicalNode == "" {
				canonical = slot
				canonicalNode = name
				continue
			}
			require.Equalf(
				t, canonical, slot,
				"era %s: %s observed transition at slot %d but %s saw it at %d",
				want.Era.Name, canonicalNode, canonical, name, slot,
			)
		}
		t.Logf(
			"all nodes agree: %s transition observed at slot %d",
			want.Era.Name, canonical,
		)
	}
}

// TestDingoProducesInEachEra asserts that pool 1 (the dingo producer)
// successfully forges at least one block in every era the chain
// enters, proving dingo's forging code path correctly handles each
// era.
//
// Pool selection is VRF-driven, but with two equal-stake pools and
// f=0.4 over a 75-slot epoch the probability that dingo wins zero
// slots in any given epoch is ≈ (1-0.225)^75 ≈ 1e-8, so VRF variance
// is not a realistic source of flakes here.
func TestDingoProducesInEachEra(t *testing.T) {
	cfg := loadConfig(t)
	schedule := cfg.ScheduledTransitions()
	require.NotEmpty(t, schedule, "eras testnet.yaml must configure forks")

	dingoVkey := readDingoColdVKey(t)
	t.Logf("dingo cold vkey: %s", hex.EncodeToString(dingoVkey))

	// Observe via the relay because it sees blocks from both producers
	// without contributing its own. Issuer vkeys on its chain therefore
	// reflect the actual producer of each block.
	endpoints := DefaultEndpoints()
	relay := endpoints[2]
	stream := NewEraStream(
		t, relay, cfg.NetworkMagic, cfg.EpochLength,
	)
	defer stream.Close()

	terminal := schedule[len(schedule)-1].Era
	timeout := transitionTimeout(cfg) * time.Duration(len(schedule))
	_, err := stream.WaitForEra(&terminal, timeout)
	require.NoErrorf(
		t, err, "relay did not reach %s within %s",
		terminal.Name, timeout,
	)
	// Wait one more epoch in the terminal era to give dingo a chance
	// to forge a block there.
	finalSlot := schedule[len(schedule)-1].Slot + cfg.EpochLength
	_, err = stream.WaitForSlot(finalSlot, transitionTimeout(cfg))
	require.NoErrorf(
		t, err, "relay did not advance past slot %d", finalSlot,
	)

	headers := stream.HeadersSnapshot()
	dingoBlocksByEra := make(map[uint]int)
	totalByEra := make(map[uint]int)
	for _, h := range headers {
		totalByEra[h.EraID]++
		if bytes.Equal(h.IssuerVkey, dingoVkey) {
			dingoBlocksByEra[h.EraID]++
		}
	}

	for _, want := range schedule {
		total := totalByEra[want.Era.Id]
		dingo := dingoBlocksByEra[want.Era.Id]
		t.Logf(
			"%s: total blocks=%d, dingo-issued=%d",
			want.Era.Name, total, dingo,
		)
		require.Greaterf(
			t, total, 0,
			"no blocks at all observed in era %s — chain stalled?",
			want.Era.Name,
		)
		require.Greaterf(
			t, dingo, 0,
			"dingo did not issue any blocks in era %s",
			want.Era.Name,
		)
	}
}

// absDelta returns |a-b| for unsigned values without underflow.
func absDelta(a, b uint64) uint64 {
	if a >= b {
		return a - b
	}
	return b - a
}
