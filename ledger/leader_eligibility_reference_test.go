// Copyright 2026 Blink Labs Software
// Licensed under the Apache License, Version 2.0.

package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type inboundReferenceSchedule struct {
	ActiveStake   string   `json:"activeStake"`
	AssignedSlots []uint64 `json:"assignedSlots"`
	DDenominator  string   `json:"dDenominator"`
	DNumerator    string   `json:"dNumerator"`
	FDenominator  string   `json:"fDenominator"`
	FNumerator    string   `json:"fNumerator"`
	FirstSlot     uint64   `json:"firstSlot"`
	Mode          string   `json:"mode"`
	NonceHex      string   `json:"nonceHex"`
	PoolStake     string   `json:"poolStake"`
	SlotCount     uint64   `json:"slotCount"`
	VRFSkeyHex    string   `json:"vrfSkeyHex"`
}

func inboundReferenceSchedules(t *testing.T) []inboundReferenceSchedule {
	t.Helper()
	fs := fixtures.EmbeddedFixtures()
	manifest, err := fs.ReadFile("upstream/e34/SHA256SUMS")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(string(manifest), "\n"))
	seen := make(map[string]bool)
	for line := range strings.SplitSeq(
		strings.TrimSuffix(string(manifest), "\n"), "\n",
	) {
		require.GreaterOrEqual(t, len(line), 67)
		require.Equal(t, "  ", line[64:66])
		digest, err := hex.DecodeString(line[:64])
		require.NoError(t, err)
		require.Len(t, digest, sha256.Size)
		path := line[66:]
		require.NotEmpty(t, path)
		require.False(t, seen[path])
		seen[path] = true
		data, err := fs.ReadFile("upstream/e34/" + path)
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		require.Equal(t, line[:64], hex.EncodeToString(sum[:]), path)
	}
	const path = "jormanager/node-schedules.json"
	require.True(t, seen[path])
	data, err := fs.ReadFile("upstream/e34/" + path)
	require.NoError(t, err)
	var corpus struct {
		Cases    []inboundReferenceSchedule `json:"cases"`
		Metadata json.RawMessage            `json:"metadata"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&corpus))
	require.ErrorIs(t, decoder.Decode(new(any)), io.EOF)
	require.Len(t, corpus.Cases, 21)
	require.NotEmpty(t, corpus.Metadata)
	for _, c := range corpus.Cases {
		require.Contains(t, []string{"praos", "tpraos"}, c.Mode)
		for _, field := range []string{c.ActiveStake, c.PoolStake, c.FNumerator, c.FDenominator, c.DNumerator, c.DDenominator} {
			inboundReferenceUint(t, field)
		}
		require.Positive(t, inboundReferenceUint(t, c.ActiveStake))
		require.Positive(t, inboundReferenceUint(t, c.FDenominator))
		require.Positive(t, inboundReferenceUint(t, c.DDenominator))
		require.LessOrEqual(
			t,
			inboundReferenceUint(t, c.DNumerator),
			inboundReferenceUint(t, c.DDenominator),
		)
		require.Positive(t, c.SlotCount)
		require.LessOrEqual(t, c.FirstSlot, uint64(1<<63-1)-c.SlotCount)
		require.NotNil(t, c.AssignedSlots)
		for i, slot := range c.AssignedSlots {
			require.GreaterOrEqual(t, slot, c.FirstSlot)
			require.Less(t, slot, c.FirstSlot+c.SlotCount)
			if i > 0 {
				require.Greater(t, slot, c.AssignedSlots[i-1])
			}
		}
		secret, err := hex.DecodeString(c.VRFSkeyHex)
		require.NoError(t, err)
		require.Len(t, secret, 64)
		nonce, err := hex.DecodeString(c.NonceHex)
		require.NoError(t, err)
		require.Len(t, nonce, 32)
	}
	return corpus.Cases
}

func inboundReferenceUint(t *testing.T, text string) uint64 {
	t.Helper()
	require.NotEmpty(t, text)
	for _, c := range text {
		require.True(t, c >= '0' && c <= '9')
	}
	value, err := strconv.ParseUint(text, 10, 64)
	require.NoError(t, err)
	return value
}

// The shared oracle supplies slot membership; legitimate VRF proofs are checked
// independently before entering the real inbound eligibility method. This test
// isolates eligibility from KES/opcert validation, which has separate coverage.
func TestLeaderEligibilityInboundReferenceSchedules(t *testing.T) {
	tested := make(map[string]bool)
	for _, c := range inboundReferenceSchedules(t) {
		if c.SlotCount != 120 || c.PoolStake != "1" || c.ActiveStake != "3" ||
			c.FNumerator != "1" || c.FDenominator != "20" || c.DNumerator != "0" {
			continue
		}
		require.False(t, tested[c.Mode])
		tested[c.Mode] = true
		t.Run(c.Mode, func(t *testing.T) {
			secret, err := hex.DecodeString(c.VRFSkeyHex)
			require.NoError(t, err)
			pk, sk, err := vrf.KeyGen(secret[:32])
			require.NoError(t, err)
			require.Equal(t, secret[32:], pk)
			nonce, err := hex.DecodeString(c.NonceHex)
			require.NoError(t, err)
			require.NotEmpty(t, c.AssignedSlots)
			unassigned := c.FirstSlot
			for slices.Contains(c.AssignedSlots, unassigned) {
				unassigned++
			}
			require.Less(t, unassigned, c.FirstSlot+c.SlotCount)
			for _, tc := range []struct {
				name     string
				slot     uint64
				eligible bool
			}{
				{"eligible", c.AssignedSlots[0], true}, {"ineligible", unassigned, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var input []byte
					mode := consensus.ConsensusModeCPraos
					if c.Mode == "tpraos" {
						mode = consensus.ConsensusModeTPraos
						input, err = vrf.MkSeedTPraos(
							int64(tc.slot),
							nonce,
							vrf.SeedL(),
						) //nolint:gosec // corpus range validated above
					} else {
						input, err = vrf.MkInputVrf(int64(tc.slot), nonce) //nolint:gosec // corpus range validated above
					}
					require.NoError(t, err)
					proof, output, err := vrf.Prove(sk, input)
					require.NoError(t, err)
					verified, err := vrf.Verify(pk, proof, output, input)
					require.NoError(t, err)
					require.True(t, verified)
					result := lcommon.VrfResult{Output: output, Proof: proof}
					var block gledger.Block
					if mode == consensus.ConsensusModeTPraos {
						body := shelley.ShelleyBlockHeaderBody{
							Slot:      tc.slot,
							VrfKey:    pk,
							LeaderVrf: result,
						}
						encoded, err := cbor.Encode(body)
						require.NoError(t, err)
						body.SetCbor(encoded)
						block = &realTPraosBlock{
							header:    &shelley.ShelleyBlockHeader{Body: body},
							era:       shelley.EraShelley,
							blockType: shelley.BlockTypeShelley,
						}
					} else {
						body := babbage.BabbageBlockHeaderBody{Slot: tc.slot, VrfKey: pk, VrfResult: result}
						encoded, err := cbor.Encode(body)
						require.NoError(t, err)
						body.SetCbor(encoded)
						block = &realBabbageBlock{
							header: &babbage.BabbageBlockHeader{Body: body},
							era:    babbage.EraBabbage,
							slot:   tc.slot,
						}
					}
					ls, db := newEligibilityTestLedger(t, nonce)
					ls.config.CardanoNodeConfig.ShelleyGenesis().ActiveSlotsCoeff = cbor.Rat{
						Rat: big.NewRat(1, 20),
					}
					require.LessOrEqual(
						t,
						c.FirstSlot+c.SlotCount,
						uint64(^uint(0)),
					)
					ls.epochCache[0].LengthInSlots = uint(
						c.FirstSlot + c.SlotCount,
					) //nolint:gosec // checked against platform uint maximum
					if mode == consensus.ConsensusModeCPraos {
						ls.epochCache[0].EraId = eras.BabbageEraDesc.Id
						ls.currentEra = eras.BabbageEraDesc
					}
					ls.publishSnapshotsLocked()
					resolvedMode, err := ls.ConsensusModeForEpoch(5)
					require.NoError(t, err)
					require.Equal(t, mode, resolvedMode)
					seedBlockPoolRegistration(t, db, block)
					require.NoError(t, ls.verifyRegisteredVrfKey(block, 5))
					counter := prometheus.NewCounter(prometheus.CounterOpts{
						Name: "test_inbound_eligibility_rejections_total",
						Help: "Actual rejections",
					})
					ls.metrics = stateMetrics{
						leaderThresholdRejections: counter,
					}
					snap := electingSnapshot{
						epoch: 5,
						kind:  models.PoolStakeSnapshotTypeActive,
						row: &models.PoolStakeSnapshot{
							TotalStake:       1,
							StakeDenominator: 3,
						},
					}
					err = ls.verifyBlockLeaderEligibilityFromSnapshot(
						block,
						5,
						snap,
					)
					if tc.eligible {
						require.NoError(t, err)
						require.Zero(t, testutil.ToFloat64(counter))
					} else {
						require.Error(t, err)
						require.Equal(t, float64(1), testutil.ToFloat64(counter))
						ls.config.SkipLeaderStakeThresholdCheck = true
						require.NoError(t, ls.verifyBlockLeaderEligibilityFromSnapshot(block, 5, snap))
						require.Equal(t, float64(1), testutil.ToFloat64(counter), "prototype acceptance is not a rejection")
					}
				})
			}
		})
	}
	require.Equal(t, map[string]bool{"praos": true, "tpraos": true}, tested)
}
