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

package ledger

import (
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type leiosOCINChainBlock struct {
	*mockBabbageBlock
	number uint64
	hash   lcommon.Blake2b256
	prev   lcommon.Blake2b256
}

func (b *leiosOCINChainBlock) BlockNumber() uint64 { return b.number }
func (b *leiosOCINChainBlock) Hash() lcommon.Blake2b256 {
	return b.hash
}
func (b *leiosOCINChainBlock) PrevHash() lcommon.Blake2b256 {
	return b.prev
}

func addLeiosOCINChainBlocks(
	t *testing.T,
	c *chain.Chain,
	count int,
) {
	t.Helper()
	var prev lcommon.Blake2b256
	for i := 1; i <= count; i++ {
		var hash lcommon.Blake2b256
		hash[0] = byte(i)
		block := &leiosOCINChainBlock{
			mockBabbageBlock: &mockBabbageBlock{slot: uint64(i * 10)},
			number:           uint64(i),
			hash:             hash,
			prev:             prev,
		}
		require.NoError(t, c.AddBlock(block, nil))
		prev = hash
	}
}

func leiosOCINHeader(
	issuer lcommon.IssuerVkey,
	sequence uint32,
) *dijkstra.DijkstraBlockHeader {
	return &dijkstra.DijkstraBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				IssuerVkey: issuer,
				OpCert: babbage.BabbageOpCert{
					SequenceNumber: sequence,
				},
			},
		},
	}
}

// TestOpCertFromHeader_Babbage verifies the per-era extractor pulls the opcert
// fields off a Babbage-family header body.
func TestOpCertFromHeader_Babbage(t *testing.T) {
	tb := createTestBlock(t, [32]byte{10}, 9, tamperNone)
	opCert, ok := opCertFromHeader(tb.block.Header())
	require.True(t, ok)
	require.NotNil(t, opCert)

	body := tb.block.header.Body
	assert.Equal(t, body.OpCert.HotVkey, opCert.KesVkey)
	assert.Equal(t, uint64(body.OpCert.SequenceNumber), opCert.IssueNumber)
	assert.Equal(t, uint64(body.OpCert.KesPeriod), opCert.KesPeriod)
	assert.Equal(t, body.OpCert.Signature, opCert.ColdSignature)
}

// TestOpCertFromHeader_NonPraosReturnsFalse verifies headers without an opcert
// (Byron) report ok=false so they are skipped by opcert validation.
func TestOpCertFromHeader_NonPraosReturnsFalse(t *testing.T) {
	opCert, ok := opCertFromHeader(&byron.ByronMainBlockHeader{})
	assert.False(t, ok)
	assert.Nil(t, opCert)
}

// TestVerifyOpCertColdSignature_RealCardanoCliCert is a known-answer test that
// pins the opcert signable representation to real cardano output. The values
// are taken from a real cardano-cli NodeOperationalCertificate
// (config/cardano/devnet/keys/opcert.cert). The signature verifies only under
// the raw 48-byte OCertSignable representation (KES vkey || counter || period);
// gouroboros' CBOR-based VerifyOpCertSignature rejects it, which is exactly why
// verifyOpCertColdSignature does not call that function. If this test ever
// fails, the inbound check has drifted from real cardano and would stall the
// chain by rejecting valid blocks.
func TestVerifyOpCertColdSignature_RealCardanoCliCert(t *testing.T) {
	mustHex := func(s string) []byte {
		b, err := hex.DecodeString(s)
		require.NoError(t, err)
		return b
	}
	opCert := &gledger.OpCert{
		KesVkey: mustHex(
			"4cd49bb05e9885142fe7af1481107995298771fd1a24e72b506a4d600ee2b312",
		),
		IssueNumber: 0,
		KesPeriod:   0,
		ColdSignature: mustHex(
			"89fc9e9f551b2ea873bf31643659d049152d5c8e8de86be4056370bccc5fa62d" +
				"d12e3f152f1664e614763e46eaa7a17ed366b5cef19958773d1ab96941442e0b",
		),
	}
	coldVkey := mustHex(
		"5a3d778e76741a009e29d23093cfe046131808d34d7c864967b515e98dfc3583",
	)

	require.NoError(
		t,
		verifyOpCertColdSignature(opCert, coldVkey),
		"real cardano-cli opcert must verify under the raw OCertSignable representation",
	)

	// A flipped signature must be rejected.
	bad := *opCert
	bad.ColdSignature = append([]byte(nil), opCert.ColdSignature...)
	bad.ColdSignature[0] ^= 0xFF
	require.Error(t, verifyOpCertColdSignature(&bad, coldVkey))
}

// TestVerifyOpCertHeaderCrypto_Valid verifies a well-formed opcert passes the
// inbound cold-signature and KES-period checks.
func TestVerifyOpCertHeaderCrypto_Valid(t *testing.T) {
	tb := createTestBlock(t, [32]byte{11}, 13, tamperNone)
	err := verifyOpCertHeaderCrypto(
		tb.block.Header(),
		tb.block.SlotNumber(),
		tb.slotsPerKesPeriod,
		62,
	)
	require.NoError(t, err)
}

// TestVerifyOpCertHeaderCrypto_TamperedColdSignature verifies a flipped
// cold-key signature is now rejected at header verification — this is the
// behavior gap issue #2608 closes.
func TestVerifyOpCertHeaderCrypto_TamperedColdSignature(t *testing.T) {
	tb := createTestBlock(t, [32]byte{12}, 14, tamperOpCertSig)
	err := verifyOpCertHeaderCrypto(
		tb.block.Header(),
		tb.block.SlotNumber(),
		tb.slotsPerKesPeriod,
		62,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cold-key signature")
}

// TestVerifyOpCertHeaderCrypto_ExpiredKESPeriod verifies an opcert evaluated
// beyond maxKesEvolutions is rejected. The generated opcert starts at KES
// period 0, so evaluating it five KES periods later with a max of three
// evolutions is expired.
func TestVerifyOpCertHeaderCrypto_ExpiredKESPeriod(t *testing.T) {
	tb := createTestBlock(t, [32]byte{13}, 15, tamperNone)
	const slotsPerKesPeriod = uint64(100)
	err := verifyOpCertHeaderCrypto(
		tb.block.Header(),
		5*slotsPerKesPeriod,
		slotsPerKesPeriod,
		3,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KES period")
}

// TestVerifyOpCertHeaderCrypto_MaxKESEvolutionsZeroSkipsExpiry verifies the
// expiry check degrades gracefully when the genesis parameter is unavailable:
// the same far-future slot that expired above passes when maxKesEvolutions is
// zero, leaving the lighter future-cert guard inside VerifyBlock in charge.
func TestVerifyOpCertHeaderCrypto_MaxKESEvolutionsZeroSkipsExpiry(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{14}, 16, tamperNone)
	const slotsPerKesPeriod = uint64(100)
	err := verifyOpCertHeaderCrypto(
		tb.block.Header(),
		5*slotsPerKesPeriod,
		slotsPerKesPeriod,
		0,
	)
	require.NoError(t, err)
}

// TestValidateOpCertCounter exercises the counter rules applied at block-apply
// time. The backward (stale) rule applies to every era; the no-gap
// (over-increment) rule is Praos-only, so the gapped cases are split by
// enforceNoGap: a TPraos block (enforceNoGap=false) accepts a jump, a Praos
// block (enforceNoGap=true) rejects it.
func TestValidateOpCertCounter(t *testing.T) {
	tests := []struct {
		name         string
		stored       uint64
		found        bool
		candidate    uint64
		enforceNoGap bool
		wantErr      string
	}{
		{
			name:         "first sighting accepts any counter",
			found:        false,
			candidate:    7,
			enforceNoGap: true,
		},
		{
			name:         "equal to last seen",
			stored:       5,
			found:        true,
			candidate:    5,
			enforceNoGap: true,
		},
		{
			name:         "exactly one greater",
			stored:       5,
			found:        true,
			candidate:    6,
			enforceNoGap: true,
		},
		{
			name:         "backward counter rejected (praos)",
			stored:       5,
			found:        true,
			candidate:    4,
			enforceNoGap: true,
			wantErr:      "below last seen",
		},
		{
			name:         "backward counter rejected (tpraos)",
			stored:       5,
			found:        true,
			candidate:    4,
			enforceNoGap: false,
			wantErr:      "below last seen",
		},
		{
			name:         "gapped counter rejected under praos",
			stored:       5,
			found:        true,
			candidate:    7,
			enforceNoGap: true,
			wantErr:      "skips ahead",
		},
		{
			name:         "gapped counter accepted under tpraos",
			stored:       5,
			found:        true,
			candidate:    7,
			enforceNoGap: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOpCertCounter(
				tt.stored,
				tt.found,
				tt.candidate,
				tt.enforceNoGap,
			)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLeiosAnnouncementOCINStalenessUsesImmutableTip(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	addLeiosOCINChainBlocks(t, cm.PrimaryChain(), 5)

	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().SecurityParam = 2
	ls := &LedgerState{
		db:         db,
		chain:      cm.PrimaryChain(),
		currentEra: eras.DijkstraEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
		},
	}
	var issuer lcommon.IssuerVkey
	issuer[0] = 0xA1
	pkh := lcommon.PoolKeyHash(issuer.Hash())
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 3, 20, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 5, 40, nil))

	tests := []struct {
		name      string
		issuer    lcommon.IssuerVkey
		candidate uint32
		want      LeiosAnnouncementOCINStaleness
	}{
		{
			name:      "lower counter is stale",
			issuer:    issuer,
			candidate: 2,
			want:      LeiosAnnouncementStaleOCIN,
		},
		{
			name:      "equal counter is fresh",
			issuer:    issuer,
			candidate: 3,
			want:      LeiosAnnouncementFreshOCIN,
		},
		{
			name:      "arbitrarily ahead counter is fresh",
			issuer:    issuer,
			candidate: 99,
			want:      LeiosAnnouncementFreshOCIN,
		},
		{
			name: "unknown issuer is stale",
			issuer: func() lcommon.IssuerVkey {
				var unknown lcommon.IssuerVkey
				unknown[0] = 0xB2
				return unknown
			}(),
			candidate: 0,
			want:      LeiosAnnouncementStaleOCIN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ls.leiosAnnouncementOCINStaleness(
				leiosOCINHeader(tt.issuer, tt.candidate),
			)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestLeiosAnnouncementOCINStalenessTreatsPreKChainAsOrigin(
	t *testing.T,
) {
	db := newTestDB(t)
	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	addLeiosOCINChainBlocks(t, cm.PrimaryChain(), 2)

	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().SecurityParam = 2
	ls := &LedgerState{
		db:         db,
		chain:      cm.PrimaryChain(),
		currentEra: eras.DijkstraEraDesc,
		config:     LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	var issuer lcommon.IssuerVkey
	issuer[0] = 0xC3
	pkh := lcommon.PoolKeyHash(issuer.Hash())
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 0, 10, nil))

	got, err := ls.leiosAnnouncementOCINStaleness(
		leiosOCINHeader(issuer, 0),
	)
	require.NoError(t, err)
	require.Equal(t, LeiosAnnouncementStaleOCIN, got)
}

func TestValidateLeiosAnnouncementHeaderRunsCryptoBeforeOCINClassification(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{0xD4}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	seedBlockPoolRegistration(t, db, tb.block)
	pkh := lcommon.PoolKeyHash(tb.block.IssuerVkey().Hash())
	seedPoolStakeSnapshot(t, db, 4, pkh.Bytes(), 1_000_000_000)

	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	addLeiosOCINChainBlocks(t, cm.PrimaryChain(), 5)
	ls.chain = cm.PrimaryChain()
	ls.currentEra = eras.BabbageEraDesc
	ls.config.CardanoNodeConfig.ShelleyGenesis().SecurityParam = 2
	ls.publishSnapshotsLocked()
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 0, 20, nil))

	staleness, err := ls.ValidateLeiosAnnouncementHeader(tb.block.Header())
	require.NoError(t, err)
	require.Equal(t, LeiosAnnouncementFreshOCIN, staleness)

	bad := createTestBlock(t, [32]byte{0xD5}, 0, tamperOpCertSig)
	staleness, err = ls.ValidateLeiosAnnouncementHeader(bad.block.Header())
	require.ErrorContains(t, err, "cold-key signature")
	require.Equal(t, LeiosAnnouncementFreshOCIN, staleness,
		"an invalid header must fail before receiving a meaningful OCIN verdict")
}

// TestOpCertNoGapRuleApplies pins the TPraos→Praos boundary: the opcert no-gap
// rule is off through Alonzo (TPraos) and on from Babbage onward (Praos).
func TestOpCertNoGapRuleApplies(t *testing.T) {
	assert.False(
		t,
		opCertNoGapRuleApplies(alonzo.EraIdAlonzo),
		"Alonzo runs TPraos; the no-gap rule must be off",
	)
	assert.True(
		t,
		opCertNoGapRuleApplies(babbage.EraIdBabbage),
		"Babbage runs Praos; the no-gap rule must be on",
	)
}
