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
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// mirQuorumTestView returns a LedgerView whose Shelley genesis delegates
// three genesis keys to the given delegate keys with an update quorum of two.
func mirQuorumTestView(
	t *testing.T,
	delegates [3]ed25519.PublicKey,
) *LedgerView {
	t.Helper()
	var genDelegs []string
	for i, delegate := range delegates {
		genDelegs = append(genDelegs, `"`+
			strings.Repeat(hex.EncodeToString([]byte{byte(0x11 * (i + 1))}), 28)+
			`": {"delegate": "`+
			hex.EncodeToString(lcommon.Blake2b224Hash(delegate).Bytes())+
			`", "vrf": "`+strings.Repeat("bb", 32)+`"}`)
	}
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"slotsPerKESPeriod": 129600,
		"maxKESEvolutions": 62,
		"updateQuorum": 2,
		"systemStart": "2022-10-25T00:00:00Z",
		"protocolParams": {"decentralisationParam": 1},
		"genDelegs": {` + strings.Join(genDelegs, ",") + `}
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(
		t,
		cfg,
		strings.NewReader(
			`{"blockVersionData":{"slotDuration":"20000"},"protocolConsts":{"k":432}}`,
		),
	))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := &LedgerState{
		db:             db,
		currentPParams: &shelley.ShelleyProtocolParameters{},
		config:         LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	ls.publishSnapshotsLocked()
	return ls.NewView(nil)
}

// TestMIRGenesisQuorumThroughEraValidation drives the genesis-delegate quorum
// for move instantaneous rewards certificates through every era's validation
// entry point that admits MIR, against a real LedgerView.
//
// Reference: Shelley UTXOW validateMIRInsufficientGenesisSigs, inherited
// unchanged through Babbage: genSig is the set of current genesis delegate key
// hashes intersected with the transaction's witness key hashes, and a
// transaction carrying a MIR certificate needs |genSig| >= Quorum. A signer
// that is not a genesis delegate contributes nothing, and a delegate that
// signs twice counts once, because genSig is a set.
func TestMIRGenesisQuorumThroughEraValidation(t *testing.T) {
	t.Parallel()

	var keys [4]ed25519.PrivateKey
	for i := range keys {
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = byte(0xa0 + i)
		keys[i] = ed25519.NewKeyFromSeed(seed)
	}
	pub := func(i int) ed25519.PublicKey {
		return keys[i].Public().(ed25519.PublicKey)
	}
	lv := mirQuorumTestView(t, [3]ed25519.PublicKey{pub(0), pub(1), pub(2)})
	delegates, err := lv.GenesisDelegateKeyHashes(0)
	require.NoError(t, err)
	require.Len(t, delegates, 3, "all three genesis delegations are active")
	quorum, err := lv.GenesisUpdateQuorum()
	require.NoError(t, err)
	require.Equal(t, uint(2), quorum)

	reward := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(make([]byte, 28)),
	}
	mir := &lcommon.MoveInstantaneousRewardsCertificate{
		CertType: uint(lcommon.CertificateTypeMoveInstantaneousRewards),
		Reward: lcommon.MoveInstantaneousRewardsCertificateReward{
			Source:  1,
			Rewards: map[*lcommon.Credential]*big.Int{&reward: big.NewInt(1)},
		},
	}
	certs := []lcommon.CertificateWrapper{
		{Type: uint(lcommon.CertificateTypeMoveInstantaneousRewards), Certificate: mir},
	}

	type buildTx func([]lcommon.VkeyWitness) lcommon.Transaction
	eraCases := []struct {
		name     string
		build    buildTx
		pparams  lcommon.ProtocolParameters
		validate func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error
		// quorum is the era's MIR genesis-quorum rule on its own. The full
		// validate path also fails on the fixture's missing inputs, so only
		// this rule can show that a met quorum is accepted.
		quorum func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error
	}{
		{
			name: "shelley",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &shelley.ShelleyTransaction{
					Body: shelley.ShelleyTransactionBody{TxCertificates: certs},
					WitnessSet: shelley.ShelleyTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &shelley.ShelleyProtocolParameters{},
			validate: eras.ValidateTxShelley,
			quorum:   shelley.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "allegra",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &allegra.AllegraTransaction{
					Body: allegra.AllegraTransactionBody{TxCertificates: certs},
					WitnessSet: shelley.ShelleyTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &allegra.AllegraProtocolParameters{},
			validate: eras.ValidateTxAllegra,
			quorum:   allegra.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "mary",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &mary.MaryTransaction{
					Body: mary.MaryTransactionBody{TxCertificates: certs},
					WitnessSet: shelley.ShelleyTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &mary.MaryProtocolParameters{},
			validate: eras.ValidateTxMary,
			quorum:   mary.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "alonzo",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &alonzo.AlonzoTransaction{
					TxIsValid: true,
					Body:      alonzo.AlonzoTransactionBody{TxCertificates: certs},
					WitnessSet: alonzo.AlonzoTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &alonzo.AlonzoProtocolParameters{},
			validate: eras.ValidateTxAlonzo,
			quorum:   alonzo.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "babbage",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &babbage.BabbageTransaction{
					TxIsValid: true,
					Body:      babbage.BabbageTransactionBody{TxCertificates: certs},
					WitnessSet: babbage.BabbageTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &babbage.BabbageProtocolParameters{},
			validate: eras.ValidateTxBabbage,
			quorum:   babbage.UtxoValidateMIRGenesisQuorum,
		},
	}
	signerCases := []struct {
		name         string
		signers      []int
		wantProvided uint
		wantRejected bool
	}{
		{"one delegate is below quorum", []int{0}, 1, true},
		{"a delegate signing twice counts once", []int{0, 0}, 1, true},
		{"a non-delegate signer does not count", []int{0, 3}, 1, true},
		{"two delegates meet quorum", []int{0, 1}, 0, false},
		{"three delegates meet quorum", []int{0, 1, 2}, 0, false},
	}
	for _, era := range eraCases {
		for _, sc := range signerCases {
			t.Run(era.name+"/"+sc.name, func(t *testing.T) {
				t.Parallel()
				witnesses := make([]lcommon.VkeyWitness, 0, len(sc.signers))
				for _, i := range sc.signers {
					witnesses = append(witnesses, lcommon.VkeyWitness{
						Vkey:      pub(i),
						Signature: make([]byte, ed25519.SignatureSize),
					})
				}
				tx := era.build(witnesses)
				err := era.validate(tx, 0, lv, era.pparams)
				var insufficient lcommon.MIRInsufficientGenesisSigsError
				if !sc.wantRejected {
					require.False(
						t,
						errors.As(err, &insufficient),
						"quorum met but MIR rejected for genesis signatures: %v",
						err,
					)
					require.NoError(t, era.quorum(tx, 0, lv, era.pparams))
					return
				}
				require.True(
					t,
					errors.As(err, &insufficient),
					"MIR below quorum was not rejected for genesis signatures: %v",
					err,
				)
				require.Equal(t, sc.wantProvided, insufficient.Provided)
				require.Equal(t, uint(2), insufficient.Required)
			})
		}
	}

	// A genesis key delegation certificate applied after genesis moves that
	// genesis key's share of the quorum to the new delegate: its signature
	// counts and the replaced delegate's no longer does.
	const redelegatedSlot = 100_000
	redelegated := mirQuorumTestView(
		t,
		[3]ed25519.PublicKey{pub(0), pub(1), pub(2)},
	)
	seedGenesisDelegation(t, redelegated.ls.db, models.GenesisDelegation{
		GenesisHash:         bytes.Repeat([]byte{0x11}, lcommon.Blake2b224Size),
		GenesisDelegateHash: lcommon.Blake2b224Hash(pub(3)).Bytes(),
		VrfKeyHash:          bytes.Repeat([]byte{0xcc}, lcommon.Blake2b256Size),
	})
	delegates, err = redelegated.GenesisDelegateKeyHashes(redelegatedSlot)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]lcommon.Blake2b224{
			lcommon.Blake2b224Hash(pub(1)),
			lcommon.Blake2b224Hash(pub(2)),
			lcommon.Blake2b224Hash(pub(3)),
		},
		delegates,
	)
	for _, era := range eraCases {
		for _, sc := range []struct {
			name         string
			signers      []int
			wantRejected bool
		}{
			{"new delegate counts", []int{3, 1}, false},
			{"replaced delegate does not count", []int{0, 1}, true},
		} {
			t.Run(era.name+"/redelegated/"+sc.name, func(t *testing.T) {
				t.Parallel()
				witnesses := make([]lcommon.VkeyWitness, 0, len(sc.signers))
				for _, i := range sc.signers {
					witnesses = append(witnesses, lcommon.VkeyWitness{
						Vkey:      pub(i),
						Signature: make([]byte, ed25519.SignatureSize),
					})
				}
				tx := era.build(witnesses)
				err := era.validate(
					tx,
					redelegatedSlot,
					redelegated,
					era.pparams,
				)
				var insufficient lcommon.MIRInsufficientGenesisSigsError
				require.Equal(
					t,
					sc.wantRejected,
					errors.As(err, &insufficient),
					"genesis quorum verdict after redelegation: %v",
					err,
				)
				quorumErr := era.quorum(
					tx,
					redelegatedSlot,
					redelegated,
					era.pparams,
				)
				if sc.wantRejected {
					require.ErrorAs(t, quorumErr, &insufficient)
					return
				}
				require.NoError(t, quorumErr)
			})
		}
	}
}
