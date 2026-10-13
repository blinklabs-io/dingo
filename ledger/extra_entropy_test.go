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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// Mainnet epoch 259 is the only epoch in Cardano's history whose protocol
// parameters carried a non-neutral extraEntropy. The values below are the real
// inputs and the real resulting eta0, so a node that drops the extraEntropy
// term computes a different leader schedule for that whole epoch and rejects
// every header in it.
//
//   - extraEntropy and the resulting nonce: Koios epoch_params for epoch 259
//     (extra_entropy / nonce); every other epoch from 208 to 656 carries
//     extra_entropy null.
//   - the same triple is pinned as a test vector in gouroboros
//     ledger/common/nonce_test.go.
//
// The carried lab (mainnetEpoch259Lab) is a mainnet block in epoch 257, which
// is what identifies the consuming epoch as 259 rather than 260: the TICKN
// state's prev-hash nonce lags the boundary by one epoch, so the boundary INTO
// epoch E mixes a block hash from epoch E-2.
const (
	mainnetEpoch259ExtraEntropy = "d982e06fd33e7440b43cefad529b7ecafbaa255e38178ad4189a37e4ce9bf1fa"
	mainnetEpoch259Candidate    = "d1340a9c1491f0face38d41fd5c82953d0eb48320d65e952414a0c5ebaf87587"
	mainnetEpoch259Lab          = "ee91d679b0a6ce3015b894c575c799e971efac35c7a8cbdc2b3f579005e69abd"
	mainnetEpoch259Nonce        = "0022cfa563a5328c4fb5c8017121329e964c26ade5d167b1bd9b2ec967772b60"
)

// TestRecordedExtraEntropyForEpochTracksParameterHistory pins the parameter
// history the nonce assembly depends on: a pparams row is written for the epoch
// its change takes effect in, and a later epoch resolves to the newest row at or
// before it. Mainnet set extraEntropy for epoch 259 and reset it for 260, so a
// value that stayed sticky past its reset would corrupt every later epoch's
// nonce -- the opposite failure to dropping it.
func TestRecordedExtraEntropyForEpochTracksParameterHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	_, entropyCbor := maryPParamsWithExtraEntropy(t, entropy)
	require.NoError(t, db.SetPParams(
		entropyCbor, 200, 259, eras.MaryEraDesc.Id, nil,
	))
	_, neutralCbor := maryPParamsWithExtraEntropy(t, nil)
	require.NoError(t, db.SetPParams(
		neutralCbor, 300, 260, eras.MaryEraDesc.Id, nil,
	))

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	for _, tc := range []struct {
		name  string
		epoch uint64
		want  []byte
	}{
		{"before the update", 258, nil},
		{"the epoch it was enacted for", 259, entropy},
		{"the epoch it was reset for", 260, nil},
		{"after the reset", 261, nil},
	} {
		got, err := ls.recordedExtraEntropyForEpoch(
			tc.epoch, eras.MaryEraDesc.Id,
		)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}

	// Conway has no extraEntropy parameter at all, so the Mary rows must not
	// leak into a later era's lookup.
	got, err := ls.recordedExtraEntropyForEpoch(259, eras.ConwayEraDesc.Id)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestExtraEntropyFromPParamsStopsAtPraos pins the era boundary of the TICKN
// extraEntropy term against the parameter set's protocol version rather than
// its Go type. A hard fork out of Alonzo enacts Alonzo-typed parameters whose
// protocol version is already Babbage's, and the first Praos epoch takes no
// extraEntropy term.
func TestExtraEntropyFromPParamsStopsAtPraos(t *testing.T) {
	t.Parallel()

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	var nonce lcommon.Nonce
	nonce.Type = lcommon.NonceTypeNonce
	copy(nonce.Value[:], entropy)

	for _, tc := range []struct {
		name   string
		params lcommon.ProtocolParameters
		want   []byte
	}{
		{
			"shelley",
			&shelley.ShelleyProtocolParameters{
				ProtocolMajor: 2,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"mary",
			&mary.MaryProtocolParameters{
				ProtocolMajor: 4,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"alonzo",
			&alonzo.AlonzoProtocolParameters{
				ProtocolMajor: 6,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"alonzo parameters carrying the babbage protocol version",
			&alonzo.AlonzoProtocolParameters{
				ProtocolMajor: babbage.MinProtocolVersionBabbage,
				ExtraEntropy:  nonce,
			},
			nil,
		},
	} {
		require.Equal(
			t,
			tc.want,
			extraEntropyFromPParams(tc.params),
			tc.name,
		)
	}
}

// TestAssembleEpochNonceNeutralCandidateReturnsEntropy pins the identity law of
// the nonce ⭒ operator on its left operand. cardano-ledger's Semigroup Nonce
// gives NeutralNonce <> x = x, so a neutral candidate with a non-neutral
// extraEntropy and no lab must yield the entropy itself.
//
// gouroboros' CalculateRollingNonce cannot serve this case: it coerces its
// right operand from a raw VRF output, so for an all-zero left operand it
// returns blake2b_256(right) rather than right.
func TestAssembleEpochNonceNeutralCandidateReturnsEntropy(t *testing.T) {
	t.Parallel()

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	neutral := make([]byte, lcommon.Blake2b256Size)

	got, err := assembleEpochNonce(neutral, nil, entropy)
	require.NoError(t, err)
	require.Equal(
		t,
		entropy,
		got,
		"NeutralNonce is the identity of the nonce operator, so a neutral "+
			"candidate must leave extraEntropy unchanged",
	)

	hashed := lcommon.Blake2b256Hash(entropy)
	require.NotEqual(
		t,
		hashed.Bytes(),
		got,
		"the entropy must not be re-hashed, which is what the rolling-nonce "+
			"helper would do for a neutral left operand",
	)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
