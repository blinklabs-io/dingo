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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package database

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// Proposals for epoch 3 are enacted at the boundary into epoch 4. Epoch 3
// covers slots 300-399; a proposal for epoch 3 stored before slot 300 was made
// during epoch 2 after its slot of no return.
const (
	classicTestSubmissionEpoch = uint64(3)
	classicTestEnactEpoch      = uint64(4)
	classicTestEpochStart      = uint64(300)
)

type classicTestProposal struct {
	genesis byte
	slot    uint64
	epoch   uint64
	cbor    []byte
}

func classicUpdate(t *testing.T, fields map[uint64]any) []byte {
	t.Helper()
	data, err := cbor.Encode(fields)
	require.NoError(t, err)
	return data
}

func classicProposal(
	t *testing.T,
	genesis byte,
	slot uint64,
	fields map[uint64]any,
) classicTestProposal {
	t.Helper()
	return classicTestProposal{
		genesis: genesis,
		slot:    slot,
		epoch:   classicTestSubmissionEpoch,
		cbor:    classicUpdate(t, fields),
	}
}

func classicTestPParams(major, minor uint) *shelley.ShelleyProtocolParameters {
	return &shelley.ShelleyProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		ProtocolMajor:      major,
		ProtocolMinor:      minor,
	}
}

func classicDecode(data []byte) (any, error) {
	var update shelley.ShelleyProtocolParameterUpdate
	_, err := cbor.Decode(data, &update)
	return update, err
}

func classicApply(
	current lcommon.ProtocolParameters,
	update any,
) (lcommon.ProtocolParameters, error) {
	pp := current.(*shelley.ShelleyProtocolParameters)
	u := update.(shelley.ShelleyProtocolParameterUpdate)
	pp.Update(&u)
	return pp, nil
}

func classicClone(
	current lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	pp := *current.(*shelley.ShelleyProtocolParameters)
	return &pp, nil
}

// runClassicEnactment stores proposals in order and returns the parameters
// ComputeAndApplyPParamUpdates, ForecastPParamUpdates and ApplyPParamUpdates
// produce for the boundary into epoch 4, after checking that all three agree.
func runClassicEnactment(
	t *testing.T,
	proposals []classicTestProposal,
	quorum int,
	current *shelley.ShelleyProtocolParameters,
) *shelley.ShelleyProtocolParameters {
	t.Helper()
	results := make([]*shelley.ShelleyProtocolParameters, 0, 3)
	for _, mode := range []string{"compute", "forecast", "apply"} {
		db, err := newTestDatabase(t, &Config{DataDir: ""})
		require.NoError(t, err)
		txn := db.Transaction(true)
		for epoch, start := range map[uint64]uint64{
			classicTestSubmissionEpoch - 1: classicTestEpochStart - 100,
			classicTestSubmissionEpoch:     classicTestEpochStart,
		} {
			require.NoError(t, db.SetEpoch(
				start, epoch, nil, nil, nil, nil, 1, 1, 100, txn,
			))
		}
		for _, p := range proposals {
			require.NoError(t, db.SetPParamUpdate(
				[]byte{p.genesis}, p.cbor, p.slot, p.epoch, txn,
			))
		}
		pp := *current
		var result lcommon.ProtocolParameters
		switch mode {
		case "compute":
			result, _, err = db.ComputeAndApplyPParamUpdates(
				classicTestEpochStart+100, classicTestEnactEpoch, 1, quorum,
				&pp, classicDecode, classicApply, nil, txn,
			)
		case "forecast":
			result, err = db.ForecastPParamUpdates(
				classicTestEnactEpoch, quorum, &pp,
				classicDecode, classicApply, classicClone, txn,
			)
		case "apply":
			result = &pp
			err = db.ApplyPParamUpdates(
				classicTestEpochStart+100, classicTestEnactEpoch, 1, quorum,
				&result, classicDecode, classicApply, txn,
			)
		}
		require.NoError(t, err, mode)
		require.NoError(t, txn.Rollback())
		txn.Release()
		require.NoError(t, db.Close())
		results = append(results, result.(*shelley.ShelleyProtocolParameters))
	}
	require.Equal(t, results[0], results[1], "forecast disagrees with rollover")
	require.Equal(t, results[0], results[2], "apply disagrees with rollover")
	return results[0]
}

func TestClassicPParamQuorumRequiresIdenticalValues(t *testing.T) {
	t.Parallel()
	unchanged := classicTestPParams(2, 0)
	withMinFeeA := func(fee uint) *shelley.ShelleyProtocolParameters {
		pp := classicTestPParams(2, 0)
		pp.MinFeeA = fee
		return pp
	}
	for _, tc := range []struct {
		name      string
		quorum    int
		proposals func(t *testing.T) []classicTestProposal
		want      *shelley.ShelleyProtocolParameters
	}{
		{
			// dingo#4542: five delegates, five different updates.
			name:   "five delegates with five different updates",
			quorum: 5,
			proposals: func(t *testing.T) []classicTestProposal {
				var ret []classicTestProposal
				for i := range 5 {
					ret = append(ret, classicProposal(
						t, byte(i+1), 300+uint64(i),
						map[uint64]any{0: 101 + i},
					))
				}
				return ret
			},
			want: unchanged,
		},
		{
			name:   "agreed value with newer dissenting proposals",
			quorum: 3,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 300, map[uint64]any{0: 200}),
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 200}),
					classicProposal(t, 4, 303, map[uint64]any{0: 300}),
					classicProposal(t, 5, 304, map[uint64]any{0: 400}),
				}
			},
			want: withMinFeeA(200),
		},
		{
			// votedFuturePParams requires exactly one value at quorum.
			name:   "two values both at quorum",
			quorum: 2,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 300, map[uint64]any{0: 200}),
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 300}),
					classicProposal(t, 4, 303, map[uint64]any{0: 300}),
				}
			},
			want: unchanged,
		},
		{
			name:   "tie below quorum",
			quorum: 3,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 300, map[uint64]any{0: 200}),
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 300}),
					classicProposal(t, 4, 303, map[uint64]any{0: 300}),
				}
			},
			want: unchanged,
		},
		{
			name:   "all delegates agree",
			quorum: 3,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 300, map[uint64]any{0: 200}),
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 200}),
				}
			},
			want: withMinFeeA(200),
		},
		{
			// Map.union keeps each genesis key's newest proposal.
			name:   "newer proposal replaces a delegate's vote",
			quorum: 3,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 300, map[uint64]any{0: 200}),
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 200}),
					classicProposal(t, 1, 303, map[uint64]any{0: 300}),
				}
			},
			want: unchanged,
		},
		{
			name:   "newer proposal completes the quorum",
			quorum: 3,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 300, map[uint64]any{0: 300}),
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 200}),
					classicProposal(t, 1, 303, map[uint64]any{0: 200}),
				}
			},
			want: withMinFeeA(200),
		},
		{
			// Two transactions in one block: insertion order is chain order.
			name:   "later proposal in the same slot replaces the earlier",
			quorum: 3,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 200}),
					classicProposal(t, 1, 305, map[uint64]any{0: 300}),
					classicProposal(t, 1, 305, map[uint64]any{0: 200}),
				}
			},
			want: withMinFeeA(200),
		},
		{
			name:   "earlier proposal in the same slot is replaced",
			quorum: 3,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 2, 301, map[uint64]any{0: 200}),
					classicProposal(t, 3, 302, map[uint64]any{0: 200}),
					classicProposal(t, 1, 305, map[uint64]any{0: 200}),
					classicProposal(t, 1, 305, map[uint64]any{0: 300}),
				}
			},
			want: unchanged,
		},
		{
			name:   "proposals for other epochs do not count",
			quorum: 2,
			proposals: func(t *testing.T) []classicTestProposal {
				other := classicUpdate(t, map[uint64]any{0: 999})
				return []classicTestProposal{
					{genesis: 1, slot: 200, epoch: 2, cbor: other},
					{genesis: 2, slot: 201, epoch: 2, cbor: other},
					{genesis: 3, slot: 360, epoch: 4, cbor: other},
					{genesis: 4, slot: 361, epoch: 4, cbor: other},
					classicProposal(t, 5, 300, map[uint64]any{0: 200}),
				}
			},
			want: unchanged,
		},
		{
			name:   "only proposals for the submission epoch are enacted",
			quorum: 2,
			proposals: func(t *testing.T) []classicTestProposal {
				other := classicUpdate(t, map[uint64]any{0: 999})
				return []classicTestProposal{
					{genesis: 1, slot: 200, epoch: 2, cbor: other},
					{genesis: 2, slot: 201, epoch: 2, cbor: other},
					classicProposal(t, 5, 300, map[uint64]any{0: 200}),
					classicProposal(t, 6, 301, map[uint64]any{0: 200}),
					{genesis: 3, slot: 360, epoch: 4, cbor: other},
					{genesis: 4, slot: 361, epoch: 4, cbor: other},
				}
			},
			want: withMinFeeA(200),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := runClassicEnactment(
				t, tc.proposals(t), tc.quorum, classicTestPParams(2, 0),
			)
			require.Equal(t, tc.want, got)
		})
	}
}

// The reference compares PParamsUpdate values: a rational is compared as a
// normalized ratio and a map by its entries, not by their CBOR encodings.
func TestClassicPParamQuorumComparesValuesNotEncodings(t *testing.T) {
	t.Parallel()
	// {0: 200, 12: 1/2}
	canonical := []byte{
		0xa2, 0x00, 0x18, 0xc8, 0x0c, 0xd8, 0x1e, 0x82, 0x01, 0x02,
	}
	// {12: 2/4, 0: 200}
	reordered := []byte{
		0xa2, 0x0c, 0xd8, 0x1e, 0x82, 0x02, 0x04, 0x00, 0x18, 0xc8,
	}
	got := runClassicEnactment(t, []classicTestProposal{
		{genesis: 1, slot: 300, epoch: 3, cbor: canonical},
		{genesis: 2, slot: 301, epoch: 3, cbor: reordered},
	}, 2, classicTestPParams(2, 0))
	want := classicTestPParams(2, 0)
	want.MinFeeA = 200
	require.Equal(t, uint(200), got.MinFeeA)
	require.NotNil(t, got.Decentralization)
	require.Zero(t, got.Decentralization.Cmp(big.NewRat(1, 2)))
	want.Decentralization = got.Decentralization
	require.Equal(t, want, got)
}

// votedFuturePParams refuses an agreed update whose result does not keep
// maxTxSize + maxBlockHeaderSize strictly below maxBlockBodySize.
func TestClassicPParamQuorumKeepsBlockSizeInvariant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		fields  map[uint64]any
		enacted bool
	}{
		// 64435 + 1100 = 65535 < 65536
		{"largest transaction size", map[uint64]any{3: 64435}, true},
		// 64436 + 1100 = 65536
		{"transaction size fills the block", map[uint64]any{3: 64436}, false},
		// 16384 + 1100 = 17484
		{"block shrinks to the limit", map[uint64]any{2: 17484}, false},
		{"block one byte above the limit", map[uint64]any{2: 17485}, true},
		// 16384 + 49152 = 65536
		{"header fills the block", map[uint64]any{4: 49152}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current := classicTestPParams(2, 0)
			got := runClassicEnactment(t, []classicTestProposal{
				classicProposal(t, 1, 300, tc.fields),
			}, 1, current)
			if !tc.enacted {
				require.Equal(t, current, got)
				return
			}
			require.NotEqual(t, current, got)
		})
	}
}

// updatePpup carries the proposals made after the previous epoch's slot of no
// return into the submission epoch only if every one of them can follow the
// submission epoch's protocol version; otherwise it discards all of them.
func TestClassicPParamCarriedOverProposals(t *testing.T) {
	t.Parallel()
	// The version proposal also changes the fee, so enacting it is observable
	// even when the version is unchanged.
	version2 := map[uint64]any{0: 150, 14: []uint64{2, 0}}
	fee := map[uint64]any{0: 200}
	for _, tc := range []struct {
		name      string
		major     uint
		proposals func(t *testing.T) []classicTestProposal
		quorum    int
		wantMajor uint
		wantFee   uint
	}{
		{
			name:  "carried-over version that cannot follow is discarded",
			major: 2,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 250, version2),
					classicProposal(t, 2, 260, version2),
				}
			},
			quorum:    2,
			wantMajor: 2,
			wantFee:   44,
		},
		{
			name:  "carried-over version that can follow is enacted",
			major: 1,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 250, version2),
					classicProposal(t, 2, 260, version2),
				}
			},
			quorum:    2,
			wantMajor: 2,
			wantFee:   150,
		},
		{
			name:  "one illegal carried-over proposal discards them all",
			major: 2,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 250, version2),
					classicProposal(t, 2, 260, fee),
					classicProposal(t, 3, 310, fee),
				}
			},
			quorum:    2,
			wantMajor: 2,
			wantFee:   44,
		},
		{
			name:  "legal carried-over proposals count with current ones",
			major: 1,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 250, version2),
					classicProposal(t, 2, 260, fee),
					classicProposal(t, 3, 310, fee),
				}
			},
			quorum:    2,
			wantMajor: 1,
			wantFee:   200,
		},
		{
			name:  "discarded carried-over proposals leave current votes",
			major: 2,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 250, version2),
					classicProposal(t, 2, 310, fee),
					classicProposal(t, 3, 311, fee),
				}
			},
			quorum:    2,
			wantMajor: 2,
			wantFee:   200,
		},
		{
			// Only a delegate's newest carried-over proposal is checked.
			name:  "replaced illegal carried-over proposal is not checked",
			major: 2,
			proposals: func(t *testing.T) []classicTestProposal {
				return []classicTestProposal{
					classicProposal(t, 1, 250, version2),
					classicProposal(t, 1, 255, fee),
					classicProposal(t, 2, 310, fee),
				}
			},
			quorum:    2,
			wantMajor: 2,
			wantFee:   200,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := runClassicEnactment(
				t, tc.proposals(t), tc.quorum, classicTestPParams(tc.major, 0),
			)
			require.Equal(t, tc.wantMajor, got.ProtocolMajor)
			require.Equal(t, tc.wantFee, got.MinFeeA)
		})
	}
}
