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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// CertifiedBoundaryStakeSlots names the credentials, pools and slots
// BoundaryEpochStateScenario.CertifiedBoundaryStake wrote, every transaction
// through LedgerDelta.apply so the UTxO events and certificates reach the set
// by the production staging path. A closure is applied at its slot and takes
// effect at its context slot, which is never after it.
//
// Pools A (the harness's), B, C and D. Snapshot is the past snapshot slot,
// between the effective and applied slots of several closures, and Current a
// snapshot slot after every transaction.
//
//   - X delegates to A in a closure effective at XEff1 and to B in one
//     effective at XEff2 > XEff1, both applied in order.
//   - X2 delegates to A in a closure effective at X2EffA and to B in one
//     applied later but effective at X2EffB < X2EffA, so the apply order is
//     the reverse of the effective order.
//   - Q registers and delegates to A in a closure applied after Snapshot but
//     effective before it.
//   - Y is deregistered in a closure applied after Snapshot and effective
//     before it.
//   - R is deregistered, then registered again in a closure applied later but
//     effective before the deregistration.
//   - W delegates to A. A closure effective before Snapshot creates WCreated
//     and one spends WSpent, both applied after it; WEarly stays; a closure
//     applied last and effective after Snapshot creates WLate.
//   - V delegates to C, which a closure applied before the epoch starting at
//     EpochStart retires for it with an effective slot before it; V2 delegates
//     to D, retired for the epoch starting after Snapshot and before Current
//     by a closure effective before that start.
//   - U, when the scenario asks for it, delegates to A and is withdrawn from
//     with a zero amount in a closure applied after Snapshot and effective
//     before it.
//
// G, when GWithdraws is set, delegates to A with a reward balance of GReward
// and, in a closure applied at GApplied after Snapshot and effective at GEff
// before it, withdraws GWithdrawn of it and delegates to B in the same
// transaction. K, when KSuperseded is set, delegates to pool E with CIP-0163
// on; a closure certificate applied at KApplied takes effect at KEff in the
// epoch before the one it was applied in, and an ordinary witness at KLater
// follows it, before Snapshot.
type CertifiedBoundaryStakeSlots struct {
	Pools                        [][]byte
	A, B, C, D                   []byte
	X, X2, Q, Y, R, W, V, V2, U  models.StakeCredentialRef
	Snapshot, Current            uint64
	WEarly, WCreated, WSpent     uint64
	WLate                        uint64
	XEff1, XEff2, X2EffA, X2EffB uint64
	XApplied1, XApplied2         uint64
	X2AppliedA, X2AppliedB       uint64
	QApplied, QEff               uint64
	UApplied, UEff               uint64
	WCreatedApplied, WCreatedEff uint64
	EpochStart                   uint64
	E                            []byte
	G, K                         models.StakeCredentialRef
	GReward, GWithdrawn          uint64
	KStake                       uint64
	GApplied, GEff               uint64
	KApplied, KEff, KLater       uint64
}

// CertifiedBoundaryOptions selects the credentials CertifiedBoundaryStake adds
// to its ordinary ones.
type CertifiedBoundaryOptions struct {
	// WithdrawAcrossSnapshot adds U's zero withdrawal and turns CIP-0163 on.
	WithdrawAcrossSnapshot bool
	// GWithdraws adds G.
	GWithdraws bool
	// KSuperseded adds K and pool E, and turns CIP-0163 on.
	KSuperseded bool
}

func (h *boundaryReplayHarness) certifiedBoundaryStake(
	opts CertifiedBoundaryOptions,
) CertifiedBoundaryStakeSlots {
	withdrawAcrossSnapshot := opts.WithdrawAcrossSnapshot
	t := h.t
	ldb := h.ls.LedgerDB()
	cur := h.ls.currentEpoch
	T := cur.StartSlot + 10_000
	ref := func(b byte) models.StakeCredentialRef {
		return models.NewStakeCredentialRef(0, bytes.Repeat([]byte{b}, 28))
	}
	s := CertifiedBoundaryStakeSlots{
		A: h.poolHash, B: bytes.Repeat([]byte{0x7a}, 28),
		C: bytes.Repeat([]byte{0x7b}, 28), D: bytes.Repeat([]byte{0x7c}, 28),
		X: ref(0x91), X2: ref(0x92), Q: ref(0x93), Y: ref(0x94),
		R: ref(0x95), W: ref(0x96), V: ref(0x97), V2: ref(0x98), U: ref(0x99),
		Snapshot: T + 35, Current: T + 70,
		WEarly: 5_000_000, WSpent: 3_000_000, WCreated: 4_000_000,
		WLate: 7_000_000,
		XEff1: T + 20, XEff2: T + 25, XApplied1: T + 30, XApplied2: T + 40,
		X2EffA: T + 28, X2EffB: T + 22, X2AppliedA: T + 31, X2AppliedB: T + 41,
		QApplied: T + 50, QEff: T + 10,
		UApplied: T + 38, UEff: T + 18,
		WCreatedApplied: T + 42, WCreatedEff: T + 22,
		EpochStart: T + 30,
		E:          bytes.Repeat([]byte{0x7d}, 28),
		G:          ref(0x9a), K: ref(0x9b),
		GReward: 9_000_000, GWithdrawn: 4_000_000, KStake: 6_000_000,
		GApplied: T + 52, GEff: T + 12,
		KApplied: T + 32, KEff: T + 24, KLater: T + 34,
	}
	s.Pools = [][]byte{s.A, s.B, s.C, s.D}
	if withdrawAcrossSnapshot || opts.KSuperseded {
		// Only with delegator inactivity does a zero withdrawal leave a
		// witness.
		h.ls.config.DelegatorInactivityEnabled = true
		h.ls.config.DelegatorInactivity = 100
	}
	cred := func(r models.StakeCredentialRef) []any {
		return []any{uint64(r.Tag), r.Key}
	}
	address := func(r models.StakeCredentialRef) []byte {
		a := []byte{0x00}
		a = append(a, bytes.Repeat([]byte{0x55}, 28)...)
		return append(a, r.Key...)
	}
	type in struct {
		hash []byte
		idx  uint64
	}
	type out struct {
		ref    models.StakeCredentialRef
		amount uint64
	}
	var seed byte = 0x20
	// apply runs one transaction through LedgerDelta.apply, as block
	// processing does, and stages the tip as the block's commit does.
	apply := func(
		slot uint64,
		ctx *uint64,
		ins []in,
		outs []out,
		withdrawals map[cbor.ByteString]uint64,
		certs ...[]any,
	) []byte {
		seed++
		h.coverSlot(slot)
		if ctx != nil {
			h.coverSlot(*ctx)
		}
		inputs := []any{}
		for _, i := range ins {
			inputs = append(inputs, []any{i.hash, i.idx})
		}
		if len(inputs) == 0 {
			inputs = []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}}
		}
		outputs := []any{}
		for _, o := range outs {
			outputs = append(outputs, []any{address(o.ref), o.amount})
		}
		// A zero fee keeps the epoch fee total the set stages with blocks equal
		// to the store's.
		fields := map[uint]any{0: inputs, 1: outputs, 2: uint64(0)}
		if len(certs) > 0 {
			fields[4] = certs
		}
		if withdrawals != nil {
			fields[5] = withdrawals
		}
		body := newRowBatchingTx(t, fields)
		tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
		require.NoError(t, err)
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		utxoOffsets := map[database.UtxoRef]database.CborOffset{}
		for i := range outs {
			utxoOffsets[database.UtxoRef{
				TxId: txHash, OutputIdx: uint32(i), //nolint:gosec // a test index
			}] = database.CborOffset{}
		}
		point := ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32))
		delta := NewLedgerDelta(point, uint(conway.EraIdConway), uint64(seed))
		defer delta.Release()
		delta.addTransaction(tx, 0)
		delta.Offsets = &database.BlockIngestionResult{
			TxOffsets:   map[[32]byte]database.CborOffset{txHash: {}},
			UtxoOffsets: utxoOffsets,
		}
		if ctx != nil {
			delta.skipConsumedInputRecovery = true
			delta.closureContextSlot = ctx
		}
		txn := ldb.Begin(t.Context(), true)
		defer txn.Release()
		require.NoError(t, delta.apply(t.Context(), h.ls, txn))
		h.ls.stageUtxoSet(txn, &point)
		require.NoError(t, txn.Commit())
		return txHash[:]
	}
	poolCert := func(pool []byte, vrf byte) []any {
		reward := append([]byte{0xe0}, bytes.Repeat([]byte{0x7e}, 28)...)
		return []any{
			uint64(3), pool, bytes.Repeat([]byte{vrf}, 32),
			uint64(1_000_000), uint64(340_000_000),
			cbor.Tag{Number: 30, Content: []uint64{1, 10}},
			reward, []any{bytes.Repeat([]byte{0x7e}, 28)},
			[]any{[]any{uint64(1), uint64(3001), "relay.example"}}, nil,
		}
	}
	openEpoch := func(slot, id uint64, length uint) {
		txn := ldb.Begin(t.Context(), true)
		defer txn.Release()
		require.NoError(t, ldb.SetEpoch(
			slot, id, nil, nil, nil, nil,
			cur.EraId, cur.SlotLength, length, txn,
		))
		h.ls.stagePoolBoundary(txn, id, slot)
		require.NoError(t, txn.Commit())
	}
	ctxAt := func(slot uint64) *uint64 { return &slot }

	// A tip before every transaction gives the set's journal a floor the
	// as-of reads can reach.
	baseline := ocommon.NewPoint(T-5, bytes.Repeat([]byte{0xe0}, 32))
	h.ls.stageUtxoSet(nil, &baseline)
	openEpoch(T, cur.EpochId+1, 30)

	pools := [][]any{poolCert(s.C, 0x71), poolCert(s.D, 0x72)}
	if opts.KSuperseded {
		pools = append(pools, poolCert(s.E, 0x73))
	}
	apply(T+3, nil, nil, nil, nil, pools...)
	var reg [][]any
	if opts.GWithdraws {
		reg = append(reg, []any{uint64(0), cred(s.G)},
			[]any{uint64(2), cred(s.G), s.A})
	}
	if opts.KSuperseded {
		reg = append(reg, []any{uint64(0), cred(s.K)},
			[]any{uint64(2), cred(s.K), s.E})
	}
	if len(reg) > 0 {
		apply(T+4, nil, nil, nil, nil, reg...)
	}
	if opts.GWithdraws {
		txn := ldb.Begin(t.Context(), true)
		require.NoError(t, ldb.AddAccountRewardByCredential(
			t.Context(), s.G.Tag, s.G.Key, s.GReward, T+7, []byte{9}, txn,
		))
		require.NoError(t, txn.Commit())
	}
	apply(T+5, nil, nil, nil, nil,
		[]any{uint64(0), cred(s.X)}, []any{uint64(0), cred(s.X2)},
		[]any{uint64(0), cred(s.Y)}, []any{uint64(0), cred(s.R)},
		[]any{uint64(0), cred(s.W)}, []any{uint64(0), cred(s.V)},
		[]any{uint64(0), cred(s.V2)}, []any{uint64(0), cred(s.U)})
	apply(T+6, nil, nil, nil, nil,
		[]any{uint64(2), cred(s.Y), s.A}, []any{uint64(2), cred(s.R), s.B},
		[]any{uint64(2), cred(s.W), s.A}, []any{uint64(2), cred(s.V), s.C},
		[]any{uint64(2), cred(s.V2), s.D}, []any{uint64(2), cred(s.U), s.A})
	ordinaryOuts := []out{
		{s.W, s.WEarly}, {s.W, s.WSpent}, {s.X, 4_000_000},
	}
	if opts.KSuperseded {
		ordinaryOuts = append(ordinaryOuts, out{s.K, s.KStake})
	}
	ordinary := apply(T+8, nil, nil, ordinaryOuts, nil)

	// C retires for the epoch that starts at EpochStart; the certificate is
	// applied before that boundary but takes effect before it too, so the
	// set reaps the pool there as the store's effective order does.
	apply(T+29, ctxAt(T+25), nil, nil, nil,
		[]any{uint64(4), s.C, cur.EpochId + 2})
	openEpoch(s.EpochStart, cur.EpochId+2, 1000)
	apply(s.XApplied1, ctxAt(s.XEff1), nil, nil, nil,
		[]any{uint64(2), cred(s.X), s.A})
	apply(s.X2AppliedA, ctxAt(s.X2EffA), nil, nil, nil,
		[]any{uint64(2), cred(s.X2), s.A})
	zero := map[cbor.ByteString]uint64{
		cbor.NewByteString(append([]byte{0xe0}, s.X.Key...)): 0,
	}
	apply(T+33, ctxAt(T+31), nil, nil, zero)
	if withdrawAcrossSnapshot {
		apply(s.UApplied, ctxAt(s.UEff), nil, nil,
			map[cbor.ByteString]uint64{
				cbor.NewByteString(append([]byte{0xe0}, s.U.Key...)): 0,
			})
	}
	if opts.KSuperseded {
		apply(s.KApplied, ctxAt(s.KEff), nil, nil, nil,
			[]any{uint64(2), cred(s.K), s.E})
		apply(s.KLater, nil, nil, nil,
			map[cbor.ByteString]uint64{
				cbor.NewByteString(append([]byte{0xe0}, s.K.Key...)): 0,
			})
	}
	apply(s.XApplied2, ctxAt(s.XEff2), nil, nil, nil,
		[]any{uint64(2), cred(s.X), s.B})
	apply(s.X2AppliedB, ctxAt(s.X2EffB), nil, nil, nil,
		[]any{uint64(2), cred(s.X2), s.B})
	apply(s.WCreatedApplied, ctxAt(s.WCreatedEff), nil,
		[]out{{s.W, s.WCreated}}, nil)
	apply(T+44, ctxAt(T+24), []in{{ordinary, 1}}, nil, nil)
	apply(T+45, ctxAt(T+25), nil, nil, nil, []any{uint64(1), cred(s.Y)})
	apply(T+46, ctxAt(T+26), nil, nil, nil, []any{uint64(1), cred(s.R)})
	apply(T+47, ctxAt(T+20), nil, nil, nil, []any{uint64(0), cred(s.R)})
	apply(T+49, ctxAt(T+36), nil, nil, nil,
		[]any{uint64(4), s.D, cur.EpochId + 3})
	apply(s.QApplied, ctxAt(s.QEff), nil, nil, nil,
		[]any{uint64(0), cred(s.Q)}, []any{uint64(2), cred(s.Q), s.A})
	if opts.GWithdraws {
		apply(s.GApplied, ctxAt(s.GEff), nil, nil,
			map[cbor.ByteString]uint64{
				cbor.NewByteString(append([]byte{0xe0}, s.G.Key...)): s.GWithdrawn,
			},
			[]any{uint64(2), cred(s.G), s.B})
	}
	apply(T+60, ctxAt(T+55), nil, []out{{s.W, s.WLate}}, nil)
	openEpoch(T+65, cur.EpochId+3, 1000)
	return s
}
