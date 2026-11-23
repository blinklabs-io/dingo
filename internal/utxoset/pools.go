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

package utxoset

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func poolKey(pool []byte) []byte {
	return append([]byte{badgerutxo.PoolTag}, pool...)
}

type poolRelayEnc struct {
	cbor.StructAsArray
	Type     int64
	Hostname string
	Port     uint64
	Ipv4     []byte
	Ipv6     []byte
}

type poolRegEnc struct {
	cbor.StructAsArray
	Vrf          []byte
	Reward       []byte
	RewardTag    uint8
	MetadataURL  string
	MetadataHash []byte
	LeiosPublic  []byte
	LeiosProof   []byte
	Owners       [][]byte
	Relays       []poolRelayEnc
	Pledge       uint64
	Cost         uint64
	Margin       string
	AddedSlot    uint64
	// LeiosKeyEpochs is the epoch the Leios key was registered in, empty
	// when it is not known. LeiosAgeUnknown marks a key imported without one.
	LeiosKeyEpochs  []uint64
	LeiosAgeUnknown bool
}

type legacyPoolRegEnc struct {
	cbor.StructAsArray
	Vrf          []byte
	Reward       []byte
	RewardTag    uint8
	MetadataURL  string
	MetadataHash []byte
	LeiosPublic  []byte
	LeiosProof   []byte
	Owners       [][]byte
	Relays       []poolRelayEnc
	Pledge       uint64
	Cost         uint64
	Margin       string
	AddedSlot    uint64
}

// The fixed-array encoding grew by appending LeiosKeyEpochs and
// LeiosAgeUnknown. Accept the old length so a node can restart on a live set
// written before those fields existed.
func (e *poolRegEnc) UnmarshalCBOR(data []byte) error {
	var fields []cbor.RawMessage
	if _, err := cbor.Decode(data, &fields); err != nil {
		return err
	}
	switch len(fields) {
	case 13:
		var legacy legacyPoolRegEnc
		if _, err := cbor.Decode(data, &legacy); err != nil {
			return err
		}
		*e = poolRegEnc{
			Vrf: legacy.Vrf, Reward: legacy.Reward,
			RewardTag: legacy.RewardTag, MetadataURL: legacy.MetadataURL,
			MetadataHash: legacy.MetadataHash, LeiosPublic: legacy.LeiosPublic,
			LeiosProof: legacy.LeiosProof, Owners: legacy.Owners,
			Relays: legacy.Relays, Pledge: legacy.Pledge, Cost: legacy.Cost,
			Margin: legacy.Margin, AddedSlot: legacy.AddedSlot,
		}
		return nil
	case 15:
		type currentPoolRegEnc poolRegEnc
		var current currentPoolRegEnc
		if _, err := cbor.Decode(data, &current); err != nil {
			return err
		}
		*e = poolRegEnc(current)
		return nil
	default:
		return fmt.Errorf(
			"utxoset: pool registration has %d fields",
			len(fields),
		)
	}
}

type poolStateEnc struct {
	cbor.StructAsArray
	Params      []poolRegEnc
	Future      []poolRegEnc
	Retiring    []uint64
	DepositHeld []uint64
}

func ipBytes(ip *net.IP) []byte {
	if ip == nil {
		return nil
	}
	return []byte(*ip)
}

func encodePoolReg(r *models.PoolRegistration) []poolRegEnc {
	if r == nil {
		return nil
	}
	e := poolRegEnc{
		Vrf: r.VrfKeyHash, Reward: r.RewardAccount,
		RewardTag: r.RewardAccountCredentialTag, MetadataURL: r.MetadataUrl,
		MetadataHash: r.MetadataHash, LeiosPublic: r.LeiosKeyPublic,
		LeiosProof: r.LeiosKeyPossessionProof, Pledge: uint64(r.Pledge),
		Cost: uint64(r.Cost), AddedSlot: r.AddedSlot,
		LeiosAgeUnknown: r.LeiosKeyRegistrationAgeUnknown,
	}
	if r.LeiosKeyRegistrationEpoch != nil {
		e.LeiosKeyEpochs = []uint64{*r.LeiosKeyRegistrationEpoch}
	}
	if r.Margin != nil && r.Margin.Rat != nil {
		e.Margin = r.Margin.String()
	}
	for _, o := range r.Owners {
		e.Owners = append(e.Owners, o.KeyHash)
	}
	for _, rl := range r.Relays {
		re := poolRelayEnc{
			Type: -1, Hostname: rl.Hostname, Port: uint64(rl.Port),
			Ipv4: ipBytes(rl.Ipv4), Ipv6: ipBytes(rl.Ipv6),
		}
		if rl.Type != nil {
			re.Type = int64(*rl.Type)
		}
		e.Relays = append(e.Relays, re)
	}
	return []poolRegEnc{e}
}

func decodePoolReg(pool []byte, es []poolRegEnc) (*models.PoolRegistration, error) {
	if len(es) == 0 {
		return nil, nil
	}
	e := es[0]
	r := &models.PoolRegistration{
		PoolKeyHash: pool, VrfKeyHash: e.Vrf, RewardAccount: e.Reward,
		RewardAccountCredentialTag: e.RewardTag, MetadataUrl: e.MetadataURL,
		MetadataHash: e.MetadataHash, LeiosKeyPublic: e.LeiosPublic,
		LeiosKeyPossessionProof: e.LeiosProof, Pledge: types.Uint64(e.Pledge),
		Cost: types.Uint64(e.Cost), AddedSlot: e.AddedSlot,
		LeiosKeyRegistrationAgeUnknown: e.LeiosAgeUnknown,
	}
	if len(e.LeiosKeyEpochs) > 0 {
		r.LeiosKeyRegistrationEpoch = &e.LeiosKeyEpochs[0]
	}
	if e.Margin != "" {
		rat, ok := new(big.Rat).SetString(e.Margin)
		if !ok {
			return nil, fmt.Errorf("utxoset: malformed pool margin %q", e.Margin)
		}
		r.Margin = &types.Rat{Rat: rat}
	}
	for _, o := range e.Owners {
		r.Owners = append(r.Owners, models.PoolRegistrationOwner{KeyHash: o})
	}
	for _, re := range e.Relays {
		rl := models.PoolRegistrationRelay{Hostname: re.Hostname, Port: uint(re.Port)}
		if re.Type >= 0 {
			t := int(re.Type)
			rl.Type = &t
		}
		if re.Ipv4 != nil {
			ip := net.IP(re.Ipv4)
			rl.Ipv4 = &ip
		}
		if re.Ipv6 != nil {
			ip := net.IP(re.Ipv6)
			rl.Ipv6 = &ip
		}
		r.Relays = append(r.Relays, rl)
	}
	return r, nil
}

func encodePoolState(s ledger.PoolState) ([]byte, error) {
	e := poolStateEnc{
		Params: encodePoolReg(s.Params),
		Future: encodePoolReg(s.Future),
	}
	if s.RetiringEpoch != nil {
		e.Retiring = []uint64{*s.RetiringEpoch}
	}
	if s.DepositHeld != nil {
		e.DepositHeld = []uint64{*s.DepositHeld}
	}
	return cbor.Encode(&e)
}

func decodePoolState(pool, v []byte) (*ledger.PoolState, error) {
	var e poolStateEnc
	if _, err := cbor.Decode(v, &e); err != nil {
		return nil, fmt.Errorf("utxoset: decode pool state: %w", err)
	}
	s := &ledger.PoolState{Pool: pool}
	var err error
	if s.Params, err = decodePoolReg(pool, e.Params); err != nil {
		return nil, err
	}
	if s.Future, err = decodePoolReg(pool, e.Future); err != nil {
		return nil, err
	}
	if len(e.Retiring) > 0 {
		s.RetiringEpoch = &e.Retiring[0]
	}
	if len(e.DepositHeld) > 0 {
		s.DepositHeld = &e.DepositHeld[0]
	}
	return s, nil
}

// poolRows converts a commit's pool changes to journaled rows.
func poolRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0, len(lc.Pools))
	for _, c := range lc.Pools {
		put := badgerutxo.RowPut{Slot: c.Slot, Key: poolKey(c.State.Pool), Delete: c.Delete}
		if !c.Delete {
			v, err := encodePoolState(c.State)
			if err != nil {
				return nil, err
			}
			put.Value = v
		}
		out = append(out, put)
	}
	return out, nil
}

// latestEpoch returns the newest epoch row, or the zero row at origin.
func latestEpoch(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) (models.Epoch, error) {
	epochs, err := ldb.GetEpochs(txn)
	if err != nil || len(epochs) == 0 {
		return models.Epoch{}, err
	}
	latest := epochs[0]
	for _, e := range epochs {
		if e.EpochId > latest.EpochId {
			latest = e
		}
	}
	return latest, nil
}

// ensurePools loads every registered pool's state from the metadata store
// into a set from before it kept them: the registration effective for the
// current epoch, a later one waiting for the next boundary, a pending
// retirement and the deposit held.
func (s *Set) ensurePools(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.PoolsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the pools marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	// With no epoch row yet, the ledger is in its first epoch from slot 0.
	epoch, err := latestEpoch(ldb, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read the current epoch: %w", err)
	}
	states, err := loadPoolStates(ctx, ldb, txn, epoch)
	if err != nil {
		return err
	}
	err = s.store.LoadPools(func(put func(k, v []byte) error) error {
		for _, st := range states {
			v, err := encodePoolState(st)
			if err != nil {
				return err
			}
			if err := put(poolKey(st.Pool), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load pools: %w", err)
	}
	s.logger.Info("loaded pools into the live UTxO set", "pools", len(states))
	return nil
}

func loadPoolStates(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	epoch models.Epoch,
) ([]ledger.PoolState, error) {
	active, err := ldb.ListPoolKeyHashes(txn)
	if err != nil {
		return nil, fmt.Errorf("utxoset: list pools: %w", err)
	}
	pkhs := make([]lcommon.PoolKeyHash, 0, len(active))
	for _, a := range active {
		pkhs = append(pkhs, lcommon.PoolKeyHash(a))
	}
	effective, err := ldb.GetPoolRegistrationsEffectiveForEpoch(
		pkhs, epoch.StartSlot, epoch.EpochId, math.MaxInt64, txn,
	)
	if err != nil {
		return nil, fmt.Errorf("utxoset: read effective pool registrations: %w", err)
	}
	pools, err := ldb.GetPools(ctx, pkhs, txn)
	if err != nil {
		return nil, fmt.Errorf("utxoset: read pools: %w", err)
	}
	deposits, err := ldb.GetPoolDepositsHeld(pkhs, txn)
	if err != nil {
		return nil, fmt.Errorf("utxoset: read pool deposits: %w", err)
	}
	byPool := make(map[string]*ledger.PoolState, len(effective))
	effectiveRegs := make(map[string]*models.PoolRegistration, len(effective))
	// byPool points into out, which must not grow once the pointers are taken.
	out := make([]ledger.PoolState, 0, len(effective))
	for i := range effective {
		r := effective[i]
		st := ledger.PoolState{Pool: r.PoolKeyHash, Params: ledger.CleanPoolRegistration(&r)}
		if d, ok := deposits[string(r.PoolKeyHash)]; ok {
			st.DepositHeld = &d
		}
		out = append(out, st)
		byPool[string(r.PoolKeyHash)] = &out[len(out)-1]
		effectiveRegs[string(r.PoolKeyHash)] = &effective[i]
	}
	for _, p := range pools {
		st, ok := byPool[string(p.PoolKeyHash)]
		if !ok {
			continue
		}
		latest, retiring := ledger.PoolLatestEvents(&p)
		if ledger.PoolRegistrationSupersedes(latest, effectiveRegs[string(p.PoolKeyHash)]) {
			st.Future = ledger.CleanPoolRegistration(latest)
		}
		st.RetiringEpoch = retiring
	}
	if len(out) != len(byPool) {
		return nil, errors.New("utxoset: duplicate effective pool registration")
	}
	return out, nil
}

// PoolsLoaded reports whether the set holds every registered pool's state.
func (s *Set) PoolsLoaded() (bool, error) {
	return s.store.PoolsLoaded()
}

// PoolState returns pool's state, or nil when it is not registered.
func (s *Set) PoolState(pool []byte) (*ledger.PoolState, error) {
	v, ok, err := s.store.Row(poolKey(pool))
	if err != nil || !ok {
		return nil, err
	}
	return decodePoolState(pool, v)
}

// PoolStateAt returns pool's state as a rollback to slot would leave it.
func (s *Set) PoolStateAt(pool []byte, slot uint64) (*ledger.PoolState, error) {
	v, ok, err := s.store.RowAt(poolKey(pool), slot)
	if err != nil || !ok {
		return nil, err
	}
	return decodePoolState(pool, v)
}

// PoolStates calls fn with every registered pool's state.
func (s *Set) PoolStates(fn func(*ledger.PoolState) error) error {
	return s.store.Rows([]byte{badgerutxo.PoolTag}, func(k, v []byte) error {
		st, err := decodePoolState(k[1:], v)
		if err != nil {
			return err
		}
		return fn(st)
	})
}
