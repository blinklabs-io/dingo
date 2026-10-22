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

// Package leioskeyepoch derives the epoch a pool's BLS key was registered in
// from the pool's earlier registrations and retirements.
package leioskeyepoch

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// Querier is the subset of database/sql the replay reads through. The
// statements use ? placeholders and the dialect-neutral SQL the sqlstore
// queryer translates.
type Querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Position is a pool certificate's position on the chain: the slot it was
// added at, the block index of its transaction, and its index within that
// transaction's certificate sequence.
type Position struct {
	Slot       uint64
	BlockIndex uint64
	CertIndex  uint64
}

func (p Position) before(other Position) bool {
	if p.Slot != other.Slot {
		return p.Slot < other.Slot
	}
	if p.BlockIndex != other.BlockIndex {
		return p.BlockIndex < other.BlockIndex
	}
	return p.CertIndex < other.CertIndex
}

// unsigned converts a SQL INTEGER to the unsigned domain it stores. A negative
// value can only come from a row altered outside normal writes.
func unsigned(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("negative unsigned SQL value %d", value)
	}
	return uint64(value), nil
}

// GenesisEpoch is the epoch the ledger registers genesis pools' BLS
// keys in: mkStakePoolState (nesEL nes) SNothing, with an initial nesEL of
// epoch 0 (cardano-ledger 9df9f98c, Shelley/Transition.hs:462 initialEpochNo and
// the staking injection using nesELL at lines 543 and 561).
const GenesisEpoch uint64 = 0

// leiosKeyState is the BLS key a pool holds and the epoch it was registered
// in, which is the ledger's BlsKeyState. A nil epoch is an epoch the database
// cannot establish: the key came from a snapshot that did not carry it.
type leiosKeyState struct {
	public []byte
	proof  []byte
	epoch  *uint64
}

func (k *leiosKeyState) sameKey(public, proof []byte) bool {
	return k != nil &&
		bytes.Equal(k.public, public) &&
		bytes.Equal(k.proof, proof)
}

// poolLeiosEvent is one earlier registration or retirement of a pool.
type poolLeiosEvent struct {
	position      Position
	executionSlot uint64
	retirement    bool
	// retireEpoch is the epoch a retirement names.
	retireEpoch uint64
	// snapshot marks a registration synthesized by a snapshot import, which
	// has no certificate and carries the key state it was imported with.
	snapshot          bool
	registrationIndex uint
	state             *leiosKeyState
}

// Registration derives the leios_key_registration_epoch and
// leios_key_registration_age_unknown columns for the pool registration
// certificate being written at `at`, which carries the BLS key public/proof.
//
// It follows cardano-ledger's Dijkstra rules (IntersectMBO/cardano-ledger at
// 9df9f98c; paths below are under eras/dijkstra/impl/src/Cardano/Ledger/
// Dijkstra/ unless they name another package). POOL (Rules/Pool.hs:261, the
// not-registered branch) builds a new pool's state with mkStakePoolState
// cEpoch SNothing, so a key registered by a first registration is registered
// in the epoch of the transaction. POOL's re-registration branch only stages
// the parameters in psFutureStakePoolParams, replacing any earlier staged
// ones. POOLREAP (Rules/PoolReap.hs:130) adopts them at the next epoch
// boundary through mkStakePoolState e (current BLS state)
// (libs/cardano-ledger-core/src/Cardano/Ledger/State/StakePool.hs:288): a key
// equal to the active one keeps its registration epoch, and any other key,
// including one added to a pool that had none, is registered in e, the epoch
// after the transaction's. BlsKey equality covers the public key and the
// possession proof. Dijkstra has no NEWEPOCH rule of its own: Era.hs:202 sets
// EraRule "NEWEPOCH" to Conway's, which on an epoch change runs EPOCH
// (eras/conway/impl/src/Cardano/Ledger/Conway/Rules/NewEpoch.hs:153-165), and
// Dijkstra's EPOCH calls POOLREAP (Rules/Epoch.hs:171).
//
// The pool's earlier registrations and retirements, strictly before `at`, are
// replayed through those rules, so the result is a function of the chain up to
// this certificate and survives rollback and replay. A snapshot-imported
// registration is the state the snapshot carried, with its epoch, or without
// one when the snapshot had none; a key kept from it keeps the absence.
//
// The returned epoch is nil with ageUnknown set when the registration keeps a
// key whose epoch is unknown, and nil with ageUnknown clear when an epoch
// cannot be placed because the epoch table does not cover a slot involved.
// Zero is never stored for an epoch that is not known.
func Registration(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	executionSlot uint64,
	public []byte,
	proof []byte,
) (epoch *uint64, ageUnknown bool, err error) {
	events, err := eventsBefore(ctx, db, poolID, at)
	if err != nil {
		return nil, false, err
	}
	epochs := map[uint64]uint64{}
	resolve := func(slot uint64) (uint64, bool, error) {
		if known, ok := epochs[slot]; ok {
			return known, true, nil
		}
		resolved, ok, err := epochAtSlot(ctx, db, slot)
		if err != nil || !ok {
			return 0, false, err
		}
		epochs[slot] = resolved
		return resolved, true, nil
	}
	current, ok, err := resolve(executionSlot)
	if err != nil || !ok {
		return nil, false, err
	}

	var (
		registered bool
		active     *leiosKeyState
		staged     *leiosKeyState // nil unless a re-registration is staged
		hasStaged  bool
		retiring   *uint64
		lastEpoch  uint64
	)
	// adopt is POOLREAP's mkStakePoolState at the boundary into epoch e.
	adopt := func(e uint64, next *leiosKeyState) *leiosKeyState {
		if next == nil {
			return nil
		}
		if active.sameKey(next.public, next.proof) {
			return active
		}
		return &leiosKeyState{
			public: next.public,
			proof:  next.proof,
			epoch:  &e,
		}
	}
	advance := func(to uint64) {
		if to > lastEpoch && hasStaged {
			active = adopt(lastEpoch+1, staged)
			staged, hasStaged = nil, false
		}
		if to > lastEpoch {
			lastEpoch = to
		}
		if retiring != nil && *retiring <= to {
			registered, active = false, nil
			staged, hasStaged, retiring = nil, false, nil
		}
	}
	for _, event := range events {
		eventEpoch, ok, err := resolve(event.executionSlot)
		if err != nil || !ok {
			return nil, false, err
		}
		advance(eventEpoch)
		switch {
		case event.retirement:
			if registered {
				value := event.retireEpoch
				retiring = &value
			}
		case event.snapshot && (!registered || event.registrationIndex == 0):
			registered, active = true, event.state
			staged, hasStaged, retiring = nil, false, nil
		case !registered:
			registered, active = true, nil
			if event.state != nil {
				value := eventEpoch
				active = &leiosKeyState{
					public: event.state.public,
					proof:  event.state.proof,
					epoch:  &value,
				}
			}
			retiring = nil
		default:
			staged, hasStaged = event.state, true
			retiring = nil
		}
	}
	advance(current)

	incoming := &leiosKeyState{public: public, proof: proof}
	var result *leiosKeyState
	if registered {
		result = adopt(current+1, incoming)
	} else {
		result = &leiosKeyState{public: public, proof: proof, epoch: &current}
	}
	if result.epoch == nil {
		return nil, true, nil
	}
	value := *result.epoch
	return &value, false, nil
}

// eventsBefore loads the pool's registrations and retirements strictly before
// `at`, in chain order. A registration and a retirement at the same position
// resolve to the retirement last, which is what the deposit bookkeeping does
// for a snapshot import's registration and its tombstone.
func eventsBefore(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
) ([]poolLeiosEvent, error) {
	registrations, err := registrationEvents(ctx, db, poolID, at)
	if err != nil {
		return nil, err
	}
	retirements, err := retirementEvents(ctx, db, poolID, at)
	if err != nil {
		return nil, err
	}
	events := append(registrations, retirements...)
	sort.SliceStable(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.position != b.position {
			return a.position.before(b.position)
		}
		if a.retirement != b.retirement {
			return !a.retirement
		}
		return a.registrationIndex < b.registrationIndex
	})
	return events, nil
}

// executionSlotSQL resolves the slot a certificate's transaction executed in:
// its Leios closure slot when it has one, the slot it was added at otherwise.
const executionSlotSQL = `COALESCE((
    SELECT lc.slot FROM leios_transaction_context lc
    JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id
    WHERE lc_cert.id = %s.certificate_id), %s.added_slot)`

func registrationEvents(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
) ([]poolLeiosEvent, error) {
	predicate, args := positionPredicate("pr", at, registrationBlockIndex)
	rows, err := db.QueryContext(ctx, `
SELECT pr.added_slot, `+fmt.Sprintf(executionSlotSQL, "pr", "pr")+`,
       `+registrationBlockIndex+`, COALESCE(c.cert_index, 0),
       pr.certificate_id, pr.registration_index,
       pr.leios_key_public, pr.leios_key_possession_proof,
       pr.leios_key_registration_epoch, pr.leios_key_registration_age_unknown
FROM pool_registration pr
LEFT JOIN certs c ON c.id = pr.certificate_id
WHERE pr.pool_id = ?
  AND `+predicate,
		append([]any{poolID}, args...)...,
	)
	if err != nil {
		return nil, fmt.Errorf("pool leios key epoch: read registrations: %w", err)
	}
	defer rows.Close()
	var events []poolLeiosEvent
	for rows.Next() {
		var (
			addedSlot, exec, blockIdx, certIdx int64
			certificateID                      int64
			registrationIndex                  uint
			public, proof                      []byte
			epoch                              sql.NullInt64
			ageUnknown                         bool
		)
		if err := rows.Scan(
			&addedSlot, &exec, &blockIdx, &certIdx, &certificateID,
			&registrationIndex, &public, &proof, &epoch, &ageUnknown,
		); err != nil {
			return nil, err
		}
		position, err := newPosition(addedSlot, blockIdx, certIdx)
		if err != nil {
			return nil, err
		}
		executionSlot, err := unsigned(exec)
		if err != nil {
			return nil, err
		}
		event := poolLeiosEvent{
			position:          position,
			executionSlot:     executionSlot,
			snapshot:          certificateID == 0,
			registrationIndex: registrationIndex,
		}
		if len(public) > 0 && len(proof) > 0 {
			event.state = &leiosKeyState{public: public, proof: proof}
			switch {
			case event.snapshot && epoch.Valid:
				value, err := unsigned(epoch.Int64)
				if err != nil {
					return nil, err
				}
				event.state.epoch = &value
			}
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func retirementEvents(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
) ([]poolLeiosEvent, error) {
	predicate, args := positionPredicate("rt", at, "COALESCE(t.block_index, 0)")
	rows, err := db.QueryContext(ctx, `
SELECT rt.epoch, rt.added_slot, `+fmt.Sprintf(executionSlotSQL, "rt", "rt")+`,
       COALESCE(t.block_index, 0), COALESCE(c.cert_index, 0)
FROM pool_retirement rt
LEFT JOIN certs c ON c.id = rt.certificate_id
LEFT JOIN "transaction" t ON t.id = c.transaction_id
WHERE rt.pool_id = ?
  AND `+predicate,
		append([]any{poolID}, args...)...,
	)
	if err != nil {
		return nil, fmt.Errorf("pool leios key epoch: read retirements: %w", err)
	}
	defer rows.Close()
	var events []poolLeiosEvent
	for rows.Next() {
		var epoch, addedSlot, exec, blockIdx, certIdx int64
		if err := rows.Scan(
			&epoch, &addedSlot, &exec, &blockIdx, &certIdx,
		); err != nil {
			return nil, err
		}
		position, err := newPosition(addedSlot, blockIdx, certIdx)
		if err != nil {
			return nil, err
		}
		executionSlot, err := unsigned(exec)
		if err != nil {
			return nil, err
		}
		retireEpoch, err := unsigned(epoch)
		if err != nil {
			return nil, err
		}
		events = append(events, poolLeiosEvent{
			position:      position,
			executionSlot: executionSlot,
			retirement:    true,
			retireEpoch:   retireEpoch,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func newPosition(slot, blockIndex, certIndex int64) (Position, error) {
	var (
		position Position
		err      error
	)
	if position.Slot, err = unsigned(slot); err != nil {
		return Position{}, err
	}
	if position.BlockIndex, err = unsigned(blockIndex); err != nil {
		return Position{}, err
	}
	if position.CertIndex, err = unsigned(certIndex); err != nil {
		return Position{}, err
	}
	return position, nil
}

// registrationBlockIndex is the block index of a registration's transaction,
// zero for a synthesized row with no certificate.
const registrationBlockIndex = `COALESCE((
    SELECT tx.block_index FROM "transaction" tx WHERE tx.id = c.transaction_id
), 0)`

// positionPredicate matches the pool certificate rows strictly before a
// position, and returns the bind arguments for it. The joined block and
// certificate indexes are NULL for a synthesized row with no certs row, which
// is why both sides COALESCE to zero.
func positionPredicate(
	table string,
	at Position,
	blockIndex string,
) (string, []any) {
	args := []any{
		at.Slot,
		at.Slot, at.BlockIndex,
		at.Slot, at.BlockIndex, at.CertIndex,
	}
	return `(
      ` + table + `.added_slot < ?
      OR (` + table + `.added_slot = ?
          AND ` + blockIndex + ` < ?)
      OR (` + table + `.added_slot = ?
          AND ` + blockIndex + ` = ?
          AND COALESCE(c.cert_index, 0) < ?)
  )`, args
}

// epochAtSlot resolves the persisted epoch containing slot. It reports
// resolved=false rather than an error when the epoch table cannot place the
// slot, because a pool registration write must not fail on missing epoch
// bookkeeping.
func epochAtSlot(
	ctx context.Context,
	db Querier,
	slot uint64,
) (uint64, bool, error) {
	var epoch, startSlot, length sql.NullInt64
	err := db.QueryRowContext(ctx, `
SELECT epoch_id, start_slot, length_in_slots
FROM epoch
WHERE start_slot <= ?
ORDER BY start_slot DESC
LIMIT 1`,
		slot,
	).Scan(&epoch, &startSlot, &length)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("epoch at slot %d: %w", slot, err)
	}
	if !epoch.Valid || !startSlot.Valid || !length.Valid {
		return 0, false, nil
	}
	end, err := unsigned(startSlot.Int64 + length.Int64)
	if err != nil {
		return 0, false, fmt.Errorf("epoch at slot %d: %w", slot, err)
	}
	if slot >= end {
		return 0, false, nil
	}
	resolved, err := unsigned(epoch.Int64)
	if err != nil {
		return 0, false, err
	}
	return resolved, true, nil
}
