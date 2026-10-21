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

//nolint:gosec,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain; cursors are closed before the next query.
package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sort"
)

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
	position      poolCertPosition
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

// poolLeiosKeyRegistration derives the leios_key_registration_epoch and
// leios_key_registration_age_unknown columns for the pool registration
// certificate being written at `at`, which carries the BLS key public/proof.
//
// It follows cardano-ledger's Dijkstra rules (IntersectMBO/cardano-ledger at
// 9df9f98c). POOL (Rules/Pool.hs, the not-registered branch) builds a new
// pool's state with mkStakePoolState cEpoch SNothing, so a key registered by a
// first registration is registered in the epoch of the transaction. POOL's
// re-registration branch only stages the parameters in
// psFutureStakePoolParams, replacing any earlier staged ones. POOLREAP
// (Rules/PoolReap.hs) adopts them at the next epoch boundary through
// mkStakePoolState e (current BLS state) (State/StakePool.hs): a key equal to
// the active one keeps its registration epoch, and any other key, including
// one added to a pool that had none, is registered in e, the epoch after the
// transaction's. BlsKey equality covers the public key and the possession
// proof.
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
func poolLeiosKeyRegistration(
	ctx context.Context,
	db queryer,
	poolID int64,
	at poolCertPosition,
	executionSlot uint64,
	public []byte,
	proof []byte,
) (epoch *uint64, ageUnknown bool, err error) {
	events, err := poolLeiosEventsBefore(ctx, db, poolID, at)
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

// poolLeiosEventsBefore loads the pool's registrations and retirements
// strictly before `at`, in chain order. A registration and a retirement at the
// same position resolve to the retirement last, which is what the deposit
// bookkeeping does for a snapshot import's registration and its tombstone.
func poolLeiosEventsBefore(
	ctx context.Context,
	db queryer,
	poolID int64,
	at poolCertPosition,
) ([]poolLeiosEvent, error) {
	const executionSlot = `COALESCE((
    SELECT lc.slot FROM leios_transaction_context lc
    JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id
    WHERE lc_cert.id = %s.certificate_id), %s.added_slot)`
	registrationPredicate, registrationArgs := poolPositionPredicate(
		"pr", at, poolRegistrationBlockIndex,
	)
	rows, err := db.QueryContext(ctx, `
SELECT pr.added_slot, `+fmt.Sprintf(executionSlot, "pr", "pr")+`,
       `+poolRegistrationBlockIndex+`, COALESCE(c.cert_index, 0),
       pr.certificate_id, pr.registration_index,
       pr.leios_key_public, pr.leios_key_possession_proof,
       pr.leios_key_registration_epoch
FROM pool_registration pr
LEFT JOIN certs c ON c.id = pr.certificate_id
WHERE pr.pool_id = ?
  AND `+registrationPredicate,
		append([]any{poolID}, registrationArgs...)...,
	)
	if err != nil {
		return nil, fmt.Errorf("pool leios key epoch: read registrations: %w", err)
	}
	var events []poolLeiosEvent
	for rows.Next() {
		var (
			addedSlot, exec, blockIdx, certIdx int64
			certificateID                      int64
			registrationIndex                  uint
			public, proof                      []byte
			epoch                              sql.NullInt64
		)
		if err := rows.Scan(
			&addedSlot, &exec, &blockIdx, &certIdx, &certificateID,
			&registrationIndex, &public, &proof, &epoch,
		); err != nil {
			rows.Close()
			return nil, err
		}
		event := poolLeiosEvent{
			position: poolCertPosition{
				slot:       uint64(addedSlot),
				blockIndex: uint64(blockIdx),
				certIndex:  uint64(certIdx),
			},
			executionSlot:     uint64(exec),
			snapshot:          certificateID == 0,
			registrationIndex: registrationIndex,
		}
		if len(public) > 0 && len(proof) > 0 {
			event.state = &leiosKeyState{public: public, proof: proof}
			if event.snapshot && epoch.Valid {
				value := uint64(epoch.Int64)
				event.state.epoch = &value
			}
		}
		events = append(events, event)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	retirementPredicate, retirementArgs := poolPositionPredicate(
		"rt", at, "COALESCE(t.block_index, 0)",
	)
	rows, err = db.QueryContext(ctx, `
SELECT rt.epoch, rt.added_slot, `+fmt.Sprintf(executionSlot, "rt", "rt")+`,
       COALESCE(t.block_index, 0), COALESCE(c.cert_index, 0)
FROM pool_retirement rt
LEFT JOIN certs c ON c.id = rt.certificate_id
LEFT JOIN "transaction" t ON t.id = c.transaction_id
WHERE rt.pool_id = ?
  AND `+retirementPredicate,
		append([]any{poolID}, retirementArgs...)...,
	)
	if err != nil {
		return nil, fmt.Errorf("pool leios key epoch: read retirements: %w", err)
	}
	for rows.Next() {
		var epoch, addedSlot, exec, blockIdx, certIdx int64
		if err := rows.Scan(
			&epoch, &addedSlot, &exec, &blockIdx, &certIdx,
		); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, poolLeiosEvent{
			position: poolCertPosition{
				slot:       uint64(addedSlot),
				blockIndex: uint64(blockIdx),
				certIndex:  uint64(certIdx),
			},
			executionSlot: uint64(exec),
			retirement:    true,
			retireEpoch:   uint64(epoch),
		})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

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
