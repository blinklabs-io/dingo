// Copyright 2025 Blink Labs Software
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

package models

import (
	"errors"
	"net"

	"github.com/blinklabs-io/dingo/database/types"
)

var ErrPoolNotFound = errors.New("pool not found")

// Error 1170 (42000): BLOB/TEXT column 'staking_key' used in key specification without a key length
type Pool struct {
	Margin               *types.Rat
	PoolKeyHash          []byte
	VrfKeyHash           []byte
	RewardAccount        []byte
	LatestOpCertSequence uint64
	// RewardAccountCredentialTag is the stake credential type of the pool's
	// reward account: 0 = key hash, 1 = script hash. The on-chain pool cert
	// encodes the reward_account as a 29-byte reward address (header + 28-byte
	// hash). The gouroboros library stores only the first 28 bytes in
	// RewardAccount (AddrKeyHash), discarding the header. We decode the raw
	// cert CBOR to preserve the credential type here.
	RewardAccountCredentialTag uint8
	// LeiosKeyPublic and LeiosKeyPossessionProof are the pool's registered
	// Dijkstra/Leios BLS voting key (96-byte compressed G2 public key) and
	// its proof of possession (48-byte compressed G1 signature), as decoded
	// from the on-chain leios_key pool-cert field. Both are nil only when
	// the pool has no leios_key. This is raw registration data -- the proof
	// is not checked here or anywhere in this package; a key with an
	// invalid proof is still stored as-is, and only excluded when read back
	// out for committee construction in ledger/leios, matching upstream's
	// "invalid proofs are treated as absent" rule.
	LeiosKeyPublic          []byte
	LeiosKeyPossessionProof []byte
	// Owners and Relays are query-only associations (no CASCADE).
	// The actual parent-child relationship is PoolRegistration -> Owners/Relays.
	// When Pool is deleted, PoolRegistrations cascade, which then cascade to Owners/Relays.
	Owners       []PoolRegistrationOwner
	Relays       []PoolRegistrationRelay
	Registration []PoolRegistration
	Retirement   []PoolRetirement
	ID           uint
	Pledge       types.Uint64
	Cost         types.Uint64
}

// PoolOpCertSequence records the operational-certificate issue number a pool
// minted each of its blocks under. It takes a row per block for the life of
// the chain and is pruned only by rollback, so reads over the whole table have
// to be served from an index rather than from its rows.
//
// idx_pool_opcert_sequence_pool_sequence exists for one of those: the
// highest-sequence-per-pool fold behind GetChainDepState's counters, which has
// no slot bound to narrow it because every row it holds is at or below the tip.
// Carrying the sequence beside the pool key hash lets that fold run without
// reading a single row, and in the index's own order, so the GROUP BY needs no
// sort.
//
// It does not make the fold sublinear everywhere. MySQL 8 can skip through the
// index a pool at a time; SQLite has no loose index scan and reads it end to
// end, so there the win is dropping the row fetches rather than dropping the
// scan. Worth the write cost for a one-shot query behind
// `leadership-schedule`; it would not be for something on a hot path.
//
// Migration v1 declares it; the schema lives in SQL rather than in tags on this
// struct.
type PoolOpCertSequence struct {
	PoolKeyHash []byte
	ID          uint
	Slot        uint64
	Sequence    uint64
}

type PoolRegistration struct {
	Margin                     *types.Rat
	Pool                       *Pool // Belongs-to relationship; CASCADE is defined on Pool.Registration
	MetadataUrl                string
	VrfKeyHash                 []byte
	PoolKeyHash                []byte
	RewardAccount              []byte
	RewardAccountCredentialTag uint8
	MetadataHash               []byte
	LeiosKeyPublic             []byte
	LeiosKeyPossessionProof    []byte
	Owners                     []PoolRegistrationOwner
	Relays                     []PoolRegistrationRelay
	Pledge                     types.Uint64
	Cost                       types.Uint64
	CertificateID              uint
	ID                         uint
	PoolID                     uint
	AddedSlot                  uint64
	DepositAmount              types.Uint64
}

type PoolRegistrationOwner struct {
	KeyHash            []byte
	ID                 uint
	PoolRegistrationID uint
	PoolID             uint
}

type PoolRegistrationRelay struct {
	Ipv4               *net.IP
	Ipv6               *net.IP
	Hostname           string
	ID                 uint
	PoolRegistrationID uint
	PoolID             uint
	Port               uint
}

// PoolRetiringRow is one pending-retirement entry returned by
// GetRetiringPools: the latest retirement certificate for a pool that
// has not been cancelled by a later registration and whose epoch is
// still in the future.
type PoolRetiringRow struct {
	PoolKeyHash []byte
	Epoch       uint64
}

type PoolRetirement struct {
	PoolKeyHash   []byte
	CertificateID uint
	ID            uint
	PoolID        uint
	Epoch         uint64
	AddedSlot     uint64
}

// PoolRetirementRefund identifies a pool retiring at an epoch boundary along
// with the reward account and deposit needed to refund its POOLREAP deposit.
// It is a query result, not a persisted table.
type PoolRetirementRefund struct {
	PoolKeyHash                []byte
	RewardAccount              []byte
	RewardAccountCredentialTag uint8
	DepositAmount              types.Uint64
}
