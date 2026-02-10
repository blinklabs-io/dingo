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

package ledgerstate

import "github.com/blinklabs-io/gouroboros/cbor"

// SnapshotTip represents the chain tip recorded in the ledger state
// snapshot.
type SnapshotTip struct {
	Slot      uint64
	BlockHash []byte
}

// RawLedgerState holds partially-decoded ledger state data from a
// Cardano node snapshot file. Large sections are kept as
// cbor.RawMessage for deferred, streaming decoding.
type RawLedgerState struct {
	// EraIndex identifies the current era (0=Byron … 6=Conway).
	EraIndex int
	// Epoch is the epoch number at the time of the snapshot.
	Epoch uint64
	// Tip is the chain tip recorded in the snapshot.
	Tip *SnapshotTip
	// Treasury is the treasury balance in lovelace.
	Treasury uint64
	// Reserves is the reserves balance in lovelace.
	Reserves uint64
	// UTxOData is the deferred CBOR for the UTxO map.
	UTxOData cbor.RawMessage
	// CertStateData is the deferred CBOR for [VState, PState, DState].
	CertStateData cbor.RawMessage
	// GovStateData is the deferred CBOR for governance state.
	GovStateData cbor.RawMessage
	// PParamsData is the deferred CBOR for protocol parameters.
	PParamsData cbor.RawMessage
	// SnapShotsData is the deferred CBOR for mark/set/go stake snapshots.
	SnapShotsData cbor.RawMessage
	// EraBounds holds the start boundaries of all eras extracted
	// from the telescope. Each entry gives the (Slot, Epoch) at
	// which that era began. Used to generate the full epoch
	// history needed for SlotToTime/TimeToSlot.
	EraBounds []EraBound
	// EraBoundSlot is the slot number at which the current era
	// started, extracted from the telescope Bound.
	EraBoundSlot uint64
	// EraBoundEpoch is the epoch number at which the current era
	// started, extracted from the telescope Bound.
	EraBoundEpoch uint64
	// EpochNonce is the epoch nonce from the consensus state,
	// used for VRF leader election. It is a 32-byte Blake2b hash,
	// or nil if the nonce is neutral.
	EpochNonce []byte
	// EraBoundsWarning holds a non-fatal error from era bounds
	// extraction. When set, epoch generation falls back to
	// the single-epoch path.
	EraBoundsWarning error
	// UTxOTablePath is the path to the UTxO table file (UTxO-HD
	// format). When set, UTxOs are streamed from this file instead
	// of from UTxOData.
	UTxOTablePath string
}

// EraBound represents the start boundary of an era in the
// HardFork telescope. Slot is the first slot of the era and
// Epoch is the epoch number at that slot.
type EraBound struct {
	Slot  uint64
	Epoch uint64
}

// ParsedUTxO represents a parsed unspent transaction output ready
// for conversion to Dingo's database model.
type ParsedUTxO struct {
	TxHash      []byte // 32 bytes
	OutputIndex uint32
	Address     []byte // raw address bytes
	PaymentKey  []byte // 28 bytes, extracted from address
	StakingKey  []byte // 28 bytes, extracted from address
	Amount      uint64 // lovelace
	Assets      []ParsedAsset
	DatumHash   []byte // optional
	Datum       []byte // optional inline datum CBOR
	ScriptRef   []byte // optional reference script CBOR
}

// ParsedAsset represents a native asset within a UTxO.
type ParsedAsset struct {
	PolicyId []byte // 28 bytes
	Name     []byte
	Amount   uint64
}

// ParsedCertState holds the parsed delegation, pool, and DRep state.
type ParsedCertState struct {
	Accounts []ParsedAccount
	Pools    []ParsedPool
	DReps    []ParsedDRep
}

// ParsedAccount represents a stake account with its delegation.
type ParsedAccount struct {
	StakingKey  []byte // 28-byte credential hash
	PoolKeyHash []byte // 28-byte pool key hash (delegation target)
	DRepCred    []byte // 28-byte DRep credential (vote delegation)
	Reward      uint64 // reward balance in lovelace
	Active      bool
}

// ParsedPool represents a stake pool registration.
type ParsedPool struct {
	PoolKeyHash   []byte // 28 bytes
	VrfKeyHash    []byte // 32 bytes
	Pledge        uint64
	Cost          uint64
	MarginNum     uint64
	MarginDen     uint64
	RewardAccount []byte
	Owners        [][]byte // list of 28-byte key hashes
	Relays        []ParsedRelay
	MetadataUrl   string
	MetadataHash  []byte // 32 bytes
	Deposit       uint64
}

// ParsedRelay represents a pool relay from the stake pool
// registration certificate.
type ParsedRelay struct {
	Type     uint8  // 0=SingleHostAddr, 1=SingleHostName, 2=MultiHostName
	Port     uint16 // 0 if not specified
	IPv4     []byte // 4 bytes, nil if not specified
	IPv6     []byte // 16 bytes, nil if not specified
	Hostname string // for SingleHostName and MultiHostName
}

// ParsedDRep represents a DRep registration.
type ParsedDRep struct {
	Credential  []byte // 28-byte credential hash
	AnchorUrl   string
	AnchorHash  []byte
	Deposit     uint64
	ExpiryEpoch uint64 // epoch when this DRep expires (0 = unknown)
	Active      bool
}

// ParsedSnapShots holds the three stake distribution snapshots
// (mark, set, go) from the ledger state.
type ParsedSnapShots struct {
	Mark ParsedSnapShot
	Set  ParsedSnapShot
	Go   ParsedSnapShot
	Fee  uint64
}

// ParsedSnapShot represents a single stake distribution snapshot.
type ParsedSnapShot struct {
	// Stake maps credential-hex to staked lovelace.
	Stake map[string]uint64
	// Delegations maps credential-hex to pool key hash.
	Delegations map[string][]byte
	// PoolParams maps pool-hex to pool parameters.
	PoolParams map[string]*ParsedPool
}

// ImportProgress reports progress during ledger state import.
type ImportProgress struct {
	Stage       string
	Current     int
	Total       int
	Description string
}
