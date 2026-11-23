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

package badgerutxo

// Every row family in the store is keyed by a one-byte tag, and every
// marker key starts with 'm'. This file is the only place either is
// declared; internal/utxoset uses the exported names. Bootstrap's
// DropPrefix and the load code treat a tag as the whole of a family, so
// two families sharing a first byte would drop or misread each other's
// rows; TestKeyFamiliesDoNotOverlap fails when they do. Each byte is
// persisted in existing stores and must never change. The layout of each
// family is documented where it is read and written.

// Row family tags.
const (
	// UTxOTag keys live outputs: u <tx id:32> <index:4 BE>.
	UTxOTag = 'u'
	// JournalTag keys the UTxO journal: j <seq:8 BE>.
	JournalTag = 'j'
	// AggTag keys the aggregator's running sums: a <caller's key>.
	AggTag = 'a'
	// NonceTag keys block nonces: n <slot:8 BE> <hash>.
	NonceTag = 'n'
	// EpochTag keys epoch rows: e <epoch:8 BE>.
	EpochTag = 'e'
	// SnapshotTag keys pool stake snapshot rows: s <epoch:8 BE> ...
	SnapshotTag = 's'
	// SnapshotSlotTag keys the pool stake snapshot capture index:
	// $ <captured slot:8 BE> <epoch:8 BE> <len:1> <type> <pool>, with no
	// value. The key after the slot is the row's key under SnapshotTag, so a
	// range of capture slots reads exactly the rows captured in it.
	SnapshotSlotTag = '$'
	// SummaryTag keys epoch summary rows: f <epoch:8 BE>.
	SummaryTag = 'f'
	// NetworkStateTag keys treasury and reserves rows: t <slot:8 BE>.
	NetworkStateTag = 't'
	// DonationTag keys donation rows: d <slot:8 BE>.
	DonationTag = 'd'
	// PParamsTag keys protocol parameter rows: p <era> <epoch> <seq>.
	PParamsTag = 'p'
	// PParamsSlotTag keys the protocol parameter rollback index:
	// q <added slot:8 BE> <seq:8 BE>.
	PParamsSlotTag = 'q'
	// AccountTag keys accounts: c <tag:1> <key>.
	AccountTag = 'c'
	// AccountPoolTag keys the account-by-pool index: b <len:1> <pool> ...
	AccountPoolTag = 'b'
	// AccountDRepTag keys the account-by-DRep index:
	// v <type:1> <len:1> <DRep> ...
	AccountDRepTag = 'v'
	// StateJournalTag keys the state journal of the computed rows
	// (accounts, DReps, DRep delegator links, the dormancy counter):
	//
	//	k <slot:8 BE> <primary key>   the row before its first change at
	//	                              slot: 0 for absent, else 1 and its value
	//
	// A rollback restores every row changed after the target, newest slot
	// first, on either rollback path; journal pruning drops the entries
	// below the rollback window.
	StateJournalTag = 'k'
	// RewardRecordTag keys applied reward credits and withdrawals:
	// r <slot:8 BE> <kind:1> <len:1> <source> <tag:1> <key>. A record
	// makes the same credit or withdrawal at the same slot apply once, as
	// the metadata store's account_reward_delta uniqueness does.
	RewardRecordTag = 'r'
	// DRepTag keys DReps: g <tag:1> <credential>.
	DRepTag = 'g'
	// DRepLinkTag keys live DRep delegator links:
	// l <tag:1> <len:1> <DRep> <tag:1> <key>.
	DRepLinkTag = 'l'
	// DormancyTag is the first byte of DormancyKey, the only row it keys.
	DormancyTag = 'z'
	// PoolTag keys a pool's computed state: w <pool key hash>.
	PoolTag = 'w'
	// PoolHistoryTag keys a pool's certificate events, oldest first:
	// y <len:1> <pool> <slot:8 BE> <position:8 BE>. The position orders
	// events inside a slot.
	PoolHistoryTag = 'y'
	// StakeHistoryTag keys stake registration events:
	// h <tag:1> <len:1> <key> <slot:8 BE> <position:8 BE>.
	StakeHistoryTag = 'h'
	// DRepDepositTag keys a DRep's latest registration:
	// i <tag:1> <len:1> <credential>.
	DRepDepositTag = 'i'
	// OpCertTag keys a pool's op-cert counter: o <pool key hash>, valued
	// with the highest sequence observed, the slot of the latest
	// observation and the sequence observed at that slot, 8 bytes big-endian
	// each. legacyOpCertsKey marked counters without the last sequence, which
	// are reloaded rather than read.
	OpCertTag = 'o'
	// PParamUpdateTag keys protocol parameter update proposals:
	// x <slot:8 BE> <index:4 BE> <genesis key hash>, valued with the epoch
	// (8 bytes BE) then the CBOR. The slot is the one the proposal was
	// stored at, so a rollback restores the rows by slot.
	PParamUpdateTag = 'x'
	// GenesisDelegationTag keys genesis delegations:
	// G <slot:8 BE> <position:8 BE>.
	GenesisDelegationTag = 'G'
	// MIRCertTag keys MIR certificates: M <slot:8 BE> <position:8 BE>.
	MIRCertTag = 'M'
	// CommitteeMemberTag keys committee member rows:
	// C <slot:8> <tag:1> <cold hash>.
	CommitteeMemberTag = 'C'
	// CommitteeQuorumTag keys committee quorum rows: Q <slot:8>.
	CommitteeQuorumTag = 'Q'
	// ConstitutionTag keys constitution rows: N <slot:8>.
	ConstitutionTag = 'N'
	// HotAuthTag keys a cold credential's latest hot-key authorization:
	// H <tag:1> <cold credential>.
	HotAuthTag = 'H'
	// ResignTag keys a cold credential's latest resignation:
	// R <tag:1> <cold credential>.
	ResignTag = 'R'
	// ProposalTag keys governance proposals:
	// P <tx hash:32> <action index:4 BE>.
	ProposalTag = 'P'
	// VoteTag keys governance votes:
	// V <tx hash:32> <action index:4 BE> <voter ...>.
	VoteTag = 'V'
	// RewardOutputTag keys credit-eligible reward account outputs.
	RewardOutputTag = 'O'
	// RewardEpochTag keys the epoch index of a RewardOutputTag row.
	RewardEpochTag = 'F'
	// RewardRoundTag keys credited reward rounds: W <epoch:8 BE>.
	RewardRoundTag = 'W'
	// RewardPostSnapTag keys credits applied after the SNAP point.
	RewardPostSnapTag = 'X'
	// ImportBaselineTag keys an account's import baseline, the virtual
	// registration a snapshot import, genesis or a catch-up import
	// established: B <tag:1> <key>.
	ImportBaselineTag = 'B'
	// WitnessTag keys the slots a stake credential was witnessed at, by a
	// stake-witnessing certificate or a reward withdrawal:
	// K <tag:1> <len:1> <key> <slot:8 BE>, with no value; a witness of a
	// Leios closure, which takes effect at an earlier slot, appends that
	// effective slot as 8 more bytes BE to the key.
	WitnessTag = 'K'
	// DelegationTag keys the pool delegation certificates of a stake
	// credential: D <tag:1> <len:1> <key> <applied slot:8 BE> <position:8 BE>,
	// valued with the effective slot (8 bytes BE) and the pool key hash.
	DelegationTag = 'D'
	// ClosureUtxoTag keys the staked outputs a Leios closure created or
	// spent at a slot before the one it was applied at:
	// L <applied slot:8 BE> <kind:1> <tx hash:32> <index:4 BE>, valued with
	// the effective slot (8 bytes BE), the lovelace (8 bytes BE) and the
	// stake sum key. Kind 0 is a created output, kind 1 a spent one.
	ClosureUtxoTag = 'L'
	// RewardBlocksTag keys the per-epoch block and fee rows reward
	// calculation reads: E <epoch:8 BE> <kind:1> ..., one prefix per epoch
	// so pruning an epoch is a prefix delete.
	RewardBlocksTag = 'E'
	// RewardStakeInputTag keys the per-credential stake of a reward
	// snapshot: I <epoch:8 BE> <pool:28> <credential tag:1> <credential:28>.
	RewardStakeInputTag = 'I'
	// RewardPoolInputTag keys a reward snapshot's per-pool inputs:
	// J <epoch:8 BE> <pool:28>.
	RewardPoolInputTag = 'J'
	// RewardSnapshotTag keys a reward snapshot's header and its imported
	// basis failure: S <epoch:8 BE> <kind:1> <snapshot type>.
	RewardSnapshotTag = 'S'
	// RewardPotsTag keys an epoch's ADA pots: A <epoch:8 BE>.
	RewardPotsTag = 'A'
	// RewardDerivedTag keys the reward outputs a precompute derives from the
	// snapshot inputs: U <epoch:8 BE> <kind:1> ...
	RewardDerivedTag = 'U'
	// RewardCursorTag keys an epoch's reward precompute cursor:
	// Z <epoch:8 BE>.
	RewardCursorTag = 'Z'
	// ImportedBlocksTag keys the block counts a snapshot import carried for
	// the epochs below its anchor, kept as imported rather than merged into
	// the windowed RewardBlocksTag rows: Y <epoch:8 BE> <kind:1> ...
	ImportedBlocksTag = 'Y'
	// DRepHistoryTag keys the DRep expiry history, the dormancy counter
	// history, the dormant expiry epoch events and the DRep delegation
	// clears, each by the slot it was recorded at:
	// T <slot:8 BE> <kind:1> ...
	DRepHistoryTag = 'T'
	// GovernanceBoundaryTag keys the governance state of each Conway epoch
	// boundary: # <epoch:8 BE>, valued with the boundary slot (8 bytes BE)
	// and the encoded snapshot. Each row is journaled at its boundary slot.
	GovernanceBoundaryTag = '#'
	// InactivityTag keys the one-time CIP-0163 delegator inactivity
	// activation: 0 <kind:1> ..., kind 0 the epoch it ran at and kind 1 a
	// credential it stamped.
	InactivityTag = '0'
	// RatifyPendingTag keys the record of a boundary whose ratification
	// decision is not written yet, the only row under it.
	RatifyPendingTag = '1'
	// RewardLikelihoodTag keys the non-myopic pool-ranking state a reward
	// update installs: 2 <kind:1> <epoch:8 BE> [<pool:28>].
	RewardLikelihoodTag = '2'
)

// MarkerPrefix starts every marker key. No row family tag may equal it.
const MarkerPrefix = 'm'

// DormancyKey holds the DRep dormancy counter. It is the one key outside
// the marker namespace, under DormancyTag.
var DormancyKey = []byte("zdormant")

// Marker keys. Each is set once the rows it names are complete, or holds a
// store-wide counter; a store without it reloads the rows.
var (
	tipKey   = []byte("mtip")
	countKey = []byte("mcount")
	// seqKey holds the next journal sequence number.
	seqKey = []byte("mseq")
	// formatKey holds formatVersion once the store's contents, journal and
	// sums are all in the current format.
	formatKey = []byte("mfmt")
	// bootstrapKey is written in the transaction that clears the markers a
	// bootstrap replaces and removed in the one that writes the new count,
	// format and tip. While it is present the contents are a partial
	// bootstrap and every read and write but Stale and BootstrapIncomplete
	// fails with ErrBootstrapIncomplete.
	bootstrapKey = []byte("mbootstrap")
	// noncesKey is set once the store holds every block nonce up to its tip.
	// legacyNoncesKey marked nonces written when a rewrite could clear a
	// checkpoint flag, which are reloaded rather than read.
	noncesKey       = []byte("mnonce2")
	legacyNoncesKey = []byte("mnonce")
	// accountsKey is set once the store holds every account up to its tip.
	// legacyAccountsKey marked accounts journaled under the earlier
	// account-only journal key, which are reloaded rather than read.
	accountsKey       = []byte("maccounts2")
	legacyAccountsKey = []byte("maccounts")
	// accountInactivityActivationKey is set once legacy account rows and
	// their retained journal states carry CIP-0163 activation provenance.
	accountInactivityActivationKey = []byte("maccountact")
	// epochStateKey is set once the store holds every epoch and pool stake
	// snapshot row up to its tip. legacyEpochStateKey marked rows without an
	// era field, which are reloaded rather than read. eraSwitchKey holds the
	// last mid-epoch era switch.
	epochStateKey       = []byte("mepoch2")
	legacyEpochStateKey = []byte("mepoch")
	eraSwitchKey        = []byte("mera")
	// snapshotSlotsKey is set once every pool stake snapshot row carries its
	// capture slot and has its SnapshotSlotTag entry. Rows written before
	// the index existed have neither; a store without the marker reads the
	// rows by their values and indexes them at the next epoch state load.
	snapshotSlotsKey = []byte("msnapslot")
	// pparamsKey is set once the store holds every protocol parameter row
	// up to its tip. pparamsSeqKey holds the next protocol parameter
	// sequence number.
	pparamsKey    = []byte("mpparams")
	pparamsSeqKey = []byte("mpseq")
	// syncMarkersKey is set once the store holds the protocol parameter sync
	// markers up to its tip. syntheticV2Key and syntheticV2ClearedKey hold
	// the synthetic PlutusV2 cost model marker and the epoch real data was
	// first confirmed at.
	syncMarkersKey        = []byte("mpmarkok")
	syntheticV2Key        = []byte("mpmarkv")
	syntheticV2ClearedKey = []byte("mpmarke")
	// inactivityKey is set once the store holds the delegator inactivity
	// activation, and ratifyPendingKey once it holds the pending
	// ratification record, up to its tip.
	inactivityKey    = []byte("minactivity")
	ratifyPendingKey = []byte("mratifypend")
	// networkStateKey is set once the store holds every network state and
	// donation row up to its tip.
	networkStateKey = []byte("mnet")
	summaryKey      = []byte("mesummary")
	committeeKey    = []byte("mcommittee")
	// governanceKey is set once the store holds every proposal and vote row
	// with its metadata store row IDs. legacyGovernanceKey marked rows
	// without them, which are reloaded rather than read.
	governanceKey       = []byte("mgov2")
	legacyGovernanceKey = []byte("mgov")
	// governanceSeqKey holds the next proposal and vote row IDs, which only
	// grow: a rollback removes rows but never lowers them.
	governanceSeqKey = []byte("mgvseq")
	// rewardSeqKey holds the next reward record sequence number.
	rewardSeqKey = []byte("mrwseq")
	// rewardRecordsKey is set once the store holds the reward records of the
	// metadata store's journal up to its tip, or marks them lost when the
	// journal was drained before they could be read.
	rewardRecordsKey = []byte("mrwrecs")
	// rewardConsumerKey is present while a reader that copies the reward
	// records needs them; rewardConsumedKey holds the sequence number below
	// which it has copied every record.
	rewardConsumerKey = []byte("mrwconsumer")
	rewardConsumedKey = []byte("mrwconsumed")
	// rewardConsumedSlotKey holds the slot through which the consumer has
	// copied the reward rows.
	rewardConsumedSlotKey = []byte("mrwthrough")
	// rewardSeqIndexPrefix keys the reward sequence index, a derived row
	// family under the marker namespace (see reward_seq_index.go);
	// rewardIndexedKey is set once every record has a number and an entry.
	rewardSeqIndexPrefix = []byte("mrwq")
	rewardIndexedKey     = []byte("mrwindexed")
	drepsKey             = []byte("mdreps")
	poolsKey             = []byte("mpools")
	// poolHistoryKey is set once the store holds every pool certificate event
	// with its effective slot. legacyPoolHistoryKey marked events without it,
	// which are reloaded rather than read.
	poolHistoryKey       = []byte("mpoolhist2")
	legacyPoolHistoryKey = []byte("mpoolhist")
	opCertsKey           = []byte("mopcerts2")
	legacyOpCertsKey     = []byte("mopcerts")
	pparamUpdatesKey     = []byte("mppupd")
	// registrationHistoryKey is set once the store holds every stake
	// registration event with its effective slot. legacyRegistrationHistoryKey
	// marked events without it, which are reloaded rather than read.
	registrationHistoryKey       = []byte("mreghist2")
	legacyRegistrationHistoryKey = []byte("mreghist")
	genesisMIRKey                = []byte("mgenmir")
	rewardOutputsKey             = []byte("mrewardout")
	// importBaselinesKey is set once the store holds every import baseline
	// up to its tip, and witnessSlotsKey once it holds every witness slot.
	importBaselinesKey = []byte("mbaseline")
	// importBaselineDelegationsKey is set once the import baselines carry the
	// delegations the import recorded, or marked lost when they cannot.
	importBaselineDelegationsKey = []byte("mbasedelegs")
	// witnessSlotsKey is set once the witness rows carry the effective slot
	// of a closure's witness. legacyWitnessSlotsKey marked rows without it,
	// which are reloaded rather than read.
	witnessSlotsKey       = []byte("mwitness2")
	legacyWitnessSlotsKey = []byte("mwitness")
	// failedKey holds the lowest slot at which the ledger could not apply a
	// commit to the store, until startup has rewound and resumed it.
	failedKey = []byte("mfailed")
	// rollbackPendingKey holds the point a rollback in more than one
	// transaction is restoring, from before its first transaction that
	// leaves the set between two states it held until its last, which
	// clears it. A store that starts with it finishes the rollback.
	rollbackPendingKey = []byte("mrbpend")
	// journalFloorKey holds the lowest slot, as a big-endian uint64, whose
	// successors' changes the state journal still holds: a read as of slot
	// s is exact only when s+1 is at least the floor. Pruning and every
	// row load raise it before they drop journal entries.
	journalFloorKey = []byte("mjfloor")
	// importKey marks a ledger-state import in progress. Its value is the
	// snapshot point followed by one byte, set once the snapshot's outputs
	// are all in the set. It is written before the import changes a row and
	// removed by EndImport, so a set that holds it is never a complete ledger
	// state.
	importKey = []byte("mimport")
	// loadFloorKey holds the tip slot plus one at the last load of a row
	// family a rollback undoes through the state journal, from rows with no
	// entries before it. A rollback below it, other than to the tip, would
	// leave the loaded rows the undone blocks wrote.
	loadFloorKey = []byte("mloadfloor")
	// exportFloorKey holds the lowest slot, as a big-endian uint64, from
	// which the state journal names every account and DRep row by the slot
	// its AddedSlot says: an entry sits at the slot of the change that set
	// it. Rows a load wrote carry older slots than their entries, so a store
	// that opens without the key takes the tip plus one.
	exportFloorKey = []byte("mexportfloor")
	// rewardBlocksKey is set once the store holds the block counts and fees
	// of every epoch reward calculation can still read, up to its tip, with
	// the overlay-inclusive counts. legacyRewardBlocksKey marked rows without
	// them, which are reloaded rather than read.
	rewardBlocksKey       = []byte("mblkfee2")
	legacyRewardBlocksKey = []byte("mblkfee")
	// importedBlocksKey is set once the store holds the imported block counts
	// of every epoch a snapshot import carried them for.
	importedBlocksKey = []byte("mimpblocks")
	// drepHistoryKey is set once the store holds the DRep history rows up to
	// its tip.
	drepHistoryLoadedKey = []byte("mdrephist")
	// delegationsKey is set once the store holds every pool delegation
	// certificate up to its tip.
	delegationsKey = []byte("mdelegations")
	// closureUtxoKey is set once the store holds every closure output effect
	// up to its tip.
	closureUtxoKey = []byte("mclosureutxo")
	// rewardInputsKey is set once the store holds the reward inputs of every
	// epoch reward calculation can still read, up to its tip.
	rewardInputsKey = []byte("mrewardin")
	// rollbackSeqKey holds a big-endian counter every rollback increments in
	// the transaction that makes it, so a reader that recorded the counter
	// knows whether any rollback has run since.
	rollbackSeqKey = []byte("mrbseq")
	// rewardDerivedKey is set once the store holds the derived reward outputs
	// and cursors of every epoch reward calculation can still read, up to
	// its tip.
	rewardDerivedKey = []byte("mroutputs")
)
