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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// headerOnlyBlock adapts a block header to the Block interface so we can
// run strict VRF/KES verification at chainsync-header time.
type headerOnlyBlock struct {
	header ledger.BlockHeader
}

func (b headerOnlyBlock) Header() ledger.BlockHeader { return b.header }
func (b headerOnlyBlock) Type() int                  { return 0 }
func (b headerOnlyBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b headerOnlyBlock) Utxorpc() (*utxorpc.Block, error) { return nil, nil }
func (b headerOnlyBlock) Hash() lcommon.Blake2b256         { return b.header.Hash() }
func (b headerOnlyBlock) PrevHash() lcommon.Blake2b256     { return b.header.PrevHash() }
func (b headerOnlyBlock) BlockNumber() uint64              { return b.header.BlockNumber() }
func (b headerOnlyBlock) SlotNumber() uint64               { return b.header.SlotNumber() }
func (b headerOnlyBlock) IssuerVkey() lcommon.IssuerVkey   { return b.header.IssuerVkey() }
func (b headerOnlyBlock) BlockBodySize() uint64            { return b.header.BlockBodySize() }
func (b headerOnlyBlock) Era() lcommon.Era                 { return b.header.Era() }
func (b headerOnlyBlock) Cbor() []byte                     { return b.header.Cbor() }
func (b headerOnlyBlock) BlockBodyHash() lcommon.Blake2b256 {
	return b.header.BlockBodyHash()
}

func (ls *LedgerState) verifyBlockHeaderOnlyCrypto(header ledger.BlockHeader) error {
	return ls.verifyBlockHeaderCrypto(headerOnlyBlock{header: header})
}

// verifyBlockHeader performs cryptographic verification of a block header.
// This includes VRF proof verification and KES signature verification.
// Byron-era blocks are skipped because they use a different consensus
// mechanism (PBFT) and do not have VRF/KES fields.
//
// Parameters:
//   - block: the block whose header to verify
//   - epochNonce: the epoch nonce (eta0) for VRF verification
//   - slotsPerKesPeriod: number of slots per KES period from Shelley genesis
//
// Returns an error if verification fails, nil if the block passes
// verification or is a Byron-era block.
func verifyBlockHeaderHex(
	block ledger.Block,
	epochNonceHex string,
	slotsPerKesPeriod uint64,
) error {
	// Skip Byron-era blocks - they use PBFT consensus, not Praos,
	// and do not have VRF/KES/OpCert fields
	if block.Era().Id == byron.EraIdByron {
		return nil
	}

	// Epoch nonce is required for post-Byron blocks
	if epochNonceHex == "" {
		return fmt.Errorf(
			"epoch nonce not available for block at slot %d",
			block.SlotNumber(),
		)
	}

	// Use gouroboros VerifyBlock for VRF + KES verification.
	// We skip body hash validation, transaction validation, and stake
	// pool validation here because:
	// - Body hash validation requires full block CBOR which may not
	//   always be available at this stage
	// - Transaction validation requires full ledger state and protocol
	//   parameters which are handled elsewhere
	// - Stake pool validation requires pool registration lookups
	//   which are handled elsewhere
	config := lcommon.VerifyConfig{
		SkipBodyHashValidation:    true,
		SkipTransactionValidation: true,
		SkipStakePoolValidation:   true,
	}

	header, err := normalizeHeaderVrfFieldsFromBodyCbor(block.Header())
	if err != nil {
		return fmt.Errorf(
			"block header verification failed at slot %d: "+
				"normalize VRF fields from header body CBOR: %w",
			block.SlotNumber(),
			err,
		)
	}

	isValid, _, _, _, err := ledger.VerifyBlock(
		headerOnlyBlock{header: header},
		epochNonceHex,
		slotsPerKesPeriod,
		config,
	)
	if err != nil {
		return fmt.Errorf(
			"block header verification failed at slot %d: %w",
			block.SlotNumber(),
			err,
		)
	}
	if !isValid {
		return fmt.Errorf(
			"block header verification returned invalid at slot %d",
			block.SlotNumber(),
		)
	}

	return nil
}

// verifyBlockHeaderCrypto extracts the necessary parameters from the
// LedgerState and delegates to verifyBlockHeader for cryptographic
// verification of a block's VRF proof and KES signature.
//
// This is called from the blockfetch path (processBlockEvents) before
// blocks are handed to the ledger processing pipeline. It performs
// epoch-aware parameter lookup: the block's slot is matched against the
// full epoch cache to find the correct epoch nonce, so blocks that
// arrive during or after an epoch transition are always verified against
// the right epoch's parameters.
//
// If no epoch with a valid nonce can be found for the block's slot
// (e.g., the epoch rollover has not yet been processed), the block is
// rejected rather than silently skipping verification. This prevents
// an attacker from forging headers that bypass verification by
// targeting the epoch boundary window.
func (ls *LedgerState) verifyBlockHeaderCrypto(
	block ledger.Block,
) error {
	// Skip Byron-era blocks early to avoid parameter lookups
	if block.Era().Id == byron.EraIdByron {
		return nil
	}

	blockSlot := block.SlotNumber()

	// Look up the epoch for this block's slot from the epoch cache.
	// This is an epoch-aware lookup that searches through all known
	// epochs rather than only the current one, ensuring that blocks
	// at epoch boundaries are verified against the correct nonce.
	epoch, err := ls.epochForSlot(blockSlot)
	if err != nil {
		// Epoch cache doesn't cover this slot yet. Blockfetch can
		// deliver blocks past the epoch boundary before the ledger
		// processing goroutine runs the full epoch rollover. Eagerly
		// compute the next epoch(s) so verification can proceed.
		epoch, err = ls.ensureEpochForSlot(blockSlot)
		if err != nil {
			return fmt.Errorf(
				"block header verification rejected: no epoch data for slot %d: %w",
				blockSlot,
				err,
			)
		}
	}

	// Reject blocks for which we have epoch data but no nonce.
	// A missing nonce means the epoch rollover has not completed
	// or the epoch is too far in the future.
	if len(epoch.Nonce) == 0 {
		return fmt.Errorf(
			"block header verification rejected: "+
				"epoch %d has no nonce for slot %d "+
				"(epoch rollover may not have been processed yet)",
			epoch.EpochId,
			blockSlot,
		)
	}

	slotsPerKesPeriod := ls.SlotsPerKESPeriod()
	if slotsPerKesPeriod == 0 {
		return fmt.Errorf(
			"shelley genesis not available for block header verification at slot %d",
			blockSlot,
		)
	}

	if err := verifyBlockHeaderHex(
		block,
		ls.epochNonceHex(epoch.EpochId, epoch.Nonce),
		slotsPerKesPeriod,
	); err != nil {
		return err
	}

	// Validate the operational certificate's cold-key signature and KES
	// period expiry. This is the stateless half of inbound opcert validation;
	// the counter-monotonicity check lives in the block-apply transaction.
	if err := verifyOpCertHeaderCrypto(
		block.Header(),
		blockSlot,
		slotsPerKesPeriod,
		ls.maxKESEvolutions(),
	); err != nil {
		return fmt.Errorf(
			"block header verification failed at slot %d: %w",
			blockSlot,
			err,
		)
	}

	// Bind the header's VRF key to the pool's on-chain registered VRF key.
	// The crypto path above verifies the VRF proof only against the key carried
	// in the header (SkipStakePoolValidation skips gouroboros' registered-key
	// check), so without this an attacker can grind VRF keys to win slots.
	if err := ls.verifyRegisteredVrfKey(block); err != nil {
		return err
	}

	return ls.verifyBlockLeaderEligibility(block, epoch.EpochId)
}

// verifyBlockLeaderEligibility checks that the block's producer pool was
// eligible to produce a block at this slot under the Praos stake-derived
// leadership threshold. This enforces the Cardano Blueprint chain validity
// requirement that header validation confirms slot-leader eligibility.
//
// The eligibility condition is:
//
//	vrfLeaderOutput < threshold(sigma, f)
//
// where sigma = poolStake / totalStake and f is the active slot coefficient
// from Shelley genesis.
//
// Stake selection follows the ledger's active pool distribution. For the
// epoch imported from a Mithril snapshot, NewEpochState.pool-distr is used
// directly. Otherwise, the active distribution is the mark snapshot from
// epoch-2, clamped to genesis for early epochs.
//
// TPraos (Shelley/Allegra/Mary/Alonzo) and CPraos (Babbage/Conway) differ in
// how the VRF leader value is derived from the output bytes; ConsensusModeForEpoch
// selects the correct path for the block's era.
//
// Byron blocks are skipped (PBFT). A missing total-stake or unavailable active
// slot coefficient is logged and skipped rather than rejecting, to tolerate
// early-chain bootstrap states where the genesis snapshot is not yet written.
func (ls *LedgerState) verifyBlockLeaderEligibility(
	block ledger.Block,
	epochId uint64,
) error {
	if block.Era().Id == byron.EraIdByron {
		return nil
	}

	// Derive pool key hash from the block's issuer verification key.
	issuerVkey := block.IssuerVkey()
	poolKeyHash := lcommon.PoolKeyHash(issuerVkey.Hash())

	poolStake, totalStake, snapshotEpoch, snapshotType, skipEligibility, err := ls.leaderEligibilityStake(block, epochId, poolKeyHash)
	if err != nil {
		return err
	}
	if skipEligibility {
		return nil
	}
	if totalStake == 0 {
		// Genesis snapshot not yet written (very early bootstrap).
		// Skip rather than reject.
		ls.config.Logger.Warn(
			"skipping leader eligibility check: total active stake is zero",
			"slot", block.SlotNumber(),
			"epoch", epochId,
			"snapshot_epoch", snapshotEpoch,
			"snapshot_type", snapshotType,
			"component", "ledger",
		)
		return nil
	}

	// Use the genesis Rat directly to avoid a float64 precision roundtrip.
	// A zero or negative coefficient would compute a zero threshold and
	// reject every non-Byron block; treat it as unavailable.
	activeSlotCoeffRat := ls.activeSlotCoeffRat()
	if activeSlotCoeffRat == nil || activeSlotCoeffRat.Sign() <= 0 {
		ls.config.Logger.Warn(
			"skipping leader eligibility check: active slot coefficient unavailable or non-positive",
			"slot", block.SlotNumber(),
			"component", "ledger",
		)
		return nil
	}

	// Consensus mode determines the VRF leader-value derivation path.
	mode := ls.ConsensusModeForEpoch(epochId)

	// Extract the VRF output from the header body CBOR.
	vrfResult, ok, err := headerVrfResultFromBodyCbor(block.Header())
	if err != nil {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"extract VRF result: %w",
			block.SlotNumber(),
			err,
		)
	}
	if !ok || len(vrfResult.Output) == 0 {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"VRF output unavailable for eligibility check",
			block.SlotNumber(),
		)
	}

	// Compute the Praos leadership threshold and compare.
	threshold := consensus.CertifiedNatThresholdWithMode(
		poolStake,
		totalStake,
		activeSlotCoeffRat,
		mode,
	)
	if !consensus.IsVRFOutputBelowThresholdWithMode(
		vrfResult.Output,
		threshold,
		mode,
	) {
		// dingo's leadership stake is delegated UTxO only; staking rewards are
		// not yet computed, so reward-account balances are missing from the
		// stake distribution. On the prototype network the dominant pool's
		// reward accrual pushes its true relative stake above the UTxO-only
		// figure, so this UTxO-only threshold spuriously rejects its eligible
		// blocks. Trust the block there (all cryptographic header checks above
		// still passed) rather than wedge the chain; enforce elsewhere. See
		// LedgerStateConfig.SkipLeaderStakeThresholdCheck.
		if ls.config.SkipLeaderStakeThresholdCheck {
			ls.config.Logger.Warn(
				"leader eligibility below stake-derived threshold; trusting block (leadership stake omits reward balances)",
				"slot", block.SlotNumber(),
				"pool", hex.EncodeToString(poolKeyHash[:]),
				"pool_stake", poolStake,
				"total_stake", totalStake,
				"epoch", epochId,
				"snapshot_epoch", snapshotEpoch,
				"snapshot_type", snapshotType,
				"component", "ledger",
			)
			return nil
		}
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"producer pool %x VRF leader value exceeds stake-derived threshold "+
				"(pool stake: %d, total stake: %d, epoch: %d, snapshot_epoch: %d, snapshot_type: %s)",
			block.SlotNumber(),
			poolKeyHash[:],
			poolStake,
			totalStake,
			epochId,
			snapshotEpoch,
			snapshotType,
		)
	}

	return nil
}

func (ls *LedgerState) leaderEligibilityStake(
	block ledger.Block,
	epochId uint64,
	poolKeyHash lcommon.PoolKeyHash,
) (uint64, uint64, uint64, string, bool, error) {
	useImportedActive, err := ls.shouldUseImportedActivePoolDistribution(
		block,
		epochId,
	)
	if err != nil {
		return 0, 0, epochId, models.PoolStakeSnapshotTypeActive, false, err
	}
	if useImportedActive {
		snapshot, err := ls.db.Metadata().GetPoolStakeSnapshot(
			epochId,
			models.PoolStakeSnapshotTypeActive,
			poolKeyHash[:],
			nil,
		)
		if err != nil {
			return 0, 0, epochId, models.PoolStakeSnapshotTypeActive, false,
				fmt.Errorf(
					"block header verification rejected at slot %d: "+
						"lookup active pool distribution: %w",
					block.SlotNumber(),
					err,
				)
		}
		if snapshot == nil ||
			snapshot.TotalStake == 0 ||
			snapshot.StakeDenominator == 0 {
			return 0, 0, epochId, models.PoolStakeSnapshotTypeActive, false,
				fmt.Errorf(
					"block header verification rejected at slot %d: "+
						"producer pool %x missing from active pool distribution for epoch %d",
					block.SlotNumber(),
					poolKeyHash[:],
					epochId,
				)
		}
		return uint64(snapshot.TotalStake),
			uint64(snapshot.StakeDenominator),
			epochId,
			models.PoolStakeSnapshotTypeActive,
			false,
			nil
	}

	snapshotEpoch := praos.StakeSnapshotEpoch(epochId)
	snapshotType := models.PoolStakeSnapshotTypeMark
	snapshot, err := ls.db.Metadata().GetPoolStakeSnapshot(
		snapshotEpoch,
		snapshotType,
		poolKeyHash[:],
		nil,
	)
	if err != nil {
		return 0, 0, snapshotEpoch, snapshotType, false,
			fmt.Errorf(
				"block header verification rejected at slot %d: "+
					"lookup pool stake: %w",
				block.SlotNumber(),
				err,
			)
	}
	if snapshot == nil || snapshot.TotalStake == 0 {
		// Mirror cardano-ledger: a pool absent from the leader stake
		// distribution is a hard rejection (the reference node's
		// VRFKeyUnknown). The reference distribution (nesPd) is always
		// populated, so an empty epoch snapshot here signals a dingo-side
		// storage or computation gap (corrupt DB, incomplete Mithril import,
		// pruned history) rather than genuine pool ineligibility. Surface that
		// distinction to operators without changing the reject decision.
		diag := "pool is absent from the epoch distribution"
		if total, terr := ls.db.Metadata().GetTotalActiveStake(
			snapshotEpoch,
			snapshotType,
			nil,
		); terr == nil && total == 0 {
			diag = "epoch mark snapshot is empty (no pools) - likely a " +
				"storage or computation gap, not pool ineligibility"
		}
		return 0, 0, snapshotEpoch, snapshotType, false,
			fmt.Errorf(
				"block header verification rejected at slot %d: "+
					"producer pool %x has no stake in epoch %d snapshot (%s)",
				block.SlotNumber(),
				poolKeyHash[:],
				snapshotEpoch,
				diag,
			)
	}
	if ls.isMithrilImportedMarkSnapshot(snapshot, snapshotEpoch) {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"skipping leader eligibility check: mark snapshot captured after target epoch start",
				"slot", block.SlotNumber(),
				"epoch", epochId,
				"snapshot_epoch", snapshotEpoch,
				"snapshot_type", snapshotType,
				"captured_slot", snapshot.CapturedSlot,
				"component", "ledger",
			)
		}
		return uint64(snapshot.TotalStake), 0, snapshotEpoch, snapshotType,
			true, nil
	}
	totalStake, err := ls.db.Metadata().GetTotalActiveStake(
		snapshotEpoch,
		snapshotType,
		nil,
	)
	if err != nil {
		return 0, 0, snapshotEpoch, snapshotType, false,
			fmt.Errorf(
				"block header verification rejected at slot %d: "+
					"lookup total active stake: %w",
				block.SlotNumber(),
				err,
			)
	}
	return uint64(snapshot.TotalStake), totalStake, snapshotEpoch, snapshotType,
		false, nil
}

func (ls *LedgerState) isMithrilImportedMarkSnapshot(
	snapshot *models.PoolStakeSnapshot,
	snapshotEpoch uint64,
) bool {
	if snapshot == nil ||
		snapshot.SnapshotType != models.PoolStakeSnapshotTypeMark ||
		snapshot.CapturedSlot == 0 {
		return false
	}

	ls.RLock()
	defer ls.RUnlock()

	if ls.mithrilLedgerSlot == 0 {
		return false
	}
	for _, ep := range ls.epochCache {
		if ep.EpochId != snapshotEpoch || ep.LengthInSlots == 0 {
			continue
		}
		return snapshot.CapturedSlot >= ep.StartSlot
	}
	return false
}

func (ls *LedgerState) shouldUseImportedActivePoolDistribution(
	block ledger.Block,
	epochId uint64,
) (bool, error) {
	if ls.mithrilLedgerSlot == 0 || block.SlotNumber() <= ls.mithrilLedgerSlot {
		return false, nil
	}
	mithrilEpoch, err := ls.epochForSlot(ls.mithrilLedgerSlot)
	if err != nil {
		return false, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"resolve Mithril trust boundary epoch: %w",
			block.SlotNumber(),
			err,
		)
	}
	return epochId == mithrilEpoch.EpochId, nil
}

// verifyRegisteredVrfKey rejects a block whose VRF verification key (carried in
// the header body) is not the VRF key the producing pool registered on-chain.
// The block's VRF proof is validated only against this embedded key, and the
// leader-eligibility threshold uses its output, so binding it to the pool's
// registered VRF key is what prevents an attacker from grinding VRF keys to
// win slots. Mirrors gouroboros VerifyBlock's stake-pool VRF-key check, which
// dingo's crypto path skips via SkipStakePoolValidation.
func (ls *LedgerState) verifyRegisteredVrfKey(
	block ledger.Block,
) error {
	// Byron (PBFT) blocks have no pool-registered VRF key.
	if block.Era().Id == byron.EraIdByron {
		return nil
	}
	issuerVkey := block.IssuerVkey()
	poolKeyHash := lcommon.PoolKeyHash(issuerVkey.Hash())
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(block.Header())
	if err != nil {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"extract VRF key: %w",
			block.SlotNumber(),
			err,
		)
	}
	if !ok || len(vrfKey) == 0 {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"VRF key unavailable for registration check",
			block.SlotNumber(),
		)
	}
	pool, err := ls.db.GetPool(poolKeyHash, true, nil)
	if err != nil {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"producer pool %x registration lookup failed: %w",
			block.SlotNumber(),
			poolKeyHash[:],
			err,
		)
	}
	registeredVrfKeyHash, ok := registeredPoolVrfKeyHash(pool)
	if !ok {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"producer pool %x registered VRF key hash unavailable",
			block.SlotNumber(),
			poolKeyHash[:],
		)
	}
	headerVrfKeyHash := lcommon.Blake2b256Hash(vrfKey)
	if !bytes.Equal(registeredVrfKeyHash.Bytes(), headerVrfKeyHash.Bytes()) {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"producer pool %x VRF key does not match registered VRF key "+
				"(header %x, registered %x)",
			block.SlotNumber(),
			poolKeyHash[:],
			headerVrfKeyHash.Bytes(),
			registeredVrfKeyHash.Bytes(),
		)
	}
	return nil
}

func registeredPoolVrfKeyHash(
	pool *models.Pool,
) (lcommon.Blake2b256, bool) {
	var vrfHash lcommon.Blake2b256
	if pool == nil {
		return vrfHash, false
	}
	if len(pool.Registration) == 0 {
		return vrfHash, false
	}
	if len(pool.Registration[0].VrfKeyHash) == len(vrfHash) {
		copy(vrfHash[:], pool.Registration[0].VrfKeyHash)
		return vrfHash, true
	}
	if len(pool.VrfKeyHash) != len(vrfHash) {
		return vrfHash, false
	}
	copy(vrfHash[:], pool.VrfKeyHash)
	return vrfHash, true
}

// maxKESEvolutions returns the maximum number of KES evolutions allowed before
// an operational certificate expires, from Shelley genesis. Returns 0 when the
// genesis is unavailable, in which case opcert KES-period expiry is left to the
// lighter future-cert guard inside VerifyBlock.
func (ls *LedgerState) maxKESEvolutions() uint64 {
	if ls.config.CardanoNodeConfig == nil {
		return 0
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil || shelleyGenesis.MaxKESEvolutions <= 0 {
		return 0
	}
	return uint64(shelleyGenesis.MaxKESEvolutions) // #nosec G115 -- guarded > 0
}

func (ls *LedgerState) epochNonceHex(epochId uint64, nonce []byte) string {
	nonceHex := hex.EncodeToString(nonce)
	ls.RLock()
	cachedNonce, ok := ls.epochNonceHexCache[epochId]
	ls.RUnlock()
	if ok && cachedNonce == nonceHex {
		return cachedNonce
	}
	ls.Lock()
	defer ls.Unlock()
	if ls.epochNonceHexCache == nil {
		ls.epochNonceHexCache = make(map[uint64]string)
	}
	ls.epochNonceHexCache[epochId] = nonceHex
	return nonceHex
}

// epochForSlot searches the epoch cache for the epoch containing the
// given slot. It takes a snapshot of the epoch cache under RLock to
// avoid racing with concurrent epoch rollover updates.
//
// Returns the matching epoch or an error if no epoch covers the slot.
func (ls *LedgerState) epochForSlot(slot uint64) (models.Epoch, error) {
	ls.RLock()
	defer ls.RUnlock()

	if len(ls.epochCache) == 0 {
		return models.Epoch{}, errors.New("epoch cache is empty")
	}

	// Search newest-to-oldest so that if cache entries overlap
	// (e.g., after rollback/rebuild), we use the most recent epoch data.
	for _, ep := range slices.Backward(ls.epochCache) {
		if ep.LengthInSlots == 0 {
			continue
		}
		epochEnd := ep.StartSlot + uint64(ep.LengthInSlots)
		if slot >= ep.StartSlot && slot < epochEnd {
			return ep, nil
		}
	}

	// Find the last epoch with a valid (non-zero) length for a
	// meaningful error message.
	var lastValidEnd uint64
	var hasValidEpoch bool
	for _, v := range slices.Backward(ls.epochCache) {
		if v.LengthInSlots > 0 {
			lastValidEnd = v.StartSlot +
				uint64(v.LengthInSlots)
			hasValidEpoch = true
			break
		}
	}
	if !hasValidEpoch {
		return models.Epoch{}, fmt.Errorf(
			"slot %d not covered by any known epoch (cache has %d epochs, all with zero length)",
			slot,
			len(ls.epochCache),
		)
	}
	return models.Epoch{}, fmt.Errorf(
		"slot %d not covered by any known epoch (cache has %d epochs, last ends at slot %d)",
		slot,
		len(ls.epochCache),
		lastValidEnd,
	)
}

// ensureEpochForSlot advances the epoch cache until it covers the target
// slot, then returns the epoch. This handles the case where blockfetch
// delivers blocks past an epoch boundary before the ledger processing
// goroutine has run the full epoch rollover. The epoch nonce is computed
// from chain data (the last block before the boundary), which is available
// because blockfetch delivers blocks in order.
func (ls *LedgerState) ensureEpochForSlot(
	targetSlot uint64,
) (models.Epoch, error) {
	const maxAdvance = 5 // Safety limit against runaway loops
	for range maxAdvance {
		if err := ls.advanceEpochCache(); err != nil {
			return models.Epoch{}, fmt.Errorf(
				"advance epoch cache: %w",
				err,
			)
		}
		epoch, err := ls.epochForSlot(targetSlot)
		if err == nil {
			return epoch, nil
		}
	}
	return models.Epoch{}, fmt.Errorf(
		"could not advance epoch cache to cover slot %d after %d advances",
		targetSlot,
		maxAdvance,
	)
}

// advanceEpochCache computes the next epoch's parameters and nonce from
// chain data and appends it to the in-memory epoch cache. This is a
// lightweight alternative to the full processEpochRollover — it only
// populates the nonce and epoch boundaries needed for header verification,
// without running pparam updates, snapshot rotation, or DB writes.
// The full rollover will run later in ledgerProcessBlocks and replace
// the cache with the authoritative DB-backed version.
func (ls *LedgerState) advanceEpochCache() error {
	// Read last epoch under read lock
	ls.RLock()
	if len(ls.epochCache) == 0 {
		ls.RUnlock()
		return errors.New("epoch cache is empty")
	}
	lastEpoch := ls.epochCache[len(ls.epochCache)-1]
	ls.RUnlock()

	if lastEpoch.LengthInSlots == 0 {
		return errors.New("last epoch has zero length")
	}

	newStartSlot := lastEpoch.StartSlot + uint64(lastEpoch.LengthInSlots)

	// Compute epoch nonce (requires DB access, done outside lock)
	nonce, evolvingNonce, candidateNonce, labNonce, err := ls.computeEpochNonceForSlot(newStartSlot, lastEpoch)
	if err != nil {
		return fmt.Errorf(
			"compute epoch nonce for epoch %d: %w",
			lastEpoch.EpochId+1,
			err,
		)
	}

	newEpoch := models.Epoch{
		EpochId:             lastEpoch.EpochId + 1,
		StartSlot:           newStartSlot,
		LengthInSlots:       lastEpoch.LengthInSlots,
		SlotLength:          lastEpoch.SlotLength,
		EraId:               lastEpoch.EraId,
		Nonce:               nonce,
		EvolvingNonce:       evolvingNonce,
		CandidateNonce:      candidateNonce,
		LastEpochBlockNonce: labNonce,
	}

	// Update cache under write lock, checking for concurrent advance
	// or rollback that may have changed the cache since we read it.
	ls.Lock()
	if len(ls.epochCache) == 0 {
		ls.Unlock()
		return nil
	}
	lastCached := ls.epochCache[len(ls.epochCache)-1]
	if lastCached.EpochId >= newEpoch.EpochId {
		// Another goroutine or ledger processing already advanced
		ls.Unlock()
		return nil
	}
	// Verify the base epoch we used for computation is still the cache
	// tail. A concurrent rollback could have pruned the cache, making
	// our computed newEpoch stale (e.g. after a hard fork or rollback).
	if lastCached.EpochId != lastEpoch.EpochId ||
		lastCached.StartSlot != lastEpoch.StartSlot ||
		lastCached.LengthInSlots != lastEpoch.LengthInSlots ||
		!bytes.Equal(lastCached.Nonce, lastEpoch.Nonce) {
		ls.Unlock()
		return nil
	}
	ls.epochCache = append(ls.epochCache, newEpoch)
	ls.Unlock()

	ls.config.Logger.Debug(
		"eagerly advanced epoch cache for header verification",
		"new_epoch", newEpoch.EpochId,
		"start_slot", newEpoch.StartSlot,
		"nonce", hex.EncodeToString(newEpoch.Nonce),
		"prev_epoch_id", lastEpoch.EpochId,
		"prev_epoch_nonce", hex.EncodeToString(lastEpoch.Nonce),
		"component", "ledger",
	)

	return nil
}

// computeEpochNonceForSlot computes the epoch nonce, evolving nonce,
// and lastEpochBlockNonce for a new epoch starting at epochStartSlot. This
// mirrors calculateEpochNonce but uses non-transactional DB lookups
// since we're not inside the ledger processing pipeline.
//
// Returns (epochNonce, evolvingNonce, candidateNonce, labNonce, error).
func (ls *LedgerState) computeEpochNonceForSlot(
	epochStartSlot uint64,
	prevEpoch models.Epoch,
) ([]byte, []byte, []byte, []byte, error) {
	if ls.config.CardanoNodeConfig == nil {
		return nil, nil, nil, nil, errors.New("CardanoNodeConfig is nil")
	}
	genesisHashHex := ls.config.CardanoNodeConfig.ShelleyGenesisHash
	if genesisHashHex == "" {
		return nil, nil, nil, nil, errors.New(
			"shelley genesis hash not available",
		)
	}
	genesisHash, err := hex.DecodeString(genesisHashHex)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"decode genesis hash: %w", err,
		)
	}

	// For the initial epoch (no nonce yet), the epoch/evolving/candidate nonces
	// are all the genesis nonce, and the carried lastEpochBlockNonce is Neutral
	// (nil): cardano-ledger initializes praosStateLastEpochBlockNonce to
	// NeutralNonce at genesis, so the first from-genesis boundary uses the
	// identity (eta = candidate ⭒ NeutralNonce = candidate). Do NOT seed this
	// with the genesis nonce (#2734). Mirrors calculateEpochNonce; the Mithril
	// bootstrap path imports a non-nil lastEpochBlockNonce and never takes this
	// branch.
	if len(prevEpoch.Nonce) == 0 {
		return genesisHash, genesisHash, genesisHash, nil, nil
	}

	prevEvolvingNonce := prevEpoch.EvolvingNonce
	if len(prevEvolvingNonce) == 0 {
		prevEvolvingNonce = genesisHash
	}

	// The candidate nonce carries across epochs independently of the
	// evolving nonce. Fall back to genesis hash when not stored (e.g.,
	// epochs created before this field existed).
	prevCandidateNonce := prevEpoch.CandidateNonce
	if len(prevCandidateNonce) == 0 {
		prevCandidateNonce = genesisHash
	}

	computeStartSlot := prevEpoch.StartSlot
	computeEpochLength := uint64(prevEpoch.LengthInSlots)
	prevEpochEndSlot := prevEpoch.StartSlot +
		uint64(prevEpoch.LengthInSlots)
	// When resuming from a snapshot, prevEpoch can carry nonce state
	// already advanced to the imported tip slot. Continue from the next
	// slot in that case instead of replaying from epoch start.
	ls.RLock()
	currentTipSlot := ls.currentTip.Point.Slot
	currentTipBlockNonce := append(
		[]byte(nil),
		ls.currentTipBlockNonce...,
	)
	ls.RUnlock()
	if currentTipSlot >= prevEpoch.StartSlot &&
		currentTipSlot < prevEpochEndSlot &&
		len(prevEpoch.CandidateNonce) == 32 &&
		len(prevEpoch.EvolvingNonce) == 32 &&
		len(currentTipBlockNonce) == 32 &&
		bytes.Equal(prevEpoch.EvolvingNonce, currentTipBlockNonce) {
		if nextSlot := currentTipSlot + 1; nextSlot < prevEpochEndSlot {
			computeStartSlot = nextSlot
			computeEpochLength = prevEpochEndSlot - nextSlot
		} else {
			// Tip already at/after epoch end: no additional blocks to fold.
			computeEpochLength = 0
		}
	} else if len(prevEpoch.EvolvingNonce) == 32 {
		// Resume fallback: when epoch nonce state was checkpointed at an
		// earlier slot (snapshot import), detect that anchor by matching
		// block nonces in this epoch range. If found, continue from the
		// following slot instead of replaying from epoch start.
		// If no anchor is found, fall through to defaults (compute from
		// epoch start) — handles genesis sync gracefully.
		nonceRows, nonceErr := ls.db.GetBlockNoncesInSlotRange(
			prevEpoch.StartSlot,
			prevEpochEndSlot,
			nil,
		)
		if nonceErr != nil {
			return nil, nil, nil, nil, fmt.Errorf(
				"fetch block nonces in epoch range: %w",
				nonceErr,
			)
		}
		for _, row := range nonceRows {
			if len(row.Nonce) == 32 &&
				bytes.Equal(prevEpoch.EvolvingNonce, row.Nonce) {
				if row.Slot+1 < prevEpochEndSlot {
					computeStartSlot = row.Slot + 1
					computeEpochLength = prevEpochEndSlot -
						computeStartSlot
				} else {
					computeEpochLength = 0
				}
				break
			}
		}
	}

	// Use prevEpoch.EraId so the candidate-freeze cutoff applies the
	// correct stability window for the source epoch's protocol family
	// (3k/f for TPraos, 4k/f for Praos). See #2125.
	candidateNonce, evolvingNonce, err := ls.computeCandidateNonce(
		nil, // non-transactional
		prevEpoch.EraId,
		prevEvolvingNonce,
		prevCandidateNonce,
		computeStartSlot,
		computeEpochLength,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"compute candidate nonce: %w", err,
		)
	}

	// The epoch nonce mixes the frozen candidate with the CARRIED
	// last-block-of-previous-epoch nonce (cardano-ledger
	// praosStateLastEpochBlockNonce), i.e. prevEpoch.LastEpochBlockNonce — NOT
	// the last block of the epoch being closed. This must match the rollover
	// path (calculateEpochNonce); see #2734.
	//   epochNonce(N+1) = candidateNonce(N) ⭒ prevEpoch(N).LastEpochBlockNonce
	labForEta := cloneNonce(prevEpoch.LastEpochBlockNonce)

	// The carried lab for the NEXT boundary is stored on the new epoch record:
	// prevHashToNonce(lastBlock.prevHash) = the PARENT hash of the last block of
	// the epoch being closed (a one-block Praos lag), NOT the last block's own
	// hash. See epochLabNonce and #2734 (eta_1349 wedge).
	labNonceToSave, err := ls.epochLabNonce(
		nil,
		prevEpoch.StartSlot,
		prevEpochEndSlot,
		prevEpoch.LastEpochBlockNonce,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if len(labForEta) == 0 {
		// NeutralNonce is the identity element of ⭒:
		//   candidateNonce ⭒ NeutralNonce = candidateNonce
		ls.config.Logger.Debug(
			"computed epoch nonce for cache advance "+
				"(NeutralNonce, using candidateNonce)",
			"new_epoch_start_slot", epochStartSlot,
			"prev_epoch_id", prevEpoch.EpochId,
			"candidate_nonce",
			hex.EncodeToString(candidateNonce),
			"epoch_nonce",
			hex.EncodeToString(candidateNonce),
			"component", "ledger",
		)
		return candidateNonce, evolvingNonce, candidateNonce, labNonceToSave, nil
	}

	result, err := lcommon.CalculateEpochNonce(
		candidateNonce,
		labForEta,
		nil,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"calculate epoch nonce: %w", err,
		)
	}

	ls.config.Logger.Debug(
		"computed epoch nonce for cache advance",
		"new_epoch_start_slot", epochStartSlot,
		"prev_epoch_id", prevEpoch.EpochId,
		"lab_for_eta",
		hex.EncodeToString(labForEta),
		"lab_nonce_to_save",
		hex.EncodeToString(labNonceToSave),
		"candidate_nonce", hex.EncodeToString(candidateNonce),
		"evolving_nonce", hex.EncodeToString(evolvingNonce),
		"epoch_nonce", hex.EncodeToString(result.Bytes()),
		"component", "ledger",
	)

	return result.Bytes(), evolvingNonce, candidateNonce, labNonceToSave, nil
}
