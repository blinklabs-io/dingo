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
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type genesisDelegation struct {
	genesisHash  []byte
	delegateHash []byte
	vrfHash      []byte
}

type genesisOverlaySlotStatus uint8

const (
	genesisOverlayNone genesisOverlaySlotStatus = iota
	genesisOverlayNonActive
	genesisOverlayActive
)

// headerOnlyBlock adapts a block header to the Block interface so we can
// run strict VRF/KES verification at chainsync-header time.
type headerOnlyBlock struct {
	header ledger.BlockHeader
}

var (
	errHeaderVerificationDeferred = errors.New(
		"header verification deferred",
	)
	errEpochCacheForecastBoundary = errors.New(
		"epoch cache forecast crosses era boundary",
	)
	// errLeaderStakeSnapshotUnavailable marks a leader-stake snapshot that
	// dingo cannot answer from: the epoch's mark/active distribution is
	// missing, empty, or has no usable denominator. This is NEVER an
	// authoritative statement that the producer pool is ineligible -- the
	// reference node's nesPd is always populated, so an empty or absent
	// snapshot is a dingo-side data gap (pruned below the retention window,
	// unwritten during catch-up, or an incomplete import), not
	// cardano-ledger's VRFKeyUnknown. Header verification classifies it as
	// deferrable so the missing state is resolved rather than misreported as
	// pool absence (issue #3727). A pool absent from a *populated* snapshot is
	// a separate, authoritative rejection that never carries this sentinel.
	errLeaderStakeSnapshotUnavailable = errors.New(
		"leader stake snapshot unavailable",
	)
	// errEpochNonceUnavailable marks an epoch-cache entry that covers the
	// requested slot but has no published Praos nonce. Byron epochs always
	// have this shape; a post-Byron entry can also have it transiently while
	// nonce state catches up. It is distinct from a slot that is outside the
	// published cache entirely.
	errEpochNonceUnavailable = errors.New(
		"epoch has no nonce for slot",
	)
	// errBlockPipelineEta0Unavailable marks an epoch-cache entry with no
	// Praos nonce. This is expected for Byron and can be transient for later
	// eras; slots outside the published cache use
	// errHeaderVerificationDeferred instead.
	errBlockPipelineEta0Unavailable = errors.New(
		"block-processing pipeline: epoch nonce unavailable",
	)
)

// IsHeaderVerificationDeferred reports whether header-only verification could
// not proceed because required ledger state, epoch data, or stake snapshot
// data is not available yet.
func IsHeaderVerificationDeferred(err error) bool {
	return errors.Is(err, errHeaderVerificationDeferred)
}

func (b headerOnlyBlock) Header() ledger.BlockHeader { return b.header }
func (b headerOnlyBlock) Type() int                  { return 0 }
func (b headerOnlyBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b headerOnlyBlock) Utxorpc() (*utxorpc.Block, error) { return nil, nil }

func (b headerOnlyBlock) Hash() lcommon.Blake2b256 { return b.header.Hash() }

func (b headerOnlyBlock) PrevHash() lcommon.Blake2b256 { return b.header.PrevHash() }

func (b headerOnlyBlock) BlockNumber() uint64 { return b.header.BlockNumber() }

func (b headerOnlyBlock) SlotNumber() uint64 { return b.header.SlotNumber() }

func (b headerOnlyBlock) IssuerVkey() lcommon.IssuerVkey { return b.header.IssuerVkey() }

func (b headerOnlyBlock) BlockBodySize() uint64 { return b.header.BlockBodySize() }

func (b headerOnlyBlock) Era() lcommon.Era { return b.header.Era() }

func (b headerOnlyBlock) Cbor() []byte { return b.header.Cbor() }
func (b headerOnlyBlock) BlockBodyHash() lcommon.Blake2b256 {
	return b.header.BlockBodyHash()
}

func (ls *LedgerState) verifyBlockHeaderOnlyCrypto(
	header ledger.BlockHeader,
) error {
	_, err := ls.verifyBlockHeaderStatelessCrypto(
		headerOnlyBlock{header: header},
		false,
	)
	return err
}

// ValidateBlockHeaderCrypto validates a header using the current ledger
// state.  It is used by protocol handlers that receive a header without its
// block body (for example LeiosNotify announcements) and must not let an
// unauthenticated header influence shared state.
func (ls *LedgerState) ValidateBlockHeaderCrypto(
	header ledger.BlockHeader,
) error {
	if header == nil {
		return errors.New("nil block header")
	}
	return ls.verifyBlockHeaderCryptoWithEpochAdvance(
		headerOnlyBlock{header: header},
		false,
		false,
	)
}

// ShouldVerifyChainSelectionHeaderCrypto reports whether a header at the
// given slot is eligible to have its cryptography verified right now via
// ValidateChainSelectionHeaderCrypto. It mirrors the same exemptions the
// ledger's own chainsync header-queue path already applies
// (shouldVerifyChainsyncHeaderCrypto): verification is skipped while bulk
// historical/catch-up loading has not yet enabled live validation, and for
// slots already covered by an imported Mithril snapshot, since those slots
// were authenticated by the certificate chain during import and the
// restored database does not retain every historical epoch nonce. A caller
// that skips verification because this returns false must still treat the
// header as eligible, not reject it -- the same trust boundary the ledger's
// own pipeline already extends to this data.
func (ls *LedgerState) ShouldVerifyChainSelectionHeaderCrypto(
	slot uint64,
) bool {
	return ls.shouldVerifyChainsyncHeaderCrypto(slot)
}

// ValidateChainSelectionHeaderCrypto verifies a header's VRF/KES cryptography
// and, where the local ledger's stake/pool state has already caught up to
// the header's epoch, its leader eligibility. It lets chain selection require
// that a peer-reported header has passed the same checks as the applied
// chain before the header is allowed to influence Genesis density or
// corroboration (dingo #3517), independent of whether that header will ever
// be applied to the ledger.
//
// It never advances the shared epoch cache (matching ValidateBlockHeaderCrypto's
// no-mutation contract for header-only validation), but unlike
// ValidateBlockHeaderCrypto it tolerates ledger state that has not yet caught
// up to the header's slot: that is the normal condition for a peer
// legitimately racing ahead of local ledger application during fast sync or
// Genesis bootstrap. Use IsHeaderVerificationDeferred to distinguish that
// case (the header must still be treated as eligible) from a header this
// node can already prove is invalid.
func (ls *LedgerState) ValidateChainSelectionHeaderCrypto(
	header ledger.BlockHeader,
) error {
	if header == nil {
		return errors.New("nil block header")
	}
	return ls.verifyBlockHeaderCryptoWithEpochAdvance(
		headerOnlyBlock{header: header},
		false,
		true,
	)
}

// verifyBlockHeader performs cryptographic verification of a block header.
// This includes VRF proof verification and KES signature verification.
// Byron-era blocks are skipped here because this helper has only Praos
// parameters. LedgerState.verifyBlockHeaderStatelessCrypto validates their
// PBFT signatures and issuer state through the configured Byron genesis.
//
// Parameters:
//   - block: the block whose header to verify
//   - epochNonce: the epoch nonce (eta0) for VRF verification
//   - slotsPerKesPeriod: number of slots per KES period from Shelley genesis
//
// Returns an error if verification fails, nil if the block passes
// verification or is a Byron-era block (validated by the LedgerState wrapper).
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
	if err := verifyTPraosNonceVrfHex(header, epochNonceHex); err != nil {
		return fmt.Errorf(
			"block header verification failed at slot %d: %w",
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
	return ls.verifyBlockHeaderCryptoWithEpochAdvance(block, true, false)
}

func (ls *LedgerState) verifyBlockHeaderCryptoBeforeApply(
	block ledger.Block,
) error {
	return ls.verifyBlockHeaderCryptoWithEpochAdvance(block, true, true)
}

func (ls *LedgerState) verifyBlockHeaderCryptoWithEpochAdvance(
	block ledger.Block,
	allowEpochCacheAdvance bool,
	allowStateDefer bool,
) error {
	epoch, err := ls.verifyBlockHeaderStatelessCrypto(
		block,
		allowEpochCacheAdvance,
	)
	if err != nil {
		return err
	}
	return ls.verifyBlockHeaderState(block, epoch.EpochId, allowStateDefer)
}

func (ls *LedgerState) verifyBlockHeaderStateWithEpochAdvance(
	block ledger.Block,
	allowEpochCacheAdvance bool,
	allowStateDefer bool,
) error {
	if block.Era().Id == byron.EraIdByron {
		return nil
	}
	epoch, err := ls.headerVerificationEpoch(
		block.SlotNumber(),
		allowEpochCacheAdvance,
	)
	if err != nil {
		return err
	}
	return ls.verifyBlockHeaderState(block, epoch.EpochId, allowStateDefer)
}

func (ls *LedgerState) verifyBlockHeaderStatelessCrypto(
	block ledger.Block,
	allowEpochCacheAdvance bool,
) (models.Epoch, error) {
	// Byron uses PBFT rather than Praos. Validate its exact signature,
	// configured genesis issuer, protocol magic, and current-slot bound before
	// avoiding the Praos epoch/nonce lookups below. Ordered active-delegation
	// and issuer-window checks run during ledger application because parallel
	// pre-validation cannot see earlier blocks in the same batch.
	if block.Era().Id == byron.EraIdByron {
		err := ls.validateByronPBFTHeaderCrypto(block)
		return models.Epoch{}, err
	}

	blockSlot := block.SlotNumber()
	epoch, err := ls.headerVerificationEpoch(
		blockSlot,
		allowEpochCacheAdvance,
	)
	if err != nil {
		return models.Epoch{}, err
	}

	slotsPerKesPeriod := ls.SlotsPerKESPeriod()
	if slotsPerKesPeriod == 0 {
		return models.Epoch{}, fmt.Errorf(
			"shelley genesis not available for block header verification at slot %d",
			blockSlot,
		)
	}

	if err := verifyBlockHeaderHex(
		block,
		ls.epochNonceHex(epoch.EpochId, epoch.Nonce),
		slotsPerKesPeriod,
	); err != nil {
		return models.Epoch{}, err
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
		return models.Epoch{}, fmt.Errorf(
			"block header verification failed at slot %d: %w",
			blockSlot,
			err,
		)
	}

	return epoch, nil
}

func (ls *LedgerState) headerVerificationEpoch(
	blockSlot uint64,
	allowEpochCacheAdvance bool,
) (models.Epoch, error) {
	// The epoch cache can be forecast forward for near-future headers, but it
	// must never be advanced past the HFC safe zone or a known era boundary.
	// Check the immutable summary first so ErrPastHorizon is surfaced before
	// ensureEpochForSlot mutates any forecasted nonce state.
	if len(ls.loadConsensusSnapshot().epochCache) > 0 {
		summary, err := ls.HardForkSummary()
		if err != nil {
			return models.Epoch{}, fmt.Errorf(
				"block header verification rejected: build forecast for slot %d: %w",
				blockSlot,
				err,
			)
		}
		if _, err := summary.SlotToEpoch(blockSlot); err != nil {
			if errors.Is(err, hardfork.ErrPastHorizon) {
				// A header past the forecast horizon cannot be verified yet:
				// the applied ledger has not advanced far enough to know its
				// epoch and nonce. This is a deferred condition, not a peer
				// fault. Classify it as deferred so the block stays queued for
				// in-order re-verification once the applied tip advances into
				// range (preserving the no-apply-past-horizon guard), instead
				// of being treated as a crypto failure that recycles the honest
				// peer. Recycling on past-horizon starves the peer pool the
				// block and Leios endorser-block fetch depend on and deadlocks
				// catch-up at epoch boundaries; the chainsync recycle paths skip
				// deferred errors.
				return models.Epoch{}, fmt.Errorf(
					"%w: block header verification deferred past era horizon "+
						"at slot %d: %w",
					errHeaderVerificationDeferred,
					blockSlot,
					err,
				)
			}
			return models.Epoch{}, fmt.Errorf(
				"block header verification rejected: forecast slot %d: %w",
				blockSlot,
				err,
			)
		}
	}

	// Look up the epoch for this block's slot from the epoch cache.
	// This is an epoch-aware lookup that searches through all known
	// epochs rather than only the current one, ensuring that blocks
	// at epoch boundaries are verified against the correct nonce.
	epoch, err := ls.epochForSlot(blockSlot)
	if err != nil {
		if !allowEpochCacheAdvance {
			return models.Epoch{}, fmt.Errorf(
				"%w: no cached epoch data for slot %d: %w",
				errHeaderVerificationDeferred,
				blockSlot,
				err,
			)
		}
		// Epoch cache doesn't cover this slot yet. Blockfetch can
		// deliver blocks past the epoch boundary before the ledger
		// processing goroutine runs the full epoch rollover. Eagerly
		// compute the next epoch(s) so verification can proceed.
		epoch, err = ls.ensureEpochForSlot(blockSlot)
		if err != nil {
			if errors.Is(err, errEpochCacheForecastBoundary) {
				return models.Epoch{}, fmt.Errorf(
					"%w: block header verification deferred at hard-fork "+
						"boundary for slot %d: %w",
					errHeaderVerificationDeferred,
					blockSlot,
					err,
				)
			}
			return models.Epoch{}, fmt.Errorf(
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
		return models.Epoch{}, fmt.Errorf(
			"%w: block header verification rejected: "+
				"epoch %d has no nonce for slot %d "+
				"(epoch rollover may not have been processed yet)",
			errEpochNonceUnavailable,
			epoch.EpochId,
			blockSlot,
		)
	}
	return epoch, nil
}

func (ls *LedgerState) verifyBlockHeaderState(
	block ledger.Block,
	epochId uint64,
	allowStateDefer bool,
) error {
	if handled, err := ls.verifyGenesisDelegateHeader(
		block,
		allowStateDefer,
	); handled || err != nil {
		return err
	}

	// Bind the header's VRF key to the pool's on-chain registered VRF key.
	// The crypto path above verifies the VRF proof only against the key carried
	// in the header (SkipStakePoolValidation skips gouroboros' registered-key
	// check), so without this an attacker can grind VRF keys to win slots.
	if err := ls.verifyRegisteredVrfKey(block); err != nil {
		if allowStateDefer &&
			errors.Is(err, models.ErrPoolNotFound) &&
			ls.ledgerTipBehindSlot(block.SlotNumber()) {
			return fmt.Errorf(
				"%w: registered VRF key state for slot %d is ahead of the ledger apply cursor: %w",
				errHeaderVerificationDeferred,
				block.SlotNumber(),
				err,
			)
		}
		return err
	}

	if err := ls.verifyBlockLeaderEligibility(block, epochId); err != nil {
		// Scope the deferral to the RECOVERABLE case only (issue #3727,
		// finding 4 -- consensus-sensitive). A leader-stake snapshot reported
		// unavailable (errLeaderStakeSnapshotUnavailable) means the epoch's
		// distribution is missing/empty, which is only *recoverable* while the
		// apply cursor is still behind this slot: the mark snapshot for the
		// slot has not been computed yet and will exist once apply catches up,
		// so defer. Once the cursor has caught up, a still-empty distribution
		// is a genuine, permanent gap for that epoch -- a producer whose
		// eligibility can never be established -- and MUST stay a hard
		// rejection, exactly as before this change: deferring it forever would
		// either adopt a block whose leader eligibility is never checked or
		// loop. The #3727 retention pin is what makes the recoverable case
		// actually resolve: it retains the mark snapshot a queued/deferred
		// header needs so that, by the time the cursor reaches the slot, the
		// snapshot is present and this path is not taken. Deferred headers on
		// abandoned forks are released by the retention guard's eviction of
		// entries the cursor has passed (see PrunePoolSnapshotsWithRetentionFloor),
		// not by deferring their headers forever. A pool absent from a
		// *populated* snapshot never carries this sentinel and hard-rejects.
		if allowStateDefer &&
			errors.Is(err, errLeaderStakeSnapshotUnavailable) &&
			ls.ledgerTipBehindSlot(block.SlotNumber()) {
			return fmt.Errorf(
				"%w: leader stake snapshot state for slot %d is ahead of the ledger apply cursor: %w",
				errHeaderVerificationDeferred,
				block.SlotNumber(),
				err,
			)
		}
		return err
	}
	return nil
}

func (ls *LedgerState) verifyGenesisDelegateHeader(
	block ledger.Block,
	allowStateDefer bool,
) (bool, error) {
	if block.Era().Id == byron.EraIdByron {
		return false, nil
	}
	if ls.config.CardanoNodeConfig == nil {
		return false, nil
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil || len(shelleyGenesis.GenDelegs) == 0 {
		return false, nil
	}
	// The overlay decision uses protocol parameters for the block's epoch.
	// Blockfetch can verify a header ahead of ledger apply, while the
	// in-memory parameters still describe the previous epoch. Defer any
	// state-dependent overlay decision until the rollover has installed the
	// target epoch's parameters. This must precede
	// genesisOverlayDelegationForBlock because stale parameters can otherwise
	// classify a future slot as having no overlay.
	if allowStateDefer && ls.ledgerTipBehindSlot(block.SlotNumber()) {
		return true, fmt.Errorf(
			"%w: genesis overlay state for slot %d is not yet authoritative",
			errHeaderVerificationDeferred,
			block.SlotNumber(),
		)
	}
	genesisDeleg, status, err := ls.genesisOverlayDelegationForBlock(
		block,
		shelleyGenesis,
	)
	if err != nil {
		return true, err
	}
	if status == genesisOverlayNone {
		return false, nil
	}
	if status == genesisOverlayNonActive {
		return true, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"slot is reserved for the genesis overlay schedule but not active",
			block.SlotNumber(),
		)
	}

	issuerHash := block.IssuerVkey().Hash()
	if !bytes.Equal(issuerHash.Bytes(), genesisDeleg.delegateHash) {
		return true, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"genesis overlay slot assigned to delegate %x, got issuer %x",
			block.SlotNumber(),
			genesisDeleg.delegateHash,
			issuerHash.Bytes(),
		)
	}

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(block.Header())
	if err != nil {
		return true, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"extract genesis delegate VRF key: %w",
			block.SlotNumber(),
			err,
		)
	}
	if !ok || len(vrfKey) == 0 {
		return true, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"genesis delegate VRF key unavailable",
			block.SlotNumber(),
		)
	}
	headerVrfKeyHash := lcommon.Blake2b256Hash(vrfKey)
	if !bytes.Equal(headerVrfKeyHash.Bytes(), genesisDeleg.vrfHash) {
		return true, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"genesis delegate %s VRF key does not match genesis VRF key "+
				"(header %x, genesis %x)",
			block.SlotNumber(),
			hex.EncodeToString(issuerHash.Bytes()),
			headerVrfKeyHash.Bytes(),
			genesisDeleg.vrfHash,
		)
	}
	return true, nil
}

// genesisOverlayDelegationForBlock resolves the overlay parameters using the
// era that encoded the block. At a hard-fork boundary, the boundary block can
// be encoded in the predecessor era while its header announces the successor.
// In that case the epoch cache already describes the successor era, but the
// block's leader was selected under the predecessor-era parameters.
func (ls *LedgerState) genesisOverlayDelegationForBlock(
	block ledger.Block,
	shelleyGenesis *shelley.ShelleyGenesis,
) (genesisDelegation, genesisOverlaySlotStatus, error) {
	return ls.genesisOverlayDelegationForSlotWithParams(
		block.SlotNumber(),
		shelleyGenesis,
		ls.genesisOverlayProtocolParamsForBlock(block),
	)
}

func (ls *LedgerState) genesisOverlayDelegationForSlotWithParams(
	slot uint64,
	shelleyGenesis *shelley.ShelleyGenesis,
	pparams lcommon.ProtocolParameters,
) (genesisDelegation, genesisOverlaySlotStatus, error) {
	genesisDelegs, err := parseShelleyGenesisDelegations(shelleyGenesis)
	if err != nil {
		return genesisDelegation{}, genesisOverlayNone, fmt.Errorf(
			"block header verification rejected at slot %d: %w",
			slot,
			err,
		)
	}
	if len(genesisDelegs) == 0 {
		return genesisDelegation{}, genesisOverlayNone, nil
	}

	epoch, err := ls.epochForSlot(slot)
	if err != nil {
		return genesisDelegation{}, genesisOverlayNone, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"resolve epoch for genesis overlay schedule: %w",
			slot,
			err,
		)
	}
	if epoch.LengthInSlots == 0 || slot < epoch.StartSlot {
		return genesisDelegation{}, genesisOverlayNone, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"invalid epoch data for genesis overlay schedule",
			slot,
		)
	}

	decentralization := decentralizationParamRat(pparams)
	overlayIndex, status := classifyGenesisOverlaySlot(
		slot-epoch.StartSlot,
		decentralization,
		shelleyGenesis.ActiveSlotsCoeff.Rat,
		uint64(len(genesisDelegs)),
	)
	if status != genesisOverlayActive {
		return genesisDelegation{}, status, nil
	}

	genesisDeleg := genesisDelegs[overlayIndex]
	activeDeleg, err := ls.activeGenesisDelegationForSlot(genesisDeleg, slot)
	if err != nil {
		return genesisDelegation{}, genesisOverlayNone, err
	}
	return activeDeleg, genesisOverlayActive, nil
}

func parseShelleyGenesisDelegations(
	shelleyGenesis *shelley.ShelleyGenesis,
) ([]genesisDelegation, error) {
	if shelleyGenesis == nil || len(shelleyGenesis.GenDelegs) == 0 {
		return nil, nil
	}
	ret := make([]genesisDelegation, 0, len(shelleyGenesis.GenDelegs))
	for genesisHashHex, genDeleg := range shelleyGenesis.GenDelegs {
		genesisHash, err := hex.DecodeString(genesisHashHex)
		if err != nil || len(genesisHash) != lcommon.Blake2b224Size {
			return nil, fmt.Errorf(
				"invalid genesis key hash %q",
				genesisHashHex,
			)
		}
		delegateHashHex := genDeleg["delegate"]
		delegateHash, err := hex.DecodeString(delegateHashHex)
		if err != nil || len(delegateHash) != lcommon.Blake2b224Size {
			return nil, fmt.Errorf(
				"invalid genesis delegate hash %q",
				delegateHashHex,
			)
		}
		vrfHashHex := genDeleg["vrf"]
		vrfHash, err := hex.DecodeString(vrfHashHex)
		if err != nil || len(vrfHash) != lcommon.Blake2b256Size {
			return nil, fmt.Errorf(
				"invalid genesis delegate VRF key hash %q",
				vrfHashHex,
			)
		}
		ret = append(ret, genesisDelegation{
			genesisHash:  genesisHash,
			delegateHash: delegateHash,
			vrfHash:      vrfHash,
		})
	}
	slices.SortFunc(ret, func(a, b genesisDelegation) int {
		return bytes.Compare(a.genesisHash, b.genesisHash)
	})
	return ret, nil
}

func (ls *LedgerState) activeGenesisDelegationForSlot(
	initial genesisDelegation,
	slot uint64,
) (genesisDelegation, error) {
	row, err := ls.db.Metadata().GetGenesisDelegationForSlot(
		initial.genesisHash,
		slot,
		nil,
	)
	if err != nil {
		return genesisDelegation{}, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"lookup active genesis delegation: %w",
			slot,
			err,
		)
	}
	if row == nil {
		return initial, nil
	}
	if len(row.GenesisDelegateHash) != lcommon.Blake2b224Size ||
		len(row.VrfKeyHash) != lcommon.Blake2b256Size {
		return genesisDelegation{}, fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"invalid active genesis delegation for genesis key %x",
			slot,
			initial.genesisHash,
		)
	}
	return genesisDelegation{
		genesisHash:  append([]byte(nil), initial.genesisHash...),
		delegateHash: append([]byte(nil), row.GenesisDelegateHash...),
		vrfHash:      append([]byte(nil), row.VrfKeyHash...),
	}, nil
}

func classifyGenesisOverlaySlot(
	relativeSlot uint64,
	decentralization *big.Rat,
	activeSlotsCoeff *big.Rat,
	genesisKeyCount uint64,
) (uint64, genesisOverlaySlotStatus) {
	if decentralization == nil ||
		decentralization.Sign() <= 0 ||
		decentralization.Cmp(big.NewRat(1, 1)) > 0 ||
		activeSlotsCoeff == nil ||
		activeSlotsCoeff.Sign() <= 0 ||
		activeSlotsCoeff.Cmp(big.NewRat(1, 1)) > 0 ||
		genesisKeyCount == 0 {
		return 0, genesisOverlayNone
	}
	position := ceilUint64Rat(relativeSlot, decentralization)
	nextPosition := ceilUint64Rat(relativeSlot+1, decentralization)
	if position >= nextPosition {
		return 0, genesisOverlayNone
	}
	activeSlotCoeffInv := activeSlotCoeffInverse(activeSlotsCoeff)
	if activeSlotCoeffInv == 0 || position%activeSlotCoeffInv != 0 {
		return 0, genesisOverlayNonActive
	}
	return (position / activeSlotCoeffInv) % genesisKeyCount,
		genesisOverlayActive
}

func ceilUint64Rat(v uint64, rat *big.Rat) uint64 {
	numerator := new(big.Int).Mul(
		new(big.Int).SetUint64(v),
		rat.Num(),
	)
	denom := rat.Denom()
	numerator.Add(numerator, new(big.Int).Sub(denom, big.NewInt(1)))
	numerator.Quo(numerator, denom)
	return numerator.Uint64()
}

func activeSlotCoeffInverse(activeSlotsCoeff *big.Rat) uint64 {
	inv := new(big.Int).Quo(
		activeSlotsCoeff.Denom(),
		activeSlotsCoeff.Num(),
	)
	return inv.Uint64()
}

// genesisOverlayProtocolParamsForBlock resolves protocol parameters for the
// block body era rather than only the epoch's current era. A hard-fork
// boundary block may be encoded in the predecessor era while its header
// announces the successor; using the successor-era parameters would disable
// a still-active genesis overlay and incorrectly send the genesis delegate
// through the registered-pool lookup.
func (ls *LedgerState) genesisOverlayProtocolParamsForBlock(
	block ledger.Block,
) lcommon.ProtocolParameters {
	slot := block.SlotNumber()
	epoch, err := ls.epochForSlot(slot)
	if err != nil {
		return ls.ProtocolParamsForSlot(slot)
	}

	paramsEpoch := epoch.EpochId
	paramsEraID := epoch.EraId
	blockEraID := uint(block.Era().Id)
	if blockEraID < epoch.EraId {
		paramsEraID = blockEraID
	}

	snapshot := ls.loadConsensusSnapshot()
	if paramsEpoch == snapshot.currentEpoch.EpochId &&
		paramsEraID == snapshot.currentEpoch.EraId {
		return snapshot.currentPParams
	}
	if ls.db != nil {
		era, ok := ls.eraById(paramsEraID)
		if ok && era != nil && era.DecodePParamsFunc != nil {
			if pparams, pparamsErr := ls.db.GetPParams(
				paramsEpoch,
				paramsEraID,
				era.DecodePParamsFunc,
				nil,
			); pparamsErr == nil && pparams != nil {
				return pparams
			}
		}
	}
	return ls.ProtocolParamsForSlot(slot)
}

func decentralizationParamRat(
	pparams lcommon.ProtocolParameters,
) *big.Rat {
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		if pp.Decentralization == nil {
			return nil
		}
		return pp.Decentralization.Rat
	case *mary.MaryProtocolParameters:
		if pp.Decentralization == nil {
			return nil
		}
		return pp.Decentralization.Rat
	case *alonzo.AlonzoProtocolParameters:
		if pp.Decentralization == nil {
			return nil
		}
		return pp.Decentralization.Rat
	default:
		return nil
	}
}

func (ls *LedgerState) ledgerTipBehindSlot(slot uint64) bool {
	return ls.loadTipSnapshot().currentTip.Point.Slot < slot
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
// The production caller first runs verifyGenesisDelegateHeader, which handles
// or rejects exact genesis-overlay slots. Reaching this function from that path
// means the block is in a Praos slot and must receive the pool threshold check,
// even when the decentralization parameter enables overlay slots elsewhere in
// the same epoch.
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

	poolStake, totalStake, snapshotEpoch, snapshotType, skipEligibility, err := ls.leaderEligibilityStake(
		block,
		epochId,
		poolKeyHash,
	)
	if err != nil {
		return err
	}
	if skipEligibility {
		return nil
	}
	if totalStake == 0 {
		// leaderEligibilityStake already rejected an absent or zero pool
		// row, so reaching here means the producer holds stake while the
		// network-wide denominator reads zero: a dingo-side storage or
		// computation gap rather than an empty network, and a threshold
		// with no denominator to divide by. Accepting the block would
		// admit a producer nothing verified, so only the explicitly
		// selected prototype profile may bypass it.
		if ls.config.SkipLeaderStakeThresholdCheck {
			ls.config.Logger.Warn(
				"leader eligibility unevaluable: total active stake is zero; trusting block (prototype profile)",
				"slot",
				block.SlotNumber(),
				"epoch",
				epochId,
				"snapshot_epoch",
				snapshotEpoch,
				"snapshot_type",
				snapshotType,
				"component",
				"ledger",
			)
			return nil
		}
		// Classified as an unavailable snapshot so header verification
		// running ahead of the ledger apply cursor defers instead of
		// rejecting (verifyBlockHeaderState); once the cursor has caught
		// up the same state is a hard rejection.
		return fmt.Errorf(
			"%w: block header verification rejected at slot %d: "+
				"total active stake for epoch %d snapshot %s is zero "+
				"while producer pool %x holds stake",
			errLeaderStakeSnapshotUnavailable,
			block.SlotNumber(),
			snapshotEpoch,
			snapshotType,
			poolKeyHash[:],
		)
	}

	// Use the genesis Rat directly to avoid a float64 precision roundtrip.
	// A zero or negative coefficient computes a zero threshold, under which
	// no VRF output is ever eligible.
	activeSlotCoeffRat := ls.activeSlotCoeffRat()
	if activeSlotCoeffRat == nil || activeSlotCoeffRat.Sign() <= 0 {
		// The coefficient is a threshold input, so without it eligibility
		// cannot be evaluated at all. Unlike a missing snapshot this is a
		// genesis/configuration fault that the apply cursor never
		// resolves, so it is rejected outright rather than deferred.
		if ls.config.SkipLeaderStakeThresholdCheck {
			ls.config.Logger.Warn(
				"leader eligibility unevaluable: active slot coefficient unavailable or non-positive; trusting block (prototype profile)",
				"slot",
				block.SlotNumber(),
				"component",
				"ledger",
			)
			return nil
		}
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"active slot coefficient unavailable or non-positive; "+
				"leader eligibility cannot be evaluated",
			block.SlotNumber(),
		)
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
	threshold, err := consensus.CertifiedNatThresholdWithMode(
		poolStake,
		totalStake,
		activeSlotCoeffRat,
		mode,
	)
	if err != nil {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"compute leadership threshold: %w",
			block.SlotNumber(),
			err,
		)
	}
	belowThreshold, err := consensus.IsVRFOutputBelowThresholdWithMode(
		vrfResult.Output,
		threshold,
		mode,
	)
	if err != nil {
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"compare VRF output against threshold: %w",
			block.SlotNumber(),
			err,
		)
	}
	// Record how close the decision was, for every decision rather than only
	// the failures. See leaderThresholdMargin for why the distribution is the
	// thing worth having.
	margin := leaderThresholdMargin(
		leaderValueForMode(vrfResult.Output, mode),
		threshold,
	)
	ls.metrics.observeLeaderThresholdMargin(margin)
	if !belowThreshold {
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
				"slot",
				block.SlotNumber(),
				"pool",
				hex.EncodeToString(poolKeyHash[:]),
				"pool_stake",
				poolStake,
				"total_stake",
				totalStake,
				"epoch",
				epochId,
				"snapshot_epoch",
				snapshotEpoch,
				"snapshot_type",
				snapshotType,
				"component",
				"ledger",
			)
			return nil
		}
		// Counted here rather than at the top of the branch: the bypass
		// above trusts the block, so counting before it would report
		// rejections that never happened.
		ls.metrics.incLeaderThresholdRejections()
		// margin is carried in the message because it is the number that
		// says whether this was a genuinely ineligible producer or a stake
		// discrepancy: a rejection sitting a fraction of a percent under
		// zero means the local stake distribution, not the block, is what
		// disagreed with the network.
		return fmt.Errorf(
			"block header verification rejected at slot %d: "+
				"producer pool %x VRF leader value exceeds stake-derived threshold "+
				"(pool stake: %d, total stake: %d, epoch: %d, snapshot_epoch: %d, snapshot_type: %s, threshold_margin: %.9f)",
			block.SlotNumber(),
			poolKeyHash[:],
			poolStake,
			totalStake,
			epochId,
			snapshotEpoch,
			snapshotType,
			margin,
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
			// Mirror the mark path below and separate a storage gap from
			// genuine ineligibility. A zero denominator leaves the
			// threshold with no divisor, and an imported distribution
			// holding no pools at all cannot be the certified nesPd
			// (which is always populated); both mean dingo cannot yet
			// answer, so they are classified as unavailable and header
			// verification ahead of the apply cursor defers. A pool
			// simply absent from a populated distribution is an
			// authoritative answer -- cardano-ledger's VRFKeyUnknown --
			// and stays a rejection.
			unavailable := snapshot != nil && snapshot.StakeDenominator == 0
			if !unavailable {
				if total, terr := ls.db.Metadata().GetTotalActiveStake(
					epochId,
					models.PoolStakeSnapshotTypeActive,
					nil,
				); terr == nil && total == 0 {
					unavailable = true
				}
			}
			if unavailable {
				return 0, 0, epochId,
					models.PoolStakeSnapshotTypeActive, false,
					fmt.Errorf(
						"%w: block header verification rejected at slot %d: "+
							"producer pool %x missing from active pool distribution "+
							"for epoch %d (imported distribution is incomplete)",
						errLeaderStakeSnapshotUnavailable,
						block.SlotNumber(),
						poolKeyHash[:],
						epochId,
					)
			}
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
			return 0, 0, snapshotEpoch, snapshotType, false,
				fmt.Errorf(
					"%w: "+
						"block header verification rejected at slot %d: "+
						"producer pool %x has no stake in epoch %d snapshot (%s)",
					errLeaderStakeSnapshotUnavailable,
					block.SlotNumber(),
					poolKeyHash[:],
					snapshotEpoch,
					diag,
				)
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
	if ls.shouldSkipPostMithrilMarkEligibility(snapshot, snapshotEpoch) {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"skipping leader eligibility check: post-Mithril mark snapshot was reconstructed after the target boundary",
				"slot",
				block.SlotNumber(),
				"epoch",
				epochId,
				"snapshot_epoch",
				snapshotEpoch,
				"snapshot_type",
				snapshotType,
				"captured_slot",
				snapshot.CapturedSlot,
				"component",
				"ledger",
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

// shouldSkipPostMithrilMarkEligibility reports whether a mark row was
// reconstructed from live state after its target boundary and therefore cannot
// safely drive hard leader-threshold rejection. New imports retain the
// certified NewEpochState.SnapShots boundary slot. Older imports used the
// Mithril anchor itself as CapturedSlot, so that exact legacy provenance is
// accepted too; startup-synthesized historical rows use another post-boundary
// slot and remain conservative.
func (ls *LedgerState) shouldSkipPostMithrilMarkEligibility(
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
	if snapshot.CapturedSlot == ls.mithrilLedgerSlot {
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

// blockPipelineEta0Provider implements gouroboros' pipeline.Eta0Provider for
// the block-processing pipeline's validate stage (issue #1894 phase 3). It
// reads only the already-published epoch cache. Unlike the admission path it
// must neither forecast nor rebuild the hard-fork summary: validate workers
// run concurrently over blocks already committed to ls.chain, and a missing
// cached nonce means this later validation is deferred under the same gate as
// the serial path. Avoiding headerVerificationEpoch here also keeps the hot
// path O(logical cache scan) instead of rebuilding the full era summary for
// every block.
//
// Byron-era slots have no Praos epoch nonce. Callers skip VRF/KES validation
// for decoded Byron blocks, exactly as the serial path's verifyBlockHeaderHex
// does. A missing nonce for any decoded post-Byron block is handled by the
// same validation-state gate as admission rather than treated as a
// cryptographic rejection.
//
// An epoch without a nonce is wrapped in errBlockPipelineEta0Unavailable.
// A slot outside the published cache is wrapped in
// errHeaderVerificationDeferred, so the error drain and enforcement path can
// distinguish missing state from a cryptographic rejection.
func (ls *LedgerState) blockPipelineEta0Provider(slot uint64) (string, error) {
	epoch, err := ls.epochForSlot(slot)
	if err != nil {
		return "", fmt.Errorf(
			"%w: block-processing pipeline nonce lookup for slot %d: %w",
			errHeaderVerificationDeferred,
			slot,
			err,
		)
	}
	if len(epoch.Nonce) == 0 {
		return "", fmt.Errorf(
			"%w: %w: epoch %d has no nonce for slot %d",
			errBlockPipelineEta0Unavailable,
			errEpochNonceUnavailable,
			epoch.EpochId,
			slot,
		)
	}
	return ls.epochNonceHex(epoch.EpochId, epoch.Nonce), nil
}

// epochForSlot searches an immutable epoch-cache snapshot for the epoch
// containing the given slot.
//
// Returns the matching epoch or an error if no epoch covers the slot.
func (ls *LedgerState) epochForSlot(slot uint64) (models.Epoch, error) {
	cache := ls.loadConsensusSnapshot().epochCache

	if len(cache) == 0 {
		return models.Epoch{}, errors.New("epoch cache is empty")
	}

	// Search newest-to-oldest so that if cache entries overlap
	// (e.g., after rollback/rebuild), we use the most recent epoch data.
	for _, ep := range slices.Backward(cache) {
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
	for _, v := range slices.Backward(cache) {
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
			len(cache),
		)
	}
	return models.Epoch{}, fmt.Errorf(
		"slot %d not covered by any known epoch (cache has %d epochs, last ends at slot %d)",
		slot,
		len(cache),
		lastValidEnd,
	)
}

// OldestRequiredSnapshotEpoch returns the oldest pool-stake snapshot epoch that
// a currently queued/deferred header still needs for leader-eligibility
// validation, so snapshot pruning can retain it instead of removing it out from
// under the deferred header (issue #3727). It locks the deferred-header set and
// delegates to oldestRequiredSnapshotEpochLocked. Prefer
// PrunePoolSnapshotsWithRetentionFloor for the prune path, which holds the lock
// across both the floor read and the prune so admission cannot interleave; this
// public method exists for observation and tests.
func (ls *LedgerState) OldestRequiredSnapshotEpoch() (uint64, bool) {
	ls.deferredHeaderValidationMu.Lock()
	defer ls.deferredHeaderValidationMu.Unlock()
	return ls.oldestRequiredSnapshotEpochLocked()
}

// oldestRequiredSnapshotEpochLocked computes the retention floor with
// ls.deferredHeaderValidationMu already held. A header deferred at slot S
// validates its producer's leader eligibility against the mark snapshot for
// StakeSnapshotEpoch(epochOf(S)); the floor is the minimum of that quantity
// over every outstanding deferred header.
//
// Return contract:
//   - (_, false): no header is deferred, so the default retention window
//     applies and nothing extra is pinned.
//   - (0, true): at least one deferred slot cannot yet be mapped to an epoch
//     (its epoch cache entry has not been published, or its key is malformed).
//     We cannot name the snapshot epoch such a header will need, and once the
//     cache catches up leaderEligibilityStake WILL need it, so we retain ALL
//     pool snapshots (floor 0 prunes nothing) until every deferred slot is
//     mappable. Skipping the slot instead would let cleanup prune the snapshot
//     the header needs and drive it into a defer loop.
//   - (min, true): every deferred slot mapped; pin at the minimum required
//     snapshot epoch.
func (ls *LedgerState) oldestRequiredSnapshotEpochLocked() (uint64, bool) {
	if len(ls.deferredHeaderValidation) == 0 {
		return 0, false
	}
	var floor uint64
	have := false
	for key := range ls.deferredHeaderValidation {
		slot, err := slotFromHeaderValidationKey(key)
		if err != nil {
			// A key we cannot parse is a deferred header whose need we cannot
			// bound. Retain everything until it is gone rather than risk
			// pruning a snapshot it turns out to require.
			return 0, true
		}
		epoch, err := ls.epochForSlot(slot)
		if err != nil {
			// The slot is not yet covered by the published epoch cache, so we
			// cannot name the snapshot epoch it needs. Retain ALL pool
			// snapshots until it becomes mappable (see the return contract):
			// pruning now would delete the snapshot leaderEligibilityStake
			// will read once the cache advances, looping the header on defer.
			return 0, true
		}
		snapshotEpoch := praos.StakeSnapshotEpoch(epoch.EpochId)
		if !have || snapshotEpoch < floor {
			floor = snapshotEpoch
			have = true
		}
	}
	return floor, have
}

// PrunePoolSnapshotsWithRetentionFloor is the snapshot manager's retention
// guard (wired via Manager.SetPoolSnapshotRetentionGuard). It holds the
// deferred-header lock across the whole retention decision AND the caller's
// pool-snapshot prune, so a deferred header cannot be admitted between the
// floor read and the prune and have its still-needed snapshot deleted (issue
// #3727 race). Under the lock it, in order:
//
//  1. Evicts abandoned deferred headers whose slot the apply cursor has already
//     passed. A canonical deferred header is consumed when the cursor applies
//     it, so one still present at/below the tip is on an abandoned fork and
//     would otherwise pin its snapshot forever (finding 5). Eviction lets the
//     floor rise; the evicted markers' persisted rows are deleted after the
//     lock is released (best effort — they cannot affect a resolved header).
//  2. Computes the retention floor over the surviving deferred headers and
//     lowers defaultBefore (cleanup's currentEpoch-3 pool boundary) to it when
//     a header needs an older snapshot (or to 0 = retain everything while any
//     deferred slot is unmappable).
//  3. Clamps the boundary UP to minBefore, a hard backstop
//     (currentEpoch - poolSnapshotRetentionMaxDepth) that bounds how many
//     historical epochs the pin can ever hold, so a stuck header cannot pin
//     pool snapshots without limit (finding 5).
//
// prune must perform and COMMIT the pool-snapshot delete before returning, so
// the rows are gone under the lock; it must not touch ledger locks or the
// deferred set. The lock scope covers only the pool-snapshot delete (a single
// indexed DELETE in its own transaction), not the reward-state prune; the only
// other holders of this mutex (mark/clear/consume) do no I/O, so no deadlock.
func (ls *LedgerState) PrunePoolSnapshotsWithRetentionFloor(
	defaultBefore uint64,
	minBefore uint64,
	prune func(before uint64) error,
) error {
	var evicted []string
	err := func() error {
		ls.deferredHeaderValidationMu.Lock()
		defer ls.deferredHeaderValidationMu.Unlock()
		evicted = ls.evictStaleDeferredHeadersLocked()
		before := defaultBefore
		if floor, ok := ls.oldestRequiredSnapshotEpochLocked(); ok &&
			floor < before {
			before = floor
		}
		if before < minBefore {
			before = minBefore
		}
		return prune(before)
	}()
	// Delete the evicted headers' persisted markers outside the lock: they are
	// abandoned, so losing the pin (already released above) is the only thing
	// that mattered; this just keeps the sync_state table from accumulating
	// dead markers across restarts.
	ls.deletePersistedDeferredMarkers(evicted)
	return err
}

// slotFromHeaderValidationKey extracts the slot from a deferred-header map key,
// which headerValidationPointKey formats as "<slot>:<hex-hash>".
func slotFromHeaderValidationKey(key string) (uint64, error) {
	sep := strings.IndexByte(key, ':')
	if sep < 0 {
		return 0, fmt.Errorf("malformed header validation key %q", key)
	}
	return strconv.ParseUint(key[:sep], 10, 64)
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

// advanceEpochCache computes the next same-era epoch's parameters and nonce
// from chain data and appends it to the in-memory epoch cache. This is a
// lightweight alternative to the full processEpochRollover — it only
// populates the nonce and epoch boundaries needed for header verification,
// without running pparam updates, snapshot rotation, or DB writes. It refuses
// to cross a confirmed or configured hard-fork boundary because only the full
// rollover owns the successor era's parameters and snapshot rotation. The full
// rollover will run later in ledgerProcessBlocks and replace the cache with the
// authoritative DB-backed version.
func (ls *LedgerState) advanceEpochCache() error {
	// Read last epoch from the lock-free consensus snapshot
	snapshot := ls.loadConsensusSnapshot()
	cache := snapshot.epochCache
	if len(cache) == 0 {
		return errors.New("epoch cache is empty")
	}
	lastEpoch := cache[len(cache)-1]
	if err := ls.validateEpochCacheForecast(
		lastEpoch,
		snapshot.currentEra.Id,
		snapshot.transitionInfo,
	); err != nil {
		return err
	}

	if lastEpoch.LengthInSlots == 0 {
		return errors.New("last epoch has zero length")
	}

	newStartSlot := lastEpoch.StartSlot + uint64(lastEpoch.LengthInSlots)

	// Compute epoch nonce (requires DB access, done outside lock)
	nonce, evolvingNonce, candidateNonce, labNonce, err := ls.computeEpochNonceForSlot(
		newStartSlot,
		lastEpoch,
	)
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
	// TransitionInfo can become known without changing the cache tail while
	// nonce computation is in flight. Recheck under the writer lock so that
	// publication cannot race a newly-confirmed boundary and append a row with
	// the source era's parameters on the other side.
	if err := ls.validateEpochCacheForecast(
		lastCached,
		ls.currentEra.Id,
		ls.transitionInfo,
	); err != nil {
		ls.Unlock()
		return err
	}
	newCache := make([]models.Epoch, len(ls.epochCache), len(ls.epochCache)+1)
	copy(newCache, ls.epochCache)
	ls.epochCache = append(newCache, newEpoch)
	ls.publishSnapshotsLocked()
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

// validateEpochCacheForecast rejects an eager cache advance that would cross
// an era boundary. A configured TriggerAtEpoch is authoritative for the cache
// tail's era. Otherwise TransitionKnown applies only when the tail still
// belongs to the current era whose transition state was published.
func (ls *LedgerState) validateEpochCacheForecast(
	lastEpoch models.Epoch,
	currentEraID uint,
	transition hardfork.TransitionInfo,
) error {
	nextEpochID := lastEpoch.EpochId + 1
	shape := ls.eraShape()
	if entry, ok := shape.EraForID(lastEpoch.EraId); ok &&
		entry.NextEraTrigger.Kind == hardfork.TriggerAtEpoch {
		if nextEpochID >= entry.NextEraTrigger.Epoch {
			return fmt.Errorf(
				"%w: cannot forecast epoch %d from era %d across configured hard-fork boundary at epoch %d",
				errEpochCacheForecastBoundary,
				nextEpochID,
				lastEpoch.EraId,
				entry.NextEraTrigger.Epoch,
			)
		}
		return nil
	}
	if lastEpoch.EraId != currentEraID ||
		transition.State != hardfork.TransitionKnown ||
		nextEpochID < transition.KnownEpoch {
		return nil
	}
	return fmt.Errorf(
		"%w: cannot forecast epoch %d from era %d across confirmed hard-fork boundary at epoch %d",
		errEpochCacheForecastBoundary,
		nextEpochID,
		lastEpoch.EraId,
		transition.KnownEpoch,
	)
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
	tipState := ls.loadTipSnapshot()
	currentTipSlot := tipState.currentTip.Point.Slot
	currentTipBlockNonce := tipState.currentTipBlockNonce
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
