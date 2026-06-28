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

package forging

import (
	"errors"
	"fmt"
	"log/slog"
	"math"

	dingoversion "github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/vrf"
	"golang.org/x/crypto/blake2b"
)

// MempoolTransaction represents a transaction in the mempool.
type MempoolTransaction struct {
	Hash string
	Cbor []byte
	Type uint
}

// MempoolProvider provides access to mempool transactions.
type MempoolProvider interface {
	Transactions() []MempoolTransaction
}

// ProtocolParamsProvider provides access to protocol parameters.
type ProtocolParamsProvider interface {
	GetCurrentPParams() lcommon.ProtocolParameters
	// ProtocolParamsForSlot returns the pparams that should govern a
	// block forged at the given slot. When the slot is in an epoch
	// beyond a scheduled fork that has not yet been applied to the
	// in-memory ledger state, the returned pparams are the
	// post-fork pparams. The forger uses this to produce
	// era-correct blocks at fork boundaries.
	ProtocolParamsForSlot(slot uint64) lcommon.ProtocolParameters
}

// ChainTipProvider provides access to the current chain tip.
type ChainTipProvider interface {
	Tip() ochainsync.Tip
}

// EpochNonceProvider provides the epoch nonce for VRF proof generation.
type EpochNonceProvider interface {
	// CurrentEpoch returns the current epoch number.
	CurrentEpoch() uint64
	// EpochForSlot returns the epoch containing the given slot.
	EpochForSlot(slot uint64) (uint64, error)
	// EpochNonce returns the nonce for the given epoch.
	EpochNonce(epoch uint64) []byte
}

// TxValidator re-validates a transaction against the current ledger
// state at block assembly time. This catches transactions whose
// inputs have been consumed since they entered the mempool, protocol
// parameter changes, or other state mutations that invalidate
// previously-accepted transactions.
type TxValidator interface {
	ValidateTx(tx ledger.Transaction) error
}

// DefaultBlockBuilder implements BlockBuilder using LedgerState components.
type DefaultBlockBuilder struct {
	logger          *slog.Logger
	mempool         MempoolProvider
	pparamsProvider ProtocolParamsProvider
	chainTip        ChainTipProvider
	epochNonce      EpochNonceProvider
	creds           *PoolCredentials
	txValidator     TxValidator
}

// BlockBuilderConfig holds configuration for the DefaultBlockBuilder.
type BlockBuilderConfig struct {
	Logger          *slog.Logger
	Mempool         MempoolProvider
	PParamsProvider ProtocolParamsProvider
	ChainTip        ChainTipProvider
	EpochNonce      EpochNonceProvider
	Credentials     *PoolCredentials
	// TxValidator optionally re-validates each transaction against
	// the current ledger state before including it in a block.
	// When nil, ledger-level re-validation is skipped (but
	// intra-block double-spend detection still applies).
	TxValidator TxValidator
}

// NewDefaultBlockBuilder creates a new DefaultBlockBuilder.
func NewDefaultBlockBuilder(cfg BlockBuilderConfig) (*DefaultBlockBuilder, error) {
	if cfg.Mempool == nil {
		return nil, errors.New("mempool provider is required")
	}
	if cfg.PParamsProvider == nil {
		return nil, errors.New("protocol params provider is required")
	}
	if cfg.ChainTip == nil {
		return nil, errors.New("chain tip provider is required")
	}
	if cfg.EpochNonce == nil {
		return nil, errors.New("epoch nonce provider is required")
	}
	if cfg.Credentials == nil {
		return nil, errors.New("pool credentials are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &DefaultBlockBuilder{
		logger:          cfg.Logger,
		mempool:         cfg.Mempool,
		pparamsProvider: cfg.PParamsProvider,
		chainTip:        cfg.ChainTip,
		epochNonce:      cfg.EpochNonce,
		creds:           cfg.Credentials,
		txValidator:     cfg.TxValidator,
	}, nil
}

// BuildBlock creates a new block for the given slot.
// Returns the block and its CBOR encoding.
func (b *DefaultBlockBuilder) BuildBlock(
	slot uint64,
	kesPeriod uint64,
) (ledger.Block, []byte, error) {
	// Get current chain tip
	currentTip := b.chainTip.Tip()

	// Block numbers are 0-indexed in Cardano: the first block after
	// genesis is BlockNo 0. When the tip is genesis (empty hash), the
	// chain has no blocks yet so the next block number is 0.
	isGenesis := len(currentTip.Point.Hash) == 0

	var nextBlockNumber uint64
	if !isGenesis {
		if currentTip.BlockNumber == math.MaxUint64 {
			return nil, nil, errors.New(
				"block number overflow: chain tip is at max uint64",
			)
		}
		nextBlockNumber = currentTip.BlockNumber + 1
	}

	// Get protocol parameters for the slot being forged. This
	// projects forward through any scheduled fork (TriggerAtEpoch)
	// whose epoch lies at or before the slot, so the forger
	// produces an era-correct block at a fork boundary even when
	// the in-memory rollover has not yet seen a post-fork peer
	// block.
	pparams := b.pparamsProvider.ProtocolParamsForSlot(slot)
	if pparams == nil {
		return nil, nil, errors.New("failed to get protocol parameters")
	}

	// Read pparams limits via per-era dispatch. The pparams type
	// drives the block layout (Shelley/Allegra/Mary/Alonzo run
	// TPraos; Babbage/Conway run Praos).
	limits, err := extractPParamsLimits(pparams)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build block: %w", err)
	}

	var (
		// Per-tx CBOR is captured as raw bytes so block construction
		// is era-agnostic at the slice level. Initialize as non-nil
		// empty so encoding emits 0x80 (empty array), not 0xF6 (null);
		// the Cardano CDDL requires arrays here.
		transactionBodies      = []cbor.RawMessage{}
		transactionWitnessSets = []cbor.RawMessage{}
		transactionMetadataSet = make(map[uint]cbor.RawMessage)
		blockSize              uint64
		totalExUnits           lcommon.ExUnits
		maxTxSize              = limits.maxTxSize
		maxBlockSize           = limits.maxBlockSize
		maxExUnits             = limits.maxExUnits
	)

	b.logger.Debug(
		"protocol parameter limits",
		"component", "forging",
		"max_tx_size", maxTxSize,
		"max_block_size", maxBlockSize,
		"max_ex_units", maxExUnits,
	)

	mempoolTxs := b.mempool.Transactions()
	b.logger.Debug(
		"found transactions in mempool",
		"component", "forging",
		"tx_count", len(mempoolTxs),
	)

	// Track UTxO inputs consumed by transactions already selected
	// for this block. This detects intra-block double-spends where
	// two mempool transactions attempt to spend the same UTxO.
	consumedInputs := make(map[string]struct{})

	// Iterate through transactions and add them until we hit limits
	for _, mempoolTx := range mempoolTxs {
		// Use raw CBOR from the mempool transaction
		txCbor := mempoolTx.Cbor
		txSize := uint64(len(txCbor))

		// Check MaxTxSize limit
		if txSize > maxTxSize {
			b.logger.Debug(
				"skipping transaction - exceeds MaxTxSize",
				"component", "forging",
				"tx_size", txSize,
				"max_tx_size", maxTxSize,
			)
			continue
		}

		// Check MaxBlockSize limit
		if blockSize+txSize > maxBlockSize {
			b.logger.Debug(
				"block size limit reached",
				"component", "forging",
				"current_size", blockSize,
				"tx_size", txSize,
				"max_block_size", maxBlockSize,
			)
			break
		}

		// Decode the transaction CBOR into a typed era-specific
		// transaction via the mempool's tx-type tag. The decoded
		// instance is used only for in-memory inspection (Inputs,
		// Witnesses, AuxiliaryData) — its raw body / witness CBOR
		// is what gets stitched into the block below.
		fullTx, err := decodeMempoolTx(mempoolTx)
		if err != nil {
			b.logger.Debug(
				"failed to decode full transaction, skipping",
				"component", "forging",
				"error", err,
			)
			continue
		}

		// Re-validate the transaction against the current ledger
		// state. Between mempool admission and block assembly,
		// UTxOs may have been consumed, protocol parameters may
		// have changed, or other state mutations may have
		// invalidated the transaction.
		if b.txValidator != nil {
			if err := b.txValidator.ValidateTx(fullTx); err != nil {
				b.logger.Debug(
					"skipping transaction - failed re-validation",
					"component", "forging",
					"tx_hash", mempoolTx.Hash,
					"error", err,
				)
				continue
			}
		}

		// Check for intra-block double-spends: if any input of
		// this transaction was already consumed by an earlier
		// transaction in this block candidate, skip it.
		txInputKeys := make([]string, 0, len(fullTx.Inputs()))
		doubleSpend := false
		for _, input := range fullTx.Inputs() {
			key := fmt.Sprintf(
				"%s:%d",
				input.Id().String(),
				input.Index(),
			)
			if _, exists := consumedInputs[key]; exists {
				b.logger.Debug(
					"skipping transaction - double-spend within block",
					"component", "forging",
					"tx_hash", mempoolTx.Hash,
					"conflicting_input", key,
				)
				doubleSpend = true
				break
			}
			txInputKeys = append(txInputKeys, key)
		}
		if doubleSpend {
			continue
		}

		// Pull ExUnits from redeemers in the witness set
		var estimatedTxExUnits lcommon.ExUnits
		var exUnitsErr error
		if witnesses := fullTx.Witnesses(); witnesses != nil {
			if redeemers := witnesses.Redeemers(); redeemers != nil {
				for _, redeemer := range redeemers.Iter() {
					estimatedTxExUnits, exUnitsErr = eras.SafeAddExUnits(
						estimatedTxExUnits,
						redeemer.ExUnits,
					)
					if exUnitsErr != nil {
						b.logger.Debug(
							"skipping transaction - ExUnits overflow",
							"component", "forging",
							"error", exUnitsErr,
						)
						break
					}
				}
			}
		}
		if exUnitsErr != nil {
			continue
		}

		// Check MaxExUnits limit - skip this tx but continue trying
		// smaller ones, matching the MaxTxSize behavior above.
		// Use SafeAddExUnits to avoid overflow in the comparison.
		candidateExUnits, addErr := eras.SafeAddExUnits(
			totalExUnits,
			estimatedTxExUnits,
		)
		if addErr != nil ||
			candidateExUnits.Memory > maxExUnits.Memory ||
			candidateExUnits.Steps > maxExUnits.Steps {
			b.logger.Debug(
				"tx exceeds remaining ex units budget, skipping",
				"component", "forging",
				"current_memory", totalExUnits.Memory,
				"current_steps", totalExUnits.Steps,
				"tx_memory", estimatedTxExUnits.Memory,
				"tx_steps", estimatedTxExUnits.Steps,
				"max_memory", maxExUnits.Memory,
				"max_steps", maxExUnits.Steps,
			)
			continue
		}

		// Handle metadata encoding before adding transaction.
		var metadataCbor cbor.RawMessage
		if aux := fullTx.AuxiliaryData(); aux != nil {
			ac := aux.Cbor()
			if len(ac) > 0 &&
				(len(ac) != 1 || (ac[0] != 0xF6 && ac[0] != 0xF5 && ac[0] != 0xF4)) {
				metadataCbor = ac
			}
		}
		if metadataCbor == nil && fullTx.Metadata() != nil {
			var err error
			metadataCbor, err = cbor.Encode(fullTx.Metadata())
			if err != nil {
				b.logger.Debug(
					"failed to encode transaction metadata",
					"component", "forging",
					"error", err,
				)
				continue
			}
		}

		// Add transaction to our lists for later block creation.
		// Splitting at the byte level keeps block assembly era-
		// agnostic: we don't need typed body / witness slices once
		// we have the canonical encoded forms. fullTx.Cbor() returns
		// the original mempool bytes (preserved via the gouroboros
		// types' DecodeStoreCbor / SetCborReference machinery).
		fullTxCbor := fullTx.Cbor()
		bodyBytes, witnessBytes, extractErr := splitTxCbor(fullTxCbor)
		if extractErr != nil {
			b.logger.Debug(
				"failed to split tx CBOR into body+witnesses, skipping",
				"component", "forging",
				"tx_hash", mempoolTx.Hash,
				"error", extractErr,
			)
			continue
		}
		transactionBodies = append(transactionBodies, bodyBytes)
		transactionWitnessSets = append(transactionWitnessSets, witnessBytes)
		if metadataCbor != nil {
			transactionMetadataSet[uint(len(transactionBodies))-1] = metadataCbor
		}
		blockSize += txSize
		// Safe to assign: overflow was already checked
		// via SafeAddExUnits when computing
		// candidateExUnits above.
		totalExUnits = candidateExUnits

		// Record consumed inputs so later transactions in this
		// block cannot spend the same UTxOs.
		for _, key := range txInputKeys {
			consumedInputs[key] = struct{}{}
		}

		b.logger.Debug(
			"added transaction to block candidate lists",
			"component", "forging",
			"tx_size", txSize,
			"block_size", blockSize,
			"tx_count", len(transactionBodies),
			"total_memory", totalExUnits.Memory,
			"total_steps", totalExUnits.Steps,
		)
	}

	// Encode the transaction metadata set (always non-nil, initialized above).
	metadataCbor, err := cbor.Encode(transactionMetadataSet)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to encode transaction metadata set: %w",
			err,
		)
	}
	var metadataSet lcommon.TransactionMetadataSet
	err = metadataSet.UnmarshalCBOR(metadataCbor)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to unmarshal transaction metadata set: %w",
			err,
		)
	}

	// Compute block body hash: blake2b_256(hash_tx || hash_wit || hash_aux [|| hash_invalid]).
	// The invalid-transactions hash component is included from Alonzo
	// onward (the era that introduced Plutus, and with it the
	// invalid_transactions list).
	bodyHash, actualBlockBodySize, err := computeBlockBodyHash(
		limits.era,
		transactionBodies,
		transactionWitnessSets,
		metadataSet,
		[]uint{},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compute block body hash: %w", err)
	}

	// Get VRF key from credentials
	vrfVKey := b.creds.GetVRFVKey()
	if len(vrfVKey) == 0 {
		return nil, nil, errors.New("VRF verification key not loaded")
	}
	if len(vrfVKey) != 32 {
		return nil, nil, fmt.Errorf(
			"invalid VRF verification key size: got %d bytes, expected 32",
			len(vrfVKey),
		)
	}

	// Compute VRF proof(s) for leader election. Use the block slot's
	// epoch rather than the ledger's current epoch because the slot
	// clock can reach a new epoch before block processing commits the
	// epoch rollover.
	//
	// Praos eras (Babbage/Conway) carry a single combined VRF result
	// in the header. TPraos eras (Shelley→Alonzo) carry two: one for
	// the epoch nonce contribution (NonceVrf, seed = SeedEta) and one
	// for leader eligibility (LeaderVrf, seed = SeedL). Both TPraos
	// proofs use the same VRF key but different inputs constructed via
	// vrf.MkSeedTPraos.
	blockEpoch, err := b.epochNonce.EpochForSlot(slot)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to resolve epoch for slot %d: %w",
			slot,
			err,
		)
	}
	epochNonce := b.epochNonce.EpochNonce(blockEpoch)
	if len(epochNonce) == 0 {
		return nil, nil, errors.New("epoch nonce not available")
	}

	// Validate slot fits in int64 before conversion for VRF input
	if slot > math.MaxInt64 {
		return nil, nil, fmt.Errorf("slot %d exceeds int64 max", slot)
	}

	var (
		praosVrf            lcommon.VrfResult
		nonceVrf, leaderVrf lcommon.VrfResult
	)
	if limits.era.isTPraos() {
		nonceInput, err := vrf.MkSeedTPraos(int64(slot), epochNonce, vrf.SeedEta()) // #nosec G115 -- validated above
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create TPraos nonce VRF input: %w", err)
		}
		nonceProof, nonceOutput, err := b.creds.VRFProve(nonceInput)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate TPraos nonce VRF proof: %w", err)
		}
		leaderInput, err := vrf.MkSeedTPraos(int64(slot), epochNonce, vrf.SeedL()) // #nosec G115 -- validated above
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create TPraos leader VRF input: %w", err)
		}
		leaderProof, leaderOutput, err := b.creds.VRFProve(leaderInput)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate TPraos leader VRF proof: %w", err)
		}
		nonceVrf = lcommon.VrfResult{Output: nonceOutput, Proof: nonceProof}
		leaderVrf = lcommon.VrfResult{Output: leaderOutput, Proof: leaderProof}
	} else {
		alpha, err := vrf.MkInputVrf(int64(slot), epochNonce) // #nosec G115 -- validated above
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create VRF input: %w", err)
		}
		vrfProof, vrfOutput, err := b.creds.VRFProve(alpha)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate VRF proof: %w", err)
		}
		praosVrf = lcommon.VrfResult{Output: vrfOutput, Proof: vrfProof}
	}

	// Get OpCert from credentials
	opCert := b.creds.GetOpCert()
	if opCert == nil {
		return nil, nil, errors.New("operational certificate not loaded")
	}
	// Validate OpCert values fit in uint32 before conversion
	if opCert.IssueNumber > math.MaxUint32 {
		return nil, nil, fmt.Errorf(
			"OpCert issue number %d exceeds uint32 max",
			opCert.IssueNumber,
		)
	}
	if opCert.KESPeriod > math.MaxUint32 {
		return nil, nil, fmt.Errorf(
			"OpCert KES period %d exceeds uint32 max",
			opCert.KESPeriod,
		)
	}
	// Get issuer vkey (cold vkey) from operational certificate.
	// The IssuerVkey identifies the pool operator via their cold key.
	issuerVKey := opCert.ColdVKey
	if len(issuerVKey) != 32 {
		return nil, nil, fmt.Errorf(
			"invalid cold verification key size: expected 32, got %d",
			len(issuerVKey),
		)
	}
	var issuerVKeyArray lcommon.IssuerVkey
	copy(issuerVKeyArray[:], issuerVKey)

	// Build header body with nullable PrevHash so the first block
	// after genesis encodes prevHash as CBOR null (required by the
	// Cardano protocol).
	var prevHash *lcommon.Blake2b256
	if !isGenesis {
		if len(currentTip.Point.Hash) != 32 {
			return nil, nil, fmt.Errorf(
				"invalid tip hash length: expected 32 (Blake2b-256), got %d",
				len(currentTip.Point.Hash),
			)
		}
		h := lcommon.NewBlake2b256(currentTip.Point.Hash)
		prevHash = &h
	}

	// The header body shape differs between Praos and TPraos eras:
	// Praos packs OpCert/ProtoVersion as nested structs and carries a
	// single combined VrfResult; TPraos uses flat OpCert/proto fields
	// and carries separate NonceVrf and LeaderVrf results. KESSign
	// signs whichever encoded body shape we hand it.
	var headerBody any
	if limits.era.isTPraos() {
		headerBody = tpraosHeaderBody{
			BlockNumber:          nextBlockNumber,
			Slot:                 slot,
			PrevHash:             prevHash,
			IssuerVkey:           issuerVKeyArray,
			VrfKey:               vrfVKey,
			NonceVrf:             nonceVrf,
			LeaderVrf:            leaderVrf,
			BlockBodySize:        actualBlockBodySize,
			BlockBodyHash:        bodyHash,
			OpCertHotVkey:        opCert.KESVKey,
			OpCertSequenceNumber: uint32(opCert.IssueNumber), // #nosec G115 -- validated above
			OpCertKesPeriod:      uint32(opCert.KESPeriod),   // #nosec G115 -- validated above
			OpCertSignature:      opCert.Signature,
			ProtoMajorVersion:    limits.protoMajor,
			ProtoMinorVersion:    dingoversion.BlockHeaderProtocolMinor,
		}
	} else {
		headerBody = nullablePrevHashHeaderBody{
			BlockNumber:   nextBlockNumber,
			Slot:          slot,
			PrevHash:      prevHash,
			IssuerVkey:    issuerVKeyArray,
			VrfKey:        vrfVKey,
			VrfResult:     praosVrf,
			BlockBodySize: actualBlockBodySize,
			BlockBodyHash: bodyHash,
			OpCert: babbage.BabbageOpCert{
				HotVkey:        opCert.KESVKey,
				SequenceNumber: uint32(opCert.IssueNumber), // #nosec G115 -- validated above
				KesPeriod:      uint32(opCert.KESPeriod),   // #nosec G115 -- validated above
				Signature:      opCert.Signature,
			},
			ProtoVersion: babbage.BabbageProtoVersion{
				Major: limits.protoMajor,
				Minor: dingoversion.BlockHeaderProtocolMinor,
			},
		}
	}

	// Sign the block header with KES.
	// First, we need to serialize the header body for signing.
	headerBodyCbor, err := cbor.Encode(headerBody)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode header body: %w", err)
	}

	signature, err := b.creds.KESSign(kesPeriod, headerBodyCbor)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sign block header: %w", err)
	}

	// Build the block CBOR using the pre-encoded header body to
	// ensure the prevHash encoding (null vs bytes) matches what was
	// signed. Re-encoding via the gouroboros struct types would
	// lose the null encoding for genesis blocks.
	headerCbor, err := cbor.Encode(rawBlockHeader{
		Body:      cbor.RawMessage(headerBodyCbor),
		Signature: signature,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode block header: %w", err)
	}
	blockCbor, err := encodeBlockCbor(
		limits.era,
		cbor.RawMessage(headerCbor),
		transactionBodies,
		transactionWitnessSets,
		metadataSet,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to marshal forged block to CBOR: %w",
			err,
		)
	}

	// Re-decode block from CBOR via the era-correct constructor.
	// Hash() and other accessors expect the original CBOR to be
	// stored on the typed block, so we round-trip through the
	// matching era's block decoder.
	ledgerBlock, err := decodeBlockFromCbor(limits.era, blockCbor)
	if err != nil {
		// The forged block failed to round-trip through its own era
		// decoder, so this winning slot would be silently dropped. Dump
		// the generated block in Cardano-aware CBOR diagnostic notation
		// so the encoder/decoder wire-shape mismatch is diagnosable from
		// the logs rather than only via the opaque unmarshal error
		// (issue #2063).
		b.logger.Error(
			"forged block failed to re-decode; dumping CBOR diagnostics",
			"component", "forging",
			"era", limits.era,
			"proto_major", limits.protoMajor,
			"tx_count", len(transactionBodies),
			"block_cbor_len", len(blockCbor),
			"error", err,
			"diagnostics", forgedBlockDiagnostics(blockCbor),
		)
		return nil, nil, fmt.Errorf(
			"failed to unmarshal forged block from generated CBOR: %w",
			err,
		)
	}

	b.logger.Debug(
		"successfully built block",
		"component", "forging",
		"era_major", limits.protoMajor,
		"slot", ledgerBlock.SlotNumber(),
		"hash", ledgerBlock.Hash(),
		"block_number", ledgerBlock.BlockNumber(),
		"prev_hash", ledgerBlock.PrevHash(),
		"block_size", blockSize,
		"tx_count", len(transactionBodies),
		"total_memory", totalExUnits.Memory,
		"total_steps", totalExUnits.Steps,
	)

	return ledgerBlock, blockCbor, nil
}

// computeBlockBodyHash computes the block body hash as per Cardano spec.
//
// Pre-Alonzo (Shelley/Allegra/Mary) hash three components:
//
//	blake2b_256(hash_tx || hash_wit || hash_aux)
//
// Alonzo and later add a fourth, the invalid_transactions list:
//
//	blake2b_256(hash_tx || hash_wit || hash_aux || hash_invalid)
//
// Each component is the blake2b_256 hash of its CBOR-encoded data.
// Returns the body hash and the total body size (sum of all
// component encoding sizes), which the header records.
//
// txBodies and witnessSets are raw, era-specific transaction-CBOR
// slices; the slice envelopes themselves encode identically across
// eras.
func computeBlockBodyHash(
	era eraKind,
	txBodies []cbor.RawMessage,
	witnessSets []cbor.RawMessage,
	metadataSet lcommon.TransactionMetadataSet,
	invalidTxs []uint,
) (lcommon.Blake2b256, uint64, error) {
	// Normalize nil slices to empty so CBOR encodes as 0x80 (empty
	// array) rather than 0xf6 (null). This must match the encoding
	// in the era's Block.MarshalCBOR, which applies the same
	// normalization before serializing the block.
	if txBodies == nil {
		txBodies = []cbor.RawMessage{}
	}
	if witnessSets == nil {
		witnessSets = []cbor.RawMessage{}
	}
	if invalidTxs == nil {
		invalidTxs = []uint{}
	}

	var bodyHashes []byte
	var totalSize uint64

	// Hash transaction bodies
	txBodiesCbor, err := cbor.Encode(txBodies)
	if err != nil {
		return lcommon.Blake2b256{}, 0, fmt.Errorf(
			"failed to encode transaction bodies: %w",
			err,
		)
	}
	txBodiesHash := blake2b.Sum256(txBodiesCbor)
	bodyHashes = append(bodyHashes, txBodiesHash[:]...)
	totalSize += uint64(len(txBodiesCbor))

	// Hash witness sets
	witnessesCbor, err := cbor.Encode(witnessSets)
	if err != nil {
		return lcommon.Blake2b256{}, 0, fmt.Errorf(
			"failed to encode witness sets: %w",
			err,
		)
	}
	witnessesHash := blake2b.Sum256(witnessesCbor)
	bodyHashes = append(bodyHashes, witnessesHash[:]...)
	totalSize += uint64(len(witnessesCbor))

	// Hash metadata set
	metadataCbor, err := cbor.Encode(metadataSet)
	if err != nil {
		return lcommon.Blake2b256{}, 0, fmt.Errorf(
			"failed to encode metadata set: %w",
			err,
		)
	}
	metadataHash := blake2b.Sum256(metadataCbor)
	bodyHashes = append(bodyHashes, metadataHash[:]...)
	totalSize += uint64(len(metadataCbor))

	if era.hasInvalidTxs() {
		// gouroboros's body-hash validator (common.ValidateBlockBodyHash)
		// hashes the raw CBOR bytes of each block field as they appear
		// on the wire — including framing — so the invalid_transactions
		// hash component must be encoded the same way encodeBlockCbor
		// emits the field. Alonzo uses indefinite-length framing for
		// this list; Babbage and Conway use definite-length.
		var invalidCbor []byte
		var err error
		if era.usesIndefInvalidList() {
			indef := make(cbor.IndefLengthList, 0, len(invalidTxs))
			for _, v := range invalidTxs {
				indef = append(indef, v)
			}
			invalidCbor, err = cbor.Encode(indef)
		} else {
			invalidCbor, err = cbor.Encode(invalidTxs)
		}
		if err != nil {
			return lcommon.Blake2b256{}, 0, fmt.Errorf(
				"failed to encode invalid transactions: %w",
				err,
			)
		}
		invalidHash := blake2b.Sum256(invalidCbor)
		bodyHashes = append(bodyHashes, invalidHash[:]...)
		totalSize += uint64(len(invalidCbor))
	}

	if era == eraDijkstra {
		// Dijkstra blocks append two nullable certificate fields after
		// invalid_transactions — leios_cert then peras_cert (CDDL
		// "<cert> / nil"). A producer that forges no endorser block emits
		// both as CBOR null, and DijkstraBlockBody.Hash hashes all six body
		// components, so the header body hash must include both null-cert
		// components or the forged block fails body-hash validation. The
		// same null encoding is emitted by encodeBlockCbor.
		nullCert, err := cbor.Encode(nil)
		if err != nil {
			return lcommon.Blake2b256{}, 0, fmt.Errorf(
				"failed to encode null Dijkstra certificate: %w",
				err,
			)
		}
		nullHash := blake2b.Sum256(nullCert)
		// leios_cert, then peras_cert.
		bodyHashes = append(bodyHashes, nullHash[:]...)
		bodyHashes = append(bodyHashes, nullHash[:]...)
		totalSize += 2 * uint64(len(nullCert))
	}

	// Final hash of concatenated hashes
	finalHash := blake2b.Sum256(bodyHashes)
	return lcommon.NewBlake2b256(finalHash[:]), totalSize, nil
}

// nullablePrevHashHeaderBody mirrors BabbageBlockHeaderBody but uses a
// pointer for PrevHash so nil encodes as CBOR null (genesis origin).
// Used for Babbage and Conway (Praos) header bodies.
type nullablePrevHashHeaderBody struct {
	cbor.StructAsArray
	BlockNumber   uint64
	Slot          uint64
	PrevHash      *lcommon.Blake2b256
	IssuerVkey    lcommon.IssuerVkey
	VrfKey        []byte
	VrfResult     lcommon.VrfResult
	BlockBodySize uint64
	BlockBodyHash lcommon.Blake2b256
	OpCert        babbage.BabbageOpCert
	ProtoVersion  babbage.BabbageProtoVersion
}

// tpraosHeaderBody mirrors ShelleyBlockHeaderBody (used by Shelley,
// Allegra, Mary, and Alonzo via embedding) but uses a pointer for
// PrevHash so nil encodes as CBOR null at the Shelley boundary. The
// flat OpCert and protocol-version fields differ from the struct-shape
// equivalents in BabbageBlockHeaderBody — TPraos pre-dates the
// header-body refactor that introduced the nested structs.
type tpraosHeaderBody struct {
	cbor.StructAsArray
	BlockNumber          uint64
	Slot                 uint64
	PrevHash             *lcommon.Blake2b256
	IssuerVkey           lcommon.IssuerVkey
	VrfKey               []byte
	NonceVrf             lcommon.VrfResult
	LeaderVrf            lcommon.VrfResult
	BlockBodySize        uint64
	BlockBodyHash        lcommon.Blake2b256
	OpCertHotVkey        []byte
	OpCertSequenceNumber uint32
	OpCertKesPeriod      uint32
	OpCertSignature      []byte
	ProtoMajorVersion    uint64
	ProtoMinorVersion    uint64
}

// rawBlockHeader encodes a block header with a pre-encoded body. This
// preserves the exact CBOR bytes that were KES-signed.
type rawBlockHeader struct {
	cbor.StructAsArray
	Body      cbor.RawMessage
	Signature []byte
}

// rawShelleyEraBlock encodes a Shelley/Allegra/Mary block: 4 elements,
// no invalid_transactions list.
type rawShelleyEraBlock struct {
	cbor.StructAsArray
	Header                 cbor.RawMessage
	TransactionBodies      []cbor.RawMessage
	TransactionWitnessSets []cbor.RawMessage
	TransactionMetadataSet lcommon.TransactionMetadataSet
}

// rawAlonzoBlock encodes an Alonzo block: 5 elements, with an
// indefinite-length invalid_transactions list (matching the on-chain
// encoding gouroboros's AlonzoBlock.MarshalCBOR emits).
type rawAlonzoBlock struct {
	cbor.StructAsArray
	Header                 cbor.RawMessage
	TransactionBodies      []cbor.RawMessage
	TransactionWitnessSets []cbor.RawMessage
	TransactionMetadataSet lcommon.TransactionMetadataSet
	InvalidTransactions    cbor.IndefLengthList
}

// rawBabbageEraBlock encodes a Babbage/Conway block: 5 elements with a
// definite-length invalid_transactions list.
type rawBabbageEraBlock struct {
	cbor.StructAsArray
	Header                 cbor.RawMessage
	TransactionBodies      []cbor.RawMessage
	TransactionWitnessSets []cbor.RawMessage
	TransactionMetadataSet lcommon.TransactionMetadataSet
	InvalidTransactions    []uint
}

// rawDijkstraBlock encodes a Dijkstra block: 7 elements — the
// Babbage/Conway five plus the two trailing nullable certificate fields
// leios_cert and peras_cert (CDDL "<cert> / nil"). The forge produces no
// endorser block, so both certificates are CBOR null.
type rawDijkstraBlock struct {
	cbor.StructAsArray
	Header                 cbor.RawMessage
	TransactionBodies      []cbor.RawMessage
	TransactionWitnessSets []cbor.RawMessage
	TransactionMetadataSet lcommon.TransactionMetadataSet
	InvalidTransactions    []uint
	LeiosCertificate       cbor.RawMessage
	PerasCertificate       cbor.RawMessage
}

// encodeBlockCbor selects the era-correct raw-block envelope and
// encodes it. The forge path never produces invalid_transactions so
// the list is always empty when present; we still emit it because
// each Alonzo+ decoder expects exactly five array elements.
func encodeBlockCbor(
	era eraKind,
	header cbor.RawMessage,
	txBodies []cbor.RawMessage,
	witnessSets []cbor.RawMessage,
	metadataSet lcommon.TransactionMetadataSet,
) ([]byte, error) {
	if era == eraDijkstra {
		// Dijkstra appends leios_cert and peras_cert after
		// invalid_transactions; both are CBOR null when no endorser
		// block is forged. The same null encoding feeds the body hash
		// in computeBlockBodyHash, so they stay consistent.
		nullCert, err := cbor.Encode(nil)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to encode null Dijkstra certificate: %w",
				err,
			)
		}
		return cbor.Encode(rawDijkstraBlock{
			Header:                 header,
			TransactionBodies:      txBodies,
			TransactionWitnessSets: witnessSets,
			TransactionMetadataSet: metadataSet,
			InvalidTransactions:    []uint{},
			LeiosCertificate:       cbor.RawMessage(nullCert),
			PerasCertificate:       cbor.RawMessage(nullCert),
		})
	}
	if !era.hasInvalidTxs() {
		return cbor.Encode(rawShelleyEraBlock{
			Header:                 header,
			TransactionBodies:      txBodies,
			TransactionWitnessSets: witnessSets,
			TransactionMetadataSet: metadataSet,
		})
	}
	if era.usesIndefInvalidList() {
		return cbor.Encode(rawAlonzoBlock{
			Header:                 header,
			TransactionBodies:      txBodies,
			TransactionWitnessSets: witnessSets,
			TransactionMetadataSet: metadataSet,
			InvalidTransactions:    cbor.IndefLengthList{},
		})
	}
	return cbor.Encode(rawBabbageEraBlock{
		Header:                 header,
		TransactionBodies:      txBodies,
		TransactionWitnessSets: witnessSets,
		TransactionMetadataSet: metadataSet,
		InvalidTransactions:    []uint{},
	})
}
