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

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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
}

// ChainTipProvider provides access to the current chain tip.
type ChainTipProvider interface {
	Tip() ochainsync.Tip
}

// EpochNonceProvider provides the epoch nonce for VRF proof generation.
type EpochNonceProvider interface {
	// CurrentEpoch returns the current epoch number.
	CurrentEpoch() uint64
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

	// Set Hash if empty (Blake2b-256 = 32 bytes)
	if len(currentTip.Point.Hash) == 0 {
		currentTip.Point.Hash = lcommon.Blake2b256{}.Bytes()
	}

	nextBlockNumber := currentTip.BlockNumber + 1

	// Get current protocol parameters for limits
	pparams := b.pparamsProvider.GetCurrentPParams()
	if pparams == nil {
		return nil, nil, errors.New("failed to get protocol parameters")
	}

	// Safely cast protocol parameters to Conway type
	conwayPParams, ok := pparams.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil, nil, errors.New("protocol parameters are not Conway type")
	}

	var (
		// Initialize as non-nil empty slices so CBOR encoding
		// produces empty arrays (0x80) rather than null (0xF6).
		// Haskell's CDDL requires arrays here, not null.
		transactionBodies      = []conway.ConwayTransactionBody{}
		transactionWitnessSets = []conway.ConwayTransactionWitnessSet{}
		transactionMetadataSet = make(map[uint]cbor.RawMessage)
		blockSize              uint64
		totalExUnits           lcommon.ExUnits
		maxTxSize              = uint64(conwayPParams.MaxTxSize)
		maxBlockSize           = uint64(conwayPParams.MaxBlockBodySize)
		maxExUnits             = conwayPParams.MaxBlockExUnits
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

		// Decode the transaction CBOR into full Conway transaction
		fullTx, err := conway.NewConwayTransactionFromCbor(txCbor)
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
		for _, redeemer := range fullTx.WitnessSet.Redeemers().Iter() {
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

		// Add transaction to our lists for later block creation
		transactionBodies = append(transactionBodies, fullTx.Body)
		transactionWitnessSets = append(
			transactionWitnessSets,
			fullTx.WitnessSet,
		)
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

	// Compute block body hash: blake2b_256(hash_tx || hash_wit || hash_aux || hash_invalid)
	// Each component is the blake2b_256 hash of its CBOR-encoded data.
	// Also returns the actual block body size for the header.
	bodyHash, actualBlockBodySize, err := computeBlockBodyHash(
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

	// Compute VRF proof for leader election
	currentEpoch := b.epochNonce.CurrentEpoch()
	epochNonce := b.epochNonce.EpochNonce(currentEpoch)
	if len(epochNonce) == 0 {
		return nil, nil, errors.New("epoch nonce not available")
	}

	// Validate slot fits in int64 before conversion for VRF input
	if slot > math.MaxInt64 {
		return nil, nil, fmt.Errorf("slot %d exceeds int64 max", slot)
	}

	// Generate VRF proof using MkInputVrf(slot, epochNonce)
	alpha := vrf.MkInputVrf(int64(slot), epochNonce) // #nosec G115 -- validated above
	vrfProof, vrfOutput, err := b.creds.VRFProve(alpha)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate VRF proof: %w", err)
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
	opCertBody := babbage.BabbageOpCert{
		HotVkey:        opCert.KESVKey,
		SequenceNumber: uint32(opCert.IssueNumber), // #nosec G115 -- validated above
		KesPeriod:      uint32(opCert.KESPeriod),   // #nosec G115 -- validated above
		Signature:      opCert.Signature,
	}

	// Get issuer vkey (cold vkey) from operational certificate
	// The IssuerVkey identifies the pool operator via their cold key
	issuerVKey := opCert.ColdVKey
	if len(issuerVKey) != 32 {
		return nil, nil, fmt.Errorf(
			"invalid cold verification key size: expected 32, got %d",
			len(issuerVKey),
		)
	}
	var issuerVKeyArray lcommon.IssuerVkey
	copy(issuerVKeyArray[:], issuerVKey)

	// Create Babbage block header body
	headerBody := babbage.BabbageBlockHeaderBody{
		BlockNumber: nextBlockNumber,
		Slot:        slot,
		PrevHash:    lcommon.NewBlake2b256(currentTip.Point.Hash),
		IssuerVkey:  issuerVKeyArray,
		VrfKey:      vrfVKey,
		VrfResult: lcommon.VrfResult{
			Output: vrfOutput,
			Proof:  vrfProof,
		},
		BlockBodySize: actualBlockBodySize,
		BlockBodyHash: bodyHash,
		OpCert:        opCertBody,
		ProtoVersion: babbage.BabbageProtoVersion{
			Major: 10, // Conway era protocol version
			Minor: 0,
		},
	}

	// Sign the block header with KES
	// First, we need to serialize the header body for signing
	headerBodyCbor, err := cbor.Encode(headerBody)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode header body: %w", err)
	}

	signature, err := b.creds.KESSign(kesPeriod, headerBodyCbor)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sign block header: %w", err)
	}

	// Create Conway block header
	conwayHeader := &conway.ConwayBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body:      headerBody,
			Signature: signature,
		},
	}

	// Create a conway block with transactions
	conwayBlock := &conway.ConwayBlock{
		BlockHeader:            conwayHeader,
		TransactionBodies:      transactionBodies,
		TransactionWitnessSets: transactionWitnessSets,
		TransactionMetadataSet: metadataSet,
		InvalidTransactions:    []uint{},
	}

	// Marshal the conway block to CBOR
	blockCbor, err := cbor.Encode(conwayBlock)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to marshal forged conway block to CBOR: %w",
			err,
		)
	}

	// Re-decode block from CBOR
	// This is needed because things like Hash() rely on having the original CBOR available
	ledgerBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to unmarshal forged Conway block from generated CBOR: %w",
			err,
		)
	}

	b.logger.Debug(
		"successfully built conway block",
		"component", "forging",
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

// computeBlockBodyHash computes the block body hash as per Cardano spec:
// blake2b_256(hash_tx || hash_wit || hash_aux || hash_invalid)
// where each hash is blake2b_256 of the CBOR-encoded data.
// Also returns the total block body size (sum of all component sizes).
func computeBlockBodyHash(
	txBodies []conway.ConwayTransactionBody,
	witnessSets []conway.ConwayTransactionWitnessSet,
	metadataSet lcommon.TransactionMetadataSet,
	invalidTxs []uint,
) (lcommon.Blake2b256, uint64, error) {
	// Normalize nil slices to empty so CBOR encodes as 0x80 (empty
	// array) rather than 0xf6 (null). This must match the encoding
	// in ConwayBlock.MarshalCBOR, which applies the same
	// normalization before serializing the block.
	if txBodies == nil {
		txBodies = []conway.ConwayTransactionBody{}
	}
	if witnessSets == nil {
		witnessSets = []conway.ConwayTransactionWitnessSet{}
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

	// Hash invalid transactions
	invalidCbor, err := cbor.Encode(invalidTxs)
	if err != nil {
		return lcommon.Blake2b256{}, 0, fmt.Errorf(
			"failed to encode invalid transactions: %w",
			err,
		)
	}
	invalidHash := blake2b.Sum256(invalidCbor)
	bodyHashes = append(bodyHashes, invalidHash[:]...)
	totalSize += uint64(len(invalidCbor))

	// Final hash of concatenated hashes
	finalHash := blake2b.Sum256(bodyHashes)
	return lcommon.NewBlake2b256(finalHash[:]), totalSize, nil
}
