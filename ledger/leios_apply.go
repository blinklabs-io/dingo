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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosEndorserBlockReferencer is implemented by a block header that announces
// a Leios endorser block via its header extension. As of prototype-2026w29 that
// is the leios_announcement field ([announced_eb, announced_eb_size]).
type leiosEndorserBlockReferencer interface {
	LeiosAnnouncement() (lcommon.Blake2b256, uint64, bool)
}

// Compile-time guard: the Dijkstra header must satisfy the announcer interface.
// A type-assertion against this interface compiles even when the header no
// longer implements it (it just returns ok=false at runtime), which previously
// let a header-accessor rename silently disable endorser-block application.
var _ leiosEndorserBlockReferencer = (*dijkstra.DijkstraBlockHeader)(nil)

// leiosEndorserBlockCertifier is implemented by a block header that can certify
// a previously announced endorser block. As of prototype-2026w29 a certifying
// ranking block (CertRB) carries a leios_certificate and certifies the endorser
// block announced by its parent (prevHash), while it may independently announce
// a new endorser block; the flag rides on the header's leios_certified extension
// field.
type leiosEndorserBlockCertifier interface {
	LeiosCertified() (certified bool, present bool)
}

var _ leiosEndorserBlockCertifier = (*dijkstra.DijkstraBlockHeader)(nil)

var errCertifiedEndorserBlockUnavailable = errors.New(
	"certified Leios endorser block unavailable",
)

// errLeiosCertificateWithoutAnnouncement is the verdict on a certifying ranking
// block whose parent announced no endorser block: there is nothing for the
// certificate to certify, so the block is invalid, as in the reference
// Forker.applyBlock (LeiosCertificateWithoutAnnouncement). It is not
// errCertifiedEndorserBlockUnavailable, which is retried while a closure that
// exists is fetched; no closure exists here, so retrying would only hold the
// pipeline on the block until the stuck-pipeline halt.
var errLeiosCertificateWithoutAnnouncement = errors.New(
	"certifying ranking block's parent announced no endorser block",
)

// ErrLeiosInvalidCertificate is the verdict on a certifying ranking block whose
// certificate does not verify against its parent's announcement, as in the
// reference Forker.applyBlock (LeiosInvalidCertificate). A
// LedgerStateConfig.ValidateLeiosCertificate implementation wraps it only for
// a verdict on the certificate itself; the ledger rejects the block for that
// error alone and retries any other.
var ErrLeiosInvalidCertificate = errors.New("invalid Leios certificate")

// rejectLeiosCertifyingBlock marks a verdict on a certifying ranking block as a
// rejected block. The block is already on the primary chain, so a plain error
// would restart the pipeline onto it until the stuck-pipeline halt; the
// headerValidationError lets the pipeline rewind past it and re-intersect, the
// path every other rejected block takes.
func (ls *LedgerState) rejectLeiosCertifyingBlock(
	block ledger.Block,
	cause error,
) error {
	point := ocommon.Point{
		Slot: block.SlotNumber(),
		Hash: block.Hash().Bytes(),
	}
	return &headerValidationError{
		BlockPoint: point,
		Cause:      cause,
		Source:     ls.deferredHeaderSource(point),
	}
}

const certifiedEndorserBlockRetryDelay = time.Second

// decodeEndorserTxEnvelope unwraps one endorser-block transaction entry and
// splits it into its transaction CBOR and its top-level array elements.
//
// leios-fetch carries each endorser transaction CBOR-in-CBOR: the tx_list entry
// is a CBOR byte string wrapping the transaction's own CBOR
// (LeiosTx = encodeBytes(txCbor)). A non-byte-string entry — major type != 2 —
// is already the bare transaction. elems[0] is the transaction body, which is
// both the transaction-offset payload and, hashed, the transaction id.
// Both decodes read bytes a peer delivered over leios-fetch, so they go
// through safedecode.Cbor: a decoder panic becomes the error this function
// already returns instead of unwinding into whatever goroutine is applying
// the endorser block. The function decodes into locals and returns them, so
// there is no shared state a contained panic could leave half-updated.
func decodeEndorserTxEnvelope(
	raw cbor.RawMessage,
) (txCbor []byte, elems []cbor.RawMessage, err error) {
	txCbor = []byte(raw)
	if len(txCbor) > 0 && txCbor[0]>>5 == 2 {
		inner, _, err := safedecode.Cbor[[]byte](txCbor)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"unwrap CBOR-in-CBOR entry: %w",
				err,
			)
		}
		txCbor = inner
	}
	elems, _, err = safedecode.Cbor[[]cbor.RawMessage](txCbor)
	if err != nil {
		return nil, nil, fmt.Errorf("decode envelope: %w", err)
	}
	if len(elems) < 2 {
		return nil, nil, fmt.Errorf(
			"envelope has %d elements, want >= 2",
			len(elems),
		)
	}
	return txCbor, elems, nil
}

// endorserBlockTxIds returns the transaction id of every transaction in an
// endorser block, without decoding the transactions themselves.
//
// A Cardano transaction id is blake2b-256 over the transaction body's CBOR —
// the definition DijkstraTransaction.Id inherits from
// BabbageTransactionBody.Id, which hashes the body bytes it decoded, i.e.
// exactly elems[0] here. Deriving the ids straight from the envelope keeps the
// cross-fork continuation audit off the full per-transaction decode path: an
// armed window can span continuationAuditBlockBudget blocks and an endorser
// block carries thousands of transactions, and the audit runs under
// chainsyncBlockfetchMutex, where a full decode of every endorser transaction
// would stall the blockfetch pipeline for a diagnostic.
func endorserBlockTxIds(rawTxs []cbor.RawMessage) ([][]byte, error) {
	ids := make([][]byte, 0, len(rawTxs))
	for i, raw := range rawTxs {
		_, elems, err := decodeEndorserTxEnvelope(raw)
		if err != nil {
			return nil, fmt.Errorf("endorser tx %d: %w", i, err)
		}
		id := lcommon.Blake2b256Hash([]byte(elems[0]))
		ids = append(ids, id.Bytes())
	}
	return ids, nil
}

// applyEndorserBlock decodes a certified Leios endorser block's standalone
// transactions, persists them as a standalone blob, and applies them to the
// ledger ahead of the certifying ranking block's own transactions, with full
// effects but without validation or consumed-input recovery: the certificate
// is what admits the closure, as in the reference applyLeiosClosure.
// It returns the number of transactions applied to the UTxO (or zero when every
// transaction was already applied).
//
// Endorser-block transactions are not part of any chain block, so — mirroring
// the genesis path (buildGenesisBlockCbor / SetGenesisCbor) — their CBOR is
// persisted as a standalone blob keyed by the endorser block's (slot, hash) and
// referenced by DOFF offsets, after which resolution works through the normal
// TieredCborCache cold-extract path. Crucially, the transactions' ledger
// effects (metadata rows, spent inputs, produced UTxOs) are recorded under the
// RANKING block's point (rbPoint), not the endorser block's: a rollback of the
// ranking block must remove them, and the ranking block is what admits the
// endorser block to the chain.
//
// It returns the number of transactions applied and the Conway donation total
// from accepted endorser-block transactions. Decode/build failures happen
// before storage is mutated and callers may treat them as best-effort. Once
// the endorser blob or transaction rows start writing, any error is wrapped in
// leiosEndorserBlockStorageError so callers can abort the outer transaction
// instead of committing a partial endorser-block application.
func (ls *LedgerState) applyEndorserBlock(
	ctx context.Context,
	txn *database.Txn,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	ebSlot uint64,
	ebHashBytes []byte,
	rawTxs []cbor.RawMessage,
) (int, uint64, error) {
	ls.publishUntickedClosureAfterCommit(ctx, txn, rbPoint)
	return ls.applyEndorserBlockInContext(ctx, txn, rbPoint, rbBlockNumber, ebSlot, ebHashBytes, rawTxs, nil)
}

func (ls *LedgerState) applyEndorserBlockInContext(
	ctx context.Context,
	txn *database.Txn,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	ebSlot uint64,
	ebHashBytes []byte,
	rawTxs []cbor.RawMessage,
	contextSlot *uint64,
) (int, uint64, error) {
	if len(rawTxs) == 0 {
		return 0, 0, nil
	}
	if len(ebHashBytes) != lcommon.Blake2b256Size {
		return 0, 0, fmt.Errorf(
			"endorser block hash must be %d bytes, got %d",
			lcommon.Blake2b256Size,
			len(ebHashBytes),
		)
	}
	var ebHash [lcommon.Blake2b256Size]byte
	copy(ebHash[:], ebHashBytes)

	// Decode each standalone endorser transaction, capturing its body CBOR
	// (the first array element) for the transaction-offset entry.
	txs := make([]lcommon.Transaction, len(rawTxs))
	bodyCbors := make([][]byte, len(rawTxs))
	for i, raw := range rawTxs {
		txCbor, elems, err := decodeEndorserTxEnvelope(raw)
		if err != nil {
			return 0, 0, fmt.Errorf("endorser tx %d: %w", i, err)
		}
		// An endorser block referenced by a Dijkstra ranking block is
		// Dijkstra-era, so decode its transactions as Dijkstra directly.
		// DetermineTransactionType is heuristic and cannot reliably identify a
		// bare standalone transaction without block/era context (it returns
		// "unknown transaction type" for these), so it must not be used here.
		// Peer-supplied transaction bytes, decoded before any storage is
		// mutated, so the guard cannot convert a crash into a partially
		// applied endorser block: every return below this loop's decode
		// failure leaves the ledger untouched.
		tx, err := safedecode.Transaction(ledger.TxTypeDijkstra, txCbor)
		if err != nil {
			return 0, 0, fmt.Errorf("decode endorser tx %d: %w", i, err)
		}
		txs[i] = tx
		bodyCbors[i] = []byte(elems[0])
	}

	// Repeated endorser transactions are kept in the blob, which is served
	// whole, but their ledger effects are recorded once.
	keepIndexes, err := ls.deduplicateEndorserBlockTransactionIndexes(
		ctx,
		txs,
		txn,
	)
	if err != nil {
		return 0, 0, err
	}

	// Build the endorser-block blob and its offsets, then persist the blob
	// under (ebSlot, ebHash) so cold-extract can resolve the DOFF refs.
	blob, offsets, err := buildEndorserBlockBlob(txs, bodyCbors, ebSlot, ebHash)
	if err != nil {
		return 0, 0, fmt.Errorf("build endorser block blob: %w", err)
	}
	// The blob commits in its own blob transaction (nil txn) so a dense Leios
	// backlog cannot overflow the shared 50-block chunk transaction with
	// ErrTxnTooBig; offset reads use a fresh blob snapshot if the shared LRU
	// misses. See DATABASE.md, "Leios endorser-block storage".
	if err := ls.db.SetGenesisCbor(ebSlot, ebHash[:], blob, nil); err != nil {
		return 0, 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("store endorser block blob: %w", err),
		}
	}
	txn.MarkBlockCborCommittedSeparately(ebSlot, ebHash)

	delta := NewLedgerDelta(
		rbPoint,
		uint(dijkstra.EraIdDijkstra),
		rbBlockNumber,
	)
	defer delta.Release()
	delta.Offsets = offsets
	delta.closureContextSlot = contextSlot
	if contextSlot != nil {
		delta.stageApplyEvents = func(events []TransactionEvent) {
			pending := &pendingLeiosClosure{point: ocommon.Point{Slot: rbPoint.Slot, Hash: bytes.Clone(rbPoint.Hash)}, events: events}
			txn.AfterCommit(func() { ls.Lock(); ls.untickedClosure = pending; ls.Unlock() })
		}
	}
	for _, idx := range keepIndexes {
		delta.addTransaction(txs[idx], idx)
	}

	// The certified closure is applied with its full effects -- produced
	// outputs, consumed inputs, certificates and governance -- but without
	// validation or consumed-input recovery, as the reference applyLeiosClosure
	// does (ruleApplyTxValidation ValidateNone): the certificate admitted it, and
	// a consumed input that is not present is a no-op rather than a conflict.
	// Omitting the produced outputs would leave the UTxO set, and the stake
	// distribution derived from it, short of what the reference holds. The delta
	// is recorded under the ranking block's point, so a rollback of the ranking
	// block removes these effects.
	delta.skipConsumedInputRecovery = true
	if err := delta.applyWithoutRecordingDonations(ctx, ls, txn); err != nil {
		return 0, 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf(
				"apply endorser block transactions: %w",
				err,
			),
		}
	}
	return len(delta.Transactions), delta.donation, nil
}

func (ls *LedgerState) deduplicateEndorserBlockTransactionIndexes(
	ctx context.Context,
	txs []lcommon.Transaction,
	txn *database.Txn,
) ([]int, error) {
	if len(txs) == 0 {
		return nil, nil
	}
	hashes := make([][]byte, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash().Bytes()
	}
	existing, err := ls.db.GetTransactionsByHashes(ctx, hashes, txn)
	if err != nil {
		return nil, fmt.Errorf("dedup endorser transactions: %w", err)
	}
	skip := make(map[string]struct{}, len(existing))
	for _, tx := range existing {
		if len(tx.Hash) == 0 {
			continue
		}
		skip[string(tx.Hash)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(txs))
	keepIndexes := make([]int, 0, len(txs))
	for i, tx := range txs {
		hashKey := string(tx.Hash().Bytes())
		if _, dup := skip[hashKey]; dup {
			continue
		}
		if _, dup := seen[hashKey]; dup {
			continue
		}
		seen[hashKey] = struct{}{}
		keepIndexes = append(keepIndexes, i)
	}
	return keepIndexes, nil
}

type leiosEndorserBlockStorageError struct {
	err error
}

func (e *leiosEndorserBlockStorageError) Error() string {
	return e.err.Error()
}

func (e *leiosEndorserBlockStorageError) Unwrap() error {
	return e.err
}

// buildEndorserBlockBlob lays out a standalone CBOR blob holding, for each
// endorser transaction, its body CBOR followed by each produced output's CBOR,
// recording the byte ranges as DOFF offsets keyed by (ebSlot, ebHash). The blob
// is not a chain block — cold-extract only slices it by offset/length — so a
// flat concatenation with precise offsets is sufficient.
func buildEndorserBlockBlob(
	txs []lcommon.Transaction,
	bodyCbors [][]byte,
	ebSlot uint64,
	ebHash [lcommon.Blake2b256Size]byte,
) ([]byte, *database.BlockIngestionResult, error) {
	var buf bytes.Buffer
	result := &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset, len(txs)),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	writeRange := func(b []byte) (uint32, uint32, error) {
		off := buf.Len()
		if off > math.MaxUint32 || len(b) > math.MaxUint32 {
			return 0, 0, errors.New(
				"endorser block blob offset out of uint32 range",
			)
		}
		buf.Write(b)
		//nolint:gosec // bounds checked above
		return uint32(off), uint32(len(b)), nil
	}
	for i, tx := range txs {
		levels := TransactionLevelsForApply(tx)
		for levelIdx, level := range levels {
			// The last level is always the enclosing transaction, and its
			// Cbor() is the whole [body, witnesses, isValid, aux] envelope
			// whether or not it carries sub-transactions. Store its body
			// element, as BlockIndexer.TxOffsets does under the same hash.
			// Only sub-transaction levels expose their own body bytes.
			bodyCbor := level.Cbor()
			if levelIdx == len(levels)-1 {
				bodyCbor = bodyCbors[i]
			}
			off, length, err := writeRange(bodyCbor)
			if err != nil {
				return nil, nil, err
			}
			var levelHash [32]byte
			copy(levelHash[:], level.Hash().Bytes())
			result.TxOffsets[levelHash] = database.CborOffset{
				BlockSlot:  ebSlot,
				BlockHash:  ebHash,
				ByteOffset: off,
				ByteLength: length,
			}
			for _, utxo := range level.Produced() {
				outCbor := utxo.Output.Cbor()
				if len(outCbor) == 0 {
					enc, err := cbor.Encode(utxo.Output)
					if err != nil {
						return nil, nil, fmt.Errorf(
							"encode endorser output: %w",
							err,
						)
					}
					outCbor = enc
				}
				off, length, err := writeRange(outCbor)
				if err != nil {
					return nil, nil, err
				}
				result.UtxoOffsets[database.UtxoRef{
					TxId:      levelHash,
					OutputIdx: utxo.Id.Index(),
				}] = database.CborOffset{
					BlockSlot:  ebSlot,
					BlockHash:  ebHash,
					ByteOffset: off,
					ByteLength: length,
				}
			}
		}
	}
	return buf.Bytes(), result, nil
}

// ensureReferencedEndorserBlocks gates delivery of a batch of blocks to
// ledgerProcessBlock on the availability of the Leios endorser blocks they
// reference. The prototype produces an endorser block and the ranking block
// that endorses it in the same slot and diffuses them together, so the ranking
// block routinely reaches the ledger a few milliseconds ahead of its endorser
// block; without this gate applyEndorserBlock always misses the cache and the
// endorser-resident outputs are never added before the ranking block spends
// them.
//
// The wait window is EndorserBlockWaitSlots (the pipeline timing's
// CertifyByDeadlineSlots, the bound for when a referenced endorser block is
// actually available to fetch) converted to wall-clock via the Shelley slot
// length, not a hardcoded duration. Callers invoke this before opening the
// block-processing DB transaction, so the wait never holds a transaction open.
//
// Referenced endorser blocks that are not cached are handled by where the
// ranking block sits relative to the live head:
//
//   - Near the head (within the wait window): the relay co-produces and
//     diffuses the endorser block with its ranking block, so it is already
//     being pushed. Only the references that applying THIS batch actually
//     reads are waited for (see splitTipWaitByApplyDependency); the rest
//     are dispatched as background prefetch and never block the pipeline.
//     The waits that remain run concurrently under one shared window and
//     dispatch an active by-point fetch up front, so a batch costs at most
//     one diffusion window rather than one per missing endorser block.
//   - Historical backlog (well below the head, e.g. during a from-scratch
//     catch-up): the relay does not diffuse these, but it does serve any
//     endorser block by point on demand, so actively fetch them -- in parallel
//     across the available relay connections -- and apply the endorser-resident
//     outputs instead of leaving the UTxO set incomplete and trusting the
//     chain. Only certified closures are fetched there, and a certified closure
//     is mandatory: an incomplete all-peer fetch returns an error before its
//     certifying ranking block can commit. This is what lets a from-scratch
//     sync build a complete ledger state instead of exposing a latent gap when
//     near-tip header validation begins.
func (ls *LedgerState) ensureReferencedEndorserBlocks(
	ctx context.Context,
	blocks []ledger.Block,
) error {
	// Index each block's announced endorser block by the block's own hash so a
	// certifying ranking block can resolve the endorser block its parent
	// announced without a store round-trip (the parent is normally in the same
	// batch, immediately before it on the chain).
	infos := make([]leiosBlockInfo, len(blocks))
	annByHash := make(map[string]leiosEbRef, len(blocks))
	for i, blk := range blocks {
		infos[i] = leiosBlockInfoFrom(blk)
		if infos[i].announces {
			annByHash[infos[i].hash] = leiosEbRef{
				slot: infos[i].slot,
				hash: infos[i].ebHash,
			}
		}
	}
	// Certificate validation precedes both asynchronous historical backfill
	// and the apply-time certified fetch. Invalid certificates therefore never
	// trigger certified endorser-block work, including during replay. Resolve a
	// parent announcement from this batch before falling back to persisted data.
	for _, block := range blocks {
		if err := ls.validateDijkstraLeiosCertificate(ctx, block, annByHash); err != nil {
			return fmt.Errorf("validate Dijkstra Leios certificate: %w", err)
		}
	}
	// Resolve CertRB parents that fall outside this batch from the block
	// store, so a certifying ranking block at a batch boundary still fetches
	// its endorser block. The parent (an already-applied ancestor) is stored.
	for _, info := range infos {
		if !info.certifies {
			continue
		}
		if _, ok := annByHash[info.prevHash]; ok {
			continue
		}
		if ls.db == nil {
			continue
		}
		parent, err := ls.BlockByHash(ctx, []byte(info.prevHash))
		if err != nil {
			continue
		}
		if ebHash, ok := leiosAnnouncementFromBlockCbor(parent.Cbor); ok {
			annByHash[info.prevHash] = leiosEbRef{
				slot: parent.Slot,
				hash: ebHash,
			}
		}
	}
	required, err := requiredCertifiedEndorserBlocks(infos, annByHash)
	if err != nil {
		return err
	}
	if ls.config.EndorserBlockProvider == nil {
		if len(required) == 0 {
			return nil
		}
		return fmt.Errorf(
			"%w: no endorser block provider configured",
			errCertifiedEndorserBlockUnavailable,
		)
	}
	// fetchErrs carries the last by-point fetch error per endorser block so an
	// unavailable certified closure reports WHY it is unavailable. Without it the
	// only field evidence for a wedged pipeline was the bare
	// "certified Leios endorser block unavailable" line -- the fetch failures
	// were logged at Debug and dropped in production.
	fetchErrs := make(map[string]error, len(required))
	ensureRequiredAvailable := func() error {
		for _, r := range required {
			if endorserBlockAvailableAt(
				ls.config.EndorserBlockProvider,
				r.hash.Bytes(),
				r.slot,
			) {
				continue
			}
			if fetchErr := fetchErrs[string(r.hash.Bytes())]; fetchErr != nil {
				return fmt.Errorf(
					"%w: slot %d, EB %s: last fetch attempt: %w",
					errCertifiedEndorserBlockUnavailable,
					r.slot,
					r.hash.String(),
					fetchErr,
				)
			}
			return fmt.Errorf(
				"%w: slot %d, EB %s",
				errCertifiedEndorserBlockUnavailable,
				r.slot,
				r.hash.String(),
			)
		}
		return nil
	}

	// fetchMissingRequired makes a bounded, retried by-point fetch of every
	// mandatory certified endorser block that is still unavailable. It is the
	// last-resort recovery step on every path that can reach
	// ensureRequiredAvailable, including the two early returns below: a
	// certified closure is mandatory whether or not the best-effort
	// announcement window is configured, and returning "unavailable" without
	// having tried to fetch it is what left the pipeline restarting on an
	// endorser block it had not fetched from any peer.
	fetchMissingRequired := func(poll time.Duration) {
		if ls.leiosBackfill == nil {
			return
		}
		batchCtx, cancel := context.WithTimeout(ctx, leiosBackfillMaxWait)
		defer cancel()
		for _, r := range required {
			if endorserBlockAvailableAt(
				ls.config.EndorserBlockProvider,
				r.hash.Bytes(),
				r.slot,
			) {
				continue
			}
			if err := ls.leiosBackfill.fetchRequired(batchCtx, r, poll); err != nil {
				fetchErrs[string(r.hash.Bytes())] = err
				return
			}
		}
	}

	// A zero wait disables best-effort announcement waiting, but a certified
	// closure remains mandatory: committing its CertRB without the closure
	// would permanently omit transaction and certificate effects.
	if ls.config.EndorserBlockWaitSlots == 0 {
		fetchMissingRequired(leiosCertifiedFetchPoll)
		return ensureRequiredAvailable()
	}
	slotLen := ls.shelleySlotLength()
	if slotLen <= 0 {
		// Without a known slot length the slot-denominated diffusion window
		// cannot be converted to wall-clock. Best-effort announcements may
		// still be skipped, but a certified closure must not be.
		fetchMissingRequired(leiosCertifiedFetchPoll)
		return ensureRequiredAvailable()
	}
	//nolint:gosec // EndorserBlockWaitSlots is a small protocol window
	timeout := time.Duration(ls.config.EndorserBlockWaitSlots) * slotLen
	// Cache re-check cadence (polling granularity, not a protocol parameter):
	// a fraction of a slot so arrival is noticed promptly, floored at 1ms so
	// the ticker interval is always positive.
	poll := max(slotLen/10, time.Millisecond)
	// wallSlot is the current wall-clock slot (the live head). A block more than
	// the wait window below it is settled backlog.
	wallSlot, wallErr := ls.CurrentSlot()
	cached := func(r leiosEbRef) bool {
		return endorserBlockAvailableAt(
			ls.config.EndorserBlockProvider,
			r.hash.Bytes(),
			r.slot,
		)
	}
	backfill, tipWait := classifyEndorserBlockFetches(
		infos,
		annByHash,
		wallSlot,
		wallErr == nil,
		ls.config.EndorserBlockWaitSlots,
		cached,
	)
	// Historical backlog: start a by-point fetch for each certified closure.
	// The fetches run concurrently in the background pool; the mandatory ones
	// are completed by fetchMissingRequired below.
	if len(backfill) > 0 && ls.leiosBackfill != nil {
		for _, r := range backfill {
			ls.leiosBackfill.spawn(ctx, r)
		}
	}
	// Near the head: block only on the references applying THIS batch reads.
	// See splitTipWaitByApplyDependency for the contract.
	blockingWait, prefetch := splitTipWaitByApplyDependency(tipWait, required)
	// Best-effort references: never block the ledger pipeline on them. The
	// fetch is dispatched in the background (deduped and concurrency-bounded by
	// the backfiller) so the endorser block is in cache by the time something
	// does depend on it, and this batch is delivered to ledgerProcessBlock
	// immediately.
	if ls.leiosBackfill != nil {
		for _, r := range prefetch {
			ls.leiosBackfill.spawn(ctx, r)
		}
	}
	// Blocking references: one shared diffusion window for the whole batch,
	// with an active by-point fetch dispatched up front for each one.
	ls.awaitEndorserBlocks(ctx, blockingWait, timeout, poll)
	// A certified closure is mandatory, so each required endorser block still
	// missing after the diffusion waits gets a bounded retry across the
	// connected peers rather than a single attempt per pipeline restart.
	// awaitFetch returns as soon as the in-flight marker clears, which a fetch
	// skipped as "connection busy" does within microseconds, so one attempt per
	// restart made at most one endorser block of progress per restart, or none
	// at all when every connection was unusable.
	fetchMissingRequired(poll)
	return ensureRequiredAvailable()
}

// splitTipWaitByApplyDependency partitions the near-head references into the
// ones ledger application of this batch depends on (blocking) and the ones it
// does not (prefetch, dispatched in the background and never waited on).
//
// Application reads only certified closures: the endorser block announced by a
// certifying ranking block's PARENT. A block's own announcement is never read
// when that block is applied; it becomes relevant only if a descendant
// certifies it, at which point it is a mandatory reference in its own right
// (requiredCertifiedEndorserBlocks). So required -- this batch's mandatory
// certified closures -- is the blocking set, and every other near-head
// reference is prefetch. Blocking on an announcement application never reads
// would stall every block queued behind it on the single ledger pipeline for
// the whole diffusion window and buy nothing.
func splitTipWaitByApplyDependency(
	tipWait, required []leiosEbRef,
) (blocking, prefetch []leiosEbRef) {
	requiredKeys := make(map[string]struct{}, len(required))
	for _, r := range required {
		requiredKeys[leiosEbRefKey(r)] = struct{}{}
	}
	for _, r := range tipWait {
		if _, ok := requiredKeys[leiosEbRefKey(r)]; ok {
			blocking = append(blocking, r)
			continue
		}
		prefetch = append(prefetch, r)
	}
	return blocking, prefetch
}

// awaitEndorserBlocks waits for every still-missing reference in refs to become
// available, CONCURRENTLY under one shared diffusion window.
//
// The waits are independent -- none of them observes another's result -- so
// running them back to back charged the ledger pipeline one full window per
// missing endorser block (k missing references cost k windows), which is where
// the multi-window apply stalls came from. Running them together bounds the
// whole batch by a single window.
//
// Each wait also dispatches an active by-point fetch up front rather than
// polling passively and only falling back to a fetch after the window has
// already been spent: the reference is wanted now, so ask for it now. The
// backfiller dedups by (slot, hash) and bounds concurrency, so a reference
// already in flight is not fetched twice.
func (ls *LedgerState) awaitEndorserBlocks(
	ctx context.Context,
	refs []leiosEbRef,
	timeout, poll time.Duration,
) {
	var wg sync.WaitGroup
	for _, r := range refs {
		if endorserBlockAvailableAt(
			ls.config.EndorserBlockProvider,
			r.hash.Bytes(),
			r.slot,
		) {
			continue
		}
		// The fetch is bound to ctx, not to the wait window, so a fetch that
		// outlives the window is not abandoned.
		if ls.leiosBackfill != nil {
			ls.leiosBackfill.spawn(ctx, r)
		}
		wg.Add(1)
		go func(r leiosEbRef) {
			defer wg.Done()
			ls.waitForEndorserBlock(ctx, r.slot, r.hash, timeout, poll)
		}(r)
	}
	wg.Wait()
}

// leiosEbRef pairs a ranking block's slot with the hash of the endorser block
// it references. The endorser block shares the ranking block's slot.
type leiosEbRef struct {
	slot uint64
	hash lcommon.Blake2b256
}

// leiosEbRefKey returns a stable per-(slot, hash) dedup key for r, not hash
// alone. The manifest is content-addressed, so the same hash can legitimately
// be a distinct requirement at two different slots at once; every
// dedup/in-flight-tracking map keyed on an endorser-block reference in this
// file uses this key, so a second, slot-distinct reference to an already-seen
// hash is never collapsed into (or suppressed by) the first.
func leiosEbRefKey(r leiosEbRef) string {
	return fmt.Sprintf("%d:%s", r.slot, r.hash.Bytes())
}

// endorserBlockAvailableAt reports whether provider already holds the
// endorser block identified by hash bound to exactly the given slot -- not
// merely present under some slot. The manifest is content-addressed, so the
// same hash can be a live, independently required occurrence at more than
// one slot at once; every call site here already knows the
// slot its own reference requires (leiosEbRef pairs them), and the provider
// itself resolves exactly that (slot, hash) occurrence rather than
// whichever one happens to be cached for the hash. Without this, a stale
// cached or persisted occurrence of the hash could silently satisfy a
// reference for a different one, and the caller would go on to apply its
// closure under the wrong slot instead of triggering the authoritative
// fetch.
func endorserBlockAvailableAt(
	provider EndorserBlockProviderFunc,
	hash []byte,
	slot uint64,
) bool {
	if provider == nil {
		return false
	}
	_, ok := provider(hash, slot)
	return ok
}

// leiosBlockInfo is the subset of a ranking block the endorser-block fetch
// policy needs: its identity (hash/prevHash/slot), the endorser block it
// announces (if any), and whether it certifies its parent's announced endorser
// block. hash and prevHash are the raw block-hash bytes as strings so they can
// key a map.
type leiosBlockInfo struct {
	hash      string
	prevHash  string
	slot      uint64
	announces bool
	ebHash    lcommon.Blake2b256
	certifies bool
}

// requiredCertifiedEndorserBlocks returns the certified parent EBs whose
// transactions are consensus ledger effects. Current announcements remain
// best-effort until a later block certifies them. A certifying block whose
// parent announcement cannot be resolved is not committed: proceeding would
// commit a ledger state known to be incomplete.
// Deduped by leiosEbRefKey (slot, hash), not hash alone: two certifying
// blocks in the same batch can legitimately require the same hash at
// different slots, and a hash-only dedup would drop the
// second requirement from the result entirely.
func requiredCertifiedEndorserBlocks(
	infos []leiosBlockInfo,
	annByHash map[string]leiosEbRef,
) ([]leiosEbRef, error) {
	required := make([]leiosEbRef, 0)
	seen := make(map[string]struct{})
	for _, info := range infos {
		if !info.certifies {
			continue
		}
		r, ok := annByHash[info.prevHash]
		if !ok {
			return nil, fmt.Errorf(
				"%w: certifying ranking block at slot %d has no resolvable parent announcement (parent %x)",
				errCertifiedEndorserBlockUnavailable,
				info.slot,
				[]byte(info.prevHash),
			)
		}
		key := leiosEbRefKey(r)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		required = append(required, r)
	}
	return required, nil
}

// leiosBlockInfoFrom extracts the fetch-policy view of a block from its header
// extension. A block with neither an announcement nor a certificate yields a
// zero-valued info (announces=false, certifies=false), which the classifier
// ignores.
func leiosBlockInfoFrom(blk ledger.Block) leiosBlockInfo {
	info := leiosBlockInfo{
		hash:     string(blk.Hash().Bytes()),
		prevHash: string(blk.PrevHash().Bytes()),
		slot:     blk.SlotNumber(),
	}
	if ref, ok := blk.Header().(leiosEndorserBlockReferencer); ok {
		if ebHash, _, ok := ref.LeiosAnnouncement(); ok {
			info.announces = true
			info.ebHash = ebHash
		}
	}
	if cert, ok := blk.Header().(leiosEndorserBlockCertifier); ok {
		if certified, present := cert.LeiosCertified(); present && certified {
			info.certifies = true
		}
	}
	return info
}

func (ls *LedgerState) validateDijkstraLeiosCertificate(
	ctx context.Context,
	block ledger.Block,
	batchAnnouncements map[string]leiosEbRef,
) error {
	dijkstraBlock, ok := block.(*dijkstra.DijkstraBlock)
	if !ok {
		return nil
	}
	certifier, ok := dijkstraBlock.Header().(leiosEndorserBlockCertifier)
	if !ok {
		return errors.New("dijkstra header has no Leios certification accessor")
	}
	certified, flagPresent := certifier.LeiosCertified()
	certificate := dijkstraBlock.BlockBody.LeiosCertificate
	if !flagPresent {
		if certificate != nil {
			return errors.New("certificate body is present without a certified header flag")
		}
		return nil
	}
	if certified != (certificate != nil) {
		return fmt.Errorf(
			"certified header flag is %t but certificate body presence is %t",
			certified,
			certificate != nil,
		)
	}
	if !certified {
		return nil
	}
	if ls.config.ValidateLeiosCertificate == nil {
		return errors.New("no Dijkstra Leios certificate validator configured")
	}
	var (
		ebSlot    uint64
		announced bool
		err       error
	)
	if batchAnnouncement, ok := batchAnnouncements[string(block.PrevHash().Bytes())]; ok {
		ebSlot, announced = batchAnnouncement.slot, true
	} else {
		_, ebSlot, announced, err = ls.leiosCertifiedAnnouncementFromParent(
			ctx,
			block.PrevHash().Bytes(),
		)
		if err != nil {
			return fmt.Errorf("%w: resolve certified parent announcement: %w", errCertifiedEndorserBlockUnavailable, err)
		}
	}
	if !announced {
		// The parent resolved and announced nothing, which is a verdict on
		// this block. A parent that does not resolve stays a retry above.
		return ls.rejectLeiosCertifyingBlock(
			block,
			errLeiosCertificateWithoutAnnouncement,
		)
	}
	epochInfo, err := ls.epochForSlot(ebSlot)
	if err != nil {
		return fmt.Errorf("resolve certified endorser-block epoch: %w", err)
	}
	if err := ls.config.ValidateLeiosCertificate(
		epochInfo.EpochId,
		block.PrevHash().Bytes(),
		certificate.Signers,
		certificate.AggregatedSignature,
	); err != nil {
		if errors.Is(err, ErrLeiosInvalidCertificate) {
			return ls.rejectLeiosCertifyingBlock(block, err)
		}
		// This node could not check the certificate (an unavailable vote
		// manager, a committee or database read failure), which says nothing
		// about the block. Rejecting it would rewind past a block that may be
		// valid, so it is retried like any other local failure.
		return fmt.Errorf("verify Leios certificate: %w", err)
	}
	return nil
}

// classifyEndorserBlockFetches decides which endorser blocks to fetch for a
// batch of ranking blocks, by where each block sits relative to the live head:
//
//   - Near the head (within waitSlots of wallSlot): current announcements are
//     fetched, since a descendant may certify them and voting needs them, and
//     so is a certifying block's parent announcement, because one ranking
//     block may certify its parent's EB and announce a new EB.
//   - Settled backlog (more than waitSlots below the head): certificate-driven.
//     A settled endorser block is fetched only once a certifying ranking block
//     certifies it -- the one announced by the CertRB's parent (prevHash).
//     Uncertified historical announcements are skipped: their transactions
//     never reach the ledger or the merged node-to-client view, and relays do
//     not reliably serve them.
//
// annByHash resolves a CertRB's parent announcement (block hash -> announced
// endorser block); the caller supplies parents outside the batch. cached
// reports whether an endorser block is already available *at r's slot*, so a
// stale occurrence of the hash under a different slot is not mistaken for
// availability and is fetched like any other missing reference.
// backfillSeen/tipWaitSeen (via appendRef's leiosEbRefKey) dedup by
// (slot, hash), not hash alone, for the same reason: two blocks in the batch
// can legitimately require the same hash at different slots, and a
// hash-only dedup would drop the second requirement's fetch entirely. When
// the wall-clock slot is unknown (wallKnown=false) every block is treated as
// near-head, preserving announcement-driven behavior rather than silently
// dropping fetches.
func classifyEndorserBlockFetches(
	infos []leiosBlockInfo,
	annByHash map[string]leiosEbRef,
	wallSlot uint64,
	wallKnown bool,
	waitSlots uint64,
	cached func(r leiosEbRef) bool,
) (backfill, tipWait []leiosEbRef) {
	backfillSeen := make(map[string]struct{})
	tipWaitSeen := make(map[string]struct{})
	appendRef := func(dst *[]leiosEbRef, seen map[string]struct{}, r leiosEbRef) {
		key := leiosEbRefKey(r)
		if _, ok := seen[key]; ok || cached(r) {
			return
		}
		seen[key] = struct{}{}
		*dst = append(*dst, r)
	}
	for _, info := range infos {
		historical := wallKnown && wallSlot > info.slot &&
			wallSlot-info.slot > waitSlots
		if info.certifies {
			// The certified EB is always the parent's announcement, independent
			// of the current block's own announcement: a block may contain both.
			if r, ok := annByHash[info.prevHash]; ok {
				if historical {
					appendRef(&backfill, backfillSeen, r)
				} else {
					appendRef(&tipWait, tipWaitSeen, r)
				}
			}
		}
		if historical || !info.announces {
			continue
		}
		appendRef(
			&tipWait,
			tipWaitSeen,
			leiosEbRef{slot: info.slot, hash: info.ebHash},
		)
	}
	return backfill, tipWait
}

// leiosAnnouncementFromBlockCbor decodes the endorser block reference a
// Dijkstra ranking block announces from its raw CBOR, or ok=false when it
// announces none. The block is [header, block_body]; the announcement rides on
// the header extension. Used to resolve a CertRB's parent announcement.
func leiosAnnouncementFromBlockCbor(
	blockCbor []byte,
) (lcommon.Blake2b256, bool) {
	top, err := safedecode.Guard(func() ([]cbor.RawMessage, error) {
		var top []cbor.RawMessage
		_, err := cbor.Decode(blockCbor, &top)
		return top, err
	})
	if err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, false
	}
	header, err := safedecode.Guard(func() (dijkstra.DijkstraBlockHeader, error) {
		var header dijkstra.DijkstraBlockHeader
		_, err := cbor.Decode(top[0], &header)
		return header, err
	})
	if err != nil {
		return lcommon.Blake2b256{}, false
	}
	ebHash, _, ok := header.LeiosAnnouncement()
	if !ok {
		return lcommon.Blake2b256{}, false
	}
	return ebHash, true
}

// leiosEndorserBlockForApply selects the EB whose transactions affect this
// ranking block. Only a certified closure is ever applied: the EB announced by
// the certifying block's parent. A ranking block without a certificate applies
// no EB, including one it announces itself, and a CertRB may also announce a
// new EB, so its current announcement must not be mistaken for the certified
// one.
// The returned expectedSlot is the slot the referenced endorser block must be
// bound to: the endorser block shares its announcing ranking block's slot
// (see leiosEbRef), which is the certifying block's parent's slot. Callers
// must check a provider result against it (endorserBlockAvailableAt) rather
// than trust whatever slot the provider itself reports, since the manifest is
// content-addressed and the same hash can legitimately recur at a different
// slot.
func (ls *LedgerState) leiosEndorserBlockForApply(
	ctx context.Context,
	block ledger.Block,
) (hash lcommon.Blake2b256, expectedSlot uint64, announced bool, err error) {
	certifier, ok := block.Header().(leiosEndorserBlockCertifier)
	if !ok {
		return lcommon.Blake2b256{}, 0, false, nil
	}
	certified, present := certifier.LeiosCertified()
	if !present || !certified {
		return lcommon.Blake2b256{}, 0, false, nil
	}
	return ls.leiosCertifiedAnnouncementFromParent(
		ctx,
		block.PrevHash().Bytes(),
	)
}

// leiosCertifiedAnnouncementFromParent resolves the endorser block a certifying
// ranking block certifies: the one its parent announced. Split out of
// leiosEndorserBlockForApply so the cross-fork continuation audit can resolve
// the same reference from a retained parent hash alone, without holding the
// certifying block, and cannot drift from what apply selects.
func (ls *LedgerState) leiosCertifiedAnnouncementFromParent(
	ctx context.Context,
	prevHash []byte,
) (hash lcommon.Blake2b256, expectedSlot uint64, announced bool, err error) {
	if ls.db == nil {
		return lcommon.Blake2b256{}, 0, false, errors.New(
			"resolve certifying block parent: database unavailable",
		)
	}
	parent, perr := ls.BlockByHash(ctx, prevHash)
	if perr != nil {
		return lcommon.Blake2b256{}, 0, false, fmt.Errorf(
			"resolve certifying block parent: %w",
			perr,
		)
	}
	// A parent whose stored bytes are not a block says nothing about what it
	// announced, so it must not read as "announced nothing", which rejects the
	// certifying block.
	if err := leiosStoredBlockDecodes(parent.Cbor); err != nil {
		return lcommon.Blake2b256{}, 0, false, fmt.Errorf(
			"resolve certifying block parent: decode stored block: %w",
			err,
		)
	}
	hash, announced = leiosAnnouncementFromBlockCbor(parent.Cbor)
	return hash, parent.Slot, announced, nil
}

// leiosStoredBlockDecodes reports whether blockCbor is a non-empty CBOR array,
// the outer shape of every stored block.
func leiosStoredBlockDecodes(blockCbor []byte) error {
	top, err := safedecode.Guard(func() ([]cbor.RawMessage, error) {
		var top []cbor.RawMessage
		_, err := cbor.Decode(blockCbor, &top)
		return top, err
	})
	if err != nil {
		return err
	}
	if len(top) == 0 {
		return errors.New("empty block array")
	}
	return nil
}

// leiosBackfillConcurrency bounds how many historical endorser blocks are
// fetched at once. The per-connection fetch guard serializes work on any one
// connection, so effective parallelism is capped by the relay connection count
// anyway; this is just an upper bound so a busy chunk cannot spawn an unbounded
// number of fetch goroutines. It is kept modest deliberately: the prototype
// relay serves endorser blocks reliably when requests are paced (one chunk's
// worth at a time, with the block-application gap between chunks) but returns
// empty manifests when hammered, so the backfill must not flood it.
const leiosBackfillConcurrency = 8

// leiosRequiredConcurrency is a SEPARATE budget for mandatory certified
// fetches, so a best-effort fetch can never starve one.
//
// spawn and fetchRequired used to share leiosBackfillConcurrency. A
// best-effort spawn holds its slot for up to leiosBackfillMaxWait, so eight
// slow ones could block a mandatory certified closure for two minutes and fail
// the chunk -- and this path now dispatches near-head prefetches through spawn
// as well, which makes that far more reachable than when only historical
// backfill used it. Mandatory fetches are few (one certified closure per
// certifying block) so a small dedicated budget is enough, and it is separate
// rather than carved out of the same channel because reserving inside one
// semaphore needs two acquisitions and can deadlock.
const leiosRequiredConcurrency = 4

// leiosBackfiller fetches historical Leios endorser blocks by point, paced one
// block-application chunk at a time, so a from-scratch sync builds a complete
// UTxO set. It dedups in-flight fetches by (slot, hash) -- not hash alone,
// since the same hash can legitimately be required at two different slots
// concurrently -- and bounds their concurrency. The prototype relay serves
// any endorser block by point on demand, so availability is not the
// constraint; pacing is.
type leiosBackfiller struct {
	fetch    EndorserBlockFetcherFunc
	provider EndorserBlockProviderFunc
	logger   *slog.Logger
	sem      chan struct{}
	reqSem   chan struct{}
	inflight sync.Map
}

// newLeiosBackfiller returns a backfiller, or nil when no endorser-block fetcher
// is configured (in which case backfill is disabled and the ledger falls back
// to the interim trust path for unresolved endorser-resident inputs).
func newLeiosBackfiller(cfg LedgerStateConfig) *leiosBackfiller {
	if cfg.EndorserBlockFetcher == nil || cfg.EndorserBlockProvider == nil {
		return nil
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &leiosBackfiller{
		fetch:    cfg.EndorserBlockFetcher,
		provider: cfg.EndorserBlockProvider,
		logger:   logger,
		sem:      make(chan struct{}, leiosBackfillConcurrency),
		reqSem:   make(chan struct{}, leiosRequiredConcurrency),
	}
}

// spawn starts a background by-point fetch of the endorser block referenced by
// r unless it is already cached or a fetch is already in flight. It returns
// immediately. Deduping by leiosEbRefKey (slot, hash) means the read-batch
// prefetch and the per-chunk gate never fetch the same endorser-block
// requirement twice, while two different slots requiring the same hash are
// still dispatched independently: a hash-only key would let the second
// requirement's spawn find the first already in flight and silently no-op,
// and then let awaitFetch's "not in flight" skip-fast fire the moment the
// *first* requirement's fetch cleared the (shared) key, even though the
// second requirement's slot was never fetched at all.
// ctx bounds the spawned fetch: it is the block-processing context, so a
// shutdown or a pipeline restart stops the fetch instead of leaving it running
// against a connection the node is tearing down.
func (b *leiosBackfiller) spawn(ctx context.Context, r leiosEbRef) {
	key := leiosEbRefKey(r)
	if _, loaded := b.inflight.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
		b.inflight.Delete(key)
		return
	}
	go func() {
		select {
		case b.sem <- struct{}{}:
		case <-ctx.Done():
			b.inflight.Delete(key)
			return
		}
		defer func() {
			<-b.sem
			b.inflight.Delete(key)
		}()
		if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
			return
		}
		fetchCtx, cancel := context.WithTimeout(ctx, leiosBackfillMaxWait)
		defer cancel()
		if err := b.fetch(fetchCtx, r.slot, r.hash.Bytes()); err != nil {
			b.logger.Debug(
				"leios endorser block backfill failed",
				"component", "ledger",
				"slot", r.slot,
				"eb_hash", r.hash.String(),
				"error", err,
			)
		}
	}()
}

// leiosCertifiedFetchAttempts bounds how many by-point fetch attempts one
// block-processing pass spends on a single mandatory certified endorser block
// before it gives up and fails the chunk. Each attempt is itself a failover
// sweep across every leios-fetch connection, so this bounds retries of the whole
// peer set, not retries of one peer. It is small because the attempts share one
// leiosBackfillMaxWait budget: the point is to survive a connection that was
// momentarily busy or has just been recycled, not to grind on peers that do not
// hold the block.
const leiosCertifiedFetchAttempts = 4

// leiosCertifiedFetchRetryBase and leiosCertifiedFetchRetryMax bound the gap
// between those attempts. The gap escalates so a fetch that fails instantly
// (every connection busy, or no connection at all) does not spin, while a
// recycled connection has time to be redialled before the next sweep.
const (
	leiosCertifiedFetchRetryBase = 250 * time.Millisecond
	leiosCertifiedFetchRetryMax  = 4 * time.Second
)

// leiosCertifiedFetchPoll is the cache re-check cadence used when no
// slot-derived polling granularity is available (the best-effort announcement
// window is disabled, or the Shelley slot length is unknown). It only affects
// how quickly a fetch already in flight for the same endorser block is noticed
// to have landed.
const leiosCertifiedFetchPoll = 10 * time.Millisecond

// fetchRequired obtains a mandatory certified endorser block, retrying the
// by-point fetch a bounded number of times within one leiosBackfillMaxWait
// budget. It returns nil as soon as the endorser block is available to the
// provider, and otherwise the last fetch error so the caller can report why the
// certified closure could not be completed.
//
// This is the ledger-side half of the recovery path: FetchEndorserBlockByPoint
// fails over across peers within one attempt (and recycles a connection whose
// leios-fetch protocol is dead), while this retries that sweep so a transient
// outcome -- every connection busy serving another endorser block, or a
// replacement connection still being dialled -- does not abort the chunk and
// force a whole pipeline restart to make one endorser block of progress.
//
// It holds no lock and opens no database transaction, so it cannot invert with
// the block-apply write path it runs ahead of.
func (b *leiosBackfiller) fetchRequired(
	ctx context.Context,
	r leiosEbRef,
	poll time.Duration,
) error {
	budgetCtx, cancel := context.WithTimeout(ctx, leiosBackfillMaxWait)
	defer cancel()
	var lastErr error
	for attempt := 1; ; attempt++ {
		if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
			return nil
		}
		if err := b.fetchOnce(budgetCtx, r, poll); err != nil {
			lastErr = err
		}
		if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
			return nil
		}
		if attempt >= leiosCertifiedFetchAttempts {
			break
		}
		//nolint:gosec // attempt is bounded by leiosCertifiedFetchAttempts
		delay := min(
			leiosCertifiedFetchRetryBase<<uint(attempt-1),
			leiosCertifiedFetchRetryMax,
		)
		timer := time.NewTimer(delay)
		select {
		case <-budgetCtx.Done():
			timer.Stop()
			if lastErr == nil {
				lastErr = budgetCtx.Err()
			}
			// budgetCtx is a timeout child of ctx, so its Done also closes
			// when the PARENT is cancelled -- node shutdown, or the
			// block-processing pass being aborted. Reporting that as "the
			// retry budget elapsed" tells an operator the peers failed to
			// serve the endorser block when in fact nothing was asked of
			// them, and it is loudest exactly when a node is shutting down.
			// Same discrimination as waitForEndorserBlock: the child's error
			// is stable once resolved, so a deadline that fires first stays
			// DeadlineExceeded even if the parent is cancelled straight after.
			if !errors.Is(budgetCtx.Err(), context.DeadlineExceeded) {
				b.logger.Debug(
					"certified leios endorser block fetch cancelled",
					"component", "ledger",
					"slot", r.slot,
					"eb_hash", r.hash.String(),
					"attempts", attempt,
					"error", lastErr,
				)
				return lastErr
			}
			b.logger.Warn(
				"certified leios endorser block fetch budget elapsed",
				"component", "ledger",
				"slot", r.slot,
				"eb_hash", r.hash.String(),
				"attempts", attempt,
				"error", lastErr,
			)
			return lastErr
		case <-timer.C:
		}
	}
	if lastErr == nil {
		lastErr = errors.New("certified endorser block fetch made no progress")
	}
	// The retry loop can also fall out of its last attempt with the parent
	// already cancelled, which never reaches the in-loop budgetCtx branch
	// above. Report that as the cancellation it is rather than as peers
	// failing to serve.
	if !errors.Is(budgetCtx.Err(), context.DeadlineExceeded) &&
		budgetCtx.Err() != nil {
		b.logger.Debug(
			"certified leios endorser block fetch cancelled",
			"component", "ledger",
			"slot", r.slot,
			"eb_hash", r.hash.String(),
			"error", lastErr,
		)
		return lastErr
	}
	// Warn, not Debug: this is the evidence an operator needs to tell a peer
	// that does not hold the endorser block from one whose leios-fetch protocol
	// is broken, and it was previously logged at Debug and lost.
	b.logger.Warn(
		"certified leios endorser block still unavailable after bounded retry",
		"component", "ledger",
		"slot", r.slot,
		"eb_hash", r.hash.String(),
		"attempts", leiosCertifiedFetchAttempts,
		"error", lastErr,
	)
	return lastErr
}

// fetchOnce runs one by-point fetch attempt for r, or waits for an equivalent
// fetch another caller already has in flight. Deduping by leiosEbRefKey (slot,
// hash) keeps a single fetch per endorser-block requirement; the waiting branch
// is why a required endorser block already being fetched by the best-effort
// spawn above is not fetched twice.
func (b *leiosBackfiller) fetchOnce(
	ctx context.Context,
	r leiosEbRef,
	poll time.Duration,
) error {
	key := leiosEbRefKey(r)
	if _, loaded := b.inflight.LoadOrStore(key, struct{}{}); loaded {
		// Another fetch for this endorser block is in flight; wait for it
		// rather than starting a second one on the same connections.
		b.awaitFetch(ctx, r, poll, leiosBackfillMaxWait)
		return nil
	}
	defer b.inflight.Delete(key)
	// Reserved budget: a mandatory certified fetch must never queue behind
	// best-effort spawns, which can hold their slots for leiosBackfillMaxWait.
	select {
	case b.reqSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-b.reqSem }()
	if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
		return nil
	}
	return b.fetch(ctx, r.slot, r.hash.Bytes())
}

// waitForEndorserBlock polls the EndorserBlockProvider until the endorser block
// identified by ebHash is fetched and cached complete, ctx is cancelled, or the
// diffusion-window timeout elapses. The concurrent leios-notify/leios-fetch
// handlers keep making progress while this blocks, so the in-flight fetch
// completes during the wait.
//
// Every wait is recorded to dingo_metrics_leios_eb_wait_seconds with its
// outcome -- arrived, timeout, or cancelled, the last being a cancellation of
// the block-processing context rather than a diffusion-window expiry -- and
// expiries additionally to dingo_metrics_leios_eb_wait_timeouts_total. This wait is taken on the single
// ledger pipeline ahead of the batch's DB transaction, so it is apply latency
// for every block queued behind the batch as well; it previously had no metric
// at all, only an Info log, which is why a producer could sit in it for tens of
// seconds per block with nothing in monitoring to show for it.
func (ls *LedgerState) waitForEndorserBlock(
	ctx context.Context,
	rbSlot uint64,
	ebHash lcommon.Blake2b256,
	timeout, poll time.Duration,
) {
	start := time.Now()
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if endorserBlockAvailableAt(
			ls.config.EndorserBlockProvider,
			ebHash.Bytes(),
			rbSlot,
		) {
			ls.metrics.observeLeiosEbWait(
				time.Since(start),
				leiosEbWaitOutcomeArrived,
			)
			return
		}
		select {
		case <-waitCtx.Done():
			// waitCtx is a timeout child of ctx, so its Done also closes when
			// the PARENT is cancelled -- node shutdown, or the block-processing
			// pass being aborted and restarted. That is not a diffusion-window
			// expiry: nothing was learned about whether the endorser block is
			// obtainable, and reporting it as one would inflate the timeout
			// rate exactly when a node is shutting down or restarting its
			// pipeline. waitCtx.Err() distinguishes the two and is stable once
			// resolved -- a deadline that fires first leaves DeadlineExceeded
			// even if the parent is cancelled immediately afterwards.
			//
			// The caller's behaviour is unchanged either way, and deliberately
			// so: this function returns, and ensureReferencedEndorserBlocks
			// then runs its mandatory-closure fetch and availability check as
			// usual, so a cancelled pass still fails the chunk when a certified
			// closure is missing and still proceeds when every reference was
			// best-effort. That is what the code did before the wait was
			// instrumented; only the classification is new.
			if !errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				ls.metrics.observeLeiosEbWait(
					time.Since(start),
					leiosEbWaitOutcomeCancelled,
				)
				ls.config.Logger.Debug(
					"endorser block wait cancelled before the diffusion window elapsed",
					"component",
					"ledger",
					"slot",
					rbSlot,
					"eb_hash",
					ebHash.String(),
					"waited_seconds",
					time.Since(start).Seconds(),
					"error",
					waitCtx.Err(),
				)
				return
			}
			ls.metrics.observeLeiosEbWait(
				time.Since(start),
				leiosEbWaitOutcomeTimeout,
			)
			ls.config.Logger.Info(
				"endorser block not fetched within diffusion window; proceeding without it",
				"component",
				"ledger",
				"slot",
				rbSlot,
				"eb_hash",
				ebHash.String(),
				"waited_seconds",
				time.Since(start).Seconds(),
			)
			return
		case <-ticker.C:
		}
	}
}

// leiosBackfillMaxWait bounds how long block processing waits for a historical
// endorser block to be backfilled before proceeding without it (leaving the
// interim trust path to cover the unresolved inputs). The relay serves
// historical endorser blocks on demand, so this is only a backstop against a
// genuinely unavailable one; it is far longer than the tip diffusion window
// because a from-scratch backfill is throughput-bound, not diffusion-bound.
const leiosBackfillMaxWait = 2 * time.Minute

// leiosFetchWaitOutcome is why awaitFetch returned. The caller cannot infer it
// afterwards: the in-flight marker and the parent context are both racy by the
// time it looks, so a hard bound that expired just before a shutdown would be
// reported as a cancellation and a fetch that completed without caching would
// be reported as a timeout. Returning the cause removes the guess.
type leiosFetchWaitOutcome int

const (
	// leiosFetchWaitCached: the endorser block is available.
	leiosFetchWaitCached leiosFetchWaitOutcome = iota
	// leiosFetchWaitUnavailable: the all-peers fetch COMPLETED without
	// caching. Nothing timed out; no peer holds the block.
	leiosFetchWaitUnavailable
	// leiosFetchWaitDeadline: maxWait elapsed with the fetch neither caching
	// nor clearing its marker.
	leiosFetchWaitDeadline
	// leiosFetchWaitCancelled: the parent context was cancelled.
	leiosFetchWaitCancelled
)

// awaitFetch waits for the in-flight by-point fetch of the endorser block
// referenced by r to finish (it has already been spawned). The spawned fetch
// (FetchEndorserBlockByPoint) tries every connected peer in turn before
// failing, so by the time it clears its in-flight marker it has tried all
// peers. If it cached the block, the referencing ranking block can apply it.
// If it finished without caching (every peer's response was flaky/incomplete,
// e.g. a single connection that cannot serve a large endorser block's tail),
// return promptly. The caller distinguishes best-effort announcements from
// mandatory certified closures: the former may advance, while the
// latter abort the chunk and are retried by the ledger pipeline.
// leiosBackfillMaxWait is a backstop against a fetch that neither caches nor
// clears (the fetch itself is bounded by the leios-fetch timeout, so this is
// rarely reached).

func (b *leiosBackfiller) awaitFetch(
	ctx context.Context,
	r leiosEbRef,
	poll, maxWait time.Duration,
) leiosFetchWaitOutcome {
	waitCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	key := leiosEbRefKey(r)
	for {
		if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
			return leiosFetchWaitCached
		}
		// Checked BEFORE the in-flight marker, because cancelling the pass
		// also makes spawn clear that marker. Looking at the marker first
		// would race, and would report a shutdown as "no peer holds it" --
		// the false diagnosis this classification exists to avoid.
		if waitCtx.Err() != nil {
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return leiosFetchWaitDeadline
			}
			return leiosFetchWaitCancelled
		}
		if _, inFlight := b.inflight.Load(key); !inFlight {
			// The all-peers fetch finished without caching: skip fast.
			return leiosFetchWaitUnavailable
		}
		select {
		case <-waitCtx.Done():
			// waitCtx is a timeout child of ctx, so its Done also closes on a
			// PARENT cancellation. Discriminate on the child's own error,
			// which is stable once resolved: a deadline that fires first
			// stays DeadlineExceeded even if the parent is cancelled straight
			// after, so a real bound is never reported as a shutdown.
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return leiosFetchWaitDeadline
			}
			return leiosFetchWaitCancelled
		case <-ticker.C:
		}
	}
}

// applyUntickedBoundaryClosure folds a prototype closure onto the parent's
// ledger before NEWEPOCH, retaining the certifying RB point for rollback.
func (ls *LedgerState) applyUntickedBoundaryClosure(
	ctx context.Context,
	txn *database.Txn,
	block ledger.Block,
	parentPoint ocommon.Point,
) error {
	if err := ls.validateBlockCheckpoint(block); err != nil {
		return err
	}
	if !bytes.Equal(block.PrevHash().Bytes(), parentPoint.Hash) {
		return fmt.Errorf("%w: boundary closure does not extend the ledger tip", errStaleChainIterator)
	}
	if err := ls.validateDijkstraLeiosCertificate(ctx, block, nil); err != nil {
		return err
	}
	hash, slot, referenced, err := ls.leiosEndorserBlockForApply(ctx, block)
	if err != nil {
		return err
	}
	if !referenced {
		return nil
	}
	if ls.config.EndorserBlockProvider == nil {
		return errCertifiedEndorserBlockUnavailable
	}
	txs, found := ls.config.EndorserBlockProvider(hash.Bytes(), slot)
	if !found {
		return errCertifiedEndorserBlockUnavailable
	}
	point := ocommon.Point{Slot: block.SlotNumber(), Hash: block.Hash().Bytes()}
	_, donation, err := ls.applyEndorserBlockInContext(ctx, txn, point, block.BlockNumber(), slot, hash.Bytes(), txs, &parentPoint.Slot)
	if err != nil {
		return err
	}
	if donation > 0 {
		ls.RLock()
		epoch := ls.currentEpoch.EpochId
		ls.RUnlock()
		if err := ls.db.Metadata().AddNetworkDonation(point.Slot, epoch, donation, txn.Metadata()); err != nil {
			return err
		}
	}
	return nil
}

type pendingLeiosClosure struct {
	point  ocommon.Point
	events []TransactionEvent
}

// publishUntickedClosureAfterCommit keeps transaction Apply notifications with
// the certifying RB's commit, including when a failed body is retried.
func (ls *LedgerState) publishUntickedClosureAfterCommit(ctx context.Context, txn *database.Txn, point ocommon.Point) {
	ls.RLock()
	pending := ls.untickedClosure
	ls.RUnlock()
	if pending == nil || !pointMatches(pending.point, point) {
		return
	}
	txn.AfterCommit(func() {
		ls.Lock()
		if ls.untickedClosure != pending {
			ls.Unlock()
			return
		}
		ls.untickedClosure = nil
		ls.Unlock()
		if ls.beforeTransactionApplyPublish != nil {
			ls.beforeTransactionApplyPublish()
		}
		for _, evt := range pending.events {
			ls.publishTransactionEvent(ctx, evt)
		}
	})
}
