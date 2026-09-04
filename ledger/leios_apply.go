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

const certifiedEndorserBlockRetryDelay = time.Second

// applyEndorserBlock decodes a Leios endorser block's standalone transactions,
// persists them as a standalone blob, and — on the CIP-conformant path
// (LeiosApplyEndorserBlockTxs) — applies them to the ledger ahead of the ranking
// block that references it, so the endorser-resident outputs the ranking block's
// transactions spend are present in the UTxO set. On the Haskell-conformant path
// (the Musashi prototype) the certified endorser block is applied with full
// effects but without validation or consumed-input recovery.
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
	txn *database.Txn,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	ebSlot uint64,
	ebHashBytes []byte,
	rawTxs []cbor.RawMessage,
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
		// leios-fetch carries each endorser transaction CBOR-in-CBOR: the
		// tx_list entry is a CBOR byte string wrapping the transaction's own
		// CBOR (LeiosTx = encodeBytes(txCbor)). Unwrap it to the inner
		// transaction bytes before decoding. (A non-byte-string entry — major
		// type != 2 — is already the bare transaction.)
		txCbor := []byte(raw)
		if len(txCbor) > 0 && txCbor[0]>>5 == 2 {
			var inner []byte
			if _, err := cbor.Decode(txCbor, &inner); err != nil {
				return 0, 0, fmt.Errorf("unwrap endorser tx %d: %w", i, err)
			}
			txCbor = inner
		}
		var elems []cbor.RawMessage
		if _, err := cbor.Decode(txCbor, &elems); err != nil {
			return 0, 0, fmt.Errorf(
				"decode endorser tx %d envelope: %w",
				i,
				err,
			)
		}
		if len(elems) < 2 {
			return 0, 0, fmt.Errorf(
				"endorser tx %d has %d elements, want >= 2",
				i,
				len(elems),
			)
		}
		// An endorser block referenced by a Dijkstra ranking block is
		// Dijkstra-era, so decode its transactions as Dijkstra directly.
		// DetermineTransactionType is heuristic and cannot reliably identify a
		// bare standalone transaction without block/era context (it returns
		// "unknown transaction type" for these), so it must not be used here.
		tx, err := ledger.NewTransactionFromCbor(ledger.TxTypeDijkstra, txCbor)
		if err != nil {
			return 0, 0, fmt.Errorf("decode endorser tx %d: %w", i, err)
		}
		txs[i] = tx
		bodyCbors[i] = []byte(elems[0])
	}

	// Reject repeated endorser transactions before recording ledger data. The
	// CIP path compacts the block to transactions that still need UTxO apply;
	// the Musashi path keeps the blob intact for serving while using the indexes
	// to suppress duplicate ledger effects.
	keepIndexes, err := ls.deduplicateEndorserBlockTransactionIndexes(txs, txn)
	if err != nil {
		return 0, 0, err
	}
	if ls.config.LeiosApplyEndorserBlockTxs {
		if len(keepIndexes) == 0 {
			return 0, 0, nil
		}
		keptTxs := make([]lcommon.Transaction, 0, len(keepIndexes))
		keptBodies := make([][]byte, 0, len(keepIndexes))
		for _, idx := range keepIndexes {
			keptTxs = append(keptTxs, txs[idx])
			keptBodies = append(keptBodies, bodyCbors[idx])
		}
		txs = keptTxs
		bodyCbors = keptBodies
	}

	// Build the endorser-block blob and its offsets, then persist the blob
	// under (ebSlot, ebHash) so cold-extract can resolve the DOFF refs.
	blob, offsets, err := buildEndorserBlockBlob(txs, bodyCbors, ebSlot, ebHash)
	if err != nil {
		return 0, 0, fmt.Errorf("build endorser block blob: %w", err)
	}
	// Persist the endorser-block blob. Which transaction commits it depends on
	// the apply path (see DATABASE.md, "Leios endorser-block storage", for the
	// full rationale):
	//   - Musashi/no-validation path (LeiosApplyEndorserBlockTxs false): commit
	//     in its own blob transaction (nil txn) to avoid overflowing the shared
	//     50-block chunk transaction with ErrTxnTooBig on a dense Leios backlog;
	//     the blob is never read back within the chunk, so an independent commit
	//     is safe.
	//   - CIP/validating path (LeiosApplyEndorserBlockTxs true): keep the blob in
	//     the shared txn so a later block spending an endorser-produced output can
	//     resolve it via read-your-writes.
	blobTxn := txn
	if !ls.config.LeiosApplyEndorserBlockTxs {
		blobTxn = nil
	}
	if err := ls.db.SetGenesisCbor(ebSlot, ebHash[:], blob, blobTxn); err != nil {
		return 0, 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("store endorser block blob: %w", err),
		}
	}

	delta := NewLedgerDelta(
		rbPoint,
		uint(dijkstra.EraIdDijkstra),
		rbBlockNumber,
	)
	defer delta.Release()
	delta.Offsets = offsets
	if ls.config.LeiosApplyEndorserBlockTxs {
		for i, tx := range txs {
			delta.addTransaction(tx, i)
		}
	} else {
		for _, idx := range keepIndexes {
			delta.addTransaction(txs[idx], idx)
		}
	}

	// Haskell-conformant path (Musashi prototype-2026w29): apply the certified
	// endorser block's transactions with their full effects — produced outputs,
	// consumed inputs, certificates, and governance — but WITHOUT validation or
	// consumed-input recovery. This mirrors the reference ledger's
	// applyLeiosClosure (ruleApplyTxValidation ValidateNone in
	// Ouroboros.Consensus.Shelley.Ledger.Leios): the endorser block was admitted
	// to the chain by its Leios certificate, so its transactions are folded onto
	// the ledger state without re-validation, and a consumed input that is not
	// present is left as a no-op instead of driving the consumed-utxo recovery
	// loop. Applying the produced outputs keeps the UTxO set — and the
	// stake distribution derived from it — complete, matching the reference;
	// recording metadata only (the previous behavior) diverged the UTxO and made
	// downstream transactions and the leader-election stake snapshot treat inputs
	// the endorser block should have produced as missing (the "utxo not found"
	// repair loop and "pool has no stake in epoch snapshot" rejection). The delta
	// is recorded under the ranking block's point, so a rollback of the ranking
	// block removes these effects.
	if !ls.config.LeiosApplyEndorserBlockTxs {
		delta.skipConsumedInputRecovery = true
		if err := delta.applyWithoutRecordingDonations(ls, txn); err != nil {
			return 0, 0, &leiosEndorserBlockStorageError{
				err: fmt.Errorf(
					"apply endorser block transactions: %w",
					err,
				),
			}
		}
		return len(delta.Transactions), delta.donation, nil
	}

	// CIP-conformant path: apply the endorser transactions as a delta recorded
	// under the ranking block's point (so a rollback removes them), with offsets
	// pointing into the endorser-block blob.
	if err := delta.applyWithoutRecordingDonations(ls, txn); err != nil {
		return 0, 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("apply endorser block transactions: %w", err),
		}
	}
	return len(txs), delta.donation, nil
}

func (ls *LedgerState) deduplicateEndorserBlockTransactionIndexes(
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
	existing, err := ls.db.GetTransactionsByHashes(hashes, txn)
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
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		off, length, err := writeRange(bodyCbors[i])
		if err != nil {
			return nil, nil, err
		}
		result.TxOffsets[txHash] = database.CborOffset{
			BlockSlot:  ebSlot,
			BlockHash:  ebHash,
			ByteOffset: off,
			ByteLength: length,
		}
		for _, utxo := range tx.Produced() {
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
				TxId:      txHash,
				OutputIdx: utxo.Id.Index(),
			}] = database.CborOffset{
				BlockSlot:  ebSlot,
				BlockHash:  ebHash,
				ByteOffset: off,
				ByteLength: length,
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
//     being pushed; wait for the in-flight leios-notify/leios-fetch to cache
//     it, with an active by-point fetch as a fallback if the window elapses.
//   - Historical backlog (well below the head, e.g. during a from-scratch
//     catch-up): the relay does not diffuse these, but it does serve any
//     endorser block by point on demand, so actively fetch them -- in parallel
//     across the available relay connections -- and apply the endorser-resident
//     outputs instead of leaving the UTxO set incomplete and trusting the
//     chain. On the Musashi certificate-driven path a certified closure is
//     mandatory: an incomplete all-peer fetch returns an error before its
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
	// On the Haskell-conformant (Musashi) path, settled-backlog fetches are
	// certificate-driven; on the CIP path they stay announcement-driven, so the
	// CIP backfill is unchanged.
	certDrivenHistorical := !ls.config.LeiosApplyEndorserBlockTxs
	if certDrivenHistorical {
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
			parent, err := ls.BlockByHash([]byte(info.prevHash))
			if err != nil {
				continue
			}
			if ebHash, _, ok := leiosAnnouncementFromBlockCbor(parent.Cbor); ok {
				annByHash[info.prevHash] = leiosEbRef{
					slot: parent.Slot,
					hash: ebHash,
				}
			}
		}
	}
	required, err := requiredCertifiedEndorserBlocks(
		infos,
		annByHash,
		certDrivenHistorical,
	)
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
	ensureRequiredAvailable := func() error {
		for _, r := range required {
			if endorserBlockAvailableAt(
				ls.config.EndorserBlockProvider,
				r.hash.Bytes(),
				r.slot,
			) {
				continue
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

	// A zero wait disables best-effort announcement waiting, but a certified
	// Musashi closure remains mandatory: committing its CertRB without the
	// closure would permanently omit transaction and certificate effects.
	if ls.config.EndorserBlockWaitSlots == 0 {
		return ensureRequiredAvailable()
	}
	slotLen := ls.shelleySlotLength()
	if slotLen <= 0 {
		// Without a known slot length the slot-denominated diffusion window
		// cannot be converted to wall-clock. Best-effort announcements may
		// still be skipped, but a certified closure must not be.
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
		certDrivenHistorical,
		cached,
	)
	// Historical backlog: ensure a by-point fetch is in flight for each
	// referenced endorser block (prefetchBatchEndorserBlocks has usually already
	// started them for the whole read batch), then wait for it to land in the
	// cache. The fetches run concurrently in the background pool; the waits below
	// are mostly cache hits, so this does not serialize catch-up on fetch latency
	// the way a per-chunk barrier did.
	if len(backfill) > 0 && ls.leiosBackfill != nil {
		for _, r := range backfill {
			ls.leiosBackfill.spawn(ctx, r)
		}
		for _, r := range backfill {
			if endorserBlockAvailableAt(
				ls.config.EndorserBlockProvider,
				r.hash.Bytes(),
				r.slot,
			) {
				continue
			}
			ls.leiosBackfill.awaitFetch(ctx, r, poll)
		}
	}
	for _, r := range tipWait {
		if endorserBlockAvailableAt(
			ls.config.EndorserBlockProvider,
			r.hash.Bytes(),
			r.slot,
		) {
			continue
		}
		ls.waitForEndorserBlock(ctx, r.slot, r.hash, timeout, poll)
		if ls.config.EndorserBlockFetcher == nil {
			continue
		}
		if !endorserBlockAvailableAt(
			ls.config.EndorserBlockProvider,
			r.hash.Bytes(),
			r.slot,
		) {
			if err := ls.config.EndorserBlockFetcher(
				ctx,
				r.slot,
				r.hash.Bytes(),
			); err != nil {
				ls.config.Logger.Debug(
					"endorser block tip fetch fallback failed",
					"component", "ledger",
					"slot", r.slot,
					"eb_hash", r.hash.String(),
					"error", err,
				)
			}
		}
	}
	return ensureRequiredAvailable()
}

// leiosEbRef pairs a ranking block's slot with the hash of the endorser block
// it references. The endorser block shares the ranking block's slot.
type leiosEbRef struct {
	slot uint64
	hash lcommon.Blake2b256
}

// leiosEbRefKey returns a stable per-(slot, hash) dedup key for r, not hash
// alone. The manifest is content-addressed, so the same hash can legitimately
// be a distinct requirement at two different slots at once (issue #3513
// review); every dedup/in-flight-tracking map keyed on an endorser-block
// reference in this file uses this key, so a second, slot-distinct reference
// to an already-seen hash is never collapsed into (or suppressed by) the
// first.
func leiosEbRefKey(r leiosEbRef) string {
	return fmt.Sprintf("%d:%s", r.slot, r.hash.Bytes())
}

// endorserBlockAvailableAt reports whether provider already holds the
// endorser block identified by hash bound to exactly the given slot -- not
// merely present under some slot. The manifest is content-addressed, so the
// same hash can be a live, independently required occurrence at more than
// one slot at once (issue #3513); every call site here already knows the
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
// transactions are consensus ledger effects on the Haskell-conformant Musashi
// path. Current announcements remain best-effort until a later block certifies
// them. A certifying block whose parent announcement cannot be resolved is
// rejected: proceeding would commit a ledger state known to be incomplete.
// Deduped by leiosEbRefKey (slot, hash), not hash alone: two certifying
// blocks in the same batch can legitimately require the same hash at
// different slots (issue #3513 review), and a hash-only dedup would drop the
// second requirement from the result entirely.
func requiredCertifiedEndorserBlocks(
	infos []leiosBlockInfo,
	annByHash map[string]leiosEbRef,
	certDrivenHistorical bool,
) ([]leiosEbRef, error) {
	if !certDrivenHistorical {
		return nil, nil
	}
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

// classifyEndorserBlockFetches decides which endorser blocks to fetch for a
// batch of ranking blocks, by where each block sits relative to the live head:
//
//   - Near the head (within waitSlots of wallSlot): current announcements are
//     fetched on both paths. On the Haskell-conformant path, a certifying block's
//     parent announcement is fetched as well, because prototype-2026w29 permits
//     one ranking block to certify its parent's EB and announce a new EB.
//   - Settled backlog (more than waitSlots below the head): the policy depends
//     on certDrivenHistorical.
//
// certDrivenHistorical selects the settled-backlog policy, following the
// endorser-block ledger path (LeiosApplyEndorserBlockTxs):
//
//   - true (Haskell-conformant path, e.g. Musashi): certificate-driven. Fetch a
//     settled endorser block only once a certifying ranking block certifies it
//     — the certified endorser block is the one announced by the CertRB's
//     parent (prevHash), per prototype-2026w29. Uncertified historical
//     announcements are skipped because their transactions are not applied on
//     this path; only certified endorser blocks affect the ledger or the merged
//     node-to-client view, and the relay does not reliably serve uncertified
//     ones.
//   - false (CIP-conformant path): announcement-driven, like the near-head
//     case. Endorser transactions are applied to the UTxO, so every referenced
//     endorser block is fetched to build a complete set. This preserves the
//     CIP-path backfill unchanged.
//
// annByHash resolves a CertRB's parent announcement (block hash -> announced
// endorser block); the caller supplies parents outside the batch. cached
// reports whether an endorser block is already available *at r's slot*, so a
// stale occurrence of the hash under a different slot is not mistaken for
// availability and is fetched like any other missing reference (issue #3513
// review). backfillSeen/tipWaitSeen (via appendRef's leiosEbRefKey) dedup by
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
	certDrivenHistorical bool,
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
		if certDrivenHistorical && info.certifies {
			// The certified EB is always the parent's announcement. Near the
			// head this is independent of the current block's own announcement:
			// prototype-2026w29 permits a block to contain both.
			if r, ok := annByHash[info.prevHash]; ok {
				if historical {
					appendRef(&backfill, backfillSeen, r)
				} else {
					appendRef(&tipWait, tipWaitSeen, r)
				}
			}
		}
		if historical && certDrivenHistorical {
			// Historical Musashi replay applies certified EBs only; do not fetch
			// the current block's uncertified announcement.
			continue
		}
		if !info.announces {
			continue
		}
		r := leiosEbRef{slot: info.slot, hash: info.ebHash}
		if historical {
			// CIP-conformant settled backlog: fetch every referenced block.
			appendRef(&backfill, backfillSeen, r)
		} else {
			appendRef(&tipWait, tipWaitSeen, r)
		}
	}
	return backfill, tipWait
}

// leiosAnnouncementFromBlockCbor decodes the endorser block reference a
// Dijkstra ranking block announces from its raw CBOR, or ok=false when it
// announces none. The block is [header, block_body]; the announcement rides on
// the header extension. Used to resolve a CertRB's parent announcement.
func leiosAnnouncementFromBlockCbor(
	blockCbor []byte,
) (lcommon.Blake2b256, uint64, bool) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &top); err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, 0, false
	}
	var header dijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(top[0], &header); err != nil {
		return lcommon.Blake2b256{}, 0, false
	}
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	if !ok {
		return lcommon.Blake2b256{}, 0, false
	}
	return ebHash, ebSize, true
}

// leiosEndorserBlockForApply selects the EB whose transactions affect this
// ranking block. The forward/CIP path applies the block's own announcement. The
// Musashi prototype-2026w29 path applies only a certified closure: the EB
// announced by the certifying block's parent. A w29 CertRB may also announce a
// new EB, so its current announcement must not be mistaken for the certified
// one.
// The returned expectedSlot is the slot the referenced endorser block must be
// bound to: the endorser block shares its announcing ranking block's slot
// (see leiosEbRef), which is this block's own slot on the CIP path or the
// certifying block's parent's slot on the Musashi path. Callers must check a
// provider result against it (endorserBlockAvailableAt) rather than trust
// whatever slot the provider itself reports, since the manifest is
// content-addressed and the same hash can legitimately recur at a different
// slot (issue #3513 review).
func (ls *LedgerState) leiosEndorserBlockForApply(
	block ledger.Block,
) (hash lcommon.Blake2b256, expectedSlot, size uint64, announced bool, err error) {
	if ls.config.LeiosApplyEndorserBlockTxs {
		ref, ok := block.Header().(leiosEndorserBlockReferencer)
		if !ok {
			return lcommon.Blake2b256{}, 0, 0, false, nil
		}
		hash, size, announced = ref.LeiosAnnouncement()
		return hash, block.SlotNumber(), size, announced, nil
	}
	certifier, ok := block.Header().(leiosEndorserBlockCertifier)
	if !ok {
		return lcommon.Blake2b256{}, 0, 0, false, nil
	}
	certified, present := certifier.LeiosCertified()
	if !present || !certified {
		return lcommon.Blake2b256{}, 0, 0, false, nil
	}
	parent, perr := ls.BlockByHash(block.PrevHash().Bytes())
	if perr != nil {
		return lcommon.Blake2b256{}, 0, 0, false, fmt.Errorf(
			"resolve certifying block parent: %w",
			perr,
		)
	}
	hash, size, announced = leiosAnnouncementFromBlockCbor(parent.Cbor)
	return hash, parent.Slot, size, announced, nil
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
// second requirement's slot was never fetched at all (issue #3513 review).
func (b *leiosBackfiller) spawn(ctx context.Context, r leiosEbRef) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := leiosEbRefKey(r)
	if _, loaded := b.inflight.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
		b.inflight.Delete(key)
		return
	}
	go func() {
		fetchCtx, cancel := context.WithTimeout(ctx, leiosBackfillMaxWait)
		defer cancel()
		select {
		case b.sem <- struct{}{}:
		case <-fetchCtx.Done():
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

// waitForEndorserBlock polls the EndorserBlockProvider until the endorser block
// identified by ebHash is fetched and cached complete, ctx is cancelled, or the
// diffusion-window timeout elapses. The concurrent leios-notify/leios-fetch
// handlers keep making progress while this blocks, so the in-flight fetch
// completes during the wait.
func (ls *LedgerState) waitForEndorserBlock(
	ctx context.Context,
	rbSlot uint64,
	ebHash lcommon.Blake2b256,
	timeout, poll time.Duration,
) {
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
			return
		}
		select {
		case <-waitCtx.Done():
			ls.config.Logger.Info(
				"endorser block not fetched within diffusion window; proceeding without it",
				"component",
				"ledger",
				"slot",
				rbSlot,
				"eb_hash",
				ebHash.String(),
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

// awaitFetch waits for the in-flight by-point fetch of the endorser block
// referenced by r to finish (it has already been spawned). The spawned fetch
// (FetchEndorserBlockByPoint) tries every connected peer in turn before
// failing, so by the time it clears its in-flight marker it has tried all
// peers. If it cached the block, the referencing ranking block can apply it.
// If it finished without caching (every peer's response was flaky/incomplete,
// e.g. a single connection that cannot serve a large endorser block's tail),
// return promptly. The caller distinguishes best-effort announcements from
// mandatory certified Musashi closures: the former may advance, while the
// latter abort the chunk and are retried by the ledger pipeline.
// leiosBackfillMaxWait is a backstop against a fetch that neither caches nor
// clears (the fetch itself is bounded by the leios-fetch timeout, so this is
// rarely reached).
func (b *leiosBackfiller) awaitFetch(
	ctx context.Context,
	r leiosEbRef,
	poll time.Duration,
) {
	waitCtx, cancel := context.WithTimeout(ctx, leiosBackfillMaxWait)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	key := leiosEbRefKey(r)
	for {
		if endorserBlockAvailableAt(b.provider, r.hash.Bytes(), r.slot) {
			return // cached: the referencing block can apply it
		}
		if _, inFlight := b.inflight.Load(key); !inFlight {
			return // the all-peers fetch finished without caching: skip fast
		}
		select {
		case <-waitCtx.Done():
			return
		case <-ticker.C:
		}
	}
}
