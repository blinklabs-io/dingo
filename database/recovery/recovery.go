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

package recovery

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

const (
	// walSubdir and checkpointSubdir sit under the configured recovery
	// directory so the two artifact kinds can be reasoned about, and
	// removed, independently.
	walSubdir        = "wal"
	checkpointSubdir = "checkpoints"
	// DefaultCheckpointInterval is how often a running node records a
	// checkpoint. Checkpoints only bound replay work, so a coarse cadence
	// costs nothing but a slightly longer journal.
	DefaultCheckpointInterval = 5 * time.Minute
)

// Repairer applies the repairs recovery decides on.
//
// It is implemented above this package, by whichever component owns the state
// being repaired, so recovery can decide what to do without knowing how the
// ledger or the stores carry it out.
type Repairer interface {
	// TrimBlobAbove removes blocks the blob store holds above slot and
	// returns how many it removed.
	TrimBlobAbove(slot uint64) (int, error)
	// RollbackTo rewinds applied state to point.
	RollbackTo(point Point) error
	// ResetCommitFence brings both stores back onto a common commit
	// timestamp so they agree again. The value is the implementation's to
	// choose; recovery only requires that the two match afterwards.
	ResetCommitFence() error
}

// ChainRewinder is optionally implemented by repairers whose persistent chain
// must be brought back with the blob store before ledger recovery runs.
type ChainRewinder interface {
	RewindPrimaryChainTo(point Point) error
}

// Config configures the recovery manager.
type Config struct {
	Logger *slog.Logger
	// Dir is the base directory for recovery artifacts. The journal and
	// checkpoints live in subdirectories of it.
	Dir string
	// CheckMode selects how much work the startup consistency checks do.
	CheckMode CheckMode
	// CheckpointInterval is how often a running node records a checkpoint.
	// Zero selects DefaultCheckpointInterval.
	CheckpointInterval time.Duration
	// CheckpointRetain is how many checkpoint generations to keep. Zero
	// selects the package default.
	CheckpointRetain int
	// MaxSegmentBytes bounds one journal segment. Zero selects the default.
	MaxSegmentBytes int64
	// SyncJournal makes each intent record durable before the commit that
	// wrote it touches the stores. Leaving this off makes the journal
	// advisory only; see WALConfig.SyncOnBegin.
	SyncJournal bool
}

// Outcome grades what a recovery run had to do.
type Outcome uint8

const (
	// OutcomeClean means the stores agreed and nothing needed repair.
	OutcomeClean Outcome = iota
	// OutcomeRepaired means recovery found divergence and fixed it.
	OutcomeRepaired
	// OutcomeUnrepaired means recovery found divergence it could not fix.
	OutcomeUnrepaired
)

// String renders an outcome for logs.
func (o Outcome) String() string {
	switch o {
	case OutcomeClean:
		return "clean"
	case OutcomeRepaired:
		return "repaired"
	case OutcomeUnrepaired:
		return "unrepaired"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(o))
	}
}

// Result describes one recovery run.
type Result struct {
	// Checkpoint is the anchor recovery loaded, if any existed.
	Checkpoint *Checkpoint
	// Report holds the startup consistency check outcomes.
	Report Report
	// Pending lists the intents that were begun but never resolved, which
	// is what a crash inside the commit window leaves behind.
	Pending []Record
	// Actions describes, in order, what recovery did.
	Actions []string
	Outcome Outcome
}

// Manager owns the journal, the checkpoints, and the startup recovery run.
type Manager struct {
	logger      *slog.Logger
	wal         *WAL
	checkpoints *CheckpointStore
	open        map[uint64]struct{}
	stop        chan struct{}
	done        chan struct{}

	cfg    Config
	mu     sync.Mutex
	closed bool
}

// New opens the journal and checkpoint store under cfg.Dir.
func New(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, errors.New("recovery directory is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.CheckpointInterval < 0 {
		return nil, errors.New("checkpoint interval cannot be negative")
	}
	if cfg.CheckpointInterval == 0 {
		cfg.CheckpointInterval = DefaultCheckpointInterval
	}
	if cfg.CheckMode == "" {
		cfg.CheckMode = CheckModeFast
	}
	wal, err := OpenWAL(WALConfig{
		Dir:             filepath.Join(cfg.Dir, walSubdir),
		MaxSegmentBytes: cfg.MaxSegmentBytes,
		SyncOnBegin:     cfg.SyncJournal,
		Logger:          logger,
	})
	if err != nil {
		return nil, err
	}
	checkpoints, err := NewCheckpointStore(
		filepath.Join(cfg.Dir, checkpointSubdir),
		cfg.CheckpointRetain,
		logger,
	)
	if err != nil {
		_ = wal.Close()
		return nil, err
	}
	return &Manager{
		cfg:         cfg,
		logger:      logger,
		wal:         wal,
		checkpoints: checkpoints,
		open:        make(map[uint64]struct{}),
	}, nil
}

// Begin records the intent of a cross-store commit and returns its sequence.
func (m *Manager) Begin(intent Intent, commitTimestamp int64) (uint64, error) {
	if m == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrWALClosed
	}
	seq, err := m.wal.Begin(intent, commitTimestamp)
	if err != nil {
		return 0, err
	}
	m.open[seq] = struct{}{}
	return seq, nil
}

// Commit marks a sequence as applied to both stores.
func (m *Manager) Commit(seq uint64) error {
	if m == nil {
		return nil
	}
	err := m.wal.Commit(seq)
	if err == nil {
		m.resolve(seq)
	}
	return err
}

// Abort marks a sequence as rolled back before either store committed.
func (m *Manager) Abort(seq uint64) error {
	if m == nil {
		return nil
	}
	err := m.wal.Abort(seq)
	if err == nil {
		m.resolve(seq)
	}
	return err
}

func (m *Manager) resolve(seq uint64) {
	m.mu.Lock()
	delete(m.open, seq)
	m.mu.Unlock()
}

// checkpointSeq returns the highest sequence a checkpoint may claim to cover.
//
// It stops below the oldest still-open commit: a checkpoint that covered an
// in-flight commit would authorise truncating away the very intent record
// recovery needs if the process died before that commit resolved.
func (m *Manager) checkpointSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldestOpen := uint64(0)
	for seq := range m.open {
		if oldestOpen == 0 || seq < oldestOpen {
			oldestOpen = seq
		}
	}
	next := m.wal.NextSeq()
	if oldestOpen > 0 {
		return oldestOpen - 1
	}
	if next == 0 {
		return 0
	}
	return next - 1
}

// Start begins recording checkpoints on the configured interval. Stop them with
// Close. Calling it more than once is a no-op after the first.
func (m *Manager) Start(source StateSource) {
	if m == nil || source == nil {
		return
	}
	m.mu.Lock()
	if m.stop != nil || m.closed {
		m.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	m.stop = stop
	m.done = done
	interval := m.cfg.CheckpointInterval
	m.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := m.Checkpoint(source); err != nil {
					m.logger.Warn(
						"failed to record recovery checkpoint",
						"error", err,
					)
				}
			}
		}
	}()
}

// Checkpoint records a checkpoint from the current store state and truncates
// journal segments the new checkpoint fully covers.
//
// The checkpoint is a summary observed without quiescing the stores, so it can
// lag a commit that landed while it was being taken. That is fine: recovery
// treats store state as authoritative and uses the checkpoint as a verified
// anchor and a truncation floor, never as a source of state to restore.
func (m *Manager) Checkpoint(source StateSource) error {
	if m == nil || source == nil {
		return nil
	}
	seq := m.checkpointSeq()
	tip, tipBlockNumber, err := source.MetadataTip()
	if err != nil {
		return fmt.Errorf("read metadata tip for checkpoint: %w", err)
	}
	blobTip, err := source.BlobTip()
	if err != nil {
		return fmt.Errorf("read blob tip for checkpoint: %w", err)
	}
	metadataTS, blobTS, err := source.CommitTimestamps()
	if err != nil {
		return fmt.Errorf("read commit timestamps for checkpoint: %w", err)
	}
	if metadataTS != blobTS {
		// The two timestamps are not read atomically, so a commit landing
		// between them looks exactly like divergence. Read once more
		// before believing it; a commit is not still in its window on the
		// second look.
		metadataTS, blobTS, err = source.CommitTimestamps()
		if err != nil {
			return fmt.Errorf(
				"re-read commit timestamps for checkpoint: %w",
				err,
			)
		}
	}
	if metadataTS != blobTS {
		// Checkpointing state the stores do not agree on would hand
		// recovery an anchor that is itself inconsistent.
		return fmt.Errorf(
			"refusing to checkpoint diverged stores: metadata %d, blob %d",
			metadataTS,
			blobTS,
		)
	}
	cp := Checkpoint{
		Seq:              seq,
		CreatedUnixMilli: time.Now().UnixMilli(),
		CommitTimestamp:  metadataTS,
		TipSlot:          tip.Slot,
		TipHash:          tip.Hash,
		TipBlockNumber:   tipBlockNumber,
		BlobTipSlot:      blobTip.Slot,
		BlobTipHash:      blobTip.Hash,
	}
	// The journal copy goes first: a checkpoint visible in the checkpoint
	// store but absent from the journal would let truncation run against a
	// journal that never recorded the boundary.
	if err := m.wal.AppendCheckpoint(cp); err != nil {
		return fmt.Errorf("append checkpoint to journal: %w", err)
	}
	if err := m.checkpoints.Write(cp); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	removed, err := m.wal.TruncateThrough(seq)
	if err != nil {
		return fmt.Errorf("truncate journal through %d: %w", seq, err)
	}
	m.logger.Debug(
		"recorded recovery checkpoint",
		"seq", seq,
		"tip_slot", tip.Slot,
		"blob_tip_slot", blobTip.Slot,
		"segments_removed", removed,
	)
	return nil
}

// Recover runs the startup consistency checks, replays the journal, and repairs
// whatever divergence the two together identify.
//
// A nil repairer runs the checks and reports findings without changing
// anything, which is what a caller that only wants a diagnosis passes.
func (m *Manager) Recover(
	source StateSource,
	repairer Repairer,
) (*Result, error) {
	if m == nil {
		return &Result{}, nil
	}
	if source == nil {
		return nil, errors.New("recovery requires a state source")
	}
	checker, err := NewChecker(source, m.cfg.CheckMode, m.logger)
	if err != nil {
		return nil, err
	}
	result := &Result{Report: checker.Run()}
	result.Report.Log(m.logger)

	if cp, err := m.checkpoints.Latest(); err == nil {
		result.Checkpoint = &cp
		m.logger.Info(
			"loaded recovery checkpoint",
			"seq", cp.Seq,
			"tip_slot", cp.TipSlot,
			"blob_tip_slot", cp.BlobTipSlot,
			"commit_timestamp", cp.CommitTimestamp,
		)
	} else if !errors.Is(err, ErrNoCheckpoint) {
		// A checkpoint store that cannot be read is worth reporting, but
		// it never blocks recovery: store state is authoritative and the
		// journal replay below does not depend on the anchor.
		m.logger.Warn("could not load recovery checkpoint", "error", err)
	}

	pending, replayErr := m.replayPending()
	if replayErr != nil {
		m.logger.Warn(
			"could not replay the recovery journal; falling back to stored state and checkpoint",
			"error", replayErr,
		)
		result.Actions = append(
			result.Actions,
			fmt.Sprintf("journal replay failed: %v", replayErr),
		)
	}
	result.Pending = pending
	for _, record := range pending {
		m.logger.Warn(
			"recovery journal holds an unresolved commit intent",
			"seq", record.Seq,
			"intent", record.Intent.Kind.String(),
			"slot", record.Intent.Slot,
			"hash", shortHash(record.Intent.Hash),
			"commit_timestamp", record.CommitTimestamp,
		)
	}

	if err := m.repair(source, repairer, result); err != nil {
		return result, err
	}
	if result.Outcome == OutcomeClean && result.Report.Failed() {
		// The stores agree on their fence, so there is no interrupted
		// commit to undo, but a check still found damage. Say so rather
		// than reporting a clean recovery.
		result.Outcome = OutcomeUnrepaired
	}
	return result, nil
}

// Replay passes every readable journal record to fn, oldest first.
//
// Recovery uses replayPending for its own decisions; this is the entry point
// for anything that wants to look at the journal itself, such as a diagnostic
// dump of what a node was doing when it died.
func (m *Manager) Replay(fn func(Record) error) error {
	if m == nil {
		return nil
	}
	return m.wal.Replay(fn)
}

// replayPending returns the begin records with no matching commit or abort.
func (m *Manager) replayPending() ([]Record, error) {
	pending := map[uint64]Record{}
	order := []uint64{}
	err := m.wal.Replay(func(r Record) error {
		switch r.Type {
		case RecordTypeBegin:
			if _, seen := pending[r.Seq]; !seen {
				order = append(order, r.Seq)
			}
			pending[r.Seq] = r
		case RecordTypeCommit, RecordTypeAbort:
			delete(pending, r.Seq)
		case RecordTypeCheckpoint:
			// A checkpoint asserts both stores agreed at its
			// sequence, so anything begun at or below it is
			// resolved whether or not its marker survived.
			for seq := range pending {
				if seq <= r.Seq {
					delete(pending, seq)
				}
			}
		}
		return nil
	})
	out := make([]Record, 0, len(pending))
	for _, seq := range order {
		if r, ok := pending[seq]; ok {
			out = append(out, r)
		}
	}
	return out, err
}

// repair diagnoses divergence between the two stores and fixes it.
//
// The cross-store commit order — blob, sync, then metadata — means only one
// direction of divergence is expected: the blob store ahead of the metadata
// store by whatever the interrupted commit had written. The other direction
// means the blob store lost a durable write, and the only way back to a
// consistent state is to rewind applied state onto what the blob store still
// holds.
func (m *Manager) repair(
	source StateSource,
	repairer Repairer,
	result *Result,
) error {
	metadataTS, blobTS, err := source.CommitTimestamps()
	if err != nil {
		return fmt.Errorf("read commit timestamps: %w", err)
	}
	metaTip, _, err := source.MetadataTip()
	if err != nil {
		return fmt.Errorf("read metadata tip: %w", err)
	}
	blobTip, err := source.BlobTip()
	if err != nil {
		return fmt.Errorf("read blob tip: %w", err)
	}
	if metadataTS == blobTS && metaTip.Equal(blobTip) {
		// The stores hold the same fence, so no commit was half applied.
		// Unresolved intents on their own do not change that: only the
		// begin record is synced, so a crash routinely loses the commit
		// marker of a commit that did land, and repairing on that evidence
		// alone would run a trim, and report a repair, for a database that
		// is intact. They are still surfaced in the result.
		result.Outcome = OutcomeClean
		if len(result.Pending) > 0 {
			result.Actions = append(result.Actions, fmt.Sprintf(
				"%d unresolved intents in the journal, but both stores hold commit timestamp %d; nothing to repair",
				len(result.Pending),
				metadataTS,
			))
		}
		return nil
	}
	if repairer == nil {
		result.Outcome = OutcomeUnrepaired
		result.Actions = append(
			result.Actions,
			"divergence detected but no repairer was supplied",
		)
		return nil
	}
	if blobTip.Slot < metaTip.Slot {
		// The blob store is behind the state the metadata store claims
		// to have applied. Rewind onto what the blob store holds.
		//
		// The fences decide this, not the tips. A blob tip below the
		// metadata tip with both fences agreeing is real damage, but
		// nothing here caused it, and rolling the ledger back on evidence
		// the stores themselves contradict is too destructive to do
		// unprompted. The tip_consistency check reports that shape as a
		// failure instead.
		m.logger.Warn(
			"blob store is behind the metadata store; rewinding applied state to the blob tip",
			"metadata_commit_timestamp", metadataTS,
			"blob_commit_timestamp", blobTS,
			"metadata_tip_slot", metaTip.Slot,
			"blob_tip_slot", blobTip.Slot,
		)
		if rewinder, ok := repairer.(ChainRewinder); ok {
			if err := rewinder.RewindPrimaryChainTo(blobTip); err != nil {
				result.Outcome = OutcomeUnrepaired
				return fmt.Errorf("rewind primary chain to blob tip %d: %w", blobTip.Slot, err)
			}
		}
		if err := repairer.RollbackTo(blobTip); err != nil {
			result.Outcome = OutcomeUnrepaired
			return fmt.Errorf(
				"rollback to blob tip %d: %w",
				blobTip.Slot,
				err,
			)
		}
		result.Actions = append(result.Actions, fmt.Sprintf(
			"rolled applied state back to blob tip slot %d",
			blobTip.Slot,
		))
		if err := repairer.ResetCommitFence(); err != nil {
			result.Outcome = OutcomeUnrepaired
			return fmt.Errorf("reset commit fence: %w", err)
		}
		result.Actions = append(
			result.Actions,
			"reset both stores onto a common commit timestamp",
		)
		result.Outcome = OutcomeRepaired
		return nil
	}
	if metaTip.Slot == blobTip.Slot && !metaTip.Equal(blobTip) {
		result.Outcome = OutcomeUnrepaired
		result.Actions = append(result.Actions, fmt.Sprintf("detected same-slot metadata/blob fork at slot %d", metaTip.Slot))
		return nil
	}

	// The blob store is ahead, the expected shape. Blocks above the trim
	// boundary are the interrupted commit's residue.
	//
	// The boundary is the chain tip where the caller can report one, not the
	// metadata tip. Between them sit blocks that are legitimately on-chain
	// and merely not applied yet — the normal shape after a snapshot
	// bootstrap — and trimming those would destroy data the node still
	// needs.
	boundary := metaTip.Slot
	boundaryName := "metadata tip"
	if chainSource, ok := source.(ChainTipSource); ok {
		chainTip, _, err := chainSource.ChainTip()
		if err != nil {
			return fmt.Errorf("read chain tip: %w", err)
		}
		if chainTip.Slot == blobTip.Slot && !chainTip.Equal(blobTip) {
			result.Outcome = OutcomeUnrepaired
			result.Actions = append(result.Actions, fmt.Sprintf(
				"detected same-slot primary-chain/blob fork at slot %d",
				blobTip.Slot,
			))
			return nil
		}
		if chainTip.Slot > boundary {
			boundary = chainTip.Slot
			boundaryName = "chain tip"
		}
	}
	if boundary == 0 && blobTip.Slot > 0 {
		// Neither the applied tip nor the chain says anything about where
		// the chain ends, but the blob store holds blocks. Trimming above
		// slot zero would erase all of them, and no repair is worth that
		// on evidence this thin — an unloaded tip looks identical to a
		// genuinely empty one from here.
		m.logger.Error(
			"refusing to trim the blob store: no tip is known, so every stored block would be removed",
			"blob_tip_slot", blobTip.Slot,
			"metadata_commit_timestamp", metadataTS,
			"blob_commit_timestamp", blobTS,
		)
		result.Actions = append(
			result.Actions,
			"declined to trim above slot 0 with no known tip",
		)
		result.Outcome = OutcomeUnrepaired
		return nil
	}
	trimmed, err := repairer.TrimBlobAbove(boundary)
	if err != nil {
		result.Outcome = OutcomeUnrepaired
		return fmt.Errorf(
			"trim blob store above slot %d: %w",
			boundary,
			err,
		)
	}
	if trimmed > 0 {
		m.logger.Warn(
			"removed blocks left above the chain by an interrupted commit",
			"removed", trimmed,
			"boundary", boundaryName,
			"boundary_slot", boundary,
		)
	}
	result.Actions = append(result.Actions, fmt.Sprintf(
		"removed %d blocks above the %s at slot %d",
		trimmed,
		boundaryName,
		boundary,
	))
	if err := repairer.ResetCommitFence(); err != nil {
		result.Outcome = OutcomeUnrepaired
		return fmt.Errorf("reset commit fence: %w", err)
	}
	result.Actions = append(
		result.Actions,
		"reset both stores onto a common commit timestamp",
	)
	result.Outcome = OutcomeRepaired
	return nil
}

// Close stops checkpointing and closes the journal.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	stop, done := m.stop, m.done
	m.stop, m.done = nil, nil
	m.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
	return m.wal.Close()
}
