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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// Check names, stable enough to match on in logs and tests.
const (
	CheckCommitTimestamps = "commit_timestamps"
	CheckTipConsistency   = "tip_consistency"
	CheckChainLedgerTip   = "chain_ledger_tip"
	CheckBlockContinuity  = "block_continuity"
	CheckUtxoIntegrity    = "utxo_integrity"
	CheckOrphanedData     = "orphaned_data"
)

// Severity grades a check outcome.
type Severity uint8

const (
	// SeverityOK means the check found nothing wrong.
	SeverityOK Severity = iota
	// SeverityWarn means the check found something an operator should see
	// but that does not by itself make the state unusable. Repairable
	// divergence between the stores lands here, because recovery is
	// expected to fix it.
	SeverityWarn
	// SeverityFail means the check found state that recovery cannot be
	// assumed to fix.
	SeverityFail
)

// String renders a severity for logs.
func (s Severity) String() string {
	switch s {
	case SeverityOK:
		return "ok"
	case SeverityWarn:
		return "warn"
	case SeverityFail:
		return "fail"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(s))
	}
}

// CheckMode selects how much work the startup checks do.
type CheckMode string

const (
	// CheckModeOff skips the startup checks entirely.
	CheckModeOff CheckMode = "off"
	// CheckModeFast runs every check against a bounded window near the tip.
	// This is the default: it is the part of the database a crash can
	// actually have damaged.
	CheckModeFast CheckMode = "fast"
	// CheckModeFull widens the same checks. It is meaningfully slower on a
	// large database and is intended for operators investigating a suspected
	// corruption, not for every start.
	CheckModeFull CheckMode = "full"
)

// ParseCheckMode maps a configured string to a CheckMode. An empty value
// selects the default.
func ParseCheckMode(v string) (CheckMode, error) {
	switch CheckMode(strings.ToLower(strings.TrimSpace(v))) {
	case "":
		return CheckModeFast, nil
	case CheckModeOff:
		return CheckModeOff, nil
	case CheckModeFast:
		return CheckModeFast, nil
	case CheckModeFull:
		return CheckModeFull, nil
	default:
		return "", fmt.Errorf(
			"invalid consistency check mode %q: expected off, fast or full",
			v,
		)
	}
}

// Window sizes per mode.
//
// Fast covers more than a security parameter's worth of blocks, which bounds
// what an interrupted commit can have touched. Full widens each window by
// roughly an order of magnitude; the block and UTxO windows are the expensive
// ones because each entry reads stored CBOR, so they stay well short of "scan
// the whole database", which on mainnet is hours of I/O rather than a startup
// check.
const (
	fastContinuityDepth = 512
	fullContinuityDepth = 4096
	fastUtxoSample      = 512
	fullUtxoSample      = 16384
	fastOrphanLimit     = 1024
	fullOrphanLimit     = 16384
)

// maxReportedBreaks bounds how many linkage breaks a check result spells out.
// The count it reports is not capped; only the examples are.
const maxReportedBreaks = 5

// continuityDepth returns how many blocks beneath the tip to link-check.
func (m CheckMode) continuityDepth() int {
	if m == CheckModeFull {
		return fullContinuityDepth
	}
	return fastContinuityDepth
}

// utxoSample returns how many live UTxOs to resolve.
func (m CheckMode) utxoSample() int {
	if m == CheckModeFull {
		return fullUtxoSample
	}
	return fastUtxoSample
}

// orphanLimit returns how many orphaned blob blocks to enumerate.
func (m CheckMode) orphanLimit() int {
	if m == CheckModeFull {
		return fullOrphanLimit
	}
	return fastOrphanLimit
}

// BlockRef is the minimal view of a stored block the checks need.
type BlockRef struct {
	Hash     []byte
	PrevHash []byte
	Slot     uint64
	Number   uint64
	ID       uint64
}

// Point returns the block's chain position.
func (b BlockRef) Point() Point {
	return Point{Slot: b.Slot, Hash: b.Hash}
}

// UtxoIntegrityResult reports what a bounded UTxO integrity scan found.
type UtxoIntegrityResult struct {
	// Unresolvable names the UTxOs whose stored CBOR could not be read
	// back, in a form safe to log.
	Unresolvable []string
	// Checked counts the UTxOs actually examined, which is at most the
	// requested limit and may be fewer on a small database.
	Checked int
}

// StateSource is the read-only view of stored state the checks need.
//
// It is declared here, and implemented by the database layer, so this package
// stays below the packages it inspects.
type StateSource interface {
	// MetadataTip returns the tip the metadata store records, which is the
	// ledger's view of how far state has been applied.
	MetadataTip() (Point, uint64, error)
	// BlobTip returns the newest block present in the blob store.
	BlobTip() (Point, error)
	// CommitTimestamps returns the cross-store fence each store holds.
	CommitTimestamps() (metadata int64, blob int64, err error)
	// RecentBlocks returns up to limit blocks ending at the blob tip,
	// newest first.
	RecentBlocks(limit int) ([]BlockRef, error)
	// OrphanBlobs returns up to limit blocks the blob store holds above
	// afterSlot, oldest first.
	OrphanBlobs(afterSlot uint64, limit int) ([]BlockRef, error)
	// CheckUtxos resolves up to limit live UTxOs against their stored CBOR.
	CheckUtxos(limit int) (UtxoIntegrityResult, error)
}

// ChainTipSource is the optional part of a source that can also report the
// chain manager's tip. Sources that only see the database do not implement it,
// and the chain-versus-ledger tip check is skipped for them.
type ChainTipSource interface {
	ChainTip() (Point, uint64, error)
}

// CheckResult is one check's outcome.
type CheckResult struct {
	Name     string
	Detail   string
	Severity Severity
}

// Report collects the outcomes of a consistency run.
type Report struct {
	Mode    CheckMode
	Results []CheckResult
}

// Worst returns the highest severity in the report.
func (r Report) Worst() Severity {
	worst := SeverityOK
	for _, res := range r.Results {
		if res.Severity > worst {
			worst = res.Severity
		}
	}
	return worst
}

// Failed reports whether any check reached SeverityFail.
func (r Report) Failed() bool {
	return r.Worst() == SeverityFail
}

// Find returns the result for a named check.
func (r Report) Find(name string) (CheckResult, bool) {
	for _, res := range r.Results {
		if res.Name == name {
			return res, true
		}
	}
	return CheckResult{}, false
}

// Log writes each result at a level matching its severity.
func (r Report) Log(logger *slog.Logger) {
	if logger == nil {
		return
	}
	for _, res := range r.Results {
		switch res.Severity {
		case SeverityFail:
			logger.Error(
				"consistency check failed",
				"check", res.Name,
				"detail", res.Detail,
			)
		case SeverityWarn:
			logger.Warn(
				"consistency check reported a problem",
				"check", res.Name,
				"detail", res.Detail,
			)
		case SeverityOK:
			logger.Debug(
				"consistency check passed",
				"check", res.Name,
				"detail", res.Detail,
			)
		default:
			logger.Warn(
				"consistency check reported an unknown severity",
				"check", res.Name,
				"severity", res.Severity.String(),
				"detail", res.Detail,
			)
		}
	}
}

// Checker runs the startup consistency checks against a StateSource.
type Checker struct {
	source StateSource
	logger *slog.Logger
	mode   CheckMode
}

// NewChecker builds a checker. A zero mode selects CheckModeFast.
func NewChecker(
	source StateSource,
	mode CheckMode,
	logger *slog.Logger,
) (*Checker, error) {
	if source == nil {
		return nil, errors.New("consistency checker requires a state source")
	}
	if mode == "" {
		mode = CheckModeFast
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Checker{source: source, mode: mode, logger: logger}, nil
}

// Run executes every check and returns the collected report.
//
// A check that cannot run because the store returned an error is recorded as a
// failure rather than aborting the run: an operator is better served by the
// full picture than by the first problem encountered.
func (c *Checker) Run() Report {
	report := Report{Mode: c.mode}
	if c.mode == CheckModeOff {
		return report
	}
	metaTip, metaBlockNumber, metaTipErr := c.source.MetadataTip()
	blobTip, blobTipErr := c.source.BlobTip()
	report.Results = append(
		report.Results,
		c.checkCommitTimestamps(),
		c.checkTipConsistency(metaTip, metaTipErr, blobTip, blobTipErr),
	)
	if chainSource, ok := c.source.(ChainTipSource); ok {
		report.Results = append(
			report.Results,
			c.checkChainLedgerTip(
				chainSource,
				metaTip,
				metaBlockNumber,
				metaTipErr,
			),
		)
	}
	report.Results = append(
		report.Results,
		c.checkBlockContinuity(),
		c.checkUtxoIntegrity(),
		c.checkOrphanedData(metaTip, metaTipErr),
	)
	return report
}

// checkCommitTimestamps compares the cross-store fence each store holds.
func (c *Checker) checkCommitTimestamps() CheckResult {
	metadataTS, blobTS, err := c.source.CommitTimestamps()
	if err != nil {
		return failed(CheckCommitTimestamps, "read commit timestamps", err)
	}
	if metadataTS <= 0 {
		return CheckResult{
			Name:     CheckCommitTimestamps,
			Severity: SeverityOK,
			Detail:   "no commits recorded yet",
		}
	}
	if metadataTS == blobTS {
		return CheckResult{
			Name:     CheckCommitTimestamps,
			Severity: SeverityOK,
			Detail: fmt.Sprintf(
				"both stores at commit timestamp %d",
				metadataTS,
			),
		}
	}
	// A mismatch is the expected signature of a crash inside the commit
	// window. Recovery repairs it, so it is a warning here rather than a
	// failure.
	return CheckResult{
		Name:     CheckCommitTimestamps,
		Severity: SeverityWarn,
		Detail: fmt.Sprintf(
			"commit timestamp mismatch: metadata %d, blob %d",
			metadataTS,
			blobTS,
		),
	}
}

// checkTipConsistency compares the metadata tip with the blob tip.
//
// The commit ordering means the blob store may legitimately be ahead by the
// blocks an interrupted commit had written. The blob store being behind is the
// dangerous direction: the ledger then references blocks that are gone.
func (c *Checker) checkTipConsistency(
	metaTip Point,
	metaTipErr error,
	blobTip Point,
	blobTipErr error,
) CheckResult {
	if metaTipErr != nil {
		return failed(CheckTipConsistency, "read metadata tip", metaTipErr)
	}
	if blobTipErr != nil {
		return failed(CheckTipConsistency, "read blob tip", blobTipErr)
	}
	switch {
	case metaTip.Equal(blobTip):
		return CheckResult{
			Name:     CheckTipConsistency,
			Severity: SeverityOK,
			Detail: fmt.Sprintf(
				"metadata and blob tips agree at slot %d",
				metaTip.Slot,
			),
		}
	case blobTip.Slot > metaTip.Slot:
		return CheckResult{
			Name:     CheckTipConsistency,
			Severity: SeverityWarn,
			Detail: fmt.Sprintf(
				"blob tip slot %d is ahead of metadata tip slot %d; blocks above the metadata tip are recoverable orphans",
				blobTip.Slot,
				metaTip.Slot,
			),
		}
	case blobTip.Slot < metaTip.Slot:
		return CheckResult{
			Name:     CheckTipConsistency,
			Severity: SeverityFail,
			Detail: fmt.Sprintf(
				"blob tip slot %d is behind metadata tip slot %d; the ledger references blocks the blob store does not hold",
				blobTip.Slot,
				metaTip.Slot,
			),
		}
	default:
		return CheckResult{
			Name:     CheckTipConsistency,
			Severity: SeverityFail,
			Detail: fmt.Sprintf(
				"metadata and blob tips disagree at slot %d: metadata %s, blob %s",
				metaTip.Slot,
				shortHash(metaTip.Hash),
				shortHash(blobTip.Hash),
			),
		}
	}
}

// checkChainLedgerTip compares the chain manager's tip with the ledger tip.
//
// The chain being ahead is normal — the ledger catches up forward — so only a
// ledger ahead of the chain, or a same-slot hash disagreement, is a problem.
func (c *Checker) checkChainLedgerTip(
	source ChainTipSource,
	ledgerTip Point,
	ledgerBlockNumber uint64,
	ledgerTipErr error,
) CheckResult {
	if ledgerTipErr != nil {
		return failed(CheckChainLedgerTip, "read ledger tip", ledgerTipErr)
	}
	chainTip, chainBlockNumber, err := source.ChainTip()
	if err != nil {
		return failed(CheckChainLedgerTip, "read chain tip", err)
	}
	switch {
	case chainTip.Equal(ledgerTip):
		return CheckResult{
			Name:     CheckChainLedgerTip,
			Severity: SeverityOK,
			Detail: fmt.Sprintf(
				"chain and ledger tips agree at slot %d",
				chainTip.Slot,
			),
		}
	case chainTip.Slot > ledgerTip.Slot:
		return CheckResult{
			Name:     CheckChainLedgerTip,
			Severity: SeverityOK,
			Detail: fmt.Sprintf(
				"chain tip slot %d is ahead of ledger tip slot %d by %d blocks; the ledger replays forward to catch up",
				chainTip.Slot,
				ledgerTip.Slot,
				saturatingSub(chainBlockNumber, ledgerBlockNumber),
			),
		}
	case chainTip.Slot < ledgerTip.Slot:
		return CheckResult{
			Name:     CheckChainLedgerTip,
			Severity: SeverityWarn,
			Detail: fmt.Sprintf(
				"ledger tip slot %d is ahead of chain tip slot %d; the ledger is rolled back to the chain tip at startup",
				ledgerTip.Slot,
				chainTip.Slot,
			),
		}
	default:
		return CheckResult{
			Name:     CheckChainLedgerTip,
			Severity: SeverityWarn,
			Detail: fmt.Sprintf(
				"chain and ledger disagree at slot %d: chain %s, ledger %s",
				chainTip.Slot,
				shortHash(chainTip.Hash),
				shortHash(ledgerTip.Hash),
			),
		}
	}
}

// checkBlockContinuity walks back from the blob tip verifying that each block
// names its predecessor.
//
// Hash linkage is the reliable invariant. Block numbers are only advisory here
// because a Byron epoch boundary block carries the same chain difficulty as the
// block before it, so a strict decrement would report false gaps.
func (c *Checker) checkBlockContinuity() CheckResult {
	depth := c.mode.continuityDepth()
	blocks, err := c.source.RecentBlocks(depth)
	if err != nil {
		return failed(CheckBlockContinuity, "read recent blocks", err)
	}
	if len(blocks) < 2 {
		return CheckResult{
			Name:     CheckBlockContinuity,
			Severity: SeverityOK,
			Detail: fmt.Sprintf(
				"only %d blocks stored; nothing to link-check",
				len(blocks),
			),
		}
	}
	// The whole window is walked so the reported count is the real extent of
	// the damage; only the printed examples are capped. Stopping the walk at
	// the cap would report "5 breaks" for a window with fifty, understating
	// it in exactly the case an operator most needs the true number.
	var breaks []string
	var breakCount, numberAnomalies int
	for i := range len(blocks) - 1 {
		newer := blocks[i]
		older := blocks[i+1]
		if len(newer.PrevHash) > 0 &&
			!bytes.Equal(newer.PrevHash, older.Hash) {
			breakCount++
			if len(breaks) < maxReportedBreaks {
				breaks = append(breaks, fmt.Sprintf(
					"block %d/%s names predecessor %s but the preceding stored block is %d/%s",
					newer.Slot,
					shortHash(newer.Hash),
					shortHash(newer.PrevHash),
					older.Slot,
					shortHash(older.Hash),
				))
			}
		}
		if newer.Number < older.Number || newer.Number > older.Number+1 {
			numberAnomalies++
		}
	}
	if breakCount > 0 {
		return CheckResult{
			Name:     CheckBlockContinuity,
			Severity: SeverityFail,
			Detail: fmt.Sprintf(
				"%d of the newest %d stored blocks do not link to their predecessor; first %d: %s",
				breakCount,
				len(blocks),
				len(breaks),
				strings.Join(breaks, "; "),
			),
		}
	}
	if numberAnomalies > 0 {
		return CheckResult{
			Name:     CheckBlockContinuity,
			Severity: SeverityWarn,
			Detail: fmt.Sprintf(
				"hash linkage is intact across the newest %d blocks but %d block-number steps are not monotonic",
				len(blocks),
				numberAnomalies,
			),
		}
	}
	return CheckResult{
		Name:     CheckBlockContinuity,
		Severity: SeverityOK,
		Detail: fmt.Sprintf(
			"hash linkage intact across the newest %d blocks",
			len(blocks),
		),
	}
}

// checkUtxoIntegrity resolves a bounded sample of live UTxOs against the CBOR
// the blob store holds for them.
//
// Live UTxOs are stored as offset references into block CBOR rather than as
// inline values, so a UTxO row whose block was lost resolves to nothing. That
// is silent until something tries to spend it, which is why it is worth
// sampling at startup.
func (c *Checker) checkUtxoIntegrity() CheckResult {
	limit := c.mode.utxoSample()
	result, err := c.source.CheckUtxos(limit)
	if err != nil {
		return failed(CheckUtxoIntegrity, "sample live utxos", err)
	}
	if len(result.Unresolvable) > 0 {
		detail := result.Unresolvable
		if len(detail) > 5 {
			detail = detail[:5]
		}
		return CheckResult{
			Name:     CheckUtxoIntegrity,
			Severity: SeverityFail,
			Detail: fmt.Sprintf(
				"%d of %d sampled live utxos could not be resolved: %s",
				len(result.Unresolvable),
				result.Checked,
				strings.Join(detail, ", "),
			),
		}
	}
	return CheckResult{
		Name:     CheckUtxoIntegrity,
		Severity: SeverityOK,
		Detail: fmt.Sprintf(
			"%d sampled live utxos resolved",
			result.Checked,
		),
	}
}

// checkOrphanedData looks for blocks the blob store holds above the metadata
// tip, the residue an interrupted commit leaves behind.
func (c *Checker) checkOrphanedData(
	metaTip Point,
	metaTipErr error,
) CheckResult {
	if metaTipErr != nil {
		return failed(CheckOrphanedData, "read metadata tip", metaTipErr)
	}
	orphans, err := c.source.OrphanBlobs(metaTip.Slot, c.mode.orphanLimit())
	if err != nil {
		return failed(CheckOrphanedData, "scan for orphaned blobs", err)
	}
	if len(orphans) == 0 {
		return CheckResult{
			Name:     CheckOrphanedData,
			Severity: SeverityOK,
			Detail: fmt.Sprintf(
				"no blocks stored above the metadata tip slot %d",
				metaTip.Slot,
			),
		}
	}
	return CheckResult{
		Name:     CheckOrphanedData,
		Severity: SeverityWarn,
		Detail: fmt.Sprintf(
			"%d blocks stored above the metadata tip slot %d, from slot %d to %d",
			len(orphans),
			metaTip.Slot,
			orphans[0].Slot,
			orphans[len(orphans)-1].Slot,
		),
	}
}

// failed builds a failure result for a check that could not run.
func failed(name, what string, err error) CheckResult {
	return CheckResult{
		Name:     name,
		Severity: SeverityFail,
		Detail:   fmt.Sprintf("could not %s: %v", what, err),
	}
}

// shortHash renders a block hash prefix for messages.
func shortHash(hash []byte) string {
	if len(hash) == 0 {
		return "<none>"
	}
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return hex.EncodeToString(hash)
}

// saturatingSub subtracts without wrapping past zero.
func saturatingSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}
