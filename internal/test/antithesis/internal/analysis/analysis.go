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

package analysis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// fileState tracks the read position within a single log file so that
// successive checks only process new lines.
type fileState struct {
	path   string
	nodeID string
	offset int64
}

// Analyzer reads node log files, parses events, and reports Antithesis
// assertions on each check interval.
type Analyzer struct {
	cfg       *Config
	metrics   *Metrics
	files     map[string]*fileState // keyed by file path
	logger    *slog.Logger
	setupDone bool
}

// NewAnalyzer creates an Analyzer with the given config and a fresh Metrics.
func NewAnalyzer(cfg *Config, logger *slog.Logger) *Analyzer {
	return &Analyzer{
		cfg:     cfg,
		metrics: NewMetrics(),
		files:   make(map[string]*fileState),
		logger:  logger,
	}
}

// Run executes the analysis loop until ctx is cancelled.
//
// It first waits for cfg.InitialWait, signals SetupComplete, then polls
// every cfg.CheckInterval.
func (a *Analyzer) Run(ctx context.Context) error {
	a.logger.Info(
		"analysis starting",
		"log_dir", a.cfg.LogDir,
		"initial_wait", a.cfg.InitialWait,
		"check_interval", a.cfg.CheckInterval,
	)

	// Initial wait: give nodes time to start up and produce blocks.
	waitTimer := time.NewTimer(a.cfg.InitialWait)
	defer waitTimer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitTimer.C:
	}

	a.logger.Info("initial wait complete; beginning analysis loop")

	ticker := time.NewTicker(a.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.check()
		}
	}
}

// check performs one analysis pass: reads new log lines, feeds them to
// metrics, then evaluates and reports all assertions.
func (a *Analyzer) check() {
	a.readNewLines()
	snap := a.metrics.Snapshot()
	if !a.setupDone && snap.TotalBlocksForged > 0 {
		SetupComplete()
		a.logger.Info("setup complete signaled")
		a.setupDone = true
	}
	a.reportSafetyAssertions(&snap)
	a.reportLivenessAssertions(&snap)
	a.reportReachable(&snap)
}

// readNewLines discovers log files matching p*.log and txpump.log in
// cfg.LogDir, then reads any new lines since the last pass for each file.
func (a *Analyzer) readNewLines() {
	// Node logs (p1.log, p2.log, ...)
	pattern := filepath.Join(a.cfg.LogDir, "p*.log")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		a.logger.Warn("glob failed", "pattern", pattern, "err", err)
	}

	// txpump transaction log
	txpumpLog := filepath.Join(a.cfg.LogDir, "txpump.log")
	if _, statErr := os.Stat(txpumpLog); statErr == nil {
		matches = append(matches, txpumpLog)
	}

	for _, path := range matches {
		a.readFile(path)
	}
}

// readFile reads new lines from a single log file starting from the last
// known offset.
func (a *Analyzer) readFile(path string) {
	state, ok := a.files[path]
	if !ok {
		nodeID := nodeIDFromPath(path)
		state = &fileState{path: path, nodeID: nodeID}
		a.files[path] = state
	}

	//nolint:gosec // log file path derived from config, not user input
	f, err := os.Open(path)
	if err != nil {
		a.logger.Warn("cannot open log file", "path", path, "err", err)
		return
	}
	defer f.Close() //nolint:errcheck // read-only open

	// Fix 7: detect file truncation (e.g. log rotation) and reset.
	info, err := f.Stat()
	if err == nil && info.Size() < state.offset {
		a.logger.Info("log file truncated, resetting offset", "path", path)
		state.offset = 0
	}

	if state.offset > 0 {
		if _, seekErr := f.Seek(state.offset, io.SeekStart); seekErr != nil {
			a.logger.Warn(
				"seek failed, resetting offset",
				"path", path,
				"err", seekErr,
			)
			state.offset = 0
		}
	}

	// Fix 2: use bufio.Reader + ReadString so we track exact byte positions
	// instead of relying on f.Seek(0, io.SeekCurrent) after a buffered
	// scanner (which would return the buffered read position, not the last
	// complete line position).
	reader := bufio.NewReader(f)
	var bytesRead int64
	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > 0 {
			// Only count bytes for lines that end with '\n'. If
			// ReadString returned a partial line at EOF (no trailing
			// newline), skip counting those bytes so the partial
			// fragment will be re-read on the next pass when the
			// line is complete.
			if readErr != nil && !strings.HasSuffix(line, "\n") {
				break
			}
			bytesRead += int64(len(line))
			line = strings.TrimRight(line, "\n\r")
			ev := ParseLogLine(line)
			if ev != nil {
				ev.NodeID = state.nodeID
				a.metrics.RecordEvent(ev)
			}
		}
		if readErr != nil {
			break // EOF or I/O error
		}
	}
	state.offset += bytesRead
}

// reportSafetyAssertions evaluates safety properties and fires assertions.
func (a *Analyzer) reportSafetyAssertions(snap *MetricsSnapshot) {
	// Safety 1: No equivocations (same node, same slot, different block hash).
	noEquivocations := len(snap.Equivocations) == 0
	Always(noEquivocations, "no-equivocations", map[string]interface{}{
		"equivocation_count": len(snap.Equivocations),
	})
	if !noEquivocations {
		for _, eq := range snap.Equivocations {
			a.logger.Error(
				"equivocation detected",
				"node_id", eq.NodeID,
				"slot", eq.Slot,
				"hash_a", eq.HashA,
				"hash_b", eq.HashB,
			)
		}
	}

	// Safety 2: Slot monotonicity — no slot went backward per node.
	noRegressions := len(snap.SlotRegressions) == 0
	Always(noRegressions, "slot-monotonicity", map[string]interface{}{
		"regression_count": len(snap.SlotRegressions),
	})

	// Safety 3: Common-prefix / fork-depth bounded by MaxForkDepth.
	// We approximate this by checking that all chain tips are within
	// MaxForkDepth of each other. Only evaluated once enough blocks have
	// been seen to avoid spurious failures during startup.
	if snap.TotalBlocksForged >= a.cfg.MinBlocksSample && len(snap.ChainTipByNode) >= 2 {
		minTip, maxTip := chainTipRange(snap.ChainTipByNode)
		forkDepth := maxTip - minTip
		withinBound := forkDepth <= uint64(a.cfg.MaxForkDepth) //nolint:gosec // MaxForkDepth validated positive in LoadConfig
		Always(withinBound, "common-prefix-bounded", map[string]interface{}{
			"min_tip":        minTip,
			"max_tip":        maxTip,
			"fork_depth":     forkDepth,
			"max_fork_depth": a.cfg.MaxForkDepth,
		})
	}

	// Safety 4: Chain quality — once we have enough blocks, no single node
	// should hold more than 60% of forged blocks (checks all configured pools,
	// not just observed ones, so nodes that forged 0 blocks are also checked).
	if snap.TotalBlocksForged >= a.cfg.MinBlocksSample {
		for i := 1; i <= a.cfg.Pools; i++ {
			nodeID := fmt.Sprintf("p%d", i)
			count := snap.BlocksByNode[nodeID]
			share := float64(count) / float64(snap.TotalBlocksForged)
			Always(share <= 0.6, "chain-quality", map[string]interface{}{
				"node_id":     nodeID,
				"share":       share,
				"block_count": count,
			})
		}
	}
}

// reportLivenessAssertions evaluates liveness properties and fires assertions.
func (a *Analyzer) reportLivenessAssertions(snap *MetricsSnapshot) {
	// Liveness 1: At least one block has been produced per pool.
	for i := 1; i <= a.cfg.Pools; i++ {
		nodeID := fmt.Sprintf("p%d", i)
		count := snap.BlocksByNode[nodeID]
		Sometimes(count > 0, "pool-produced-block", map[string]interface{}{
			"node_id":     nodeID,
			"block_count": count,
		})
	}

	// Liveness 2: Chain growth — total forged block count is increasing.
	// "Sometimes" is the right predicate: we just want to see growth at some
	// point.
	Sometimes(snap.TotalBlocksForged > 0, "chain-growth", map[string]interface{}{
		"total_blocks_forged": snap.TotalBlocksForged,
	})

	// Liveness 3: Mempool activity — at least one transaction has been added.
	Sometimes(snap.MempoolTxCount > 0, "mempool-activity", map[string]interface{}{
		"mempool_tx_count": snap.MempoolTxCount,
	})

	// Liveness 4: Epoch boundary crossed — once MinBlocksSample blocks have
	// been seen we expect at least one epoch transition (epoch > 0).
	if snap.TotalBlocksForged >= a.cfg.MinBlocksSample {
		maxSlot := globalMaxSlot(snap.MaxSlotByNode)
		currentEpoch := maxSlot / a.cfg.EpochLength
		Sometimes(
			currentEpoch > 0,
			"epoch-boundary-crossed",
			map[string]interface{}{
				"max_slot":      maxSlot,
				"current_epoch": currentEpoch,
			},
		)
	}

	// Liveness 5: Delegations processed (from txpump.log).
	Sometimes(
		snap.DelegationsProcessed > 0,
		"delegations-processed",
		map[string]interface{}{
			"delegations_processed": snap.DelegationsProcessed,
		},
	)

	// Liveness 6: Governance actions processed (from txpump.log).
	Sometimes(
		snap.GovernanceProcessed > 0,
		"governance-processed",
		map[string]interface{}{
			"governance_processed": snap.GovernanceProcessed,
		},
	)
}

// reportReachable fires Reachable markers for important code paths.
func (a *Analyzer) reportReachable(snap *MetricsSnapshot) {
	if snap.TotalBlocksForged > 0 {
		Reachable("block-forged", map[string]interface{}{
			"total_blocks_forged": snap.TotalBlocksForged,
		})
	}
	if snap.MempoolTxCount > 0 {
		Reachable("mempool-tx-added", map[string]interface{}{
			"mempool_tx_count": snap.MempoolTxCount,
		})
	}
	if len(snap.ChainTipByNode) > 0 {
		Reachable("chain-tip-advanced", map[string]interface{}{
			"node_count": len(snap.ChainTipByNode),
		})
	}
	if len(snap.BlocksByNode) > 1 {
		Reachable("multiple-pools-producing", map[string]interface{}{
			"producing_pools": len(snap.BlocksByNode),
		})
	}
	if snap.PlutusProcessed > 0 {
		Reachable("plutus-tx-submitted", map[string]interface{}{
			"plutus_processed": snap.PlutusProcessed,
		})
	}
}

// nodeIDFromPath extracts a node identifier from a log file path.
// For example "/logs/p1.log" returns "p1".
func nodeIDFromPath(path string) string {
	base := filepath.Base(path)
	// Strip extension
	if idx := strings.LastIndex(base, "."); idx > 0 {
		base = base[:idx]
	}
	return base
}

// chainTipRange returns the minimum and maximum values in the map.
func chainTipRange(tips map[string]uint64) (min, max uint64) {
	first := true
	for _, v := range tips {
		if first || v < min {
			min = v
		}
		if first || v > max {
			max = v
		}
		first = false
	}
	return min, max
}

// globalMaxSlot returns the largest slot across all nodes.
func globalMaxSlot(maxByNode map[string]uint64) uint64 {
	var m uint64
	for _, v := range maxByNode {
		if v > m {
			m = v
		}
	}
	return m
}
