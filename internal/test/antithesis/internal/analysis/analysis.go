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
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

var nodeLogName = regexp.MustCompile(
	`^p([0-9]+)(?:[._-].*)?\.log(?:\.[0-9]+)?$`,
)

// fileState tracks the read position within a single log file so that
// successive checks only process new lines.
type fileState struct {
	path     string
	nodeID   string
	identity string
	offset   int64
	modTime  time.Time
	warned   bool
}

type ingestionStats struct {
	nodeFiles      map[string]struct{}
	nodeReadable   map[string]struct{}
	nodeBytes      int64
	nodeEvents     int
	openFailures   int
	txpumpReadable bool
	txpumpEvents   int
}

// Analyzer reads node log files, parses events, and reports Antithesis
// assertions on each check interval.
type Analyzer struct {
	cfg                 *Config
	metrics             *Metrics
	files               map[string]*fileState // keyed by file path
	filesByIdentity     map[string]*fileState
	logger              *slog.Logger
	setupDone           bool
	setupComplete       func()
	ingestion           ingestionStats
	observabilityWarned bool
	walkWarnings        map[string]struct{}
}

// NewAnalyzer creates an Analyzer with the given config and a fresh Metrics.
func NewAnalyzer(cfg *Config, logger *slog.Logger) *Analyzer {
	return &Analyzer{
		cfg:             cfg,
		metrics:         NewMetrics(),
		files:           make(map[string]*fileState),
		filesByIdentity: make(map[string]*fileState),
		logger:          logger,
		setupComplete:   SetupComplete,
		ingestion: ingestionStats{
			nodeFiles:    make(map[string]struct{}),
			nodeReadable: make(map[string]struct{}),
		},
		walkWarnings: make(map[string]struct{}),
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

	// Signal readiness independently of observed workload. A test run can
	// legitimately have no forged blocks yet, but Antithesis still needs this
	// lifecycle event before it will begin fault injection.
	a.signalSetupComplete()
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
	a.reportObservability(snap)
	a.reportSafetyAssertions(&snap)
	if a.ingestion.nodeEvents > 0 {
		a.reportLivenessAssertions(&snap)
	}
	a.reportReachable(&snap)
}

func (a *Analyzer) reportObservability(snap MetricsSnapshot) {
	Always(
		len(a.ingestion.nodeReadable) > 0,
		"node-log-observability",
		map[string]interface{}{
			"node_files":          len(a.ingestion.nodeFiles),
			"readable_node_files": len(a.ingestion.nodeReadable),
			"node_log_bytes":      a.ingestion.nodeBytes,
			"node_events":         a.ingestion.nodeEvents,
			"open_failures":       a.ingestion.openFailures,
			"forged_blocks":       snap.TotalBlocksForged,
			"txpump_readable":     a.ingestion.txpumpReadable,
			"txpump_events":       a.ingestion.txpumpEvents,
			"mempool_events":      snap.MempoolTxCount,
		},
	)
	if a.ingestion.nodeEvents == 0 && !a.observabilityWarned {
		a.logger.Warn(
			"no node log events ingested; workload assertions are pending",
			"node_files",
			len(a.ingestion.nodeFiles),
			"readable_node_files",
			len(a.ingestion.nodeReadable),
			"node_log_bytes",
			a.ingestion.nodeBytes,
			"open_failures",
			a.ingestion.openFailures,
		)
		a.observabilityWarned = true
	}
}

// signalSetupComplete emits the Antithesis readiness event once per analyzer.
func (a *Analyzer) signalSetupComplete() {
	if a.setupDone {
		return
	}
	a.setupComplete()
	a.logger.Info("setup complete signaled")
	a.setupDone = true
}

// readNewLines discovers node and txpump logs anywhere below cfg.LogDir,
// including numbered rotations, then reads new complete lines.
func (a *Analyzer) readNewLines() {
	// Shared volumes may expose logs in a subdirectory and loggers may leave
	// numbered rotations behind. Walk the volume and select only known names.
	currentNodeFiles := make(map[string]struct{})
	currentReadable := make(map[string]struct{})
	a.ingestion.txpumpReadable = false
	type logFile struct{ path, role, nodeID string }
	var logFiles []logFile
	_ = filepath.WalkDir(
		a.cfg.LogDir,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if _, warned := a.walkWarnings[path]; !warned {
					a.logger.Warn(
						"cannot inspect log path",
						"path",
						path,
						"err",
						walkErr,
					)
					a.walkWarnings[path] = struct{}{}
				}
				return nil
			}
			if entry.IsDir() {
				return nil
			}
			role, nodeID, ok := logRole(path)
			if !ok {
				return nil
			}
			if role == "node" {
				currentNodeFiles[path] = struct{}{}
			}
			logFiles = append(
				logFiles,
				logFile{path: path, role: role, nodeID: nodeID},
			)
			return nil
		},
	)
	// Prefer active files over rotations so a renamed active file is not used
	// as the canonical state when both paths refer to the same inode.
	sort.Slice(logFiles, func(i, j int) bool {
		iRotated := isRotatedLog(logFiles[i].path)
		jRotated := isRotatedLog(logFiles[j].path)
		if iRotated != jRotated {
			return !iRotated
		}
		return logFiles[i].path < logFiles[j].path
	})
	for _, logFile := range logFiles {
		if a.readFile(logFile.path, logFile.role, logFile.nodeID) {
			if logFile.role == "node" {
				currentReadable[logFile.path] = struct{}{}
			} else {
				a.ingestion.txpumpReadable = true
			}
		}
	}
	a.ingestion.nodeFiles = currentNodeFiles
	a.ingestion.nodeReadable = currentReadable
}

// readFile reads new lines from a single log file starting from the last
// known offset.
func (a *Analyzer) readFile(path, role, nodeID string) bool {
	state, ok := a.files[path]
	if !ok {
		state = &fileState{path: path, nodeID: nodeID}
		a.files[path] = state
	}
	//nolint:gosec // log file path derived from config, not user input
	f, err := os.Open(path)
	if err != nil {
		a.ingestion.openFailures++
		if !state.warned {
			a.logger.Warn("cannot read log file", "path", path, "err", err)
			state.warned = true
		}
		return false
	}
	defer f.Close() //nolint:errcheck // read-only open
	info, err := f.Stat()
	identity := fileIdentity(info)
	if ok && state.identity != "" && state.identity != identity {
		state = nil
	}
	if state == nil && identity != "" {
		state = a.filesByIdentity[identity]
	}
	if state == nil {
		state = &fileState{path: path, nodeID: nodeID}
	}
	if identity != "" {
		if existing := a.filesByIdentity[identity]; existing != nil &&
			existing != state {
			// A rotated file is the same inode under a new path. It has already
			// been consumed during this pass under the active path.
			return true
		}
		state.identity = identity
		a.filesByIdentity[identity] = state
	}
	state.path = path
	state.nodeID = nodeID
	a.files[path] = state
	state.warned = false

	// Detect file truncation (e.g. a logger restarting in place) and reset.
	if err == nil && (info.Size() < state.offset ||
		(!state.modTime.IsZero() && !info.ModTime().Equal(state.modTime) && info.Size() <= state.offset)) {
		a.logger.Info("log file truncated, resetting offset", "path", path)
		state.offset = 0
	}
	if err == nil {
		state.modTime = info.ModTime()
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
				if role == "node" {
					a.ingestion.nodeEvents++
				} else {
					a.ingestion.txpumpEvents++
				}
			}
		}
		if readErr != nil {
			break // EOF or I/O error
		}
	}
	state.offset += bytesRead
	if role == "node" {
		a.ingestion.nodeBytes += bytesRead
	}
	return true
}

func fileIdentity(info os.FileInfo) string {
	if info == nil {
		return ""
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
}

func isRotatedLog(path string) bool {
	base := filepath.Base(path)
	return strings.Contains(base, ".log.")
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

	if snap.TotalBlocksForged >= a.cfg.MinBlocksSample &&
		len(snap.ChainTipByNode) >= 2 {
		if minTip, maxTip, ok := chainTipRange(snap.ChainTipByNode); ok {
			lag := maxTip - minTip
			a.logger.Info("sync-lag",
				"min_tip", minTip,
				"max_tip", maxTip,
				"lag", lag,
				"max_fork_depth", a.cfg.MaxForkDepth,
			)
		}
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
	Sometimes(
		snap.TotalBlocksForged > 0,
		"chain-growth",
		map[string]interface{}{
			"total_blocks_forged": snap.TotalBlocksForged,
		},
	)

	// Liveness 3: Mempool activity — at least one transaction has been added.
	Sometimes(
		snap.MempoolTxCount > 0,
		"mempool-activity",
		map[string]interface{}{
			"mempool_tx_count": snap.MempoolTxCount,
		},
	)

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
	if match := nodeLogName.FindStringSubmatch(base); len(match) > 1 {
		return "p" + match[1]
	}
	return base
}

func logRole(path string) (role, nodeID string, ok bool) {
	base := filepath.Base(path)
	if strings.HasPrefix(base, "txpump.log") {
		return "txpump", "txpump", true
	}
	if nodeID := nodeIDFromPath(path); nodeID != base {
		return "node", nodeID, true
	}
	return "", "", false
}

// chainTipRange returns the minimum and maximum values in the map.
func chainTipRange(tips map[string]uint64) (min, max uint64, ok bool) {
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
	return min, max, !first
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
