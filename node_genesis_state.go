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

package dingo

import (
	"errors"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// caughtUpMarkerName is the file in the data directory that records the
// selector left Genesis selection. It is the counterpart of the reference's
// CaughtUpMarker: its presence at startup, with a tip that is still young,
// starts the node in Praos selection.
const caughtUpMarkerName = "CaughtUpMarker"

func (n *Node) caughtUpMarkerPath() string {
	if n.config.dataDir == "" {
		return ""
	}
	return filepath.Join(n.config.dataDir, caughtUpMarkerName)
}

// initialGenesisSelectionMode reports whether the chain selector starts in
// Genesis mode. The configured overrides that force Praos win; otherwise a
// node starts in Genesis unless a caught-up marker names a tip younger than
// the maximum caught-up age.
func (n *Node) initialGenesisSelectionMode() bool {
	var tip ochainsync.Tip
	var slotTime func(uint64) (time.Time, error)
	if n.ledgerState != nil {
		tip = n.ledgerState.Tip()
		slotTime = n.ledgerState.SlotToTime
	}
	return decideInitialGenesisMode(
		n.config.genesisSelectionEligible(),
		n.caughtUpMarkerPath(),
		tip,
		slotTime,
		time.Now(),
		chainselection.DefaultMaxCaughtUpAge,
		n.config.logger,
	)
}

// decideInitialGenesisMode is the startup decision of the Genesis state
// machine. A stale marker is removed, and a marker that cannot be read or
// removed is logged and otherwise ignored: this start is in Genesis either
// way. A marker that could not be removed survives, though, and starts a later
// run in Praos if the tip is young again by then.
func decideInitialGenesisMode(
	eligible bool,
	markerPath string,
	tip ochainsync.Tip,
	slotTime func(uint64) (time.Time, error),
	now time.Time,
	maxAge time.Duration,
	logger *slog.Logger,
) bool {
	if !eligible {
		return false
	}
	if markerPath == "" {
		return true
	}
	if _, err := os.Stat(markerPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logMarkerWarn(logger, "cannot read caught-up marker", err)
		}
		return true
	}
	if tipYoungerThan(tip, slotTime, now, maxAge) {
		return false
	}
	deleteCaughtUpMarkerFile(markerPath, logger)
	return true
}

// tipYoungerThan reports whether the slot of tip starts less than maxAge
// before now. A tip that is the origin, or whose slot cannot be converted to a
// time, is too old.
func tipYoungerThan(
	tip ochainsync.Tip,
	slotTime func(uint64) (time.Time, error),
	now time.Time,
	maxAge time.Duration,
) bool {
	if slotTime == nil || (tip.Point.Slot == 0 && len(tip.Point.Hash) == 0) {
		return false
	}
	start, err := slotTime(tip.Point.Slot)
	if err != nil {
		return false
	}
	return now.Before(start.Add(maxAge))
}

func logMarkerWarn(logger *slog.Logger, msg string, err error) {
	if logger != nil {
		logger.Warn(msg, "component", "genesis", "error", err)
	}
}

func deleteCaughtUpMarkerFile(path string, logger *slog.Logger) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		logMarkerWarn(logger, "cannot remove caught-up marker", err)
	}
}

func (n *Node) removeCaughtUpMarker() {
	deleteCaughtUpMarkerFile(n.caughtUpMarkerPath(), n.config.logger)
}

func createCaughtUpMarkerFile(path string, logger *slog.Logger) {
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		logMarkerWarn(logger, "cannot write caught-up marker", err)
		return
	}
	if err := file.Close(); err != nil {
		logMarkerWarn(logger, "cannot write caught-up marker", err)
	}
}

// subscribeCaughtUpMarkerEvents keeps the marker in step with the selector:
// written when it leaves Genesis, removed when it returns. A missed exit only
// costs a start in Genesis. A missed entry leaves a marker the tip-age check
// treats as stale only while the tip stays old, so both events are required
// subscriptions.
func (n *Node) subscribeCaughtUpMarkerEvents() {
	n.subscribeRequiredEvent(
		chainselection.GenesisModeExitedEventType,
		func(event.Event) {
			createCaughtUpMarkerFile(n.caughtUpMarkerPath(), n.config.logger)
		},
	)
	n.subscribeRequiredEvent(
		chainselection.GenesisModeEnteredEventType,
		func(event.Event) { n.removeCaughtUpMarker() },
	)
}

func (n *Node) selectorSlotTime(slot uint64) (time.Time, error) {
	ls := n.wallClockLedger.Load()
	if ls == nil {
		return time.Time{}, errors.New("ledger unavailable")
	}
	return ls.SlotToTime(slot)
}

func (n *Node) selectorLocalTipPraosView(
	tip ochainsync.Tip,
) (chainselection.PraosTiebreakerView, bool) {
	ls := n.wallClockLedger.Load()
	if ls == nil {
		return chainselection.PraosTiebreakerView{}, false
	}
	return ls.LocalTipPraosView(tip)
}

// genesisHistoricityCutoff returns the age beyond which an idle peer's tip is
// too old for an honest caught-up peer: the network's stability window 3k/f,
// in time, plus one hour. A configured Genesis density window does not move
// it while the Shelley genesis parameters are known. Otherwise it is zero,
// and the selector's default, derived from the Genesis window it was given,
// applies.
func (n *Node) genesisHistoricityCutoff() time.Duration {
	if n.config.cardanoNodeConfig == nil {
		return 0
	}
	sg := n.config.cardanoNodeConfig.ShelleyGenesis()
	if sg == nil || sg.SlotLength.Rat == nil || sg.SlotLength.Sign() <= 0 ||
		sg.SecurityParam <= 0 {
		return 0
	}
	windowSlots := chainselection.GenesisWindowSlotsForParams(
		uint64(sg.SecurityParam),
		sg.ActiveSlotsCoeff.Rat,
	)
	if windowSlots == 0 {
		return 0
	}
	return historicityCutoffFor(windowSlots, sg.SlotLength.Rat)
}

func historicityCutoffFor(windowSlots uint64, slotLength *big.Rat) time.Duration {
	seconds := new(big.Rat).Mul(
		new(big.Rat).SetUint64(windowSlots),
		slotLength,
	)
	nanos := new(big.Rat).Mul(seconds, big.NewRat(int64(time.Second), 1))
	value, _ := nanos.Float64()
	if value >= float64(time.Duration(1<<62)) {
		return time.Duration(1 << 62)
	}
	return time.Duration(value) + time.Hour
}

// historicityDenyDuration bounds how long a peer disconnected for idling at a
// historic tip stays on the deny list.
const historicityDenyDuration = 10 * time.Minute

// onHistoricityDisconnect disconnects a peer that reported itself idle at a
// tip too old for an honest caught-up peer. It is a separate callback from the
// density disconnector's because that report describes a dominating peer and
// densities that do not apply, and its counter would misattribute the cause.
func (n *Node) onHistoricityDisconnect(d chainselection.HistoricityDisconnect) {
	n.networkingCoreMu.Lock()
	defer n.networkingCoreMu.Unlock()
	denied := false
	if d.ConnectionId.RemoteAddr != nil {
		denied = n.denyPeer(
			d.ConnectionId.RemoteAddr.String(),
			historicityDenyDuration,
		)
	}
	n.config.logger.Warn(
		"disconnecting peer idle at a historic tip",
		"connection_id", d.ConnectionId.String(),
		"slot", d.Point.Slot,
		"age", d.Age,
		"cutoff", d.Cutoff,
		"denied", denied,
	)
	if n.connManager == nil {
		return
	}
	if conn := n.connManager.GetConnectionById(d.ConnectionId); conn != nil {
		conn.Close()
	}
}
