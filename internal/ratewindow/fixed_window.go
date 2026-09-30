// Package ratewindow provides bounded, externally synchronized fixed-window
// counters for peer admission and misbehavior tracking.
package ratewindow

import "time"

// Decision describes why a fixed-window admission was denied.
type Decision uint8

const (
	Admitted Decision = iota
	PeerBudgetExceeded
	ProcessBudgetExceeded
	PeerTrackingCapacityExceeded
)

// FixedWindow tracks per-peer counts in one shared fixed-duration window.
// Callers must serialize access to a FixedWindow with their own mutex.
type FixedWindow struct {
	window       time.Duration
	perPeerLimit int
	processLimit int
	maxPeers     int
	started      time.Time
	processCount int
	counts       map[string]int
}

// NewFixedWindow creates a fixed-window counter. A zero limit disables that
// limit; maxPeers bounds the number of peer keys retained in one window.
func NewFixedWindow(
	window time.Duration,
	perPeerLimit int,
	processLimit int,
	maxPeers int,
) *FixedWindow {
	return &FixedWindow{
		window:       window,
		perPeerLimit: perPeerLimit,
		processLimit: processLimit,
		maxPeers:     maxPeers,
		counts:       make(map[string]int),
	}
}

// Admit reserves one unit if both the per-peer and process-wide limits allow
// it. It does not increment counters when admission is denied.
func (w *FixedWindow) Admit(peer string, now time.Time) Decision {
	w.advance(now)
	count, exists := w.counts[peer]
	if !exists && w.maxPeers > 0 && len(w.counts) >= w.maxPeers {
		return PeerTrackingCapacityExceeded
	}
	if w.processLimit > 0 && w.processCount >= w.processLimit {
		return ProcessBudgetExceeded
	}
	if w.perPeerLimit > 0 && count >= w.perPeerLimit {
		return PeerBudgetExceeded
	}
	w.counts[peer] = count + 1
	if w.processLimit > 0 {
		w.processCount++
	}
	return Admitted
}

// Record increments one peer's counter. It returns false when the peer map is
// full and the key is new; this keeps misbehavior tracking bounded without
// scanning stale entries on each update.
func (w *FixedWindow) Record(peer string, now time.Time) (int, bool) {
	w.advance(now)
	count, exists := w.counts[peer]
	if !exists && w.maxPeers > 0 && len(w.counts) >= w.maxPeers {
		return 0, false
	}
	count++
	w.counts[peer] = count
	return count, true
}

func (w *FixedWindow) advance(now time.Time) {
	if w.started.IsZero() || now.Sub(w.started) >= w.window {
		w.started = now
		w.processCount = 0
		clear(w.counts)
	}
}
