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

package bark

import (
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// DefaultArchiveLookupsPerSecond is the default for
	// BarkConfig.ArchiveLookupsPerSecond.
	DefaultArchiveLookupsPerSecond = 200
	// DefaultArchiveCallerLookupsPerSecond is the default for
	// BarkConfig.ArchiveCallerLookupsPerSecond.
	DefaultArchiveCallerLookupsPerSecond = 50

	// maxArchiveCallers bounds the per-caller table so a client cycling
	// source addresses cannot grow it without limit.
	maxArchiveCallers = 4096

	// archiveCallerSweepInterval bounds how often a full caller table is
	// scanned for evictable entries. The scan is O(maxArchiveCallers) under
	// archiveBudget.mu and runs before either bucket can refuse, so without
	// this bound a client rotating source addresses makes every request pay
	// it.
	archiveCallerSweepInterval = time.Second
)

// archiveBudget rations the storage lookups ArchiveService performs for
// anonymous callers: one global token bucket shared by every caller, and one
// bucket per caller address. A hash+slot reference is served without reading
// any local index (see resolveBlockPoint), so each reference, present or not,
// costs a cloud lookup; the concurrency cap alone does not limit how often
// that can be repeated.
type archiveBudget struct {
	mu          sync.Mutex
	now         func() time.Time
	global      *rate.Limiter
	callerRate  rate.Limit
	callerBurst int
	callers     map[string]*rate.Limiter
	lastSweep   time.Time
	// overflow serves every caller that arrives while callers is full of
	// active entries, so the table stays bounded without exempting anyone.
	overflow *rate.Limiter
}

// archiveBurst sizes a bucket so that one maximum-size batch always fits,
// whatever the configured rate.
func archiveBurst(perSecond int) int {
	return max(2*perSecond, DefaultMaxFetchBlockRefs)
}

func newArchiveBudget(globalPerSecond, callerPerSecond int) *archiveBudget {
	return &archiveBudget{
		now: time.Now,
		global: rate.NewLimiter(
			rate.Limit(globalPerSecond),
			archiveBurst(globalPerSecond),
		),
		callerRate:  rate.Limit(callerPerSecond),
		callerBurst: archiveBurst(callerPerSecond),
		callers:     make(map[string]*rate.Limiter),
		overflow: rate.NewLimiter(
			rate.Limit(callerPerSecond),
			archiveBurst(callerPerSecond),
		),
	}
}

// archiveCallerKey identifies the caller by host, ignoring the ephemeral
// source port so reconnecting does not mint a fresh allowance. An IPv6 caller
// is keyed by its /64: a single subscriber is routinely delegated at least
// that much, so keying by full address would hand one client an allowance
// per address it chooses to source from.
func archiveCallerKey(peerAddr string) string {
	host, _, err := net.SplitHostPort(peerAddr)
	if err != nil {
		host = peerAddr
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	addr = addr.Unmap().WithZone("")
	if addr.Is6() {
		return netip.PrefixFrom(addr, 64).Masked().String()
	}
	return addr.String()
}

// allow takes n lookups from the caller's allowance and the global budget,
// or none at all when either is short.
func (b *archiveBudget) allow(caller string, n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()

	callerLimiter := b.callerLimiter(caller, now)
	callerRes := callerLimiter.ReserveN(now, n)
	if !callerRes.OK() || callerRes.DelayFrom(now) > 0 {
		callerRes.CancelAt(now)
		return false
	}
	globalRes := b.global.ReserveN(now, n)
	if !globalRes.OK() || globalRes.DelayFrom(now) > 0 {
		globalRes.CancelAt(now)
		// Return the caller's tokens: a request refused for global
		// exhaustion must not also drain that caller's own allowance.
		callerRes.CancelAt(now)
		return false
	}
	return true
}

func (b *archiveBudget) callerLimiter(
	caller string,
	now time.Time,
) *rate.Limiter {
	if l, ok := b.callers[caller]; ok {
		return l
	}
	if len(b.callers) >= maxArchiveCallers {
		if now.Sub(b.lastSweep) >= archiveCallerSweepInterval {
			b.lastSweep = now
			// An entry holding a full bucket has no history worth keeping.
			for k, l := range b.callers {
				if l.TokensAt(now) >= float64(b.callerBurst) {
					delete(b.callers, k)
				}
			}
		}
		if len(b.callers) >= maxArchiveCallers {
			return b.overflow
		}
	}
	l := rate.NewLimiter(b.callerRate, b.callerBurst)
	b.callers[caller] = l
	return l
}
