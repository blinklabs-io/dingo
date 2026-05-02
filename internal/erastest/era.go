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

//go:build erastest

package erastest

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// HeaderInfo is the era-relevant portion of a chain-sync RollForward
// callback's parsed BlockHeader.
type HeaderInfo struct {
	Slot        uint64
	BlockNumber uint64
	EraID       uint
	IssuerVkey  []byte
}

// EraTransition records the first header observed in a new era. It
// marks the boundary between the previous era and the new one as seen
// on the node's chain.
type EraTransition struct {
	FromEraID uint
	ToEraID   uint
	// Slot of the first block in the new era.
	Slot uint64
	// Approximate epoch the transition crossed at, computed against the
	// configured EpochLength. Zero if no Config was supplied.
	Epoch uint64
}

// EraStream subscribes to a single node's chain via N2N chain-sync,
// streams every header from origin, and exposes the era of each block
// as it arrives. Designed for the era-transitions test scenarios that
// need to observe forks live rather than poll the tip.
type EraStream struct {
	endpoint     NodeEndpoint
	networkMagic uint32
	epochLength  uint64

	conn  *ouroboros.Connection
	rawTC net.Conn

	mu          sync.Mutex
	cond        *sync.Cond
	headers     []HeaderInfo
	transitions []EraTransition
	closed      bool
	streamErr   error
}

// NewEraStream opens an N2N connection to endpoint, starts a chain-sync
// stream from origin, and begins recording every header. Headers
// arrive asynchronously; callers use WaitForEra / WaitForSlot or read
// snapshots via HeadersSnapshot / Transitions.
//
// epochLength should be the configured Shelley epoch length so observed
// transitions can be tagged with an approximate epoch. Pass zero to
// leave the Epoch field of EraTransition unset.
//
// The caller MUST defer Close to release the underlying TCP connection.
func NewEraStream(
	t *testing.T,
	endpoint NodeEndpoint,
	networkMagic uint32,
	epochLength uint64,
) *EraStream {
	t.Helper()
	s := &EraStream{
		endpoint:     endpoint,
		networkMagic: networkMagic,
		epochLength:  epochLength,
	}
	s.cond = sync.NewCond(&s.mu)

	rawConn, err := net.DialTimeout("tcp", endpoint.Address, 10*time.Second)
	require.NoError(
		t, err,
		"EraStream: dial %s (%s)", endpoint.Name, endpoint.Address,
	)
	s.rawTC = rawConn

	chainSyncCfg := chainsync.NewConfig(
		chainsync.WithRollForwardFunc(s.onRollForward),
		chainsync.WithRollBackwardFunc(s.onRollBackward),
	)

	conn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(s.networkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithChainSyncConfig(chainSyncCfg),
	)
	if err != nil {
		_ = rawConn.Close()
		t.Fatalf(
			"EraStream: ouroboros handshake to %s failed: %v",
			endpoint.Name, err,
		)
	}
	s.conn = conn

	if err := conn.ChainSync().Client.Sync(
		[]pcommon.Point{pcommon.NewPointOrigin()},
	); err != nil {
		_ = conn.Close()
		t.Fatalf(
			"EraStream: ChainSync.Sync(origin) on %s failed: %v",
			endpoint.Name, err,
		)
	}
	return s
}

// Close stops the chain-sync stream and releases the connection. Safe
// to call multiple times.
func (s *EraStream) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()

	if s.conn != nil {
		_ = s.conn.Close()
	}
}

// onRollForward is the chain-sync callback invoked once per header.
func (s *EraStream) onRollForward(
	_ chainsync.CallbackContext,
	_ uint,
	blockOrHeader any,
	_ chainsync.Tip,
) error {
	header, ok := blockOrHeader.(ledger.BlockHeader)
	if !ok {
		// N2C path delivers a Block; this stream is N2N so a non-header
		// here would be a gouroboros-internal contract violation.
		// Ignore rather than failing the whole stream.
		return nil
	}
	issuer := header.IssuerVkey()
	info := HeaderInfo{
		Slot:        header.SlotNumber(),
		BlockNumber: header.BlockNumber(),
		EraID:       uint(header.Era().Id),
		IssuerVkey:  append([]byte(nil), issuer[:]...),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}

	// Detect era transition: first header whose era differs from the
	// previous header's era. The very first header is treated as a
	// transition from era 0 (Byron) when its era is non-zero, so a
	// chain that begins in Shelley still records that as a transition
	// (FromEraID=0, ToEraID=1).
	prevEra := uint(0)
	if n := len(s.headers); n > 0 {
		prevEra = s.headers[n-1].EraID
	}
	if info.EraID != prevEra {
		epoch := uint64(0)
		if s.epochLength > 0 {
			epoch = info.Slot / s.epochLength
		}
		s.transitions = append(s.transitions, EraTransition{
			FromEraID: prevEra,
			ToEraID:   info.EraID,
			Slot:      info.Slot,
			Epoch:     epoch,
		})
	}

	s.headers = append(s.headers, info)
	s.cond.Broadcast()
	return nil
}

// onRollBackward records nothing — chain rollbacks during a healthy
// devnet are rare and not relevant to era observation. We still need
// to register the callback so gouroboros doesn't reject the message.
func (s *EraStream) onRollBackward(
	_ chainsync.CallbackContext,
	_ pcommon.Point,
	_ chainsync.Tip,
) error {
	return nil
}

// LatestHeader returns the most recently observed header and true, or a
// zero value and false if no header has streamed yet.
func (s *EraStream) LatestHeader() (HeaderInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.headers) == 0 {
		return HeaderInfo{}, false
	}
	return s.headers[len(s.headers)-1], true
}

// HeadersSnapshot returns a copy of every header observed so far. Order
// matches arrival order (chain order).
func (s *EraStream) HeadersSnapshot() []HeaderInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]HeaderInfo, len(s.headers))
	copy(out, s.headers)
	return out
}

// Transitions returns a copy of the observed era transitions in chain
// order.
func (s *EraStream) Transitions() []EraTransition {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]EraTransition, len(s.transitions))
	copy(out, s.transitions)
	return out
}

// WaitForEra blocks until a header is observed whose era ID equals
// target.Id, and returns that header. Returns an error on timeout or
// when the stream is closed before the target era is reached.
func (s *EraStream) WaitForEra(
	target *eras.EraDesc,
	timeout time.Duration,
) (HeaderInfo, error) {
	if target == nil {
		return HeaderInfo{}, errors.New("WaitForEra: nil target era")
	}
	deadline := time.Now().Add(timeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed {
			return HeaderInfo{}, fmt.Errorf(
				"WaitForEra(%s): stream closed before target reached",
				target.Name,
			)
		}
		// Scan the most recent transitions first; the target era is
		// usually the latest one.
		for i := len(s.transitions) - 1; i >= 0; i-- {
			if s.transitions[i].ToEraID == target.Id {
				return s.headerAtSlotLocked(s.transitions[i].Slot), nil
			}
		}
		// Also check if the latest header is in target era (e.g. the
		// chain started directly in this era and hasn't transitioned).
		if n := len(s.headers); n > 0 &&
			s.headers[n-1].EraID == target.Id {
			return s.headers[n-1], nil
		}
		if s.streamErr != nil {
			return HeaderInfo{}, fmt.Errorf(
				"WaitForEra(%s): stream error: %w",
				target.Name, s.streamErr,
			)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return HeaderInfo{}, fmt.Errorf(
				"WaitForEra(%s): timed out after %s",
				target.Name, timeout,
			)
		}
		s.waitWithTimeoutLocked(remaining)
	}
}

// WaitForSlot blocks until a header at or beyond targetSlot is observed
// and returns that header.
func (s *EraStream) WaitForSlot(
	targetSlot uint64,
	timeout time.Duration,
) (HeaderInfo, error) {
	deadline := time.Now().Add(timeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed {
			return HeaderInfo{}, fmt.Errorf(
				"WaitForSlot(%d): stream closed", targetSlot,
			)
		}
		if n := len(s.headers); n > 0 && s.headers[n-1].Slot >= targetSlot {
			return s.headers[n-1], nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return HeaderInfo{}, fmt.Errorf(
				"WaitForSlot(%d): timed out after %s",
				targetSlot, timeout,
			)
		}
		s.waitWithTimeoutLocked(remaining)
	}
}

// headerAtSlotLocked returns the header recorded at slot, or the
// closest-following header if the exact slot was not seen. Caller must
// hold s.mu.
func (s *EraStream) headerAtSlotLocked(slot uint64) HeaderInfo {
	for _, h := range s.headers {
		if h.Slot >= slot {
			return h
		}
	}
	if n := len(s.headers); n > 0 {
		return s.headers[n-1]
	}
	return HeaderInfo{}
}

// waitWithTimeoutLocked drops s.mu, waits up to d for cond.Broadcast,
// and reacquires s.mu before returning. Caller must hold s.mu before
// and after.
func (s *EraStream) waitWithTimeoutLocked(d time.Duration) {
	timer := time.AfterFunc(d, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	s.cond.Wait()
}
