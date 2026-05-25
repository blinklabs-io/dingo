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

// Package consensus contains the capture-sidecar runtime, scenario
// orchestration helpers, and JSON test-vector format for the
// consensus-conformance harness. See README.md for the shared-base +
// per-scenario directory layout and how to add a scenario.
package consensus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// Config drives a single capture run. Mirrors the cmd/capture-sidecar
// CLI flags 1:1.
type Config struct {
	// Address is the cardano-node NtN TCP endpoint to dial
	// (host:port).
	Address string
	// NetworkMagic identifies the testnet (per the scenario's
	// testnet.yaml).
	NetworkMagic uint32
	// ConversationPath is the path to the scenario's
	// capture-conversation.json.
	ConversationPath string
	// OutputPath is the path where the resulting JSON vector is
	// written.
	OutputPath string
	// PeerID is the peer id stamped on the recorder's PeerInput.
	// Defaults to 0 for the single-peer capture scenarios; the
	// multi-peer composer assigns one per peer.
	PeerID uint64
	// DialTimeout bounds how long Dial waits to establish the TCP
	// connection. Defaults to 10s when zero.
	DialTimeout time.Duration
	// Title is the vector's top-level title. Defaults to the
	// conversation's Name when zero.
	Title string
}

// Sidecar holds the live capture state. One Sidecar per cardano-node
// connection. Not safe for concurrent use beyond the gouroboros
// callbacks, which run on their own goroutines and serialise through
// the recorder's mutex.
type Sidecar struct {
	cfg          Config
	conversation Conversation

	conn     *ouroboros.Connection
	recorder *Recorder
}

// NewSidecar wires up a Sidecar from a Config and a parsed
// conversation script. The TCP connection is not opened here — call
// Connect for that.
func NewSidecar(cfg Config, conv Conversation) *Sidecar {
	return &Sidecar{
		cfg:          cfg,
		conversation: conv,
		recorder:     NewRecorder(cfg.PeerID),
	}
}

// Connect dials cardano-node over TCP, runs the gouroboros handshake,
// and registers chainsync callbacks on the resulting connection.
func (s *Sidecar) Connect() error {
	timeout := s.cfg.DialTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	raw, err := net.DialTimeout("tcp", s.cfg.Address, timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.cfg.Address, err)
	}

	chainSyncCfg := chainsync.NewConfig(
		// PipelineLimit=1 keeps the chainsync syncLoop's
		// "request, wait, request" cadence to a single in-flight
		// message at a time. That makes the per-step capture
		// counting deterministic — each RequestNextStep maps to
		// exactly one incoming RollForward/RollBackward. Without
		// this, gouroboros's default pipeline depth (75) can
		// burst-receive enough messages that the per-step
		// "wait for next" semantic stops corresponding to one
		// served block.
		chainsync.WithPipelineLimit(1),
		chainsync.WithRollForwardRawFunc(s.recorder.OnRollForwardRaw),
		chainsync.WithRollBackwardFunc(s.recorder.OnRollBackward),
	)

	oConn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(raw),
		ouroboros.WithNetworkMagic(s.cfg.NetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithChainSyncConfig(chainSyncCfg),
	)
	if err != nil {
		_ = raw.Close()
		return fmt.Errorf("ouroboros handshake: %w", err)
	}
	s.conn = oConn
	return nil
}

// Run executes every conversation step in order, then drains and
// closes the connection.
func (s *Sidecar) Run(ctx context.Context) error {
	if s.conn == nil {
		return errors.New("sidecar: Connect must be called before Run")
	}
	for i, step := range s.conversation.Steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := step.Run(ctx, s); err != nil {
			return fmt.Errorf(
				"step[%d] (%s): %w", i, step.Type(), err,
			)
		}
	}
	return nil
}

// Vector builds a category=consensus TestVector from the recorded
// state. One Sidecar emits exactly one peer's worth of recordings,
// so the returned vector always has len(Capture.Peers) == 1. For
// multi-peer scenarios, run one Sidecar per upstream peer and merge
// the resulting single-peer vectors with cmd/compose-consensus-vector
// (which assigns peer_id by argument order and lifts the observation
// node's served trace into ExpectedOutput.DownstreamChainSync).
//
// For a single-peer capture, ExpectedOutput.DownstreamChainSync
// mirrors the peer's served trace verbatim (no chain-selection
// ambiguity with one upstream); the composer replaces this with the
// observation-node capture.
func (s *Sidecar) Vector() format.TestVector {
	served := s.recorder.Snapshot()
	final := lastRollForwardTip(served)
	title := s.cfg.Title
	if title == "" {
		title = s.conversation.Name
	}
	return format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         title,
		Category:      format.CategoryConsensus,
		Capture: &format.ConsensusCapture{
			Peers: []format.PeerInput{
				{PeerID: s.recorder.PeerID(), Served: served},
			},
			ExpectedOutput: format.ExpectedOutput{
				DownstreamChainSync: served,
				FinalTip:            final,
			},
		},
	}
}

// Close stops the gouroboros protocol clients and releases the TCP
// connection. Safe to call multiple times.
func (s *Sidecar) Close() error {
	var firstErr error
	if s.conn != nil {
		// Best-effort: Stop the chainsync client cleanly so the
		// MsgDone is sent. Errors are recorded but do not prevent
		// further cleanup.
		if err := s.conn.ChainSync().Client.Stop(); err != nil {
			firstErr = fmt.Errorf("chainsync stop: %w", err)
		}
		if err := s.conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("conn close: %w", err)
		}
		s.conn = nil
	}
	return firstErr
}

// lastRollForwardTip walks the served list to find the most recent
// RollForward and returns its Tip. Returns the zero Tip when no
// RollForward has been observed. Used as the structured chain-state
// sanity check baked into ExpectedOutput.FinalTip.
func lastRollForwardTip(served []format.ServedMessage) format.Tip {
	for _, m := range slices.Backward(served) {
		if m.Protocol != format.ProtocolChainSync ||
			m.MsgType != format.ChainSyncMsgRollForward ||
			m.Tip == nil {
			continue
		}
		return format.Tip{
			Slot:        m.Tip.Slot,
			Hash:        append(format.HexBytes(nil), m.Tip.Hash...),
			BlockNumber: m.Tip.BlockNumber,
		}
	}
	return format.Tip{}
}
