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

package consensus

import (
	"errors"
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// ComposeArgs is the input to Compose.
type ComposeArgs struct {
	// PeerCapturePaths is one path per upstream peer; the index is
	// assigned as peer_id in the resulting vector. Order matters.
	PeerCapturePaths []string
	// ObservationCapturePath is the capture taken from the observation
	// node after it has stabilized; its served trace becomes the
	// vector's expected_output.downstream_chainsync.
	ObservationCapturePath string
	// Title is the composed vector's top-level title. Defaults to
	// "multi-peer-<N>" when empty.
	Title string
}

// Compose merges N single-peer captures and one observation capture
// into a multi-peer consensus vector. Each input is expected to be a
// category=consensus vector with exactly one entry in
// capture.peers[]. The composer assigns peer_id = i for the i-th
// PeerCapturePath. expected_output.final_tip is derived from the last
// roll_forward in the observation capture's served trace.
//
// The composed vector is validated by the format encoder before
// return — Compose surfaces format-level violations (duplicate peer
// ids, wrong category in an input, etc.) as errors rather than
// emitting an invalid file.
func Compose(args ComposeArgs) (format.TestVector, error) {
	if len(args.PeerCapturePaths) == 0 {
		return format.TestVector{},
			errors.New("compose: at least one -peer is required")
	}
	if args.ObservationCapturePath == "" {
		return format.TestVector{},
			errors.New("compose: -observation is required")
	}

	peers := make([]format.PeerInput, 0, len(args.PeerCapturePaths))
	for i, path := range args.PeerCapturePaths {
		v, err := loadCaptureVector(path)
		if err != nil {
			return format.TestVector{}, fmt.Errorf(
				"peer[%d] %s: %w", i, path, err,
			)
		}
		if len(v.Capture.Peers) != 1 {
			return format.TestVector{}, fmt.Errorf(
				"peer[%d] %s: expected exactly one peer in input, got %d",
				i, path, len(v.Capture.Peers),
			)
		}
		peers = append(peers, format.PeerInput{
			PeerID: uint64(i), //nolint:gosec // small loop index
			Served: cloneServedSlice(v.Capture.Peers[0].Served),
		})
	}

	obs, err := loadCaptureVector(args.ObservationCapturePath)
	if err != nil {
		return format.TestVector{}, fmt.Errorf(
			"observation %s: %w", args.ObservationCapturePath, err,
		)
	}
	if len(obs.Capture.Peers) != 1 {
		return format.TestVector{}, fmt.Errorf(
			"observation %s: expected exactly one peer in input, got %d",
			args.ObservationCapturePath, len(obs.Capture.Peers),
		)
	}
	downstream := cloneServedSlice(obs.Capture.Peers[0].Served)

	title := args.Title
	if title == "" {
		title = fmt.Sprintf("multi-peer-%d", len(peers))
	}

	vec := format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         title,
		Category:      format.CategoryConsensus,
		Capture: &format.ConsensusCapture{
			Peers: peers,
			ExpectedOutput: format.ExpectedOutput{
				DownstreamChainSync: downstream,
				FinalTip:            lastRollForwardTip(downstream),
			},
		},
	}
	// Round-trip through the encoder so format-level invariants
	// (per-msg-type field-set, schema version, category exclusivity)
	// are enforced now rather than at first replay.
	if _, err := format.EncodeTestVector(vec); err != nil {
		return format.TestVector{}, fmt.Errorf("compose: %w", err)
	}
	return vec, nil
}

// loadCaptureVector reads a JSON vector file, decodes it via
// format.DecodeTestVector, and confirms it's a consensus-category
// vector with a non-nil Capture.
func loadCaptureVector(path string) (format.TestVector, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return format.TestVector{}, err
	}
	v, err := format.DecodeTestVector(raw)
	if err != nil {
		return format.TestVector{}, err
	}
	if v.Category != format.CategoryConsensus || v.Capture == nil {
		return format.TestVector{}, fmt.Errorf(
			"expected consensus-category capture, got category=%q",
			v.Category,
		)
	}
	return v, nil
}

// cloneServedSlice deep-copies a served slice so the composed vector
// does not alias buffers owned by the loaded inputs.
func cloneServedSlice(in []format.ServedMessage) []format.ServedMessage {
	out := make([]format.ServedMessage, len(in))
	for i, m := range in {
		out[i] = cloneServedMessage(m)
	}
	return out
}
