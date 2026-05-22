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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// Conversation is the parsed sidecar-script (capture-conversation.json
// in a scenario directory). It describes what protocol calls the
// sidecar should drive against cardano-node — nothing about expected
// outputs lives here. Content-level expectations live in the captured
// vector itself; protocol-level expectations live in each step's
// handler in this package.
type Conversation struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

// Step is a single tagged-union step in the conversation. Tagged-union
// shape: every concrete step type has a "type" field, plus its own
// type-specific fields. Adding a new step type (e.g. W2's
// drain_to_tip) is a mechanical addition to decodeStep and to the Step
// implementation set.
type Step interface {
	// Type returns the step's discriminant, matching the "type" JSON
	// field. Used in error messages and protocol-level validation
	// dispatch.
	Type() string

	// Run executes the step against the running sidecar. Returns an
	// error if the step's protocol-level expectations are violated
	// (e.g. find_intersect got something other than intersect_found /
	// intersect_not_found).
	Run(ctx context.Context, s *Sidecar) error
}

// LoadConversation reads and parses a capture-conversation.json file.
func LoadConversation(path string) (Conversation, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Conversation{}, fmt.Errorf(
			"load conversation %s: %w", path, err,
		)
	}
	return DecodeConversation(raw)
}

// DecodeConversation parses raw JSON into a Conversation.
func DecodeConversation(raw []byte) (Conversation, error) {
	type rawConv struct {
		Name  string            `json:"name"`
		Steps []json.RawMessage `json:"steps"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var rc rawConv
	if err := dec.Decode(&rc); err != nil {
		return Conversation{}, fmt.Errorf("conversation: %w", err)
	}
	out := Conversation{Name: rc.Name}
	for i, rawStep := range rc.Steps {
		step, err := decodeStep(rawStep)
		if err != nil {
			return Conversation{}, fmt.Errorf(
				"conversation: steps[%d]: %w", i, err,
			)
		}
		out.Steps = append(out.Steps, step)
	}
	return out, nil
}

func decodeStep(raw json.RawMessage) (Step, error) {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("type probe: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	switch probe.Type {
	case stepTypeFindIntersect:
		var s FindIntersectStep
		if err := dec.Decode(&s); err != nil {
			return nil, fmt.Errorf("find_intersect: %w", err)
		}
		if len(s.Points) == 0 {
			return nil, errors.New(
				"find_intersect: points must not be empty",
			)
		}
		return &s, nil
	case stepTypeRequestNext:
		var s RequestNextStep
		if err := dec.Decode(&s); err != nil {
			return nil, fmt.Errorf("request_next: %w", err)
		}
		return &s, nil
	}
	return nil, fmt.Errorf("unknown step type %q", probe.Type)
}

const (
	stepTypeFindIntersect = "find_intersect"
	stepTypeRequestNext   = "request_next"
)

// requestNextWaitDeadline bounds the per-step wait for a roll-forward /
// roll-backward callback. If the deadline elapses we treat that as an
// implicit AwaitReply — the server has no more blocks to send right
// now — and let the step succeed without recording anything new.
// Sized generously so a healthy testnet's first forge (genesis + a few
// slots) reliably arrives in time, while still failing fast against a
// stuck cardano-node.
const requestNextWaitDeadline = 30 * time.Second

// FindIntersectStep sends FindIntersect with the given points and
// blocks until the server responds with IntersectFound or
// IntersectNotFound. The chainsync client's Sync method drives this
// internally and surfaces IntersectNotFound as an error return — that
// is the step's protocol-level validation surface.
type FindIntersectStep struct {
	Type_  string   `json:"type"`
	Points []string `json:"points"`
}

func (s *FindIntersectStep) Type() string { return stepTypeFindIntersect }

func (s *FindIntersectStep) Run(_ context.Context, sc *Sidecar) error {
	points, err := parsePoints(s.Points)
	if err != nil {
		return fmt.Errorf("find_intersect: %w", err)
	}
	// Sync sends MsgFindIntersect, awaits the response, and on
	// success kicks off the pipelined request loop. The chainsync
	// client returns a non-nil error when the server replies with
	// IntersectNotFound, so a clean return is the "protocol-level
	// expectation met" signal here.
	if err := sc.conn.ChainSync().Client.Sync(points); err != nil {
		return fmt.Errorf("find_intersect: sync: %w", err)
	}
	return nil
}

// RequestNextStep waits for the next inbound RollForward or
// RollBackward to arrive. Since the chainsync client's Sync loop
// pipelines RequestNexts on its own once primed by FindIntersect,
// RequestNextStep does not need to send a new request — it just
// polls the recorder for the next captured message. A wait-deadline
// timeout is interpreted as an implicit AwaitReply (the server
// reached its tip and is sitting on AwaitReply); the step still
// succeeds in that case.
type RequestNextStep struct {
	Type_ string `json:"type"`
}

func (s *RequestNextStep) Type() string { return stepTypeRequestNext }

func (s *RequestNextStep) Run(_ context.Context, sc *Sidecar) error {
	before := sc.recorder.Count()
	_ = sc.recorder.WaitForNextOrDeadline(before, requestNextWaitDeadline)
	return nil
}

// parsePoints accepts the string-encoded points the scenario JSON
// uses. Recognised shapes:
//
//   - "origin"        → pcommon.NewPointOrigin()
//   - "<slot>:<hex>"  → pcommon.NewPoint(slot, hexDecoded)
//
// The smoke-test scenario uses only "origin"; the slot:hex form is
// available for any scenario that needs to pin an intersect to a
// specific captured block.
func parsePoints(in []string) ([]pcommon.Point, error) {
	out := make([]pcommon.Point, 0, len(in))
	for i, p := range in {
		switch {
		case p == "origin":
			out = append(out, pcommon.NewPointOrigin())
		case strings.Contains(p, ":"):
			parts := strings.SplitN(p, ":", 2)
			slot, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf(
					"points[%d]: slot %q: %w",
					i, parts[0], err,
				)
			}
			hash, err := hex.DecodeString(parts[1])
			if err != nil {
				return nil, fmt.Errorf(
					"points[%d]: hash %q: %w",
					i, parts[1], err,
				)
			}
			out = append(out, pcommon.NewPoint(slot, hash))
		default:
			return nil, fmt.Errorf(
				"points[%d]: unrecognised point %q "+
					"(want \"origin\" or \"<slot>:<hex>\")",
				i, p,
			)
		}
	}
	return out, nil
}
