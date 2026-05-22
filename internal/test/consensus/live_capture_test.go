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

//go:build consensuscapture

package consensus_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// TestCaptureScenarioLiveStack drives the capture-scenario.sh dispatcher
// against the intersect_origin_one_rollforward scenario end to end:
// build images, bring the docker-compose stack up, run the capture
// sidecar, copy out the resulting vector, tear down. The produced
// vector must decode cleanly via format.DecodeTestVector and contain
// at least one served RollForward.
//
// Build-tagged so `make test` skips it — docker is required, and the
// scenario takes upwards of two minutes to settle (60s genesis-start
// delay plus the first forge).
func TestCaptureScenarioLiveStack(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}

	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "vector.json")
	scriptPath, err := filepath.Abs("capture-scenario.sh")
	if err != nil {
		t.Fatalf("resolve capture-scenario.sh: %v", err)
	}

	// Up to 4 minutes for the whole lifecycle: image build (cached
	// after first run) + 60s system-start delay + a few seconds to
	// forge + sidecar handshake + capture + teardown.
	cmd := exec.Command(
		scriptPath,
		"intersect_origin_one_rollforward",
		"-out", outPath,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("capture-scenario.sh: %v", err)
		}
	case <-time.After(6 * time.Minute):
		_ = cmd.Process.Kill()
		t.Fatal("capture-scenario.sh: timed out after 6 minutes")
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read produced vector: %v", err)
	}
	v, err := format.DecodeTestVector(raw)
	if err != nil {
		t.Fatalf("DecodeTestVector: %v", err)
	}
	if v.Category != format.CategoryConsensus {
		t.Fatalf("category = %q, want %q",
			v.Category, format.CategoryConsensus,
		)
	}
	if v.Capture == nil || len(v.Capture.Peers) != 1 {
		t.Fatalf("expected exactly one peer, got %+v", v.Capture)
	}
	peer := v.Capture.Peers[0]
	rollForwards := 0
	for _, m := range peer.Served {
		if m.Protocol == format.ProtocolChainSync &&
			m.MsgType == format.ChainSyncMsgRollForward {
			rollForwards++
		}
	}
	if rollForwards == 0 {
		t.Fatalf("expected at least one RollForward, served=%+v",
			peer.Served,
		)
	}
	if v.Capture.ExpectedOutput.FinalTip.Slot == 0 ||
		len(v.Capture.ExpectedOutput.FinalTip.Hash) == 0 {
		t.Fatalf("expected non-zero final_tip, got %+v",
			v.Capture.ExpectedOutput.FinalTip,
		)
	}
}
