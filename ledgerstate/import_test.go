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

package ledgerstate

import "testing"

func TestSnapshotImportTargetsAlignWithRotation(t *testing.T) {
	snapshots := &ParsedSnapShots{}

	targets := snapshotImportTargets(1237, snapshots)
	if len(targets) != 3 {
		t.Fatalf("expected 3 targets, got %d", len(targets))
	}

	expected := []struct {
		name  string
		epoch uint64
	}{
		{name: "mark", epoch: 1237},
		{name: "set", epoch: 1236},
		{name: "go", epoch: 1235},
	}

	for i, target := range targets {
		if target.name != expected[i].name {
			t.Fatalf(
				"target %d: expected name %q, got %q",
				i,
				expected[i].name,
				target.name,
			)
		}
		if target.targetEpoch != expected[i].epoch {
			t.Fatalf(
				"target %d: expected epoch %d, got %d",
				i,
				expected[i].epoch,
				target.targetEpoch,
			)
		}
	}
}

func TestSnapshotImportTargetsSkipNegativeEpochs(t *testing.T) {
	snapshots := &ParsedSnapShots{}

	targets0 := snapshotImportTargets(0, snapshots)
	if len(targets0) != 1 {
		t.Fatalf("epoch 0: expected 1 target, got %d", len(targets0))
	}
	if targets0[0].name != "mark" || targets0[0].targetEpoch != 0 {
		t.Fatalf("epoch 0: unexpected target %+v", targets0[0])
	}

	targets1 := snapshotImportTargets(1, snapshots)
	if len(targets1) != 2 {
		t.Fatalf("epoch 1: expected 2 targets, got %d", len(targets1))
	}
	if targets1[0].name != "mark" || targets1[0].targetEpoch != 1 {
		t.Fatalf("epoch 1 mark: unexpected target %+v", targets1[0])
	}
	if targets1[1].name != "set" || targets1[1].targetEpoch != 0 {
		t.Fatalf("epoch 1 set: unexpected target %+v", targets1[1])
	}
}
