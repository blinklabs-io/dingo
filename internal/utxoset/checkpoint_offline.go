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

package utxoset

import (
	"io"
	"log/slog"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// OfflineCheckpoints lists the checkpoints of a set that no node has open, for
// an operation such as a truncate that must know how far back the set can be
// brought.
type OfflineCheckpoints struct {
	set *Set
}

// NewOfflineCheckpoints returns the checkpoints in dir for store.
func NewOfflineCheckpoints(
	store *badgerutxo.Store,
	dir string,
) *OfflineCheckpoints {
	s := &Set{
		store:  store,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s.SetCheckpointDir(dir, 0)
	return &OfflineCheckpoints{set: s}
}

// Points lists the checkpoints' tips, newest first.
func (o *OfflineCheckpoints) Points() ([]ocommon.Point, error) {
	cps, err := o.set.Checkpoints()
	if err != nil {
		return nil, err
	}
	out := make([]ocommon.Point, 0, len(cps))
	for _, cp := range cps {
		out = append(out, cp.Point())
	}
	return out, nil
}
