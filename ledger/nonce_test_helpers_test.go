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

package ledger

import (
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type memNonceSet struct {
	mu     sync.Mutex
	nonces map[string]models.BlockNonce
}

func (*memNonceSet) Get([]byte, uint32) ([]byte, bool, error) { return nil, false, nil }
func (*memNonceSet) Sum([]byte) (uint64, error)               { return 0, nil }
func (*memNonceSet) ApplyUtxoChanges(*ocommon.Point, []UtxoEvent) error {
	return nil
}
func (*memNonceSet) ApplyLedgerChanges(*ocommon.Point, LedgerChanges) error {
	return nil
}
func (*memNonceSet) NoncesLoaded() (bool, error) { return true, nil }
func (*memNonceSet) BlockNonce(ocommon.Point) ([]byte, bool, error) {
	return nil, false, nil
}
func (*memNonceSet) LatestBlockNonce() (models.BlockNonce, bool, error) {
	return models.BlockNonce{}, false, nil
}
func (s *memNonceSet) BlockNoncesInSlotRange(start, end uint64) ([]models.BlockNonce, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []models.BlockNonce
	for _, nonce := range s.nonces {
		if nonce.Slot >= start && nonce.Slot < end {
			out = append(out, nonce)
		}
	}
	return out, nil
}
