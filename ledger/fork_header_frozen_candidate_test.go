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
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// A source epoch without a published nonce is seeded with the genesis nonce
// only when it is the initial epoch. Substituting the genesis hash for a later
// epoch would hand a valid fork header the wrong candidate nonce and reject
// it, so that case defers instead.
func TestFrozenCandidateNonceDefersForLaterEpochWithoutNonce(t *testing.T) {
	t.Parallel()

	cfg := newHighFreqShelleyGenesisCfg(t)
	genesisHash := bytes.Repeat([]byte{0x07}, common.Blake2b256Size)
	cfg.ShelleyGenesisHash = hex.EncodeToString(genesisHash)
	ls := &LedgerState{config: LedgerStateConfig{CardanoNodeConfig: cfg}}

	epoch := func(id uint64, nonce []byte) models.Epoch {
		return models.Epoch{
			EpochId:       id,
			StartSlot:     id * 75,
			SlotLength:    1_000,
			LengthInSlots: 75,
			EraId:         eras.BabbageEraDesc.Id,
			Nonce:         nonce,
		}
	}

	t.Run("later epoch without a nonce defers", func(t *testing.T) {
		t.Parallel()
		_, err := ls.frozenCandidateNonce(context.Background(), epoch(5, nil))
		require.Error(t, err)
		require.True(t, IsHeaderVerificationDeferred(err))
	})
	t.Run("initial epoch keeps the genesis seed", func(t *testing.T) {
		t.Parallel()
		got, err := ls.frozenCandidateNonce(context.Background(), epoch(0, nil))
		require.NoError(t, err)
		require.True(t, bytes.Equal(genesisHash, got))
	})
}
