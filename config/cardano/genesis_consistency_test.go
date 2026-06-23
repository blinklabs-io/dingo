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

package cardano

import (
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestValidateGenesisConsistencyNoGenesis(t *testing.T) {
	// With neither (or only one) genesis loaded there is nothing to
	// cross-check, so the consistency check must pass.
	require.NoError(t, (&CardanoNodeConfig{}).validateGenesisConsistency())

	onlyByron := &CardanoNodeConfig{
		byronGenesis: &byron.ByronGenesis{StartTime: 1000},
	}
	require.NoError(t, onlyByron.validateGenesisConsistency())
}

func TestValidateGenesisConsistencyMatch(t *testing.T) {
	c := &CardanoNodeConfig{
		byronGenesis:   &byron.ByronGenesis{StartTime: 1666656000},
		shelleyGenesis: &shelley.ShelleyGenesis{SystemStart: time.Unix(1666656000, 0).UTC()},
	}
	require.NoError(t, c.validateGenesisConsistency())
}

func TestValidateGenesisConsistencyMismatch(t *testing.T) {
	c := &CardanoNodeConfig{
		byronGenesis:   &byron.ByronGenesis{StartTime: 1506203091},
		shelleyGenesis: &shelley.ShelleyGenesis{SystemStart: time.Unix(1666656000, 0).UTC()},
	}
	require.Error(t, c.validateGenesisConsistency())
}
