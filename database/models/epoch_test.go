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

package models

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func TestPointerStakeCountedInEra(t *testing.T) {
	t.Parallel()
	for era, counted := range map[uint]bool{
		ledger.EraIdByron:    false,
		ledger.EraIdShelley:  true,
		ledger.EraIdAllegra:  true,
		ledger.EraIdMary:     true,
		ledger.EraIdAlonzo:   true,
		ledger.EraIdBabbage:  true,
		ledger.EraIdConway:   false,
		ledger.EraIdDijkstra: false,
	} {
		require.Equal(t, counted, PointerStakeCountedInEra(era), "era %d", era)
	}
}
