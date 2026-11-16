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

package node

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestLoadWithDBRefusesADrainedDatabase latches a family as an earlier node
// run does, with the database's cache cold, and requires load to refuse
// before it touches the database: it has no live UTxO set to serve the
// drained rows.
func TestLoadWithDBRefusesADrainedDatabase(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		t.Context(),
		nodesettings.Values{
			nodesettings.LedgerStateDrainedGateName: nodesettings.
				EncodeFamilyMask(
					nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces),
				),
		},
		0, 0,
	))
	require.False(t, db.FamilyDrained(nodesettings.FamilyNonces))

	err := LoadWithDB(
		context.Background(),
		&config.Config{Network: "preview"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"unused",
		db,
	)
	require.ErrorIs(t, err, nodesettings.ErrFamilyDrained)
	require.ErrorContains(t, err, "drained into the live UTxO set")
}
