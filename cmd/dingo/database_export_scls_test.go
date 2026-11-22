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

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/go-scls"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func newExportCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())
	return cmd
}

func TestExportSCLSReleasesTheSetAndPublishesAVerifiedFile(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, err := badgerutxo.Open(badgerutxo.DirFor(dataDir))
	require.NoError(t, err)
	tip := ocommon.Point{Slot: 77, Hash: bytes.Repeat([]byte{9}, 32)}
	txOut, err := cbor.Encode([]any{bytes.Repeat([]byte{1}, 29), uint64(5)})
	require.NoError(t, err)
	require.NoError(t, store.Bootstrap(
		ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{8}, 32)},
		func(func(badgerutxo.Output) error) error { return nil },
	))
	_, err = store.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Changes: []badgerutxo.Change{{
			Point: tip,
			Added: []badgerutxo.Output{{
				Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{2}, 32), 3),
				Cbor: txOut,
			}},
		}},
		Epochs: []badgerutxo.EpochRow{
			{Epoch: 0, Era: ledgerstate.EraConway, Value: []byte{0}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	out := filepath.Join(t.TempDir(), "state.scls")
	res, err := exportSCLS(newExportCmd(t), dataDir, out)
	require.NoError(t, err)
	require.Equal(t, uint64(77), res.Slot)
	require.Equal(t, uint64(1), res.Entries)

	f, err := os.Open(out)
	require.NoError(t, err)
	defer f.Close()
	verified, err := scls.Verify(f, scls.VerifyFull)
	require.NoError(t, err)
	require.Equal(t, res.Root, verified.RootHash)

	// The set's directory lock is released, so it opens again.
	again, err := badgerutxo.Open(badgerutxo.DirFor(dataDir))
	require.NoError(t, err)
	require.NoError(t, again.Close())
}

func TestExportSCLSRefusesADataDirWithoutASet(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "state.scls")
	_, err := exportSCLS(newExportCmd(t), t.TempDir(), out)
	require.ErrorContains(t, err, "no live UTxO set")
	_, statErr := os.Stat(out)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
