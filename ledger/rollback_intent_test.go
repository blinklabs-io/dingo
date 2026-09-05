// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestRollbackIntentRoundTrip(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	point := ocommon.Point{Slot: 42, Hash: []byte{1, 2, 3}}
	require.NoError(t, persistRollbackIntent(db, point))

	got, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, point, got)
	require.NoError(t, clearRollbackIntent(db))
	_, pending, err = loadRollbackIntent(db)
	require.NoError(t, err)
	require.False(t, pending)
}

func TestRollbackIntentRejectsCorruptRecord(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	for _, raw := range []string{
		"not-json",
		`{}`,
		`{"slot": 0}`,
		`{"slot": 0, "hash": "01"}`,
		`{"slot": 42, "hash": ""}`,
		`{"slot": 42, "hash": "zz"}`,
	} {
		require.NoError(t, db.SetSyncState(durableRollbackIntentSyncKey, raw, nil))
		_, _, err = loadRollbackIntent(db)
		require.Error(t, err, raw)
	}
}
