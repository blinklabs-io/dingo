// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sqlstore

import (
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestLoadUtxoAssetsBatchPreservesGrouping(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(`
CREATE TABLE asset (
 name BLOB, name_hex BLOB, policy_id BLOB, fingerprint BLOB,
 id INTEGER PRIMARY KEY, utxo_id INTEGER, amount TEXT
)`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
INSERT INTO asset (name, name_hex, policy_id, fingerprint, id, utxo_id, amount)
VALUES ('a', '61', 'p', 'f1', 1, 10, '18446744073709551615'),
       ('b', '62', 'p', 'f2', 2, 20, '7')`)
	require.NoError(t, err)
	utxos := map[string][]models.Utxo{
		"first":  {{ID: 10}},
		"second": {{ID: 20}},
	}
	require.NoError(t, store.loadUtxoAssetsBatch(store.writeDB, utxos))
	require.Len(t, utxos["first"][0].Assets, 1)
	require.Equal(t, uint64(math.MaxUint64), uint64(utxos["first"][0].Assets[0].Amount))
	require.Len(t, utxos["second"][0].Assets, 1)
	require.Equal(t, uint64(7), uint64(utxos["second"][0].Assets[0].Amount))
}
