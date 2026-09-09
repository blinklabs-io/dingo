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
	require.NoError(t, store.loadUtxoAssetsBatch(t.Context(), store.writeDB, utxos))
	first := utxos["first"]
	second := utxos["second"]
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("expected hydrated UTxOs")
	}
	if len(first[0].Assets) == 0 {
		t.Fatal("expected first asset")
	}
	require.Equal(t, uint64(math.MaxUint64), uint64(first[0].Assets[0].Amount))
	if len(second[0].Assets) == 0 {
		t.Fatal("expected second asset")
	}
	require.Equal(t, uint64(7), uint64(second[0].Assets[0].Amount))
}

func TestLoadUtxoAssetsDeduplicatesIDsAcrossChunks(t *testing.T) {
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
VALUES ('a', '61', 'p', 'f1', 1, 10, '1')`)
	require.NoError(t, err)

	// Use enough repeated instances to force the same ID into two parameter
	// chunks.  Each instance still needs one asset, but the asset row must be
	// queried only once.
	utxos := make([]models.Utxo, 1000)
	for i := range utxos {
		utxos[i].ID = 10
	}
	pointers := make([]*models.Utxo, len(utxos))
	for i := range utxos {
		pointers[i] = &utxos[i]
	}
	require.NoError(t, store.loadUtxoAssets(t.Context(), store.writeDB, pointers))
	for i := range utxos {
		require.Len(t, utxos[i].Assets, 1)
	}
}
