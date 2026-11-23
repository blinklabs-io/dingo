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

package sqlstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// fixtureCredentialID returns the dictionary id of hash on any dialect,
// adding the hash when it is not held, for a fixture that inserts utxo or
// address_transaction rows directly. An empty hash yields nil, which binds as
// NULL. It writes through the store's write handle, so a fixture holding an
// open SQLite transaction uses testutil.CredentialID on that transaction.
func fixtureCredentialID(tb testing.TB, store *Store, hash any) any {
	tb.Helper()
	key, _ := hash.([]byte)
	return fixtureDictionaryID(tb, store, credentialDictionary, key)
}

func fixtureDictionaryID(
	tb testing.TB,
	store *Store,
	dict dictionary,
	hash []byte,
) any {
	tb.Helper()
	ids, err := dict.ensure(
		context.Background(),
		newDialectQueryer(store.writeDB, store.dialect.Name()),
		[][]byte{hash},
		store.dialect.ParameterLimit(),
	)
	require.NoError(tb, err)
	if id, ok := ids[string(hash)]; ok {
		return id
	}
	return nil
}
