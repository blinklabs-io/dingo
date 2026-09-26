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
	"encoding/hex"
	"fmt"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestGetCommitteeHotAuthorizationsSinceSQLite(t *testing.T) {
	t.Parallel()
	exerciseCommitteeHotAuthorizationsSince(t, newManagementTestStore(t))
}

// exerciseCommitteeHotAuthorizationsSince checks that the window filter keeps
// exactly each cold credential's latest authorization when that row is in the
// window, including the slot tie broken by certificate_id and a script
// credential sharing a key credential's hash.
func exerciseCommitteeHotAuthorizationsSince(t *testing.T, store *Store) {
	t.Helper()
	const (
		keyTag    = uint8(lcommon.CredentialTypeAddrKeyHash)
		scriptTag = uint8(lcommon.CredentialTypeScriptHash)
	)
	seed := func(coldTag uint8, cold byte, hot byte, certificateID, slot uint64) {
		_, err := store.writeDB.Exec(
			store.dialect.Rebind(`
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?, ?)`),
			coldTag, credentialHash(cold), keyTag, credentialHash(hot),
			certificateID, slot,
		)
		require.NoError(t, err)
	}
	// A key cold credential re-authorized inside the window.
	seed(keyTag, 0xa1, 0x01, 1, 10)
	seed(keyTag, 0xa1, 0x02, 2, 200)
	// A key cold credential whose only authorization predates the window.
	seed(keyTag, 0xb1, 0x03, 3, 50)
	// A script cold credential sharing 0xa1's hash, with two authorizations
	// in one slot.
	seed(scriptTag, 0xa1, 0x04, 4, 150)
	seed(scriptTag, 0xa1, 0x05, 5, 150)
	// A key cold credential authorized exactly at the window start.
	seed(keyTag, 0xc1, 0x06, 6, 100)

	collect := func(minSlot uint64) map[string]string {
		rows, err := store.GetCommitteeHotAuthorizationsSince(minSlot, nil)
		require.NoError(t, err)
		ret := make(map[string]string, len(rows))
		for _, row := range rows {
			key := fmt.Sprintf(
				"%d:%s",
				row.ColdCredentialTag,
				hex.EncodeToString(row.ColdCredential),
			)
			require.NotContains(t, ret, key, "one row per cold credential")
			ret[key] = fmt.Sprintf(
				"%s@%d",
				hex.EncodeToString(row.HotCredential[:1]),
				row.AddedSlot,
			)
		}
		return ret
	}
	cold := func(tag uint8, seed byte) string {
		return fmt.Sprintf(
			"%d:%s",
			tag,
			hex.EncodeToString(credentialHash(seed)),
		)
	}

	require.Equal(t, map[string]string{
		cold(keyTag, 0xa1):    "02@200",
		cold(scriptTag, 0xa1): "05@150",
		cold(keyTag, 0xc1):    "06@100",
	}, collect(100))
	require.Equal(t, map[string]string{
		cold(keyTag, 0xa1):    "02@200",
		cold(keyTag, 0xb1):    "03@50",
		cold(scriptTag, 0xa1): "05@150",
		cold(keyTag, 0xc1):    "06@100",
	}, collect(0))
	require.Empty(t, collect(201))
}
