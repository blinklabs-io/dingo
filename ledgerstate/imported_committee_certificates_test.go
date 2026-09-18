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

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestPersistImportedCommitteeCertificatesWritesRows exercises the write path
// itself rather than the fee helper in isolation.
//
// persistImportedCommitteeCertificates carries the imported authorizations
// through SetTransactionMetadataOnly on a synthetic transaction that embeds
// TransactionBodyBase without overriding Fee, and TransactionBodyBase.Fee
// returns nil. Reverting either the nil guard or the setTransaction call site
// back to transaction.Fee().Uint64() panics here with a nil dereference, which
// is what took down a mainnet Mithril bootstrap immediately after the
// committee decoded for the first time. A test over the helper alone stays
// green through that revert, so it has to run this function.
func TestPersistImportedCommitteeCertificatesWritesRows(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cold := Credential{
		Type: CredentialTypeKey,
		Hash: bytes.Repeat([]byte{0xc1}, 28),
	}
	hot := Credential{
		Type: CredentialTypeScript,
		Hash: bytes.Repeat([]byte{0x40}, 28),
	}
	resigned := Credential{
		Type: CredentialTypeScript,
		Hash: bytes.Repeat([]byte{0xc2}, 28),
	}

	certState := &ParsedCertState{
		CommitteeHotKeys: []ParsedCommitteeHotKey{
			{Cold: cold, Hot: hot},
		},
		CommitteeResignations: []Credential{resigned},
	}

	const slot = uint64(197789347)
	require.NotPanics(t, func() {
		require.NoError(t, persistImportedCommitteeCertificates(
			db, certState, slot, nil,
		))
	})

	// The authorization must be readable back by the same cold-credential
	// lookup the Conway unknown-voter rule uses.
	member, err := db.Metadata().GetCommitteeMember(
		uint8(cold.Type), cold.Hash, 0, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, member)
	require.Equal(t, hot.Hash, member.HotCredential)
	require.Equal(t, uint8(hot.Type), member.HotCredentialTag)
}
