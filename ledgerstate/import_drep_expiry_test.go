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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestImportDRepsCarriesExpiryEpoch pins the snapshot's DRep expiry through the
// import write path.
//
// ParseCertState decodes DRepState[0] into ParsedDRep.ExpiryEpoch, but
// importDReps built its models.Drep literal without that field, so every
// Mithril-imported DRep landed with expiry_epoch = 0. Zero is exempt from expiry
// in both places that decide it -- drepActiveAtEpoch
// (ledger/governance/epoch.go) and the SQL expiry sweep, whose predicate is
// `expiry_epoch > 0 AND expiry_epoch <= ?` -- so imported DReps stayed in
// countActiveDReps permanently and inflated the ratification quorum denominator
// for the life of the database (issue #4492).
//
// The two DReps carry distinct non-zero expiries, so dropping the field fails
// both assertions and a fix that stamped one shared constant would fail too.
// Asserting a zero expiry would prove nothing here: that is the pre-fix value,
// so such an assertion holds with the fix reverted.
func TestImportDRepsCarriesExpiryEpoch(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	const (
		keyExpiry    = uint64(700)
		scriptExpiry = uint64(812)
		slot         = uint64(197983346)
		deposit      = uint64(500000000)
	)

	keyCred := Credential{
		Type: CredentialTypeKey,
		Hash: bytes.Repeat([]byte{0xd1}, 28),
	}
	scriptCred := Credential{
		Type: CredentialTypeScript,
		Hash: bytes.Repeat([]byte{0xd2}, 28),
	}

	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	require.NoError(t, importDReps(
		context.Background(),
		cfg,
		[]ParsedDRep{
			{
				Credential:  keyCred,
				ExpiryEpoch: keyExpiry,
				Deposit:     deposit,
				Active:      true,
			},
			{
				Credential:  scriptCred,
				ExpiryEpoch: scriptExpiry,
				Deposit:     deposit,
				Active:      true,
			},
		},
		slot,
	))

	for _, tc := range []struct {
		name       string
		cred       Credential
		wantExpiry uint64
	}{
		{name: "key credential", cred: keyCred, wantExpiry: keyExpiry},
		{
			name:       "script credential",
			cred:       scriptCred,
			wantExpiry: scriptExpiry,
		},
	} {
		tag, err := models.CredentialTagFromUint(uint(tc.cred.Type))
		require.NoError(t, err, tc.name)

		got, err := db.GetDrepByCredential(tag, tc.cred.Hash, true, nil)
		require.NoError(t, err, tc.name)
		require.NotNil(t, got, tc.name)
		require.Equal(
			t,
			tc.wantExpiry,
			got.ExpiryEpoch,
			"%s: imported DRep must keep the snapshot expiry; a zero here is "+
				"treated as never-expiring and inflates the ratification quorum",
			tc.name,
		)
	}
}
