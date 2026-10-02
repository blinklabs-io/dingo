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

package utxorpc

import (
	"bytes"
	"context"
	"testing"

	connect "connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

type datumLookupStub struct {
	tipHeightLedgerStub
	lookups int
}

func (s *datumLookupStub) Datum([]byte) (*models.Datum, error) {
	s.lookups++
	return nil, database.ErrDatumNotFound
}

// TestReadDataRejectsWrongLengthKey covers a datum key that is not a
// Blake2b-256 hash. It is a malformed request, not a missing datum, and must
// not reach the lookup, which would zero-pad it into a different hash.
func TestReadDataRejectsWrongLengthKey(t *testing.T) {
	stub := &datumLookupStub{}
	_, querySrv := newTipHeightServers(t, stub)

	_, err := querySrv.ReadData(
		context.Background(),
		connect.NewRequest(&query.ReadDataRequest{
			Keys: [][]byte{bytes.Repeat([]byte{0x01}, 31)},
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Zero(t, stub.lookups)
}
