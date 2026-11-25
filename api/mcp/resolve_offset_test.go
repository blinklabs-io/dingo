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

package mcp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type fixedPayloads struct{ payload []byte }

func (f fixedPayloads) ResolvePayload(offset []byte) ([]byte, error) {
	if types.IsExpiredPayload(offset) {
		return nil, errors.New("history expired")
	}
	return f.payload, nil
}

// The datum and script columns hold the offset of their bytes in a block, and
// resolve_datum_or_script reads the bytes through the payload resolver rather
// than reporting the offset as the datum.
func TestResolveDatumOrScriptResolvesStoredOffsets(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`
		CREATE TABLE datum (hash BLOB NOT NULL UNIQUE, raw_datum BLOB NOT NULL,
			id INTEGER PRIMARY KEY, added_slot INTEGER NOT NULL);
		CREATE TABLE script (hash BLOB UNIQUE, content BLOB,
			id INTEGER PRIMARY KEY, created_slot INTEGER, type INTEGER);`)
	require.NoError(t, err)
	datumCbor := []byte{0x18, 0x2a}
	offset := (&types.CborOffset{
		BlockSlot: 5, BlockHash: [32]byte{0x01}, ByteOffset: 10, ByteLength: 2,
	}).Encode()
	datumHash := bytes.Repeat([]byte{0x31}, 32)
	_, err = db.Exec(
		"INSERT INTO datum (hash, raw_datum, id, added_slot) VALUES (?, ?, 1, 5)",
		datumHash, offset,
	)
	require.NoError(t, err)
	expiredHash := bytes.Repeat([]byte{0x32}, 32)
	_, err = db.Exec(
		"INSERT INTO datum (hash, raw_datum, id, added_slot) VALUES (?, ?, 2, 5)",
		expiredHash, types.EncodeExpiredPayload(5, bytes.Repeat([]byte{0x01}, 32)),
	)
	require.NoError(t, err)

	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1.0"}, nil)
	limits := newReadLimits()
	limits.payloads = fixedPayloads{payload: datumCbor}
	registerCardanoTools(server, db, nil, nil, "preview", 5*time.Second, limits)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "1.0"}, nil).
		Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": hex.EncodeToString(datumHash)},
	})
	require.NoError(t, err)
	require.False(t, res.IsError)
	text := res.Content[0].(*mcp.TextContent).Text
	require.Contains(t, text, "**Size**: 2 bytes")
	require.Contains(t, text, "182a")

	res, err = cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "resolve_datum_or_script",
		Arguments: map[string]any{"hash": hex.EncodeToString(expiredHash)},
	})
	require.NoError(t, err)
	require.True(t, res.IsError, "an expired payload is an error, not a datum")
	require.Contains(t, res.Content[0].(*mcp.TextContent).Text, "history expired")
}
