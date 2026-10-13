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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const addressCandidateBudget = 2000

type addressCursor struct {
	Version int             `json:"v"`
	Query   string          `json:"q"`
	After   addressPosition `json:"after"`
}

type addressPosition struct {
	Slot       uint64 `json:"slot"`
	BlockIndex uint32 `json:"block_index"`
	OutputIdx  uint32 `json:"output_idx"`
	TxId       []byte `json:"tx_id"`
}

func exactAddressPage(
	queryCtx, requestCtx context.Context,
	lookup utxoAddressLookup,
	pattern models.UtxoAddressPattern,
	network, address, cursor string,
	limit, offset int,
) (*mcp.CallToolResult, any, error) {
	if offset != 0 {
		return nil, nil, errors.New(
			"full-address queries use cursor pagination; offset must be zero",
		)
	}
	digest := sha256.Sum256(
		append([]byte(network+":"), pattern.ExactAddress...),
	)
	queryID := hex.EncodeToString(digest[:])
	var after *models.UtxoOrderingCursor
	if cursor != "" {
		if len(cursor) > 1024 {
			return nil, nil, errors.New("invalid address cursor")
		}
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, nil, errors.New("invalid address cursor encoding")
		}
		var value addressCursor
		if err := json.Unmarshal(raw, &value); err != nil ||
			value.Version != 1 ||
			value.Query != queryID ||
			len(value.After.TxId) != 32 {
			return nil, nil, errors.New(
				"invalid address cursor or cursor belongs to another address/network",
			)
		}
		position := models.UtxoOrderingCursor(value.After)
		after = &position
	}
	page, err := lookup(queryCtx, &models.UtxoWithOrderingQuery{
		AddressPatterns: []models.UtxoAddressPattern{pattern},
		After:           after, Limit: min(limit, 100),
	}, addressCandidateBudget)
	if requestCtx.Err() != nil {
		return nil, nil, requestCtx.Err()
	}
	timedOut := errors.Is(err, context.DeadlineExceeded)
	if err != nil && (!timedOut || page.Scanned == 0) {
		return nil, nil, fmt.Errorf("exact address lookup: %w", err)
	}
	next := ""
	if page.Next != nil {
		raw, err := json.Marshal(
			addressCursor{
				Version: 1,
				Query:   queryID,
				After:   addressPosition(*page.Next),
			},
		)
		if err != nil {
			return nil, nil, err
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	records := make([]map[string]any, 0, len(page.Utxos))
	var rows [][]string
	for _, u := range page.Utxos {
		txID := hex.EncodeToString(u.TxId)
		amount := fmt.Sprint(u.Amount)
		records = append(
			records,
			map[string]any{
				"tx_id":      txID,
				"output_idx": u.OutputIdx,
				"amount":     amount,
			},
		)
		rows = append(
			rows,
			[]string{txID, strconv.FormatUint(uint64(u.OutputIdx), 10), amount},
		)
	}
	reason := "exhausted"
	if next != "" {
		switch {
		case timedOut:
			reason = "deadline"
		case len(page.Utxos) >= limit:
			reason = "result_limit"
		default:
			reason = "candidate_limit"
		}
	}
	text := fmt.Sprintf(
		"### UTxOs for `%s`\n\nShowing %d results; examined %d candidates.\n\n",
		address,
		len(rows),
		page.Scanned,
	)
	text += FormatMarkdownTable([]string{"tx_id", "output_idx", "amount"}, rows)
	if next != "" {
		text += "\nSearch incomplete. Continue with cursor: `" + next + "`\n"
	} else {
		text += "\nSearch complete for this live view.\n"
	}
	text += "\nPages observe live ledger state, not a fixed snapshot. Restart after rollbacks. These pages cannot establish a balance at a single ledger snapshot.\n"
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
		StructuredContent: map[string]any{
			"utxos": records, "next_cursor": next, "complete": next == "",
			"candidates_scanned": page.Scanned, "stop_reason": reason,
			"consistency": "live",
		},
	}, nil, nil
}
