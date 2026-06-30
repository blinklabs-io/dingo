// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrAPINotFound is returned when the Dingo API returns 404 (endpoint not implemented).
var ErrAPINotFound = fmt.Errorf("dingo api: endpoint not found (404)")

// ErrAPIError is returned when the Dingo API returns 5xx.
type ErrAPIError struct {
	Status  int
	Message string
}

func (e *ErrAPIError) Error() string {
	return fmt.Sprintf("dingo api error: HTTP %d: %s", e.Status, e.Message)
}

// DingoEpochResp is the Blockfrost /epochs/{number} response shape.
type DingoEpochResp struct {
	Epoch       uint64  `json:"epoch"`
	StartTime   int64   `json:"start_time"`
	EndTime     int64   `json:"end_time"`
	BlockCount  int     `json:"block_count"`
	TxCount     int     `json:"tx_count"`
	Output      string  `json:"output"`
	Fees        string  `json:"fees"`
	ActiveStake *string `json:"active_stake"`
}

// DingoPoolExtendedResp is one item from GET /api/v0/pools/extended.
type DingoPoolExtendedResp struct {
	PoolID      string  `json:"pool_id"`
	Hex         string  `json:"hex"`
	ActiveStake string  `json:"active_stake"`
	LiveStake   string  `json:"live_stake"`
}

// DingoPoolHistoryItem is one epoch entry from GET /api/v0/pools/{pool_id}/history.
type DingoPoolHistoryItem struct {
	Epoch       uint64 `json:"epoch"`
	Blocks      int    `json:"blocks"`
	ActiveStake string `json:"active_stake"`
	ActiveSize  float64 `json:"active_size"`
	Delegators  int    `json:"delegators_count"`
	Rewards     string `json:"rewards"`
	Fees        string `json:"fees"`
}

// BlockfrostClient queries Dingo's Blockfrost-compatible API.
type BlockfrostClient struct {
	baseURL string
	http    *http.Client
}

// NewBlockfrostClient creates a client for the given Dingo API base URL.
// baseURL example: "http://127.0.0.1:8080"
func NewBlockfrostClient(baseURL string) *BlockfrostClient {
	return &BlockfrostClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (b *BlockfrostClient) get(ctx context.Context, path string) ([]byte, int, error) {
	url := b.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("dingo GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("dingo GET %s read: %w", path, err)
	}
	return body, resp.StatusCode, nil
}

// GetLatestEpoch returns the current epoch number from Dingo's /api/v0/epochs/latest.
func (b *BlockfrostClient) GetLatestEpoch(ctx context.Context) (uint64, error) {
	body, status, err := b.get(ctx, "/api/v0/epochs/latest")
	if err != nil {
		return 0, err
	}
	if status == http.StatusNotFound {
		return 0, ErrAPINotFound
	}
	if status >= 500 {
		return 0, &ErrAPIError{Status: status, Message: string(body)}
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("dingo /epochs/latest: status %d", status)
	}
	var epoch DingoEpochResp
	if err := json.Unmarshal(body, &epoch); err != nil {
		return 0, fmt.Errorf("dingo /epochs/latest decode: %w", err)
	}
	return epoch.Epoch, nil
}

// GetEpoch fetches epoch info for a specific epoch number.
// Returns ErrAPINotFound if Dingo hasn't implemented this endpoint.
func (b *BlockfrostClient) GetEpoch(ctx context.Context, epoch uint64) (*DingoEpochResp, error) {
	body, status, err := b.get(ctx, fmt.Sprintf("/api/v0/epochs/%d", epoch))
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, ErrAPINotFound
	}
	if status >= 500 {
		return nil, &ErrAPIError{Status: status, Message: string(body)}
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("dingo /epochs/%d: status %d", epoch, status)
	}
	var ep DingoEpochResp
	if err := json.Unmarshal(body, &ep); err != nil {
		return nil, fmt.Errorf("dingo /epochs/%d decode: %w", epoch, err)
	}
	return &ep, nil
}

// GetPoolsExtended fetches all pools from /api/v0/pools/extended (paginated).
func (b *BlockfrostClient) GetPoolsExtended(ctx context.Context) ([]DingoPoolExtendedResp, error) {
	var all []DingoPoolExtendedResp
	for page := 1; ; page++ {
		body, status, err := b.get(
			ctx,
			fmt.Sprintf("/api/v0/pools/extended?count=100&page=%d", page),
		)
		if err != nil {
			return nil, err
		}
		if status == http.StatusNotFound {
			return nil, ErrAPINotFound
		}
		if status >= 500 {
			return nil, &ErrAPIError{Status: status, Message: string(body)}
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("dingo /pools/extended: status %d", status)
		}
		var page_ []DingoPoolExtendedResp
		if err := json.Unmarshal(body, &page_); err != nil {
			return nil, fmt.Errorf("dingo /pools/extended decode: %w", err)
		}
		all = append(all, page_...)
		if len(page_) < 100 {
			break
		}
	}
	return all, nil
}

// GetPoolHistory fetches a pool's full history from /api/v0/pools/{pool_id}/history.
// Returns ErrAPINotFound if Dingo hasn't implemented this endpoint.
func (b *BlockfrostClient) GetPoolHistory(
	ctx context.Context,
	poolID string,
) ([]DingoPoolHistoryItem, error) {
	var all []DingoPoolHistoryItem
	for page := 1; ; page++ {
		body, status, err := b.get(
			ctx,
			fmt.Sprintf("/api/v0/pools/%s/history?count=100&page=%d", poolID, page),
		)
		if err != nil {
			return nil, err
		}
		if status == http.StatusNotFound {
			return nil, ErrAPINotFound
		}
		if status >= 500 {
			return nil, &ErrAPIError{Status: status, Message: string(body)}
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("dingo /pools/%s/history: status %d", poolID, status)
		}
		var items []DingoPoolHistoryItem
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, fmt.Errorf("dingo /pools/%s/history decode: %w", poolID, err)
		}
		all = append(all, items...)
		if len(items) < 100 {
			break
		}
	}
	return all, nil
}

// GetPoolHistoryForEpoch returns the single history entry for (pool, epoch).
// Returns nil, nil if the pool has no entry for that epoch.
func (b *BlockfrostClient) GetPoolHistoryForEpoch(
	ctx context.Context,
	poolID string,
	epoch uint64,
) (*DingoPoolHistoryItem, error) {
	items, err := b.GetPoolHistory(ctx, poolID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].Epoch == epoch {
			return &items[i], nil
		}
	}
	return nil, nil
}
