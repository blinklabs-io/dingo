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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	koiosPageSize     = 1000
	koiosMaxRetries   = 3
	koiosRetryBackoff = 2 * time.Second
)

// koiosBaseURLs maps network name to Koios v1 base URL.
var koiosBaseURLs = map[string]string{
	"preview": "https://preview.koios.rest/api/v1",
	"preprod": "https://preprod.koios.rest/api/v1",
}

// KoiosEpochInfoResp is the Koios /epoch_info response shape.
type KoiosEpochInfoResp struct {
	EpochNo      uint64 `json:"epoch_no"`
	ActiveStake  string `json:"active_stake"`
	PoolCnt      int    `json:"pool_cnt"`
	DelegatorCnt int    `json:"delegator_cnt"`
	Fees         string `json:"fees"`
	TotalRewards string `json:"total_rewards"`
}

// KoiosPoolHistoryItem is one epoch entry from /pool_history.
type KoiosPoolHistoryItem struct {
	PoolIDBech32 string `json:"pool_id_bech32"`
	EpochNo      uint64 `json:"epoch_no"`
	ActiveStake  string `json:"active_stake"`
	BlockCnt     int    `json:"block_cnt"`
	DelegatorCnt int    `json:"delegator_cnt"`
}

// KoiosTipResp is the shape of /tip.
type KoiosTipResp struct {
	EpochNo uint64 `json:"epoch_no"`
}

// KoiosClient queries the Koios v1 REST API.
type KoiosClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// NewKoiosClient creates a client for the given network.
func NewKoiosClient(network, apiKey string) (*KoiosClient, error) {
	base, ok := koiosBaseURLs[network]
	if !ok {
		return nil, fmt.Errorf("unsupported network %q; supported: preview, preprod", network)
	}
	return &KoiosClient{
		baseURL: base,
		apiKey:  apiKey,
		http: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

// get executes a GET request against the Koios API with optional Range header.
// rangeStart/rangeEnd < 0 means no Range header.
func (k *KoiosClient) get(
	ctx context.Context,
	path string,
	rangeStart, rangeEnd int,
) (*http.Response, error) {
	url := k.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if k.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+k.apiKey)
	}
	req.Header.Set("Accept", "application/json")
	if rangeStart >= 0 {
		req.Header.Set("Range", fmt.Sprintf("%d-%d", rangeStart, rangeEnd))
	}

	var resp *http.Response
	for attempt := range koiosMaxRetries {
		resp, err = k.http.Do(req.Clone(ctx))
		if err != nil {
			if attempt < koiosMaxRetries-1 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(koiosRetryBackoff):
				}
				continue
			}
			return nil, fmt.Errorf("koios GET %s: %w", path, err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			if attempt < koiosMaxRetries-1 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(koiosRetryBackoff * time.Duration(attempt+1)):
				}
				continue
			}
			return nil, fmt.Errorf("koios rate-limited after %d retries on %s", koiosMaxRetries, path)
		}
		break
	}
	return resp, nil
}

// GetTipEpoch returns the current tip epoch number.
func (k *KoiosClient) GetTipEpoch(ctx context.Context) (uint64, error) {
	resp, err := k.get(ctx, "/tip", -1, -1)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("koios /tip: unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("koios /tip read body: %w", err)
	}
	var tips []KoiosTipResp
	if err := json.Unmarshal(body, &tips); err != nil {
		return 0, fmt.Errorf("koios /tip decode: %w", err)
	}
	if len(tips) == 0 {
		return 0, errors.New("koios /tip: empty response")
	}
	return tips[0].EpochNo, nil
}

// GetEpochInfo fetches epoch info for a specific epoch.
func (k *KoiosClient) GetEpochInfo(ctx context.Context, epoch uint64) (*KoiosEpochInfoResp, error) {
	path := fmt.Sprintf("/epoch_info?_epoch_no=%d", epoch)
	resp, err := k.get(ctx, path, -1, -1)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("koios /epoch_info: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("koios /epoch_info read: %w", err)
	}
	var items []KoiosEpochInfoResp
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("koios /epoch_info decode: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("koios /epoch_info: no data for epoch %d", epoch)
	}
	return &items[0], nil
}

// GetAllPoolHistoryForEpoch returns all pool history entries for a given epoch.
// Using _epoch_no without _pool_bech32 returns every pool that was active in
// the epoch, including retired pools that /pool_list omits.
func (k *KoiosClient) GetAllPoolHistoryForEpoch(ctx context.Context, epoch uint64) ([]KoiosPoolHistoryItem, error) {
	path := fmt.Sprintf(
		"/pool_history?_epoch_no=%d&select=pool_id_bech32,epoch_no,active_stake,block_cnt,delegator_cnt",
		epoch,
	)
	var all []KoiosPoolHistoryItem
	for start := 0; ; start += koiosPageSize {
		end := start + koiosPageSize - 1
		resp, err := k.get(ctx, path, start, end)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("koios /pool_history read: %w", readErr)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("koios /pool_history: status %d", resp.StatusCode)
		}
		var page []KoiosPoolHistoryItem
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("koios /pool_history decode: %w", err)
		}
		all = append(all, page...)
		if len(page) < koiosPageSize {
			break
		}
		total := parseTotalFromContentRange(resp.Header.Get("Content-Range"))
		if total > 0 && start+len(page) >= total {
			break
		}
	}
	return all, nil
}

// parseTotalFromContentRange extracts the total count from a Content-Range header
// like "0-999/5000". Returns -1 on parse failure.
func parseTotalFromContentRange(header string) int {
	// Format: "start-end/total" or "*/total"
	idx := strings.LastIndex(header, "/")
	if idx < 0 {
		return -1
	}
	total, err := strconv.Atoi(header[idx+1:])
	if err != nil {
		return -1
	}
	return total
}
