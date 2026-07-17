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
	koiosPageSize   = 1000
	koiosMaxRetries = 3

	// Koios publishes rate limiting as a daily request quota, not a
	// per-second window (https://koios.rest/pricing/Pricing.html): the
	// unauthenticated Public tier caps at 5,000 req/day and is shared across
	// every anonymous caller, while an API key (Free tier and up) gets
	// 50,000+ req/day on its own allotment. No per-second/burst figure is
	// published for either tier, so these backoffs are a conservative
	// heuristic — deliberately longer for the shared anonymous pool — not a
	// value derived from a documented rate.
	koiosRetryBackoffAnon  = 5 * time.Second
	koiosRetryBackoffKeyed = 2 * time.Second
)

// koiosBaseURLs maps network name to Koios v1 base URL.
var koiosBaseURLs = map[string]string{
	"preview": "https://preview.koios.rest/api/v1",
	"preprod": "https://preprod.koios.rest/api/v1",
}

// KoiosEpochInfoResp is the Koios /epoch_info response shape.
// Note: pool_cnt and delegator_cnt are not returned by preview/preprod and are omitted.
// active_stake, fees, and total_rewards are nullable on early epochs (pre-staking, pre-rewards).
type KoiosEpochInfoResp struct {
	EpochNo      uint64  `json:"epoch_no"`
	EndTime      int64   `json:"end_time"` // Unix timestamp of epoch boundary
	ActiveStake  *string `json:"active_stake"`
	Fees         *string `json:"fees"`
	TotalRewards *string `json:"total_rewards"`
}

// KoiosPoolHistoryItem is one epoch entry from /pool_history.
// pool_id_bech32 is excluded from the projection — the caller already knows the pool ID.
type KoiosPoolHistoryItem struct {
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

// retryBackoff returns the base retry backoff for this client's tier: longer
// for the shared anonymous pool, shorter once an API key gives it its own
// daily quota (see the koiosRetryBackoff* comment for why these aren't
// derived from a published per-second rate).
func (k *KoiosClient) retryBackoff() time.Duration {
	if k.apiKey != "" {
		return koiosRetryBackoffKeyed
	}
	return koiosRetryBackoffAnon
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

	backoff := k.retryBackoff()
	var resp *http.Response
	for attempt := range koiosMaxRetries {
		resp, err = k.http.Do(req.Clone(ctx))
		if err != nil {
			if attempt < koiosMaxRetries-1 {
				select {
				case <-ctx.Done():
					if ctxErr := ctx.Err(); ctxErr != nil {
						return nil, ctxErr
					}
					return nil, context.Canceled
				case <-time.After(backoff):
				}
				continue
			}
			return nil, fmt.Errorf("koios GET %s: %w", path, err)
		}
		// http.Client.Do guarantees non-nil resp when err is nil, but nilaway
		// can't see that invariant through the stdlib. Guard explicitly.
		if resp == nil {
			return nil, errors.New("koios: http.Do returned nil response without error")
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			// Koios returns this exact message when the tier's request quota is
			// exhausted (as opposed to a transient burst throttle). Retrying
			// with backoff cannot help — the quota only resets on Koios's
			// schedule or with a higher-tier API key — so fail immediately
			// instead of burning the retry budget.
			if strings.Contains(string(body), "Exceeded Tier Limit") {
				hint := "Public tier caps at 5,000 requests/day with no API key; set --api-key/KOIOS_API_KEY for the Free tier's 50,000/day or higher"
				if k.apiKey != "" {
					hint = "your API-keyed tier's daily quota is exhausted; wait for Koios's daily reset or move to a higher tier"
				}
				return nil, fmt.Errorf("koios tier quota exceeded on %s: %s (%s)", path, strings.TrimSpace(string(body)), hint)
			}
			if attempt < koiosMaxRetries-1 {
				select {
				case <-ctx.Done():
					if ctxErr := ctx.Err(); ctxErr != nil {
						return nil, ctxErr
					}
					return nil, context.Canceled
				case <-time.After(backoff * time.Duration(attempt+1)):
				}
				continue
			}
			return nil, fmt.Errorf("koios rate-limited after %d retries on %s: %s", koiosMaxRetries, path, strings.TrimSpace(string(body)))
		}
		break
	}
	if resp == nil {
		// Unreachable: the loop always returns early on permanent error or breaks
		// on a successful Do(). Guard satisfies nilaway's nil-flow analysis.
		return nil, errors.New("koios: internal: no response after retry loop")
	}
	return resp, nil
}

// GetTipEpoch returns the current tip epoch number.
func (k *KoiosClient) GetTipEpoch(ctx context.Context) (uint64, error) {
	resp, err := k.get(ctx, "/tip", -1, -1)
	if err != nil {
		return 0, err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return 0, fmt.Errorf("koios /tip read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("koios /tip: status %d body: %s", resp.StatusCode, body)
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
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("koios /epoch_info read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("koios /epoch_info: status %d body: %s", resp.StatusCode, body)
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

// GetAllHistoricalPoolIDs returns the bech32 ID of every pool known to Koios,
// including pools that have since retired (pool_status = "retired").
//
// /pool_list is the correct endpoint: it returns all pools with their current
// status and is pageable via Range headers. /pool_registrations does not exist
// as a pageable GET endpoint on preview/preprod.
func (k *KoiosClient) GetAllHistoricalPoolIDs(ctx context.Context) ([]string, error) {
	type listItem struct {
		PoolIDBech32 string `json:"pool_id_bech32"`
	}
	seen := make(map[string]bool)
	var ids []string
	for start := 0; ; start += koiosPageSize {
		end := start + koiosPageSize - 1
		resp, err := k.get(ctx, "/pool_list?select=pool_id_bech32", start, end)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("koios /pool_list read: %w", readErr)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("koios /pool_list: status %d body: %s", resp.StatusCode, body)
		}
		var page []listItem
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("koios /pool_list decode: %w", err)
		}
		for _, item := range page {
			if !seen[item.PoolIDBech32] {
				seen[item.PoolIDBech32] = true
				ids = append(ids, item.PoolIDBech32)
			}
		}
		if len(page) < koiosPageSize {
			break
		}
		total := parseTotalFromContentRange(resp.Header.Get("Content-Range"))
		if total > 0 && start+len(page) >= total {
			break
		}
	}
	return ids, nil
}

// GetPoolEpochHistory fetches a pool's history entry for a specific epoch.
// Returns nil, nil if the pool has no row for that epoch.
// _pool_bech32 is a required Koios function parameter; _epoch_no filters
// server-side so only one row is returned instead of the full pool history.
func (k *KoiosClient) GetPoolEpochHistory(
	ctx context.Context,
	poolBech32 string,
	epoch uint64,
) (*KoiosPoolHistoryItem, error) {
	path := fmt.Sprintf(
		"/pool_history?_pool_bech32=%s&_epoch_no=%d&select=epoch_no,active_stake,block_cnt,delegator_cnt",
		poolBech32, epoch,
	)
	resp, err := k.get(ctx, path, -1, -1)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("koios /pool_history read: %w", readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("koios /pool_history: status %d body: %s", resp.StatusCode, body)
	}
	var items []KoiosPoolHistoryItem
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("koios /pool_history decode: %w", err)
	}
	if len(items) == 0 {
		return nil, nil
	}
	return &items[0], nil
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
