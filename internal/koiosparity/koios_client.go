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
	"sync"
	"time"
)

const (
	koiosPageSize = 1000
	// koiosMaxRetries covers transport errors, 5xx, and burst 429s. Burst
	// cooldowns are ~60s each (see koiosBurstCooldown), so three attempts already
	// span a few minutes of waiting.
	koiosMaxRetries = 3

	// Published Koios limits (https://koios.rest/tiers.html and the OpenAPI
	// "Limits" section at https://api.koios.rest/koiosapi.yaml):
	//
	//   Burst:  Public/Free 100 req / 10s; Pro 250/10s; Premium 500/10s.
	//           Crossing the burst window returns HTTP 429 and the monitoring
	//           layer sleeps the client for ~60 seconds.
	//   Daily:  Public 5,000; Free 50,000; Pro 500,000; Premium 1.2M.
	//           Exhausting the daily allotment also returns 429, typically with
	//           body text containing "Exceeded Tier Limit". Retrying cannot
	//           help until Koios's daily reset (or a higher tier key).
	//
	// Successful responses do not currently advertise X-RateLimit-* /
	// Retry-After headers in practice, so the client enforces the burst window
	// itself and falls back to the documented 60s sleep on 429 when
	// Retry-After is absent.
	koiosBurstWindow      = 10 * time.Second
	koiosBurstLimitPublic = 100
	// Stay under the published Public/Free burst ceiling so concurrent
	// epoch×pool workers don't trip the monitoring layer.
	koiosBurstLimitSafe = 80
	koiosBurstCooldown  = 60 * time.Second

	koiosRetryBackoff5xx = 2 * time.Second
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
//
// Reward-related fields (margin, fixed_cost, pool_fees, deleg_rewards,
// member_rewards) are part of the documented pool_history schema and are
// stored so the cache holds a complete reward reference for each pool epoch.
type KoiosPoolHistoryItem struct {
	EpochNo       uint64   `json:"epoch_no"`
	ActiveStake   string   `json:"active_stake"`
	BlockCnt      int      `json:"block_cnt"`
	DelegatorCnt  int      `json:"delegator_cnt"`
	Margin        *float64 `json:"margin"`
	FixedCost     string   `json:"fixed_cost"`
	PoolFees      string   `json:"pool_fees"`
	DelegRewards  string   `json:"deleg_rewards"`
	MemberRewards *string  `json:"member_rewards"`
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
	limiter *burstLimiter
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
		// Public and Free tiers share the 100/10s burst cap; Pro/Premium are
		// higher, but we don't learn the tier from the key alone, so stay at
		// the Free-safe ceiling for every client.
		limiter: newBurstLimiter(koiosBurstLimitSafe, koiosBurstWindow),
	}, nil
}

// burstLimiter enforces a sliding-window request budget matching Koios's
// published burst window (N requests per 10s).
type burstLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	times  []time.Time
}

func newBurstLimiter(limit int, window time.Duration) *burstLimiter {
	return &burstLimiter{limit: limit, window: window}
}

func (b *burstLimiter) wait(ctx context.Context) error {
	if b == nil || b.limit <= 0 {
		return nil
	}
	for {
		b.mu.Lock()
		now := time.Now()
		cutoff := now.Add(-b.window)
		i := 0
		for i < len(b.times) && b.times[i].Before(cutoff) {
			i++
		}
		if i > 0 {
			b.times = append([]time.Time(nil), b.times[i:]...)
		}
		if len(b.times) < b.limit {
			b.times = append(b.times, now)
			b.mu.Unlock()
			return nil
		}
		sleepUntil := b.times[0].Add(b.window)
		b.mu.Unlock()
		wait := time.Until(sleepUntil)
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		select {
		case <-ctx.Done():
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return context.Canceled
		case <-time.After(wait):
		}
	}
}

// isDailyQuotaExceeded reports whether a 429 body indicates the tier's daily
// request allotment is exhausted (as opposed to the short burst window).
// Observed Koios monitoring-layer body: "Exceeded Tier Limit".
func isDailyQuotaExceeded(body string) bool {
	return strings.Contains(body, "Exceeded Tier Limit")
}

// retryAfterDelay returns how long to wait after a burst 429. Prefer the
// Retry-After header when present; otherwise use the documented 60s cooldown.
func retryAfterDelay(resp *http.Response) time.Duration {
	if resp == nil {
		return koiosBurstCooldown
	}
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return koiosBurstCooldown
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(ra)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(ra); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return koiosBurstCooldown
}

func waitCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return context.Canceled
	case <-time.After(d):
		return nil
	}
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
		if err := k.limiter.wait(ctx); err != nil {
			return nil, err
		}
		resp, err = k.http.Do(req.Clone(ctx))
		if err != nil {
			if attempt < koiosMaxRetries-1 {
				if waitErr := waitCtx(ctx, koiosRetryBackoff5xx*time.Duration(attempt+1)); waitErr != nil {
					return nil, waitErr
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
			bodyStr := strings.TrimSpace(string(body))
			// Daily quota: retrying with the burst cooldown cannot help.
			if isDailyQuotaExceeded(bodyStr) {
				hint := "Public tier caps at 5,000 requests/day with no API key; set --api-key/KOIOS_API_KEY for the Free tier's 50,000/day or higher"
				if k.apiKey != "" {
					hint = "your API-keyed tier's daily quota is exhausted; wait for Koios's daily reset or move to a higher tier"
				}
				return nil, fmt.Errorf("koios daily tier quota exceeded on %s: %s (%s)", path, bodyStr, hint)
			}
			// Burst 429: OpenAPI documents a ~60s sleep for the IP; honour
			// Retry-After when the gateway sends it.
			if attempt < koiosMaxRetries-1 {
				delay := retryAfterDelay(resp)
				if waitErr := waitCtx(ctx, delay); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			return nil, fmt.Errorf(
				"koios burst rate-limited after %d retries on %s (Public/Free = %d req/%s; wait ~%s between bursts): %s",
				koiosMaxRetries, path, koiosBurstLimitPublic, koiosBurstWindow, koiosBurstCooldown, bodyStr,
			)
		}
		if resp.StatusCode >= 500 {
			// 5xx here is Koios's load balancer or backend having a transient
			// hiccup (e.g. 503 "No server is available to handle this
			// request"), not a permanent rejection of the request — retry
			// with backoff like a transport error instead of failing fast.
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if attempt < koiosMaxRetries-1 {
				if waitErr := waitCtx(ctx, koiosRetryBackoff5xx*time.Duration(attempt+1)); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			return nil, fmt.Errorf("koios server error after %d retries on %s: status %d body: %s", koiosMaxRetries, path, resp.StatusCode, strings.TrimSpace(string(body)))
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
//
// The select list includes every reward-related column from the documented
// pool_history schema (inputs: margin/fixed_cost; outputs: pool_fees/
// deleg_rewards/member_rewards) plus the stake/block counts used for
// reward-input parity.
func (k *KoiosClient) GetPoolEpochHistory(
	ctx context.Context,
	poolBech32 string,
	epoch uint64,
) (*KoiosPoolHistoryItem, error) {
	path := fmt.Sprintf(
		"/pool_history?_pool_bech32=%s&_epoch_no=%d&select=epoch_no,active_stake,block_cnt,delegator_cnt,margin,fixed_cost,pool_fees,deleg_rewards,member_rewards",
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
