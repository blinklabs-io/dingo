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

// Package koios provides a lightweight Koios REST API client and reward
// parity checker for comparing Dingo's closed-epoch reward state against the
// Koios public API on preview and preprod networks.
package koios

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	NetworkPreview = "preview"
	NetworkPreprod = "preprod"
)

var baseURLs = map[string]string{
	NetworkPreview: "https://preview.koios.rest/api/v1",
	NetworkPreprod: "https://preprod.koios.rest/api/v1",
}

// Client is a minimal Koios REST API client that supports the endpoints
// needed for epoch-level reward parity checks.
type Client struct {
	httpClient *http.Client
	baseURL    string
	network    string
	apiKey     string // optional Bearer token for higher rate limits
}

// NewClient constructs a Client for the named network (preview|preprod).
// Pass an empty apiKey to use anonymous, rate-limited access.
func NewClient(network, apiKey string) (*Client, error) {
	base, ok := baseURLs[network]
	if !ok {
		return nil, fmt.Errorf(
			"unknown network %q: want %q or %q",
			network, NetworkPreview, NetworkPreprod,
		)
	}
	return &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    base,
		network:    network,
		apiKey:     apiKey,
	}, nil
}

// EpochInfo is a single record from the Koios /epoch_info endpoint.
// Fields that are null before an epoch's snapshot is ready are pointers.
type EpochInfo struct {
	EpochNo      uint64  `json:"epoch_no"`
	Fees         string  `json:"fees"`
	ActiveStake  *string `json:"active_stake"`
	TotalRewards *string `json:"total_rewards"`
	PoolCnt      *int64  `json:"pool_cnt"`
	DelegatorCnt *int64  `json:"delegator_cnt"`
}

// PoolHistoryEntry is a single record from the Koios /pool_history endpoint.
type PoolHistoryEntry struct {
	EpochNo     uint64  `json:"epoch_no"`
	ActiveStake string  `json:"active_stake"`
	BlockCnt    *uint64 `json:"block_cnt"`
	Delegators  *uint64 `json:"delegators"`
}

// AccountInfo is a single record from the Koios /account_info endpoint.
type AccountInfo struct {
	StakeAddress     string `json:"stake_address"`
	Status           string `json:"status"`
	RewardsAvailable string `json:"rewards_available"`
}

// GetEpochInfo fetches epoch aggregate data for the given epoch number.
// Returns nil, nil when Koios has no data for the epoch yet.
func (c *Client) GetEpochInfo(ctx context.Context, epoch uint64) (*EpochInfo, error) {
	params := url.Values{
		"_epoch_no":           {strconv.FormatUint(epoch, 10)},
		"_include_next_epoch": {"false"},
	}
	var results []EpochInfo
	if err := c.get(ctx, "/epoch_info", params, &results); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return &results[0], nil
}

// GetPoolHistory fetches per-epoch pool performance data.
// Returns nil, nil when Koios has no entry for that pool/epoch combination.
func (c *Client) GetPoolHistory(
	ctx context.Context,
	poolBech32 string,
	epoch uint64,
) (*PoolHistoryEntry, error) {
	params := url.Values{
		"_pool_bech32": {poolBech32},
		"_epoch_no":    {strconv.FormatUint(epoch, 10)},
	}
	var results []PoolHistoryEntry
	if err := c.get(ctx, "/pool_history", params, &results); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return &results[0], nil
}

// GetAccountInfo fetches current account balances for up to 50 stake addresses
// in a single request. Callers must batch larger sets themselves.
func (c *Client) GetAccountInfo(
	ctx context.Context,
	stakeAddresses []string,
) ([]AccountInfo, error) {
	body := map[string]any{"_stake_addresses": stakeAddresses}
	var results []AccountInfo
	if err := c.post(ctx, "/account_info", body, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// get issues a GET request with query parameters and decodes the JSON response.
func (c *Client) get(
	ctx context.Context,
	path string,
	params url.Values,
	out any,
) error {
	rawURL := c.baseURL + path + "?" + params.Encode()
	return c.do(ctx, http.MethodGet, rawURL, nil, out)
}

// post issues a POST request with a JSON body and decodes the JSON response.
func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}
	return c.do(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf), out)
}

// do performs the HTTP request with up to 3 retries (4 total attempts) on
// 429 Too Many Requests. No wait is inserted after the final attempt.
func (c *Client) do(
	ctx context.Context,
	method, rawURL string,
	body io.ReadSeeker,
	out any,
) error {
	// maxAttempts = 1 initial + 3 retries.
	const maxAttempts = 4
	for attempt := range maxAttempts {
		if attempt > 0 && body != nil {
			if _, err := body.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("rewind request body: %w", err)
			}
		}

		req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
		if err != nil {
			return fmt.Errorf("new request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+c.apiKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("koios %s %s: %w", method, rawURL, err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			// Don't wait after the last attempt; fall through to the error.
			if attempt == maxAttempts-1 {
				break
			}
			wait := time.Duration(attempt+1) * 2 * time.Second
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, parseErr := strconv.Atoi(ra); parseErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response body: %w", readErr)
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf(
				"koios %s %s: HTTP %d: %s",
				method, rawURL, resp.StatusCode, truncate(data, 200),
			)
		}
		return json.Unmarshal(data, out)
	}
	return fmt.Errorf("koios %s %s: exceeded %d attempts", method, rawURL, maxAttempts)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
