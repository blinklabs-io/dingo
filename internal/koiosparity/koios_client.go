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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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

	// koiosAccountChunkSize bounds how many stake addresses go into a single
	// /account_reward_history POST request. Koios does not document a hard
	// limit on the _stake_addresses array for this endpoint, so this is a
	// conservative, deliberately small choice: it keeps both the outbound
	// request body and the worst-case response bounded regardless of how
	// large the full requested address universe is, and limits the "blast
	// radius" of a single failed/timed-out request to a small slice of the
	// epoch's account universe rather than the whole thing. This was the
	// minimal viable chunking for #3097; shaping requests further by actual
	// encoded byte size and mid-fetch resumable checkpointing is #3099's
	// scope, delivered in fetchAccountRewardsForEpoch (see its doc comment)
	// via chunkAddressesByCountAndSize — koiosAccountChunkSize remains the
	// default address-count bound when an operator hasn't tuned
	// --account-chunk-size.
	koiosAccountChunkSize = 100

	// koiosMaxResponseBytes caps every Koios response body read (GET and
	// POST) — dingo #3099's "bound response/body memory" requirement.
	// Existing GET responses are already page-bounded to koiosPageSize rows
	// and never approach this; it exists specifically as a defensive
	// ceiling for /account_reward_history's POST responses, whose size
	// isn't otherwise bounded by anything but Koios's own internal paging
	// (see GetAccountRewardHistory's koiosPageSize truncation-detection).
	koiosMaxResponseBytes = 32 * 1024 * 1024
)

// koiosBaseURLs maps network name to Koios v1 base URL.
var koiosBaseURLs = map[string]string{
	"preview": "https://preview.koios.rest/api/v1",
	"preprod": "https://preprod.koios.rest/api/v1",
}

// KoiosEpochInfoResp is the Koios /epoch_info response shape, covering every
// field in the documented schema (components/schemas/epoch_info in
// https://api.koios.rest/koiosapi.yaml).
// Note: pool_cnt and delegator_cnt are not returned by preview/preprod and are omitted.
// active_stake, fees, and total_rewards are documented nullable on early
// epochs (pre-staking, pre-rewards). out_sum and avg_blk_reward are not
// documented nullable but are defensively typed as pointers because Koios has
// been observed returning null for them on the same early epochs as fees.
type KoiosEpochInfoResp struct {
	EpochNo        uint64  `json:"epoch_no"`
	Era            string  `json:"era"`
	OutSum         *string `json:"out_sum"`
	Fees           *string `json:"fees"`
	TxCount        int64   `json:"tx_count"`
	BlkCount       int64   `json:"blk_count"`
	StartTime      int64   `json:"start_time"`
	EndTime        int64   `json:"end_time"` // Unix timestamp of epoch boundary
	FirstBlockTime int64   `json:"first_block_time"`
	LastBlockTime  int64   `json:"last_block_time"`
	ActiveStake    *string `json:"active_stake"`
	TotalRewards   *string `json:"total_rewards"`
	AvgBlkReward   *string `json:"avg_blk_reward"`
}

// KoiosPoolHistoryItem is one epoch entry from /pool_history, covering every
// field in the documented pool_history_info schema.
// pool_id_bech32 is excluded from the projection — the caller already knows the pool ID.
//
// Reward-related fields (margin, fixed_cost, pool_fees, deleg_rewards,
// member_rewards, epoch_ros) are part of the documented pool_history schema
// and are stored so the cache holds a complete reward reference for each pool
// epoch.
type KoiosPoolHistoryItem struct {
	EpochNo        uint64   `json:"epoch_no"`
	ActiveStake    string   `json:"active_stake"`
	ActiveStakePct *float64 `json:"active_stake_pct"`
	SaturationPct  float64  `json:"saturation_pct"`
	BlockCnt       int      `json:"block_cnt"`
	DelegatorCnt   int      `json:"delegator_cnt"`
	Margin         *float64 `json:"margin"`
	FixedCost      string   `json:"fixed_cost"`
	PoolFees       string   `json:"pool_fees"`
	DelegRewards   string   `json:"deleg_rewards"`
	MemberRewards  *string  `json:"member_rewards"`
	EpochRos       float64  `json:"epoch_ros"`
}

// KoiosTipResp is the shape of /tip.
type KoiosTipResp struct {
	EpochNo uint64 `json:"epoch_no"`
}

// KoiosTotalsResp is the Koios /totals response shape, covering every field
// in the documented schema (components/schemas/totals in
// https://api.koios.rest/koiosapi.yaml).
//
// /totals and /epoch_info both have a "fees" field, and /totals additionally
// has "reward" versus /epoch_info's "total_rewards" — these are NOT the same
// quantities despite the naming overlap:
//   - epoch_info.fees is the sum of transaction fees for txs included in that
//     epoch's blocks (raw block/tx accounting).
//   - totals.fees is "the amount in the fee pot" — the ledger AdaPots fee-pot
//     value at the epoch boundary, which is what Dingo's reward_ada_pots.Fees
//     actually stores. Verified empirically against a live preview node: for
//     the same epoch, totals.fees matched Dingo's reward_ada_pots.Fees
//     exactly while epoch_info.fees did not.
//   - totals.reward ("rewards accumulated as of given epoch") is a lagged
//     cumulative accumulator, while reward_ada_pots.Rewards is a per-epoch
//     flow. It is cached but intentionally not compared; see
//     CompareEpochTotals.
type KoiosTotalsResp struct {
	EpochNo uint64 `json:"epoch_no"`
	// Circulation, Supply, DepositsStake, DepositsDRep, DepositsProposal,
	// TreasuryDonation, TreasuryWithdrawal, and ReservesWithdrawal are stored
	// for reference (see KoiosTotals) but not compared: Dingo's AdaPots model
	// (models.RewardAdaPots) only tracks treasury/reserves/rewards/fees — the
	// same four pots the core ledger AdaPots type tracks. Circulation/supply
	// require a live UTxO-set scan and the deposit/donation/withdrawal fields
	// require replaying registration/deregistration/governance events; both
	// are out of scope for this cache-based checker.
	Circulation        string `json:"circulation"`
	Treasury           string `json:"treasury"`
	Reward             string `json:"reward"`
	Supply             string `json:"supply"`
	Reserves           string `json:"reserves"`
	Fees               string `json:"fees"`
	DepositsStake      string `json:"deposits_stake"`
	DepositsDRep       string `json:"deposits_drep"`
	DepositsProposal   string `json:"deposits_proposal"`
	TreasuryDonation   string `json:"treasury_donation"`
	TreasuryWithdrawal string `json:"treasury_withdrawal"`
	ReservesWithdrawal string `json:"reserves_withdrawal"`
}

// ErrKoiosPermanent marks a Koios failure that cannot succeed by retrying:
// daily-quota exhaustion, or any other non-2xx/206 status get() didn't
// already retry internally (401/403 auth failures, 400/404/422 bad request
// or unsupported query, etc. — 429 bursts and 5xx are retried inside get()
// and only reach the caller once their retries are exhausted, so those
// remain unwrapped/transient). Callers use errors.Is(err, ErrKoiosPermanent)
// to decide whether to keep scheduling further work or abort immediately.
var ErrKoiosPermanent = errors.New("koios: permanent error")

// KoiosClient queries the Koios v1 REST API.
type KoiosClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
	limiter *burstLimiter
}

// validateKoiosNetwork rejects any network this tool doesn't support
// (currently "preview"/"preprod" only, via koiosBaseURLs). Called both by
// NewKoiosClient (the live-fetch path) and by Check/CheckEpoch (the
// cache-only path, which never constructs a KoiosClient and so would
// otherwise let an unsupported network — e.g. "mainnet" — reach
// compareEpochAccounts/StakeAddressFromCredential unvalidated;
// StakeAddressFromCredential hardcodes the testnet address network ID since
// preview/preprod are the only networks this tool ever validates against,
// so an unvalidated "mainnet" would silently generate wrong-network stake
// addresses instead of erroring).
func validateKoiosNetwork(network string) error {
	if _, ok := koiosBaseURLs[network]; !ok {
		return fmt.Errorf(
			"unsupported network %q; supported: preview, preprod",
			network,
		)
	}
	return nil
}

// NewKoiosClient creates a client for the given network.
//
// baseURL overrides the public koios.rest host for the network, for a
// self-hosted or mirrored Koios instance. It is the full v1 API root, e.g.
// "https://preview-koios.example.com/api/v1"; a trailing slash is trimmed so
// the caller does not have to care. Empty selects the public host.
//
// A custom host also drops the burst cap. koiosBurstLimitSafe describes
// koios.rest's own published Public/Free tier window and says nothing about
// another deployment, so applying it there would throttle against a limit that
// does not exist. The per-request retry and timeout handling is unchanged, so a
// host that does rate-limit still backs off correctly on 429.
func NewKoiosClient(
	network, apiKey, baseURL string,
	allowInsecureHTTP bool,
) (*KoiosClient, error) {
	if err := validateKoiosNetwork(network); err != nil {
		return nil, err
	}
	base := koiosBaseURLs[network]
	burstLimit := koiosBurstLimitSafe
	if trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/"); trimmed != "" {
		if err := validateKoiosBaseURL(trimmed, allowInsecureHTTP); err != nil {
			return nil, err
		}
		base = trimmed
		burstLimit = 0
	}
	return &KoiosClient{
		baseURL: base,
		apiKey:  apiKey,
		http: &http.Client{
			Timeout: 60 * time.Second,
		},
		// Public and Free tiers share the 100/10s burst cap; Pro/Premium are
		// higher, but we don't learn the tier from the key alone, so stay at
		// the Free-safe ceiling for every client on the public host.
		limiter: newBurstLimiter(burstLimit, koiosBurstWindow),
	}, nil
}

// validateKoiosBaseURL rejects a custom host this client must not send an API
// key to, or trust reference data from.
//
// get and post attach APIKey as a Bearer token to every request, so plain HTTP
// puts the token on the wire in cleartext. It also leaves the reference data
// this tool compares Dingo against tamperable in flight, and a comparison
// against forged reference data can report a false PASS -- the one outcome a
// parity checker must never produce. allowInsecureHTTP is the local dev/test
// escape hatch, mirroring Mithril.AllowInsecureHTTP.
func validateKoiosBaseURL(rawURL string, allowInsecureHTTP bool) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse koios base URL %q: %w", rawURL, err)
	}
	if parsed.Host == "" {
		return fmt.Errorf(
			"koios base URL %q has no host; give the full v1 API root, e.g. https://host/api/v1",
			rawURL,
		)
	}
	switch parsed.Scheme {
	case "https":
		return nil
	case "http":
		if allowInsecureHTTP {
			return nil
		}
		return fmt.Errorf(
			"koios base URL %q uses plain HTTP, which would send the API key in cleartext and leave the reference data tamperable; use https or set allowInsecureHttp for local dev/test",
			rawURL,
		)
	default:
		return fmt.Errorf(
			"koios base URL %q must use http or https, got scheme %q",
			rawURL,
			parsed.Scheme,
		)
	}
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
		wait := max(time.Until(sleepUntil), time.Millisecond)
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
	if secs, err := strconv.Atoi(strings.TrimSpace(ra)); err == nil &&
		secs > 0 {
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

// koiosResponse is a fully-drained Koios HTTP response: the body is read out
// and the underlying connection closed before get() returns, so callers never
// need to manage resp.Body themselves and a body read failure can be retried
// exactly like a transport or 5xx error (see get()).
type koiosResponse struct {
	StatusCode int
	Body       []byte
	Header     http.Header
}

// errKoiosResponseTooLarge marks a response body that reached
// koiosMaxResponseBytes without terminating — a response that big means
// something is wrong upstream (or the request itself was shaped too large),
// not a transient blip. It wraps ErrKoiosPermanent so classifyFetchErr
// (and any caller checking errors.Is(err, ErrKoiosPermanent), e.g.
// fetchAccountRewardsForEpoch's per-chunk classification) treats this the
// same as any other permanent failure — never retried within a call, and
// never automatically retried again on a future fetch run either, since
// retrying an oversized chunk at the same configured size would just fail
// the same way forever; the resolution is a smaller --account-chunk-size
// or --account-chunk-max-bytes, not a retry.
var errKoiosResponseTooLarge = fmt.Errorf(
	"%w: koios response body exceeded the maximum allowed size",
	ErrKoiosPermanent,
)

// readBodyLimited reads r fully, capped at koiosMaxResponseBytes — dingo
// #3099's "bound response/body memory" requirement, applied uniformly to
// every Koios call (GET and POST). Reading koiosMaxResponseBytes+1 bytes
// means the true body is at or past the cap, so it fails hard with
// errKoiosResponseTooLarge rather than silently returning a truncated
// prefix as if it were the complete response.
func readBodyLimited(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, koiosMaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > koiosMaxResponseBytes {
		return nil, fmt.Errorf(
			"%w: exceeded %d bytes",
			errKoiosResponseTooLarge,
			koiosMaxResponseBytes,
		)
	}
	return body, nil
}

// get executes a GET request against the Koios API with optional Range header,
// retrying transport errors, 5xx responses, burst 429s, and body-read failures.
// rangeStart/rangeEnd < 0 means no Range header.
//
// The body is read to completion inside the retry loop (not left to the
// caller) so a connection that drops mid-transfer — after a successful status
// line but before the full body arrives — is retried the same as a transport
// error, instead of surfacing as a hard, non-retried failure.
func (k *KoiosClient) get(
	ctx context.Context,
	path string,
	rangeStart, rangeEnd int,
) (*koiosResponse, error) {
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

	// retryOrFail waits `delay` and returns nil (meaning: loop back and retry)
	// when attempts remain, or the formatted failure error on the final
	// attempt. A non-nil error can also mean the wait itself was interrupted
	// by ctx cancellation — either way, the caller should return it as-is.
	retryOrFail := func(attempt int, delay time.Duration, failFmt string, failArgs ...any) error {
		if attempt < koiosMaxRetries-1 {
			return waitCtx(ctx, delay)
		}
		return fmt.Errorf(failFmt, failArgs...)
	}

	for attempt := range koiosMaxRetries {
		if err := k.limiter.wait(ctx); err != nil {
			return nil, err
		}
		resp, doErr := k.http.Do(req.Clone(ctx))
		if doErr != nil {
			if err := retryOrFail(attempt,
				koiosRetryBackoff5xx*time.Duration(attempt+1),
				"koios GET %s: %w", path, doErr,
			); err != nil {
				return nil, err
			}
			continue
		}
		// http.Client.Do guarantees non-nil resp when err is nil, but nilaway
		// can't see that invariant through the stdlib. Guard explicitly.
		if resp == nil {
			return nil, errors.New(
				"koios: http.Do returned nil response without error",
			)
		}

		body, readErr := readBodyLimited(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			if errors.Is(readErr, errKoiosResponseTooLarge) {
				// A response this large means something is wrong upstream, not
				// a transient blip — never retry it (see readBodyLimited).
				return nil, fmt.Errorf("koios GET %s: %w", path, readErr)
			}
			// Treat any other body-read failure (e.g. connection reset
			// mid-transfer) exactly like a transport error: it's transient
			// and safe to retry.
			if err := retryOrFail(attempt,
				koiosRetryBackoff5xx*time.Duration(attempt+1),
				"koios GET %s: read body: %w", path, readErr,
			); err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			bodyStr := strings.TrimSpace(string(body))
			// Daily quota: retrying with the burst cooldown cannot help.
			if isDailyQuotaExceeded(bodyStr) {
				hint := "Public tier caps at 5,000 requests/day with no API key; set --api-key/KOIOS_API_KEY for the Free tier's 50,000/day or higher"
				if k.apiKey != "" {
					hint = "your API-keyed tier's daily quota is exhausted; wait for Koios's daily reset or move to a higher tier"
				}
				return nil, fmt.Errorf(
					"%w: koios daily tier quota exceeded on %s: %s (%s)",
					ErrKoiosPermanent,
					path,
					bodyStr,
					hint,
				)
			}
			// Burst 429: OpenAPI documents a ~60s sleep for the IP; honour
			// Retry-After when the gateway sends it.
			if err := retryOrFail(attempt, retryAfterDelay(resp),
				"koios burst rate-limited after %d retries on %s (Public/Free = %d req/%s; wait ~%s between bursts): %s",
				koiosMaxRetries, path, koiosBurstLimitPublic, koiosBurstWindow, koiosBurstCooldown, bodyStr,
			); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode >= 500 {
			// 5xx here is Koios's load balancer or backend having a transient
			// hiccup (e.g. 503 "No server is available to handle this
			// request"), not a permanent rejection of the request — retry
			// with backoff like a transport error instead of failing fast.
			if err := retryOrFail(attempt,
				koiosRetryBackoff5xx*time.Duration(attempt+1),
				"koios server error after %d retries on %s: status %d body: %s",
				koiosMaxRetries, path, resp.StatusCode, strings.TrimSpace(string(body)),
			); err != nil {
				return nil, err
			}
			continue
		}

		// Every other non-2xx status (401/403 auth failures, 400/404/422 bad
		// request or unsupported query, etc.) was never retried above and
		// will never succeed by retrying — mark it permanent so callers stop
		// scheduling further doomed requests instead of treating it as an
		// isolated, retryable blip.
		if resp.StatusCode != http.StatusOK &&
			resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf(
				"%w: koios GET %s: status %d body: %s",
				ErrKoiosPermanent,
				path,
				resp.StatusCode,
				strings.TrimSpace(string(body)),
			)
		}

		return &koiosResponse{
			StatusCode: resp.StatusCode,
			Body:       body,
			Header:     resp.Header,
		}, nil
	}
	// Unreachable: every loop iteration either returns or continues; the range
	// is bounded by koiosMaxRetries and the last iteration always returns via
	// retryOrFail's fail branch. Guard satisfies nilaway's nil-flow analysis.
	return nil, errors.New("koios: internal: no response after retry loop")
}

// post executes a POST request with a JSON-encoded body against the Koios
// API, retrying transport errors, 5xx responses, burst 429s, and body-read
// failures with exactly the same policy as get() (see get()'s doc comment
// for the retry/classification rationale) — the only structural difference
// is that a POST body must be rebuilt fresh on every attempt (a
// bytes.Reader, once drained by http.Client.Do, cannot be replayed the way
// get()'s bodyless request can via req.Clone).
func (k *KoiosClient) post(
	ctx context.Context,
	path string,
	payload any,
) (*koiosResponse, error) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf(
			"koios POST %s: marshal request body: %w",
			path,
			err,
		)
	}
	url := k.baseURL + path

	// See get()'s identical helper for the meaning of retryOrFail's return.
	retryOrFail := func(attempt int, delay time.Duration, failFmt string, failArgs ...any) error {
		if attempt < koiosMaxRetries-1 {
			return waitCtx(ctx, delay)
		}
		return fmt.Errorf(failFmt, failArgs...)
	}

	for attempt := range koiosMaxRetries {
		if err := k.limiter.wait(ctx); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			url,
			bytes.NewReader(bodyBytes),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"koios POST %s: build request: %w",
				path,
				err,
			)
		}
		if k.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+k.apiKey)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")

		resp, doErr := k.http.Do(req)
		if doErr != nil {
			if err := retryOrFail(attempt,
				koiosRetryBackoff5xx*time.Duration(attempt+1),
				"koios POST %s: %w", path, doErr,
			); err != nil {
				return nil, err
			}
			continue
		}
		if resp == nil {
			return nil, errors.New(
				"koios: http.Do returned nil response without error",
			)
		}

		body, readErr := readBodyLimited(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			if errors.Is(readErr, errKoiosResponseTooLarge) {
				// A response this large means something is wrong upstream, not
				// a transient blip — never retry it (see readBodyLimited).
				return nil, fmt.Errorf("koios POST %s: %w", path, readErr)
			}
			if err := retryOrFail(attempt,
				koiosRetryBackoff5xx*time.Duration(attempt+1),
				"koios POST %s: read body: %w", path, readErr,
			); err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			bodyStr := strings.TrimSpace(string(body))
			if isDailyQuotaExceeded(bodyStr) {
				hint := "Public tier caps at 5,000 requests/day with no API key; set --api-key/KOIOS_API_KEY for the Free tier's 50,000/day or higher"
				if k.apiKey != "" {
					hint = "your API-keyed tier's daily quota is exhausted; wait for Koios's daily reset or move to a higher tier"
				}
				return nil, fmt.Errorf(
					"%w: koios daily tier quota exceeded on %s: %s (%s)",
					ErrKoiosPermanent,
					path,
					bodyStr,
					hint,
				)
			}
			if err := retryOrFail(attempt, retryAfterDelay(resp),
				"koios burst rate-limited after %d retries on %s (Public/Free = %d req/%s; wait ~%s between bursts): %s",
				koiosMaxRetries, path, koiosBurstLimitPublic, koiosBurstWindow, koiosBurstCooldown, bodyStr,
			); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode >= 500 {
			if err := retryOrFail(attempt,
				koiosRetryBackoff5xx*time.Duration(attempt+1),
				"koios server error after %d retries on %s: status %d body: %s",
				koiosMaxRetries, path, resp.StatusCode, strings.TrimSpace(string(body)),
			); err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode != http.StatusOK &&
			resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf(
				"%w: koios POST %s: status %d body: %s",
				ErrKoiosPermanent,
				path,
				resp.StatusCode,
				strings.TrimSpace(string(body)),
			)
		}

		return &koiosResponse{
			StatusCode: resp.StatusCode,
			Body:       body,
			Header:     resp.Header,
		}, nil
	}
	// Unreachable: see get()'s identical comment.
	return nil, errors.New("koios: internal: no response after retry loop")
}

// GetTipEpoch returns the current tip epoch number.
func (k *KoiosClient) GetTipEpoch(ctx context.Context) (uint64, error) {
	resp, err := k.get(ctx, "/tip", -1, -1)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf(
			"koios /tip: status %d body: %s",
			resp.StatusCode,
			resp.Body,
		)
	}
	var tips []KoiosTipResp
	if err := json.Unmarshal(resp.Body, &tips); err != nil {
		return 0, fmt.Errorf("koios /tip decode: %w", err)
	}
	if len(tips) == 0 {
		return 0, errors.New("koios /tip: empty response")
	}
	return tips[0].EpochNo, nil
}

// GetEpochInfo fetches epoch info for a specific epoch.
func (k *KoiosClient) GetEpochInfo(
	ctx context.Context,
	epoch uint64,
) (*KoiosEpochInfoResp, error) {
	path := fmt.Sprintf("/epoch_info?_epoch_no=%d", epoch)
	resp, err := k.get(ctx, path, -1, -1)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"koios /epoch_info: status %d body: %s",
			resp.StatusCode,
			resp.Body,
		)
	}
	var items []KoiosEpochInfoResp
	if err := json.Unmarshal(resp.Body, &items); err != nil {
		return nil, fmt.Errorf("koios /epoch_info decode: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("koios /epoch_info: no data for epoch %d", epoch)
	}
	if len(items) != 1 || items[0].EpochNo != epoch {
		return nil, fmt.Errorf(
			"koios /epoch_info: requested epoch %d, got %d row(s) beginning with epoch %d",
			epoch,
			len(items),
			items[0].EpochNo,
		)
	}
	return &items[0], nil
}

// GetTotals fetches network-wide tokenomic totals (treasury, reserves,
// rewards, fees, etc.) for a specific epoch.
func (k *KoiosClient) GetTotals(
	ctx context.Context,
	epoch uint64,
) (*KoiosTotalsResp, error) {
	path := fmt.Sprintf("/totals?_epoch_no=%d", epoch)
	resp, err := k.get(ctx, path, -1, -1)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"koios /totals: status %d body: %s",
			resp.StatusCode,
			resp.Body,
		)
	}
	var items []KoiosTotalsResp
	if err := json.Unmarshal(resp.Body, &items); err != nil {
		return nil, fmt.Errorf("koios /totals decode: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("koios /totals: no data for epoch %d", epoch)
	}
	if len(items) != 1 || items[0].EpochNo != epoch {
		return nil, fmt.Errorf(
			"koios /totals: requested epoch %d, got %d row(s) beginning with epoch %d",
			epoch,
			len(items),
			items[0].EpochNo,
		)
	}
	return &items[0], nil
}

// GetAllHistoricalPoolIDs returns the bech32 ID of every pool known to Koios,
// including pools that have since retired (pool_status = "retired").
//
// /pool_list is the correct endpoint: it returns all pools with their current
// status and is pageable via Range headers. /pool_registrations does not exist
// as a pageable GET endpoint on preview/preprod.
func (k *KoiosClient) GetAllHistoricalPoolIDs(
	ctx context.Context,
) ([]string, error) {
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
		if resp.StatusCode != http.StatusOK &&
			resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf(
				"koios /pool_list: status %d body: %s",
				resp.StatusCode,
				resp.Body,
			)
		}
		var page []listItem
		if err := json.Unmarshal(resp.Body, &page); err != nil {
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

// GetPoolFirstActiveEpochs returns, for every pool with at least one
// documented update, the earliest active_epoch_no across its full
// registration/update history — the epoch it first became eligible for
// delegation and could have any /pool_history row.
//
// /pool_list's own active_epoch_no is NOT usable for this: it reflects only
// the pool's CURRENT (most recent) registration. A pool that updated its
// pledge/margin/etc. after its original registration would report an
// active_epoch_no long after its true first-active epoch, and treating that
// later epoch as a lower bound would wrongly skip real history for every
// epoch in between. /pool_updates instead returns one row per historical
// update (across ALL pools, paginated, no per-pool request needed), so the
// minimum active_epoch_no per pool here is a safe, correct lower bound.
func (k *KoiosClient) GetPoolFirstActiveEpochs(
	ctx context.Context,
) (map[string]uint64, error) {
	type updateItem struct {
		PoolIDBech32  string  `json:"pool_id_bech32"`
		ActiveEpochNo *uint64 `json:"active_epoch_no"`
	}
	first := make(map[string]uint64)
	for start := 0; ; start += koiosPageSize {
		end := start + koiosPageSize - 1
		resp, err := k.get(
			ctx,
			"/pool_updates?select=pool_id_bech32,active_epoch_no",
			start,
			end,
		)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK &&
			resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf(
				"koios /pool_updates: status %d body: %s",
				resp.StatusCode,
				resp.Body,
			)
		}
		var page []updateItem
		if err := json.Unmarshal(resp.Body, &page); err != nil {
			return nil, fmt.Errorf("koios /pool_updates decode: %w", err)
		}
		for _, item := range page {
			// Null active_epoch_no is not documented but defensively skipped
			// rather than treated as epoch 0 — better to miss this
			// optimization for one pool than to draw a wrong conclusion from
			// a null value.
			if item.ActiveEpochNo == nil {
				continue
			}
			if cur, ok := first[item.PoolIDBech32]; !ok ||
				*item.ActiveEpochNo < cur {
				first[item.PoolIDBech32] = *item.ActiveEpochNo
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
	return first, nil
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
		"/pool_history?_pool_bech32=%s&_epoch_no=%d&select=epoch_no,active_stake,active_stake_pct,block_cnt,delegator_cnt,margin,fixed_cost,saturation_pct,pool_fees,deleg_rewards,member_rewards,epoch_ros",
		poolBech32,
		epoch,
	)
	resp, err := k.get(ctx, path, -1, -1)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"koios /pool_history: status %d body: %s",
			resp.StatusCode,
			resp.Body,
		)
	}
	var items []KoiosPoolHistoryItem
	if err := json.Unmarshal(resp.Body, &items); err != nil {
		return nil, fmt.Errorf("koios /pool_history decode: %w", err)
	}
	if len(items) == 0 {
		return nil, nil
	}
	if len(items) != 1 || items[0].EpochNo != epoch {
		return nil, fmt.Errorf(
			"koios /pool_history: pool %s requested epoch %d, got %d row(s) beginning with epoch %d",
			poolBech32,
			epoch,
			len(items),
			items[0].EpochNo,
		)
	}
	return &items[0], nil
}

// GetAllAccountAddresses returns the bech32 stake address of every account
// Koios knows about, including accounts with zero current stake or that have
// since deregistered — the Koios-side "master list" for #3097's per-account
// address universe, exactly analogous to GetAllHistoricalPoolIDs's role for
// pools. /account_list is Range-paginated the same way /pool_list is.
//
// This alone is not sufficient to build a correct per-epoch address universe
// — see FetchAccountRewardsForEpoch, which unions this with Dingo's own
// committed reward_account_output addresses so a Koios-only-known account
// Dingo never recorded a reward for still gets checked (and a Dingo-only
// account not yet visible to /account_list still gets checked too).
func (k *KoiosClient) GetAllAccountAddresses(
	ctx context.Context,
) ([]string, error) {
	return k.getAllAccountAddresses(ctx, nil)
}

// GetAllAccountAddressesWithProgress is GetAllAccountAddresses with a progress
// line every accountListLogEveryPages pages. The logger is a parameter rather
// than a client field because the same client serves the concurrent chunk
// fetchers, and a field written here would be read by them (dingo #3796).
func (k *KoiosClient) GetAllAccountAddressesWithProgress(
	ctx context.Context,
	logger *slog.Logger,
) ([]string, error) {
	return k.getAllAccountAddresses(ctx, logger)
}

func (k *KoiosClient) getAllAccountAddresses(
	ctx context.Context,
	logger *slog.Logger,
) ([]string, error) {
	type listItem struct {
		StakeAddress string `json:"stake_address"`
	}
	seen := make(map[string]bool)
	var addrs []string
	total := 0
	pages := 0
	for start := 0; ; start += koiosPageSize {
		end := start + koiosPageSize - 1
		resp, err := k.get(
			ctx,
			"/account_list?select=stake_address",
			start,
			end,
		)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK &&
			resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf(
				"koios /account_list: status %d body: %s",
				resp.StatusCode,
				resp.Body,
			)
		}
		var page []listItem
		if err := json.Unmarshal(resp.Body, &page); err != nil {
			return nil, fmt.Errorf("koios /account_list decode: %w", err)
		}
		for _, item := range page {
			if item.StakeAddress == "" {
				continue
			}
			if !seen[item.StakeAddress] {
				seen[item.StakeAddress] = true
				addrs = append(addrs, item.StakeAddress)
			}
		}
		// Preview answers 303k accounts in 304 sequential pages. Without a
		// progress line the whole walk is silent, which is indistinguishable
		// from a stalled fetch (dingo #3796). Emitted after the page is
		// folded in, so a crawl ending on exactly a milestone page still
		// reports it before the loop breaks below.
		pages++
		if logger != nil && pages%accountListLogEveryPages == 0 {
			logger.Info(
				"koiosparity: crawling Koios account list",
				"pages", pages,
				"fetched", len(addrs),
				"total", total,
			)
		}
		if len(page) < koiosPageSize {
			break
		}
		total = parseTotalFromContentRange(resp.Header.Get("Content-Range"))
		if total > 0 && start+len(page) >= total {
			break
		}
	}
	return addrs, nil
}

// accountListLogEveryPages is how many /account_list pages pass between
// progress lines during the universe crawl.
const accountListLogEveryPages = 50

// KoiosAccountRewardHistoryItem is one row from /account_reward_history,
// covering every documented field. PoolIDBech32 is null for reward types with
// no associated pool (treasury/reserves/refund; see CompareAccountEpoch's
// doc comment on which Koios reward types are currently in scope).
//
// /account_rewards (the older endpoint some Koios docs still reference) is
// deprecated; /account_reward_history is the replacement, taking the same
// stake_addresses_with_epoch_no request body shape.
type KoiosAccountRewardHistoryItem struct {
	StakeAddress string `json:"stake_address"`
	EarnedEpoch  uint64 `json:"earned_epoch"`
	// SpendableEpoch is stored for reference (KoiosAccountRewards) but not
	// currently compared against anything in Dingo's schema.
	SpendableEpoch uint64  `json:"spendable_epoch"`
	Amount         string  `json:"amount"`
	Type           string  `json:"type"`
	PoolIDBech32   *string `json:"pool_id_bech32"`
}

// GetAccountRewardHistory fetches Koios reward-history rows for the given
// stake addresses filtered to epoch, via a single POST to
// /account_reward_history. epoch is assumed to filter by the row's
// earned_epoch — consistent with FetchAccountRewardsForEpoch always storing
// the response under the koiosStakeEpoch-derived Koios reporting epoch it
// requested (see check.go's koiosStakeEpoch/ARCHITECTURE.md's Epoch
// alignment section), the same way /pool_history's _epoch_no filter already
// behaves for GetPoolEpochHistory. This assumption could not be verified
// against a live Koios instance in this environment (no network access);
// EarnedEpoch is preserved on every returned item precisely so a future
// caller with live access can cross-check it, and FetchAccountRewardsForEpoch
// stores whatever Koios reports without silently overwriting EarnedEpoch
// with the requested epoch.
//
// stakeAddresses must not exceed koiosAccountChunkSize — chunking is the
// caller's responsibility (FetchAccountRewardsForEpoch), matching this
// package's convention of keeping the low-level client method a single
// request and putting chunking/concurrency in the fetch orchestration layer.
// Returns nil, nil for an empty stakeAddresses slice without making a
// request.
func (k *KoiosClient) GetAccountRewardHistory(
	ctx context.Context,
	stakeAddresses []string,
	epoch uint64,
) ([]KoiosAccountRewardHistoryItem, error) {
	if len(stakeAddresses) == 0 {
		return nil, nil
	}
	payload := struct {
		StakeAddresses []string `json:"_stake_addresses"`
		EpochNo        uint64   `json:"_epoch_no"`
	}{
		StakeAddresses: stakeAddresses,
		EpochNo:        epoch,
	}
	resp, err := k.post(ctx, "/account_reward_history", payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"koios /account_reward_history: status %d body: %s",
			resp.StatusCode,
			resp.Body,
		)
	}
	var items []KoiosAccountRewardHistoryItem
	if err := json.Unmarshal(resp.Body, &items); err != nil {
		return nil, fmt.Errorf("koios /account_reward_history decode: %w", err)
	}
	// dingo #3099: /account_reward_history does not honor the Range header
	// the way GET table-view endpoints (/pool_list, /account_list) do —
	// verified live against preview: repeated requests with different Range
	// values return the same first koiosPageSize-row window rather than
	// paging further, so there is no working way to fetch a "next page" for
	// this endpoint. A response landing at that same row-count ceiling is
	// therefore indistinguishable from a silently truncated one; rather than
	// accept it as a complete, trustworthy answer, this fails hard and
	// permanently so the caller (fetchAccountRewardsForEpoch) aborts instead
	// of committing a reference set that might be missing rows. The
	// resolution is a smaller --account-chunk-size, not a retry — retrying
	// the same chunk would hit the exact same ceiling again.
	if len(items) >= koiosPageSize {
		return nil, fmt.Errorf(
			"%w: koios /account_reward_history returned %d rows (>= the %d-row page ceiling) for a %d-address chunk — this endpoint is not Range-paginated, so the response may be silently truncated; reduce --account-chunk-size and retry",
			ErrKoiosPermanent,
			len(items),
			koiosPageSize,
			len(stakeAddresses),
		)
	}
	return items, nil
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
