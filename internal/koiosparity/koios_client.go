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
	"net"
	"net/http"
	"net/url"
	"sort"
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

	// koios408MaxRetriesDefault/koios408InitialBackoffDefault/
	// koios408MaxBackoffDefault give HTTP 408 ("Request Time-out") its own,
	// much longer retry budget than koiosMaxRetries/koiosRetryBackoff5xx
	// (dingo #4486). 408 means the upstream (or an intermediate gateway --
	// the observed incident's body was the literal
	// "<h1>408 Request Time-out</h1>") took too long to answer *this*
	// request; it says nothing about whether the next one will succeed,
	// unlike the 400/404/422/401/403 class the fallthrough in get()/post()
	// still treats as permanent. The dingo #4486 incident saw Koios degrade
	// for close to an hour; koiosMaxRetries's budget (3 attempts, exhausting
	// in a few minutes even combined with burst-429 cooldowns) gives up long
	// before a real outage like that clears -- and under
	// --koios-parity-strict=true, that premature give-up is what took the
	// node down.
	//
	// The shape is a capped exponential backoff bounded by attempt count,
	// not wall-clock elapsed time, so it stays finite and its progress is
	// observable (each retry is logged -- see get()/post()) instead of one
	// huge, silent sleep. With today's defaults (16 attempts, 15 backoff
	// waits, 20s initial, 5m cap) the wait sequence is 20s, 40s, 80s, 160s,
	// then eleven waits at the 5-minute cap: 20+40+80+160=300s plus
	// 11*300s=3300s is 3600s -- 60 minutes total before giving up, chosen to
	// match the incident's own degraded-window length rather than an
	// arbitrary smaller number.
	//
	// Exhausting the budget returns ErrKoiosPermanent, where an exhausted
	// 5xx budget returns a plain error. That asymmetry is deliberate and is
	// about the layer above: Observer.fetchPoolsIfNeeded/
	// fetchAccountsIfNeeded/fetchParamsWithRetry retry a non-permanent
	// error FetchRetryAttempts times (5 by default) and return a permanent
	// one immediately, so a non-permanent 408 exhaustion would multiply
	// this 60-minute budget into a five-hour stall. ErrKoiosPermanent caps
	// the total wait at one budget. It does not change the eventual
	// outcome: Observer.fail fires FatalFunc in strict mode for either
	// error class once the observer stops retrying.
	//
	// Exposed as KoiosClient fields (koios408MaxRetries/
	// koios408InitialBackoff/koios408MaxBackoff) defaulted from these
	// constants in NewKoiosClient, so tests can shrink the budget instead of
	// paying the real wall-clock cost.
	koios408MaxRetriesDefault     = 16
	koios408InitialBackoffDefault = 20 * time.Second
	koios408MaxBackoffDefault     = 5 * time.Minute

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

	// The following bound the connection-establishment and response-wait
	// phases of a single Koios HTTP attempt independently of the overall
	// koiosRequestTimeout (below), as defense-in-depth rather than a
	// replacement for it. Without them, only two of the four phases below
	// are actually bounded by anything narrower than koiosRequestTimeout:
	// http.DefaultTransport's own defaults already give a dial a 30s budget
	// and a TLS handshake a 10s budget (see net/http.DefaultTransport in the
	// Go standard library), but it sets no ResponseHeaderTimeout at all, so
	// "connection accepted, server writes nothing" is caught today only by
	// the coarse, whole-round-trip koiosRequestTimeout -- which also has to
	// cover dial, TLS, and body reads, so a slow dial/handshake eats into
	// the time actually available to notice a stuck server. Every value
	// here is chosen to be clearly shorter than koiosRequestTimeout so a
	// phase-specific failure attributes cleanly instead of surfacing as an
	// undifferentiated overall timeout. koiosResponseHeaderTimeout in
	// particular must stay below koiosRequestTimeout, or it could never
	// fire before koiosRequestTimeout already had, making it dead weight.
	koiosDialTimeout           = 10 * time.Second
	koiosDialKeepAlive         = 30 * time.Second
	koiosTLSHandshakeTimeout   = 10 * time.Second
	koiosResponseHeaderTimeout = 15 * time.Second
	koiosExpectContinueTimeout = 1 * time.Second

	// koiosIdleConnTimeout bounds how long an idle keep-alive connection
	// stays in the client's pool before Go proactively closes it. Shorter
	// than http.DefaultTransport's 90s default, so that a stale
	// server/CDN-closed keep-alive connection is dropped rather than reused
	// into the write-succeeds-then-read-hangs stall koiosRequestTimeout's
	// own doc comment describes. It narrows the window for that stall
	// rather than closing it, which is why koiosRequestTimeout bounds it
	// too.
	koiosIdleConnTimeout = 30 * time.Second

	// koiosRequestTimeout bounds a single HTTP request/response round
	// trip. Confirmed live against a Koios mirror that individual requests
	// occasionally stalled for almost exactly 60s (this client's prior
	// overall timeout) before an immediate retry succeeded in well under a
	// second -- observed on /tx_info and /pool_history calls, including
	// under CheckStakeDistribution's own bounded concurrency, so this is
	// not purely a sequential-reuse artifact and may also be transient
	// overload on a community-hosted mirror under concurrent load. Every
	// real (non-stalled) call measured during this investigation completed
	// in under 2s, so 20s leaves a wide margin above normal latency while
	// cutting the wall-clock cost of a stall from 60s to 20s per
	// occurrence, and (with koiosMaxRetries=3) the pathological worst case
	// from minutes to well under a minute.
	//
	// Applies to every KoiosClient, not only node-parity's live
	// per-epoch checking that this investigation was run against:
	// observer.go's in-process Observer and fetch.go's koios-parity fetch
	// both construct their client through this same NewKoiosClient, and
	// each one makes is still a single bounded, page-sized Koios API call
	// (koiosPageSize) -- a long-running bulk fetch is many such calls over
	// time, not one call carrying a larger payload, so it has no distinct
	// need for a longer per-request bound.
	// A request that genuinely stalls past 20s is retried as a transport
	// error (get's own doc comment) up to koiosMaxRetries times regardless
	// of caller, so this tightening only shortens how long a stall is
	// tolerated before that retry fires -- it does not lower the ceiling
	// on how long a legitimately slow bulk operation may run in total.
	koiosRequestTimeout = 20 * time.Second
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

// KoiosEpochParamsResp is the Koios /epoch_params response shape for the
// per-epoch protocol parameters (dingo #3931). A wrong stored protocol
// parameter changes what the node accepts, so it is wedge-class in exactly
// the way a wrong validation rule is (#3928) — and nothing else in this
// checker looked at it.
//
// Every numeric field is a json.Number rather than a concrete Go numeric
// type. Koios publishes rationals as decimals, sometimes in exponent form
// (price_step is "7.21e-05"), while Dingo stores them as exact rationals
// ("721/10000000"). Keeping Koios's literal text means the value reaching
// the comparison has been through no float round-trip at all, and
// rationalsEqual reconciles the two forms exactly.
//
// Pointers mark fields Koios returns as null on eras that do not define the
// parameter (everything from price_mem down is null before Alonzo). "" in the
// cached KoiosEpochParams means exactly that "not defined", never zero.
//
// Deliberately not modeled: the Conway governance parameters
// (pvt_*/dvt_*/committee_*/gov_action_*/drep_*/min_fee_ref_script_cost_per_byte),
// and nonce/block_hash/extra_entropy. Both are classified explicitly in
// koiosCoverageMatrix; see CompareEpochProtocolParams for why each is left to
// follow-up work rather than compared unverified.
type KoiosEpochParamsResp struct {
	EpochNo uint64 `json:"epoch_no"`
	Era     string `json:"era"`

	MinFeeA            *json.Number `json:"min_fee_a"`
	MinFeeB            *json.Number `json:"min_fee_b"`
	MaxBlockSize       *json.Number `json:"max_block_size"`
	MaxTxSize          *json.Number `json:"max_tx_size"`
	MaxBhSize          *json.Number `json:"max_bh_size"`
	KeyDeposit         *string      `json:"key_deposit"`
	PoolDeposit        *string      `json:"pool_deposit"`
	MaxEpoch           *json.Number `json:"max_epoch"`
	OptimalPoolCount   *json.Number `json:"optimal_pool_count"`
	Influence          *json.Number `json:"influence"`
	MonetaryExpandRate *json.Number `json:"monetary_expand_rate"`
	TreasuryGrowthRate *json.Number `json:"treasury_growth_rate"`
	Decentralisation   *json.Number `json:"decentralisation"`
	ProtocolMajor      *json.Number `json:"protocol_major"`
	ProtocolMinor      *json.Number `json:"protocol_minor"`
	MinUtxoValue       *string      `json:"min_utxo_value"`
	MinPoolCost        *string      `json:"min_pool_cost"`
	// CostModels is Koios's name-keyed dict of per-language Plutus operation
	// prices ("PlutusV1", "PlutusV2", ...). Kept as a raw map of arrays so
	// the fetch layer can normalise it without committing to a language set.
	CostModels map[string][]int64 `json:"cost_models"`

	PriceMem            *json.Number `json:"price_mem"`
	PriceStep           *json.Number `json:"price_step"`
	MaxTxExMem          *json.Number `json:"max_tx_ex_mem"`
	MaxTxExSteps        *json.Number `json:"max_tx_ex_steps"`
	MaxBlockExMem       *json.Number `json:"max_block_ex_mem"`
	MaxBlockExSteps     *json.Number `json:"max_block_ex_steps"`
	MaxValSize          *json.Number `json:"max_val_size"`
	CollateralPercent   *json.Number `json:"collateral_percent"`
	MaxCollateralInputs *json.Number `json:"max_collateral_inputs"`
	CoinsPerUtxoSize    *string      `json:"coins_per_utxo_size"`
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

	// koios408MaxRetries/koios408InitialBackoff/koios408MaxBackoff configure
	// the extended 408 retry budget -- see the koios408*Default constants'
	// doc comment for the reasoning. NewKoiosClient defaults these to the
	// package constants; tests override them directly to avoid paying the
	// real wall-clock budget.
	koios408MaxRetries     int
	koios408InitialBackoff time.Duration
	koios408MaxBackoff     time.Duration
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
// The override is a parameter rather than a rewrite of this network's entry
// in the process-wide koiosBaseURLs map, because that map is a global every
// concurrently constructed client reads through validateKoiosNetwork. Tests
// point a client at an httptest server through this parameter, which is what
// lets this package's tests run in parallel.
//
// The network is validated before baseURL is consulted, so an unsupported
// network is rejected whether or not an override is supplied.
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
		// The cap is dropped for a custom deployment, not for a custom
		// spelling of the public one. An override naming a koios.rest host is
		// still subject to that host's published window, and dropping the cap
		// there would earn avoidable 429 cooldowns.
		if !isPublicKoiosHost(trimmed) {
			burstLimit = 0
		}
	}
	return &KoiosClient{
		baseURL: base,
		apiKey:  apiKey,
		http: &http.Client{
			Timeout: koiosRequestTimeout,
			Transport: newKoiosTransport(
				koiosDialTimeout,
				koiosDialKeepAlive,
				koiosTLSHandshakeTimeout,
				koiosResponseHeaderTimeout,
				koiosExpectContinueTimeout,
			),
		},
		// Public and Free tiers share the 100/10s burst cap; Pro/Premium are
		// higher, but we don't learn the tier from the key alone, so stay at
		// the Free-safe ceiling for every client on the public host.
		limiter:                newBurstLimiter(burstLimit, koiosBurstWindow),
		koios408MaxRetries:     koios408MaxRetriesDefault,
		koios408InitialBackoff: koios408InitialBackoffDefault,
		koios408MaxBackoff:     koios408MaxBackoffDefault,
	}, nil
}

// newKoiosTransport builds the HTTP transport backing a KoiosClient, with
// explicit, independent timeouts for each connection-establishment phase, in
// addition to (never instead of) the http.Client-level Timeout set alongside
// it in NewKoiosClient.
//
// It starts from http.DefaultTransport.Clone() rather than a bare
// &http.Transport{} so this client keeps DefaultTransport's other tuning
// (HTTP/2 negotiation, proxy-from-environment, idle connection pooling) and
// only overrides the fields this package cares about giving explicit,
// shorter-than-the-client-timeout bounds.
//
// dialTimeout/dialKeepAlive configure the net.Dialer used for
// DialContext -- redundant with DefaultTransport's own dial defaults today,
// but explicit here so this client's dial bound does not silently change if
// a future Go release ever changes DefaultTransport's defaults.
// tlsHandshakeTimeout is likewise explicit for the same reason.
// responseHeaderTimeout is the phase DefaultTransport leaves unbounded: it
// caps how long the transport waits for the server to start sending a
// response after the request is fully written, independently of dial/TLS
// time and independently of the client-level Timeout.
// expectContinueTimeout caps waiting for a "100 Continue" status before
// sending a request body when the client sets the Expect header; this
// client never sets Expect, so it is inert today and included purely for
// completeness against a future caller of this transport that does.
//
// Also sets IdleConnTimeout to koiosIdleConnTimeout, shorter than
// DefaultTransport's 90s default -- see that constant's doc comment for
// why (a stale, server/CDN-closed keep-alive connection reused anyway is
// suspected to contribute to the stalls koiosRequestTimeout's doc comment
// describes).
func newKoiosTransport(
	dialTimeout, dialKeepAlive time.Duration,
	tlsHandshakeTimeout, responseHeaderTimeout, expectContinueTimeout time.Duration,
) *http.Transport {
	// http.DefaultTransport is documented as *http.Transport today, but
	// nothing enforces that at compile time; a comma-ok assertion with a
	// safe fallback (matching mithril/download.go's newDownloadTransport)
	// means a future replacement of the package-level default degrades to a
	// fresh transport with this function's explicit timeouts still applied,
	// instead of panicking.
	var transport *http.Transport
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = base.Clone()
	} else {
		transport = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	transport.DialContext = (&net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: dialKeepAlive,
	}).DialContext
	transport.TLSHandshakeTimeout = tlsHandshakeTimeout
	transport.ResponseHeaderTimeout = responseHeaderTimeout
	transport.ExpectContinueTimeout = expectContinueTimeout
	transport.IdleConnTimeout = koiosIdleConnTimeout
	return transport
}

// ResolvedBaseURL reports the API root this client actually queries, with any
// userinfo removed so it is safe to log or persist.
//
// The resolved host is the identity of the oracle a parity run is judging
// Dingo against, and it is not otherwise visible anywhere: an override that
// silently failed to apply produces a run indistinguishable from one against
// the intended host. Callers record it (Cache.RecordKoiosSource) and log it
// once at startup for exactly that reason.
func (c *KoiosClient) ResolvedBaseURL() string {
	return redactKoiosBaseURL(c.baseURL)
}

// redactKoiosBaseURL strips userinfo from a Koios API root.
//
// validateKoiosBaseURL already rejects a query string and a fragment, so
// userinfo is the only place a credential can survive into a validated base
// URL, and dropping it leaves scheme, host and path — the parts that identify
// the oracle — intact. An unparseable value is reported as a placeholder
// rather than echoed, on the same rule redactURLError follows.
func redactKoiosBaseURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "invalid URL"
	}
	parsed.User = nil
	return parsed.String()
}

// isPublicKoiosHost reports whether a base URL names a koios.rest deployment,
// whose published tier window applies however the URL was spelled -- as a
// built-in default or as an override naming the same host.
func isPublicKoiosHost(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// Unparseable never reaches here (validateKoiosBaseURL runs first),
		// but treat it as public so an unexpected shape keeps the cap rather
		// than losing it.
		return true
	}
	// A single terminal dot is a valid DNS spelling of the same name, so
	// "preview.koios.rest." must not read as a different, non-public host and
	// lose the cap.
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	return host == "koios.rest" || strings.HasSuffix(host, ".koios.rest")
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
		// rawURL is never echoed: an operator can put credentials in it as
		// userinfo or as a credential-shaped query parameter, and a validation
		// error is written to the same log the URI redaction protects.
		return fmt.Errorf("parse koios base URL: %w", redactURLError(err))
	}
	if parsed.Host == "" {
		return errors.New(
			"koios base URL has no host; give the full v1 API root, e.g. https://host/api/v1",
		)
	}
	// get and post build an endpoint by appending a path and its own query to
	// this root. A root that already carries a query or fragment would put the
	// appended path after that delimiter, so the request would silently reach
	// a different endpoint than intended.
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return errors.New(
			"koios base URL must not carry a query string; give the bare v1 API root, e.g. https://host/api/v1",
		)
	}
	// url.URL has no ForceFragment counterpart to ForceQuery, so a bare "#"
	// parses to an empty Fragment and would otherwise be accepted — and the
	// appended endpoint path would still land after the delimiter.
	if parsed.Fragment != "" || strings.Contains(rawURL, "#") {
		return errors.New(
			"koios base URL must not carry a fragment; give the bare v1 API root, e.g. https://host/api/v1",
		)
	}
	switch parsed.Scheme {
	case "https":
		return nil
	case "http":
		if allowInsecureHTTP {
			return nil
		}
		return errors.New(
			"koios base URL uses plain HTTP, which would send the API key in cleartext and leave the reference data tamperable; use https or set allowInsecureHttp for local dev/test",
		)
	default:
		return fmt.Errorf(
			"koios base URL must use http or https, got scheme %q",
			parsed.Scheme,
		)
	}
}

// redactURLError strips the URL from a *url.Error so a parse failure cannot
// carry credentials into a log. url.Parse wraps the offending string in the
// error it returns, which is exactly the value being kept out of logs.
func redactURLError(err error) error {
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		return fmt.Errorf("%s: %w", urlErr.Op, urlErr.Err)
	}
	return errors.New("invalid URL")
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

// koios408BackoffDelay returns the wait before the 408 retry that follows
// attempt (0-based). It doubles from initial and caps at maxDelay -- see
// koios408MaxRetriesDefault's doc comment for why 408 gets this shape
// instead of koiosRetryBackoff5xx's flat multiply-by-attempt.
func koios408BackoffDelay(
	attempt int,
	initial, maxDelay time.Duration,
) time.Duration {
	if attempt <= 0 {
		if initial > maxDelay {
			return maxDelay
		}
		return initial
	}
	// attempt is bounded by koios408MaxRetries, a small caller-controlled
	// constant (16 by default), so this shift cannot overflow even from an
	// initial in the tens-of-seconds range; delay <= 0 defends against it
	// anyway rather than relying on that bound alone.
	delay := initial << uint(attempt)
	if delay <= 0 || delay > maxDelay {
		return maxDelay
	}
	return delay
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
// retrying transport errors, 5xx responses, burst 429s, body-read failures,
// and 408 request timeouts (the last via its own, much longer backoff budget
// -- see koios408MaxRetriesDefault's doc comment). rangeStart/rangeEnd < 0
// means no Range header.
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

	// The two retryable classifications count their own attempts, because a
	// shared counter is not a shared budget: a degraded Koios window mixes
	// 408s with 502/503s, and charging the 408 retries against
	// koiosMaxRetries made the first interleaved 5xx fail the whole request
	// after about a minute -- collapsing the 408 budget this fix exists to
	// provide. otherAttempt is the transport/5xx/429/body-read counter that
	// retryOrFail bounds at koiosMaxRetries; timeoutAttempt is the 408
	// counter bounded at k.koios408MaxRetries. The loop bound is their sum,
	// so exhausting both in one call still cannot fall out of the loop.
	otherAttempt := 0
	timeoutAttempt := 0
	maxAttempts := koiosMaxRetries + k.koios408MaxRetries

	for range maxAttempts {
		if err := k.limiter.wait(ctx); err != nil {
			return nil, err
		}
		resp, doErr := k.http.Do(req.Clone(ctx))
		if doErr != nil {
			if err := retryOrFail(otherAttempt,
				koiosRetryBackoff5xx*time.Duration(otherAttempt+1),
				"koios GET %s: %w", path, doErr,
			); err != nil {
				return nil, err
			}
			otherAttempt++
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
			if err := retryOrFail(otherAttempt,
				koiosRetryBackoff5xx*time.Duration(otherAttempt+1),
				"koios GET %s: read body: %w", path, readErr,
			); err != nil {
				return nil, err
			}
			otherAttempt++
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
			if err := retryOrFail(otherAttempt, retryAfterDelay(resp),
				"koios burst rate-limited after %d retries on %s (Public/Free = %d req/%s; wait ~%s between bursts): %s",
				koiosMaxRetries, path, koiosBurstLimitPublic, koiosBurstWindow, koiosBurstCooldown, bodyStr,
			); err != nil {
				return nil, err
			}
			otherAttempt++
			continue
		}
		if resp.StatusCode >= 500 {
			// 5xx here is Koios's load balancer or backend having a transient
			// hiccup (e.g. 503 "No server is available to handle this
			// request"), not a permanent rejection of the request — retry
			// with backoff like a transport error instead of failing fast.
			if err := retryOrFail(otherAttempt,
				koiosRetryBackoff5xx*time.Duration(otherAttempt+1),
				"koios server error after %d retries on %s: status %d body: %s",
				koiosMaxRetries, path, resp.StatusCode, strings.TrimSpace(string(body)),
			); err != nil {
				return nil, err
			}
			otherAttempt++
			continue
		}
		if resp.StatusCode == http.StatusRequestTimeout {
			// 408 is a gateway/upstream timeout on this specific request
			// (dingo #4486), not a deterministic rejection of it like the
			// 400/404/422/401/403 class below -- give it its own, much
			// longer capped-exponential budget instead of failing fast.
			bodyStr := strings.TrimSpace(string(body))
			if timeoutAttempt < k.koios408MaxRetries-1 {
				delay := koios408BackoffDelay(
					timeoutAttempt,
					k.koios408InitialBackoff,
					k.koios408MaxBackoff,
				)
				// Logged through the package-level default rather than
				// an injected logger: one KoiosClient serves the
				// concurrent chunk fetchers, so it deliberately holds no
				// logger field (dingo #3796). cmd/dingo and
				// cmd/node-parity call slog.SetDefault before building
				// one, and cmd/koios-parity logs through slog.Default()
				// itself, so this lands wherever the binary's own output
				// goes.
				slog.Warn(
					"koiosparity: koios request timed out (408), retrying",
					"path", path,
					"attempt", timeoutAttempt+1,
					"max_retries", k.koios408MaxRetries,
					"backoff", delay,
				)
				if err := waitCtx(ctx, delay); err != nil {
					return nil, err
				}
				timeoutAttempt++
				continue
			}
			return nil, fmt.Errorf(
				"%w: koios GET %s: status 408 request timeout after %d retries: body: %s",
				ErrKoiosPermanent,
				path,
				k.koios408MaxRetries,
				bodyStr,
			)
		}

		// Every other non-2xx status (401/403 auth failures, 400/404/422 bad
		// request or unsupported query, etc. -- 408 is handled above, not
		// here) was never retried above and will never succeed by
		// retrying — mark it permanent so callers stop scheduling further
		// doomed requests instead of treating it as an isolated, retryable
		// blip.
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
	// is bounded by maxAttempts and the last iteration always returns via
	// retryOrFail's fail branch. Guard satisfies nilaway's nil-flow analysis.
	return nil, errors.New("koios: internal: no response after retry loop")
}

// post executes a POST request with a JSON-encoded body against the Koios
// API, retrying transport errors, 5xx responses, burst 429s, body-read
// failures, and 408 request timeouts with exactly the same policy as get()
// (see get()'s doc comment for the retry/classification rationale) — the
// only structural difference is that a POST body must be rebuilt fresh on
// every attempt (a bytes.Reader, once drained by http.Client.Do, cannot be
// replayed the way get()'s bodyless request can via req.Clone).
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

	// See get()'s identical comment for why each classification counts its
	// own attempts and why the loop bound is their sum.
	otherAttempt := 0
	timeoutAttempt := 0
	maxAttempts := koiosMaxRetries + k.koios408MaxRetries

	for range maxAttempts {
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
			if err := retryOrFail(otherAttempt,
				koiosRetryBackoff5xx*time.Duration(otherAttempt+1),
				"koios POST %s: %w", path, doErr,
			); err != nil {
				return nil, err
			}
			otherAttempt++
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
			if err := retryOrFail(otherAttempt,
				koiosRetryBackoff5xx*time.Duration(otherAttempt+1),
				"koios POST %s: read body: %w", path, readErr,
			); err != nil {
				return nil, err
			}
			otherAttempt++
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
			if err := retryOrFail(otherAttempt, retryAfterDelay(resp),
				"koios burst rate-limited after %d retries on %s (Public/Free = %d req/%s; wait ~%s between bursts): %s",
				koiosMaxRetries, path, koiosBurstLimitPublic, koiosBurstWindow, koiosBurstCooldown, bodyStr,
			); err != nil {
				return nil, err
			}
			otherAttempt++
			continue
		}
		if resp.StatusCode >= 500 {
			if err := retryOrFail(otherAttempt,
				koiosRetryBackoff5xx*time.Duration(otherAttempt+1),
				"koios server error after %d retries on %s: status %d body: %s",
				koiosMaxRetries, path, resp.StatusCode, strings.TrimSpace(string(body)),
			); err != nil {
				return nil, err
			}
			otherAttempt++
			continue
		}
		if resp.StatusCode == http.StatusRequestTimeout {
			// See get()'s identical branch for the classification rationale.
			bodyStr := strings.TrimSpace(string(body))
			if timeoutAttempt < k.koios408MaxRetries-1 {
				delay := koios408BackoffDelay(
					timeoutAttempt,
					k.koios408InitialBackoff,
					k.koios408MaxBackoff,
				)
				// See get()'s identical branch for why this logs through
				// the package-level default.
				slog.Warn(
					"koiosparity: koios request timed out (408), retrying",
					"path", path,
					"attempt", timeoutAttempt+1,
					"max_retries", k.koios408MaxRetries,
					"backoff", delay,
				)
				if err := waitCtx(ctx, delay); err != nil {
					return nil, err
				}
				timeoutAttempt++
				continue
			}
			return nil, fmt.Errorf(
				"%w: koios POST %s: status 408 request timeout after %d retries: body: %s",
				ErrKoiosPermanent,
				path,
				k.koios408MaxRetries,
				bodyStr,
			)
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

// GetEpochParams fetches the protocol parameters in force for a specific
// epoch. Mirrors GetTotals' single-row contract exactly: a response that is
// empty, has more than one row, or names a different epoch is an error rather
// than something silently accepted, so a cached parameter set can never
// belong to the wrong epoch.
func (k *KoiosClient) GetEpochParams(
	ctx context.Context,
	epoch uint64,
) (*KoiosEpochParamsResp, error) {
	path := fmt.Sprintf("/epoch_params?_epoch_no=%d", epoch)
	resp, err := k.get(ctx, path, -1, -1)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"koios /epoch_params: status %d body: %s",
			resp.StatusCode,
			resp.Body,
		)
	}
	var items []KoiosEpochParamsResp
	if err := json.Unmarshal(resp.Body, &items); err != nil {
		return nil, fmt.Errorf("koios /epoch_params decode: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf(
			"koios /epoch_params: no data for epoch %d",
			epoch,
		)
	}
	if len(items) != 1 || items[0].EpochNo != epoch {
		return nil, fmt.Errorf(
			"koios /epoch_params: requested epoch %d, got %d row(s) beginning with epoch %d",
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
// doc comment on which Koios reward types are currently in scope). For
// in-scope rows it identifies the pool contribution used during aggregation.
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

// KoiosTxInfoBatchSize bounds how many transaction hashes go into a single
// /tx_info request: each 64-char hex hash plus JSON quoting/comma overhead is
// ~70 bytes, so this many hashes stays comfortably under Koios's request-body
// size cap (confirmed live: an unbatched request for a full block's worth of
// hashes was rejected outright with a plain-text "Payload too large" body
// that fails JSON decoding, rather than any structured error).
//
// Exported so a caller accumulating hashes across multiple blocks before
// calling GetTxInfos (e.g. nodeparity's from-genesis UTxO reconstruction,
// blinklabs-io/dingo#1900) can flush at the same size GetTxInfos itself
// batches at, rather than duplicating this number.
const KoiosTxInfoBatchSize = 40

// KoiosTxInfoUtxoRef is one entry in a KoiosTxInfoItem's Inputs: just enough
// to identify a UTxO ref ("<tx_hash>#<tx_index>"), not its content -- an
// input is being consumed, so only its identity is ever needed to remove it
// from a running reconstruction, never what it contained.
type KoiosTxInfoUtxoRef struct {
	TxHash  string `json:"tx_hash"`
	TxIndex int    `json:"tx_index"`
}

// KoiosTxInfoAsset is one multi-asset entry on a KoiosTxInfoOutput.
// PolicyID and AssetName are both hex, matching gouroboros'
// Blake2b224.String()/hex.EncodeToString conventions exactly (Koios's own
// documented examples are lowercase hex the same way), which is what lets
// CanonicalKoiosUTxOEntry produce a string directly comparable to
// nodeparity's canonicalUTxOEntry without any re-encoding.
type KoiosTxInfoAsset struct {
	PolicyID  string `json:"policy_id"`
	AssetName string `json:"asset_name"`
	Quantity  string `json:"quantity"`
}

// KoiosTxInfoAssetList is /tx_info's asset_list, which Koios serialises two
// different ways for the same content: a JSON array on inputs, outputs and
// collateral_inputs, but a JSON *string* on collateral_output.
//
// The string form is NOT JSON-inside-a-string. It is cardano-ledger's Haskell
// `Show` rendering of the output's MultiAsset value, passed through verbatim
// (confirmed against the live preview API -- see
// TestAssetListDecodesLedgerShowMultiAsset for captured real responses):
//
//	[]                                      -- no assets
//	[(PolicyID {policyID = ScriptHash "65a9..."},[("494e4459",32200000000000)])]
//	[(PolicyID {policyID = ScriptHash "09e5..."},[("474f565f4e4654",1)]),(PolicyID {policyID = ScriptHash "65a9..."},[("494e4459",32200000000000)])]
//
// The empty case, "[]", happens to also be valid JSON, which is why the
// original string-form support (dingo #1900, the phase-2-invalid collateral
// fix) looked correct: every transaction whose collateral return carried no
// tokens decoded fine. A collateral return that actually carries tokens --
// the normal case for a phase-2-invalid Plutus transaction, whose whole
// purpose is returning the collateral inputs' assets -- renders as the form
// above, and feeding that to json.Unmarshal fails on its first "(" with
// "invalid character '(' looking for beginning of value".
//
// That failure was not contained to the one field: it failed the enclosing
// transaction, which failed its whole 40-hash /tx_info chunk, which tainted
// the epoch and skipped its UTxO comparison entirely -- measured at 5 of 10
// consecutive chunks on real preview data, matching the ~50% of epochs that
// were losing UTxO validation.
//
// Policy IDs, asset names and quantities in the Show form use exactly the
// same encodings as the JSON array form (lowercase hex, hex, decimal), which
// is what lets both branches produce the same KoiosTxInfoAsset and cross-check
// against each other: a transaction's collateral_inputs (array form) and its
// collateral_output (Show form) report the same tokens identically.
//
// Marshalling is the plain array form, so a cached row written from this
// struct always reads back through the array branch.
type KoiosTxInfoAssetList []KoiosTxInfoAsset

func (a *KoiosTxInfoAssetList) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		*a = nil
		return nil
	}

	// Array form: inputs, outputs, collateral_inputs, and anything this
	// struct marshalled itself (including every cached row).
	if trimmed[0] != '"' {
		var items []KoiosTxInfoAsset
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return fmt.Errorf("asset_list array form: %w", err)
		}
		*a = items
		return nil
	}

	var inner string
	if err := json.Unmarshal(trimmed, &inner); err != nil {
		return fmt.Errorf("asset_list string form: %w", err)
	}
	inner = strings.TrimSpace(inner)
	if inner == "" {
		*a = nil
		return nil
	}

	// Try JSON first: it covers the "[]" empty case, and keeps working if
	// Koios ever switches the string form to real embedded JSON.
	var items []KoiosTxInfoAsset
	if err := json.Unmarshal([]byte(inner), &items); err == nil {
		*a = items
		return nil
	}

	parsed, err := parseLedgerShowMultiAsset(inner)
	if err != nil {
		return fmt.Errorf(
			"asset_list string form is neither JSON nor a cardano-ledger "+
				"MultiAsset rendering: %w: %s",
			err,
			koiosBodyExcerpt([]byte(inner)),
		)
	}
	*a = parsed
	return nil
}

// parseLedgerShowMultiAsset parses cardano-ledger's Haskell `Show` rendering
// of a MultiAsset -- the form Koios emits for collateral_output.asset_list.
// See KoiosTxInfoAssetList's doc comment for real examples.
//
// The grammar accepted is exactly:
//
//	list   := "[" [ entry { "," entry } ] "]"
//	entry  := "(" policy "," assets ")"
//	policy := "PolicyID" "{" "policyID" "=" "ScriptHash" quoted "}"
//	assets := "[" [ asset { "," asset } ] "]"
//	asset  := "(" quoted "," integer ")"
//
// It is deliberately strict rather than a lenient regex scrape. A lenient
// parser that silently skipped an entry it did not recognise would drop
// tokens from a reconstructed UTxO, and the UTxO comparison this feeds
// (nodeparity's from-genesis check) would then report a content mismatch
// that is this parser's fault rather than Dingo's -- the exact class of false
// result the check exists to detect. Failing loudly, with the offending text
// in the error, keeps a future ledger Show change a one-line diagnosis
// instead of a phantom ledger bug.
func parseLedgerShowMultiAsset(s string) ([]KoiosTxInfoAsset, error) {
	sc := &showScanner{s: s}
	if err := sc.expect("["); err != nil {
		return nil, err
	}
	var out []KoiosTxInfoAsset
	if !sc.accept("]") {
		for {
			policy, assets, err := sc.entry()
			if err != nil {
				return nil, err
			}
			for _, as := range assets {
				as.PolicyID = policy
				out = append(out, as)
			}
			if sc.accept(",") {
				continue
			}
			if err := sc.expect("]"); err != nil {
				return nil, err
			}
			break
		}
	}
	sc.skipSpace()
	if sc.pos != len(sc.s) {
		return nil, fmt.Errorf("trailing input at offset %d", sc.pos)
	}
	return out, nil
}

// showScanner is a tiny cursor over a Haskell `Show` rendering. It is not a
// general Haskell parser -- only the MultiAsset shape above is accepted.
type showScanner struct {
	s   string
	pos int
}

func (sc *showScanner) skipSpace() {
	for sc.pos < len(sc.s) {
		switch sc.s[sc.pos] {
		case ' ', '\t', '\n', '\r':
			sc.pos++
		default:
			return
		}
	}
}

func (sc *showScanner) accept(lit string) bool {
	sc.skipSpace()
	if strings.HasPrefix(sc.s[sc.pos:], lit) {
		sc.pos += len(lit)
		return true
	}
	return false
}

func (sc *showScanner) expect(lit string) error {
	if sc.accept(lit) {
		return nil
	}
	return fmt.Errorf("expected %q at offset %d", lit, sc.pos)
}

// quoted reads a Haskell string literal. Policy IDs and asset names are hex,
// so no escape beyond the pass-through handled here has ever been observed,
// but \" and \\ are honoured so a name that did need them cannot truncate
// the scan and silently drop the rest of the list.
func (sc *showScanner) quoted() (string, error) {
	sc.skipSpace()
	if sc.pos >= len(sc.s) || sc.s[sc.pos] != '"' {
		return "", fmt.Errorf("expected a quoted string at offset %d", sc.pos)
	}
	sc.pos++
	var b strings.Builder
	for sc.pos < len(sc.s) {
		switch c := sc.s[sc.pos]; c {
		case '\\':
			if sc.pos+1 >= len(sc.s) {
				return "", fmt.Errorf(
					"dangling escape at offset %d", sc.pos,
				)
			}
			b.WriteByte(sc.s[sc.pos+1])
			sc.pos += 2
		case '"':
			sc.pos++
			return b.String(), nil
		default:
			b.WriteByte(c)
			sc.pos++
		}
	}
	return "", fmt.Errorf("unterminated quoted string at offset %d", sc.pos)
}

func (sc *showScanner) integer() (string, error) {
	sc.skipSpace()
	start := sc.pos
	if sc.pos < len(sc.s) && sc.s[sc.pos] == '-' {
		sc.pos++
	}
	for sc.pos < len(sc.s) && sc.s[sc.pos] >= '0' && sc.s[sc.pos] <= '9' {
		sc.pos++
	}
	if sc.pos == start || (sc.pos == start+1 && sc.s[start] == '-') {
		return "", fmt.Errorf("expected an integer at offset %d", start)
	}
	return sc.s[start:sc.pos], nil
}

// entry parses one "(PolicyID {...},[(name,qty),...])" pair, returning the
// policy ID and its assets with PolicyID left for the caller to fill in.
func (sc *showScanner) entry() (string, []KoiosTxInfoAsset, error) {
	for _, lit := range []string{
		"(", "PolicyID", "{", "policyID", "=", "ScriptHash",
	} {
		if err := sc.expect(lit); err != nil {
			return "", nil, err
		}
	}
	policy, err := sc.quoted()
	if err != nil {
		return "", nil, err
	}
	for _, lit := range []string{"}", ",", "["} {
		if err := sc.expect(lit); err != nil {
			return "", nil, err
		}
	}
	var assets []KoiosTxInfoAsset
	if !sc.accept("]") {
		for {
			if err := sc.expect("("); err != nil {
				return "", nil, err
			}
			name, err := sc.quoted()
			if err != nil {
				return "", nil, err
			}
			if err := sc.expect(","); err != nil {
				return "", nil, err
			}
			qty, err := sc.integer()
			if err != nil {
				return "", nil, err
			}
			if err := sc.expect(")"); err != nil {
				return "", nil, err
			}
			assets = append(assets, KoiosTxInfoAsset{
				AssetName: name,
				Quantity:  qty,
			})
			if sc.accept(",") {
				continue
			}
			if err := sc.expect("]"); err != nil {
				return "", nil, err
			}
			break
		}
	}
	if err := sc.expect(")"); err != nil {
		return "", nil, err
	}
	return policy, assets, nil
}

// koiosBodyExcerpt renders a short, single-line, printable prefix of some
// response text for an error message. A malformed field can be long, so the
// whole value is useless in a log line -- but its first few characters are
// exactly what identifies the form it arrived in, which is the one thing the
// bare "invalid character '(' looking for beginning of value" this replaced
// never told us.
func koiosBodyExcerpt(body []byte) string {
	const maxExcerpt = 200
	s := strings.TrimSpace(string(body))
	if s == "" {
		return "<empty>"
	}
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxExcerpt {
		return s[:maxExcerpt] + "..."
	}
	return s
}

// KoiosTxInfoInlineDatum is the non-null shape of a KoiosTxInfoOutput's
// InlineDatum -- only its presence matters here (to distinguish the
// "inline" and "hash-only" datum forms), not its content.
type KoiosTxInfoInlineDatum struct {
	Bytes string `json:"bytes"`
}

// KoiosTxInfoReferenceScript is the non-null shape of a KoiosTxInfoOutput's
// ReferenceScript.
type KoiosTxInfoReferenceScript struct {
	Hash string `json:"hash"`
}

// KoiosTxInfoOutput is one output from a KoiosTxInfoItem's Outputs, with
// enough content to build the same canonical encoding
// nodeparity.canonicalUTxOEntry builds from a live LocalStateQuery
// GetUTxOWhole result: address, ADA value, multi-asset tokens, datum
// presence/form, and reference script hash.
type KoiosTxInfoOutput struct {
	TxHash      string `json:"tx_hash"`
	TxIndex     int    `json:"tx_index"`
	PaymentAddr struct {
		Bech32 string `json:"bech32"`
	} `json:"payment_addr"`
	// Value is the ADA-only amount (lovelace, as a plain decimal string,
	// e.g. "157832856") -- multi-asset tokens are reported separately in
	// AssetList, mirroring gouroboros' own TransactionOutput.Amount()
	// (ADA only) versus .Assets() (everything else) split.
	Value string `json:"value"`
	// DatumHash is non-nil whenever the output carries ANY datum, hash-only
	// or inline -- mirroring gouroboros' TransactionOutput.DatumHash(),
	// which is likewise set for both forms (only .Datum() itself
	// distinguishes them, matching InlineDatum here).
	DatumHash       *string                     `json:"datum_hash"`
	InlineDatum     *KoiosTxInfoInlineDatum     `json:"inline_datum"`
	ReferenceScript *KoiosTxInfoReferenceScript `json:"reference_script"`
	AssetList       KoiosTxInfoAssetList        `json:"asset_list"`
}

// KoiosTxInfoPlutusContract is one entry in a KoiosTxInfoItem's
// PlutusContracts, decoded only far enough to read the phase-2 validation
// verdict.
//
// /tx_info reports no top-level validity flag (confirmed against the live
// API: tx_info objects carry no valid_contract key at all). The ledger's
// single per-transaction is_valid flag is denormalised onto every one of
// that transaction's Plutus contract rows instead, so any entry answers for
// the whole transaction.
type KoiosTxInfoPlutusContract struct {
	// ValidContract is a pointer so that a null or absent flag is "no
	// verdict reported" rather than a silent "invalid": only an explicit
	// false marks a transaction phase-2-invalid.
	ValidContract *bool `json:"valid_contract"`
}

// KoiosTxInfoItem is one transaction from /tx_info: which refs it consumes
// (Inputs) and which outputs it creates, with enough content on each output
// for full content-based UTxO comparison (see CanonicalKoiosUTxOEntry), not
// merely existence. Requesting with _inputs/_assets/_scripts:true (done by
// GetTxInfos) is required for Inputs, AssetList, and the datum/reference-
// script fields to be populated at all -- Koios omits all of them by
// default.
type KoiosTxInfoItem struct {
	TxHash  string               `json:"tx_hash"`
	Inputs  []KoiosTxInfoUtxoRef `json:"inputs"`
	Outputs []KoiosTxInfoOutput  `json:"outputs"`

	// CollateralInputs, CollateralOutput and PlutusContracts exist for the
	// phase-2-invalid case only: Inputs/Outputs describe what the
	// transaction body asked for, which the ledger does not apply when
	// phase-2 validation fails. See Consumed and Produced.
	CollateralInputs []KoiosTxInfoUtxoRef        `json:"collateral_inputs"`
	CollateralOutput *KoiosTxInfoOutput          `json:"collateral_output"`
	PlutusContracts  []KoiosTxInfoPlutusContract `json:"plutus_contracts"`
}

// IsValid reports whether the ledger applied this transaction's body, i.e.
// whether phase-2 script validation passed. A transaction with no Plutus
// contracts is always valid -- phase-2 validation only applies to script
// transactions -- and so is one whose contracts report no verdict (see
// KoiosTxInfoPlutusContract.ValidContract).
func (i KoiosTxInfoItem) IsValid() bool {
	for _, pc := range i.PlutusContracts {
		if pc.ValidContract != nil && !*pc.ValidContract {
			return false
		}
	}
	return true
}

// Consumed reports the refs this transaction actually removed from the UTxO
// set, mirroring gouroboros' Transaction.Consumed() exactly: the body inputs
// for a valid transaction, the collateral inputs for a phase-2-invalid one.
func (i KoiosTxInfoItem) Consumed() []KoiosTxInfoUtxoRef {
	if i.IsValid() {
		return i.Inputs
	}
	return i.CollateralInputs
}

// Produced reports the outputs this transaction actually added to the UTxO
// set, mirroring gouroboros' Transaction.Produced() exactly: the body
// outputs for a valid transaction, and for a phase-2-invalid one the single
// collateral return if it declared one, nothing otherwise. Koios indexes the
// collateral return at len(outputs), the same index gouroboros assigns it.
func (i KoiosTxInfoItem) Produced() []KoiosTxInfoOutput {
	if i.IsValid() {
		return i.Outputs
	}
	if i.CollateralOutput == nil {
		return nil
	}
	return []KoiosTxInfoOutput{*i.CollateralOutput}
}

// CanonicalKoiosUTxOEntry builds a deterministic string encoding of out,
// directly comparable (byte-for-byte, when the two sides genuinely agree)
// to nodeparity's own canonicalUTxOEntry -- same field order, same "|"
// separators, same asset sort order (policy then asset name, both already
// hex so a plain string sort matches gouroboros' own raw-byte
// bytes.Compare sort), same datum "form:hash" encoding. Kept in this
// package (not nodeparity) since it depends only on Koios's own response
// shape, not on gouroboros.
func CanonicalKoiosUTxOEntry(out KoiosTxInfoOutput) string {
	var sb strings.Builder
	sb.WriteString(out.PaymentAddr.Bech32)
	sb.WriteString("|")
	sb.WriteString(out.Value)

	if len(out.AssetList) > 0 {
		assets := make([]KoiosTxInfoAsset, len(out.AssetList))
		copy(assets, out.AssetList)
		sort.Slice(assets, func(i, j int) bool {
			if assets[i].PolicyID != assets[j].PolicyID {
				return assets[i].PolicyID < assets[j].PolicyID
			}
			return assets[i].AssetName < assets[j].AssetName
		})
		for _, a := range assets {
			fmt.Fprintf(&sb, "|%s.%s=%s", a.PolicyID, a.AssetName, a.Quantity)
		}
	}

	if out.DatumHash != nil {
		form := "hash"
		if out.InlineDatum != nil {
			form = "inline"
		}
		fmt.Fprintf(&sb, "|datum=%s:%s", form, *out.DatumHash)
	}
	if out.ReferenceScript != nil {
		fmt.Fprintf(&sb, "|scriptref=%s", out.ReferenceScript.Hash)
	}
	return sb.String()
}

// GetTxInfos fetches input refs and full output content for every one of
// txHashes, batched to stay under Koios's request-size cap.
//
// Unlike GetPoolEpochHistory's "missing means missing" contract (a
// legitimate outcome there -- a pool with no snapshot row yet), a
// transaction hash given to GetTxInfos is never optional: the caller
// (nodeparity's from-genesis UTxO reconstruction, blinklabs-io/dingo#1900)
// asked for it because a block it already trusts contains that exact
// transaction, so Koios omitting it from the response means either
// transient incompleteness or that this specific hash isn't indexed yet --
// applying only the hashes that did come back would silently and
// permanently lose that transaction's spends/creates from the running
// reconstruction. Requires exactly one result per requested hash --
// erroring, not silently dropping, on any that are missing or duplicated --
// and returns them in request order (not Koios's response order) so a
// caller applying dependent transactions (a UTxO created by one hash and
// spent by a later one in the same request) does so in the same order the
// chain itself does.
func (k *KoiosClient) GetTxInfos(
	ctx context.Context,
	txHashes []string,
) ([]KoiosTxInfoItem, error) {
	byHash := make(map[string]KoiosTxInfoItem, len(txHashes))
	for start := 0; start < len(txHashes); start += KoiosTxInfoBatchSize {
		end := min(start+KoiosTxInfoBatchSize, len(txHashes))
		payload := struct {
			TxHashes []string `json:"_tx_hashes"`
			Inputs   bool     `json:"_inputs"`
			Assets   bool     `json:"_assets"`
			Scripts  bool     `json:"_scripts"`
		}{
			TxHashes: txHashes[start:end],
			Inputs:   true,
			Assets:   true,
			Scripts:  true,
		}
		resp, err := k.post(ctx, "/tx_info", payload)
		if err != nil {
			return nil, fmt.Errorf("tx_info batch [%d:%d]: %w", start, end, err)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf(
				"koios /tx_info: status %d body: %s",
				resp.StatusCode,
				resp.Body,
			)
		}
		var items []KoiosTxInfoItem
		if err := json.Unmarshal(resp.Body, &items); err != nil {
			return nil, describeTxInfoDecodeFailure(resp.Body, err)
		}
		for _, item := range items {
			if _, dup := byHash[item.TxHash]; dup {
				return nil, fmt.Errorf(
					"koios /tx_info: duplicate result for tx hash %s",
					item.TxHash,
				)
			}
			byHash[item.TxHash] = item
		}
	}

	all := make([]KoiosTxInfoItem, len(txHashes))
	for i, hash := range txHashes {
		item, ok := byHash[hash]
		if !ok {
			return nil, fmt.Errorf(
				"koios /tx_info: no result for requested tx hash %s",
				hash,
			)
		}
		all[i] = item
	}
	return all, nil
}

// describeTxInfoDecodeFailure turns a whole-response /tx_info decode failure
// into an error that names the offending transaction and preserves the
// underlying cause, by re-decoding the response one transaction at a time.
//
// This is diagnosis, not recovery: the returned error still fails the entire
// chunk, exactly as before. That is deliberate. GetTxInfos' contract is that
// every requested hash comes back, because nodeparity's caller applies these
// to a running UTxO reconstruction -- dropping the one transaction that
// failed to decode would silently lose its spends and creates, and the
// reconstruction would then differ from Dingo for a reason that has nothing
// to do with Dingo. Failing the chunk keeps the caller's existing
// taint-and-re-baseline path in charge, so the epoch is honestly reported as
// "utxo check did not run" instead of being compared against a corrupted set.
//
// What was wrong before was only the blast radius of the *message*: one
// unparseable field in one transaction surfaced as a bare
// "koios /tx_info decode: invalid character '(' ..." with no field, no
// transaction and no chunk identity, which is what made the underlying
// collateral_output.asset_list bug (see KoiosTxInfoAssetList) expensive to
// find.
//
// Runs only on the failure path, so the extra decode costs nothing in the
// normal case.
func describeTxInfoDecodeFailure(body []byte, decodeErr error) error {
	var rawItems []json.RawMessage
	if err := json.Unmarshal(body, &rawItems); err != nil {
		// The response isn't even an array -- report the original error
		// plus what actually arrived.
		return fmt.Errorf(
			"koios /tx_info decode: %w: response was not a JSON array: %s",
			decodeErr,
			koiosBodyExcerpt(body),
		)
	}
	for i, raw := range rawItems {
		var one KoiosTxInfoItem
		if err := json.Unmarshal(raw, &one); err == nil {
			continue
		} else {
			var probe struct {
				TxHash string `json:"tx_hash"`
			}
			hash := "<unidentified>"
			if perr := json.Unmarshal(raw, &probe); perr == nil &&
				probe.TxHash != "" {
				hash = probe.TxHash
			}
			return fmt.Errorf(
				"koios /tx_info decode: transaction %s (response index %d): %w",
				hash,
				i,
				err,
			)
		}
	}
	// Every item decodes alone but the array did not: report the original.
	return fmt.Errorf("koios /tx_info decode: %w", decodeErr)
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
