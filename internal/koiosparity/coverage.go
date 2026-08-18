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

package koiosparity

// CoverageClass describes how one Koios field participates in epoch/pool
// parity. A PASS applies only to exact-match and derived-match entries.
type CoverageClass string

const (
	CoverageExactMatch                CoverageClass = "exact-match"
	CoverageDerivedMatch              CoverageClass = "derived-match"
	CoverageIntentionallyIncomparable CoverageClass = "intentionally-incomparable"
	CoverageUnsupported               CoverageClass = "unsupported"
)

// KoiosFieldCoverage documents one field in the Koios endpoints used by the
// epoch/pool checker. Account reward fields are deliberately out of scope
// until #3097 and #3099 are implemented.
type KoiosFieldCoverage struct {
	Endpoint   string        `json:"endpoint"`
	Field      string        `json:"field"`
	Class      CoverageClass `json:"class"`
	DingoField string        `json:"dingo_field,omitempty"`
	Reason     string        `json:"reason"`
}

// KoiosCoverageMatrix returns the complete field contract for the Koios
// endpoints consumed by epoch/pool parity. Return a copy so report callers
// cannot mutate the package contract.
func KoiosCoverageMatrix() []KoiosFieldCoverage {
	return append([]KoiosFieldCoverage(nil), koiosCoverageMatrix...)
}

var koiosCoverageMatrix = []KoiosFieldCoverage{
	// /tip and pool discovery control the reference range and pool universe.
	{Endpoint: "/tip", Field: "epoch_no", Class: CoverageDerivedMatch, DingoField: "closed epoch range", Reason: "tip minus one selects safely closed epochs"},
	{Endpoint: "/pool_list", Field: "pool_id_bech32", Class: CoverageDerivedMatch, DingoField: "reward_pool_input.pool_key_hash", Reason: "bech32 IDs are decoded and compared as complete set membership"},
	{Endpoint: "/pool_updates", Field: "pool_id_bech32", Class: CoverageDerivedMatch, DingoField: "pool history request universe", Reason: "identifies the pool whose first active epoch bounds history requests"},
	{Endpoint: "/pool_updates", Field: "active_epoch_no", Class: CoverageDerivedMatch, DingoField: "pool history request lower bound", Reason: "minimum update epoch avoids requests before a pool existed"},

	// /epoch_info.
	{Endpoint: "/epoch_info", Field: "epoch_no", Class: CoverageExactMatch, DingoField: "reporting epoch", Reason: "response identity must equal the requested Koios epoch K"},
	{Endpoint: "/epoch_info", Field: "era", Class: CoverageUnsupported, Reason: "Dingo has no persisted per-epoch era aggregate"},
	{Endpoint: "/epoch_info", Field: "out_sum", Class: CoverageUnsupported, Reason: "Dingo has no persisted per-epoch transaction output sum"},
	{Endpoint: "/epoch_info", Field: "fees", Class: CoverageUnsupported, Reason: "raw transaction fees differ from Dingo's boundary fee-pot balance"},
	{Endpoint: "/epoch_info", Field: "tx_count", Class: CoverageUnsupported, Reason: "Dingo has no persisted per-epoch transaction count aggregate"},
	{Endpoint: "/epoch_info", Field: "blk_count", Class: CoverageUnsupported, Reason: "Dingo has no persisted network-wide per-epoch block count aggregate"},
	{Endpoint: "/epoch_info", Field: "start_time", Class: CoverageUnsupported, Reason: "Dingo has no persisted epoch wall-clock range aggregate"},
	{Endpoint: "/epoch_info", Field: "end_time", Class: CoverageDerivedMatch, DingoField: "closed/reference-lag state", Reason: "establishes closure and the configured grace window; it is not a value-parity assertion"},
	{Endpoint: "/epoch_info", Field: "first_block_time", Class: CoverageUnsupported, Reason: "Dingo has no persisted per-epoch first-block time aggregate"},
	{Endpoint: "/epoch_info", Field: "last_block_time", Class: CoverageUnsupported, Reason: "Dingo has no persisted per-epoch last-block time aggregate"},
	{Endpoint: "/epoch_info", Field: "active_stake", Class: CoverageDerivedMatch, DingoField: "epoch_summary.total_active_stake at K-1", Reason: "exact lovelace equality after the established stake-epoch alignment"},
	{Endpoint: "/epoch_info", Field: "total_rewards", Class: CoverageUnsupported, Reason: "Dingo has no matching raw per-performance-epoch reward aggregate"},
	{Endpoint: "/epoch_info", Field: "avg_blk_reward", Class: CoverageUnsupported, Reason: "Dingo has no persisted average block reward aggregate"},
	{Endpoint: "/epoch_info", Field: "pool_cnt", Class: CoverageUnsupported, Reason: "Koios preview/preprod do not return this documented field"},
	{Endpoint: "/epoch_info", Field: "delegator_cnt", Class: CoverageUnsupported, Reason: "Koios preview/preprod do not return this documented field"},

	// /totals.
	{Endpoint: "/totals", Field: "epoch_no", Class: CoverageExactMatch, DingoField: "reporting epoch", Reason: "response identity must equal the requested epoch"},
	{Endpoint: "/totals", Field: "circulation", Class: CoverageUnsupported, Reason: "requires a network-wide live UTxO-set aggregate"},
	{Endpoint: "/totals", Field: "treasury", Class: CoverageExactMatch, DingoField: "reward_ada_pots.treasury", Reason: "point-in-time boundary pot balance"},
	{Endpoint: "/totals", Field: "reward", Class: CoverageIntentionallyIncomparable, Reason: "Koios exposes a lagged cumulative accumulator; Dingo stores a per-epoch reward flow"},
	{Endpoint: "/totals", Field: "supply", Class: CoverageUnsupported, Reason: "requires a network-wide live UTxO-set aggregate"},
	{Endpoint: "/totals", Field: "reserves", Class: CoverageExactMatch, DingoField: "reward_ada_pots.reserves", Reason: "point-in-time boundary pot balance"},
	{Endpoint: "/totals", Field: "fees", Class: CoverageExactMatch, DingoField: "reward_ada_pots.fees", Reason: "point-in-time boundary fee-pot balance"},
	{Endpoint: "/totals", Field: "deposits_stake", Class: CoverageUnsupported, Reason: "Dingo has no persisted stake-deposit pot aggregate"},
	{Endpoint: "/totals", Field: "deposits_drep", Class: CoverageUnsupported, Reason: "Dingo has no persisted DRep-deposit pot aggregate"},
	{Endpoint: "/totals", Field: "deposits_proposal", Class: CoverageUnsupported, Reason: "Dingo has no persisted proposal-deposit pot aggregate"},
	{Endpoint: "/totals", Field: "treasury_donation", Class: CoverageUnsupported, Reason: "Dingo has no matching persisted cumulative governance aggregate"},
	{Endpoint: "/totals", Field: "treasury_withdrawal", Class: CoverageUnsupported, Reason: "Dingo has no matching persisted cumulative governance aggregate"},
	{Endpoint: "/totals", Field: "reserves_withdrawal", Class: CoverageUnsupported, Reason: "Dingo has no matching persisted cumulative reserve-withdrawal aggregate"},

	// /pool_history. pool_id_bech32 is supplied as a request parameter and
	// excluded from the projection, but remains part of the parity identity.
	{Endpoint: "/pool_history", Field: "pool_id_bech32", Class: CoverageDerivedMatch, DingoField: "reward_pool_input.pool_key_hash", Reason: "requested bech32 ID is decoded to Dingo's pool key hash"},
	{Endpoint: "/pool_history", Field: "epoch_no", Class: CoverageExactMatch, DingoField: "reporting epoch", Reason: "response identity must equal the requested Koios epoch K"},
	{Endpoint: "/pool_history", Field: "active_stake", Class: CoverageDerivedMatch, DingoField: "reward_pool_input.delegated_stake at K-1", Reason: "exact lovelace equality after stake-epoch alignment"},
	{Endpoint: "/pool_history", Field: "active_stake_pct", Class: CoverageUnsupported, Reason: "Dingo has no persisted per-pool active-stake percentage"},
	{Endpoint: "/pool_history", Field: "saturation_pct", Class: CoverageUnsupported, Reason: "Dingo has no persisted per-pool saturation percentage"},
	{Endpoint: "/pool_history", Field: "block_cnt", Class: CoverageDerivedMatch, DingoField: "reward_pool_input.blocks_produced at K+1", Reason: "exact integer equality after parameter-epoch alignment"},
	{Endpoint: "/pool_history", Field: "delegator_cnt", Class: CoverageDerivedMatch, DingoField: "reward_pool_input.delegator_count at K-1", Reason: "exact integer equality after stake-epoch alignment"},
	{Endpoint: "/pool_history", Field: "margin", Class: CoverageDerivedMatch, DingoField: "reward_pool_input.margin at K+1", Reason: "compared as equivalent rational numbers"},
	{Endpoint: "/pool_history", Field: "fixed_cost", Class: CoverageDerivedMatch, DingoField: "reward_pool_input.cost at K+1", Reason: "exact lovelace equality after parameter-epoch alignment"},
	{Endpoint: "/pool_history", Field: "pool_fees", Class: CoverageIntentionallyIncomparable, Reason: "Koios approximation omits the pledge/owner-stake bonus"},
	{Endpoint: "/pool_history", Field: "deleg_rewards", Class: CoverageIntentionallyIncomparable, Reason: "derived from Koios's approximate pool fee and rounded components"},
	{Endpoint: "/pool_history", Field: "member_rewards", Class: CoverageDerivedMatch, DingoField: "reward_pool_output.member_reward_total at K-1", Reason: "exact lovelace equality against Dingo's per-member aggregate"},
	{Endpoint: "/pool_history", Field: "epoch_ros", Class: CoverageUnsupported, Reason: "Dingo has no persisted annualised return-on-stake aggregate"},
}
