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
// epoch/pool/account checker. #3097 wired up exact per-account comparison
// (CompareAccountEpoch, /account_reward_history) against Dingo's committed
// reward_account_output rows; #3099's chunked/resumable large-account fetch
// is still open and does not change which fields are compared, only how
// reliably the account universe is fetched.
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
	{
		Endpoint:   "/tip",
		Field:      "epoch_no",
		Class:      CoverageDerivedMatch,
		DingoField: "closed epoch range",
		Reason:     "tip minus one selects safely closed epochs",
	},
	{
		Endpoint:   "/pool_list",
		Field:      "pool_id_bech32",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_input.pool_key_hash",
		Reason:     "bech32 IDs are decoded and compared as complete set membership",
	},
	{
		Endpoint:   "/pool_updates",
		Field:      "pool_id_bech32",
		Class:      CoverageDerivedMatch,
		DingoField: "pool history request universe",
		Reason:     "identifies the pool whose first active epoch bounds history requests",
	},
	{
		Endpoint:   "/pool_updates",
		Field:      "active_epoch_no",
		Class:      CoverageDerivedMatch,
		DingoField: "pool history request lower bound",
		Reason:     "minimum update epoch avoids requests before a pool existed",
	},

	// /epoch_info.
	{
		Endpoint:   "/epoch_info",
		Field:      "epoch_no",
		Class:      CoverageExactMatch,
		DingoField: "reporting epoch",
		Reason:     "response identity must equal the requested Koios epoch K",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "era",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted per-epoch era aggregate",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "out_sum",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted per-epoch transaction output sum",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "fees",
		Class:    CoverageUnsupported,
		Reason:   "raw transaction fees differ from Dingo's boundary fee-pot balance",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "tx_count",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted per-epoch transaction count aggregate",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "blk_count",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted network-wide per-epoch block count aggregate",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "start_time",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted epoch wall-clock range aggregate",
	},
	{
		Endpoint:   "/epoch_info",
		Field:      "end_time",
		Class:      CoverageDerivedMatch,
		DingoField: "closed/reference-lag state",
		Reason:     "establishes closure and the configured grace window; it is not a value-parity assertion",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "first_block_time",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted per-epoch first-block time aggregate",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "last_block_time",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted per-epoch last-block time aggregate",
	},
	{
		Endpoint:   "/epoch_info",
		Field:      "active_stake",
		Class:      CoverageDerivedMatch,
		DingoField: "epoch_summary.total_active_stake at K-1",
		Reason:     "exact lovelace equality after the established stake-epoch alignment",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "total_rewards",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no matching raw per-performance-epoch reward aggregate",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "avg_blk_reward",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted average block reward aggregate",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "pool_cnt",
		Class:    CoverageUnsupported,
		Reason:   "Koios preview/preprod do not return this documented field",
	},
	{
		Endpoint: "/epoch_info",
		Field:    "delegator_cnt",
		Class:    CoverageUnsupported,
		Reason:   "Koios preview/preprod do not return this documented field",
	},

	// /totals.
	{
		Endpoint:   "/totals",
		Field:      "epoch_no",
		Class:      CoverageExactMatch,
		DingoField: "reporting epoch",
		Reason:     "response identity must equal the requested epoch",
	},
	{
		Endpoint: "/totals",
		Field:    "circulation",
		Class:    CoverageUnsupported,
		Reason:   "requires a network-wide live UTxO-set aggregate",
	},
	{
		Endpoint:   "/totals",
		Field:      "treasury",
		Class:      CoverageExactMatch,
		DingoField: "reward_ada_pots.treasury",
		Reason:     "point-in-time boundary pot balance",
	},
	{
		Endpoint: "/totals",
		Field:    "reward",
		Class:    CoverageIntentionallyIncomparable,
		Reason:   "Koios exposes a lagged cumulative accumulator; Dingo stores a per-epoch reward flow",
	},
	{
		Endpoint: "/totals",
		Field:    "supply",
		Class:    CoverageUnsupported,
		Reason:   "requires a network-wide live UTxO-set aggregate",
	},
	{
		Endpoint:   "/totals",
		Field:      "reserves",
		Class:      CoverageExactMatch,
		DingoField: "reward_ada_pots.reserves",
		Reason:     "point-in-time boundary pot balance",
	},
	{
		Endpoint:   "/totals",
		Field:      "fees",
		Class:      CoverageExactMatch,
		DingoField: "reward_ada_pots.fees",
		Reason:     "point-in-time boundary fee-pot balance",
	},
	{
		Endpoint: "/totals",
		Field:    "deposits_stake",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted stake-deposit pot aggregate",
	},
	{
		Endpoint: "/totals",
		Field:    "deposits_drep",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted DRep-deposit pot aggregate",
	},
	{
		Endpoint: "/totals",
		Field:    "deposits_proposal",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted proposal-deposit pot aggregate",
	},
	{
		Endpoint: "/totals",
		Field:    "treasury_donation",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no matching persisted cumulative governance aggregate",
	},
	{
		Endpoint: "/totals",
		Field:    "treasury_withdrawal",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no matching persisted cumulative governance aggregate",
	},
	{
		Endpoint: "/totals",
		Field:    "reserves_withdrawal",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no matching persisted cumulative reserve-withdrawal aggregate",
	},

	// /pool_history. pool_id_bech32 is supplied as a request parameter and
	// excluded from the projection, but remains part of the parity identity.
	{
		Endpoint:   "/pool_history",
		Field:      "pool_id_bech32",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_input.pool_key_hash",
		Reason:     "requested bech32 ID is decoded to Dingo's pool key hash",
	},
	{
		Endpoint:   "/pool_history",
		Field:      "epoch_no",
		Class:      CoverageExactMatch,
		DingoField: "reporting epoch",
		Reason:     "response identity must equal the requested Koios epoch K",
	},
	{
		Endpoint:   "/pool_history",
		Field:      "active_stake",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_input.delegated_stake at K-1",
		Reason:     "exact lovelace equality after stake-epoch alignment",
	},
	{
		Endpoint: "/pool_history",
		Field:    "active_stake_pct",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted per-pool active-stake percentage",
	},
	{
		Endpoint: "/pool_history",
		Field:    "saturation_pct",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted per-pool saturation percentage",
	},
	{
		Endpoint:   "/pool_history",
		Field:      "block_cnt",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_input.blocks_produced at K+1",
		Reason:     "exact integer equality after parameter-epoch alignment",
	},
	{
		Endpoint:   "/pool_history",
		Field:      "delegator_cnt",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_input.delegator_count at K-1",
		Reason:     "exact integer equality after stake-epoch alignment",
	},
	{
		Endpoint:   "/pool_history",
		Field:      "margin",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_input.margin at K-1",
		Reason:     "compared as equivalent rational numbers",
	},
	{
		Endpoint:   "/pool_history",
		Field:      "fixed_cost",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_input.cost at K-1",
		Reason:     "exact lovelace equality after stake-epoch alignment",
	},
	{
		Endpoint: "/pool_history",
		Field:    "pool_fees",
		Class:    CoverageIntentionallyIncomparable,
		Reason:   "Koios approximation omits the pledge/owner-stake bonus",
	},
	{
		Endpoint: "/pool_history",
		Field:    "deleg_rewards",
		Class:    CoverageIntentionallyIncomparable,
		Reason:   "derived from Koios's approximate pool fee and rounded components",
	},
	{
		Endpoint:   "/pool_history",
		Field:      "member_rewards",
		Class:      CoverageDerivedMatch,
		DingoField: "reward_pool_output.member_reward_total at K-1",
		Reason:     "exact lovelace equality against Dingo's per-member aggregate",
	},
	{
		Endpoint: "/pool_history",
		Field:    "epoch_ros",
		Class:    CoverageUnsupported,
		Reason:   "Dingo has no persisted annualised return-on-stake aggregate",
	},

	// /epoch_params (dingo #3931): CompareEpochProtocolParams compares the
	// effective pparams row for the epoch — resolved from the epoch's own era,
	// since the table holds one row per parameter change — against every
	// parameter below classified exact-match. Numeric comparison goes through
	// rationalsEqual, so Koios's decimal/exponent forms and Dingo's exact
	// num/denom rationals reconcile without either side being rounded.
	{
		Endpoint:   "/epoch_params",
		Field:      "epoch_no",
		Class:      CoverageExactMatch,
		DingoField: "reporting epoch",
		Reason:     "response identity must equal the requested epoch",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "era",
		Class:      CoverageExactMatch,
		DingoField: "epoch era from the `epoch` table",
		Reason:     "the era decides which validation rules run at all",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "min_fee_a",
		Class:      CoverageExactMatch,
		DingoField: "pparams min_fee_a",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "min_fee_b",
		Class:      CoverageExactMatch,
		DingoField: "pparams min_fee_b",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_block_size",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_block_body_size",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_tx_size",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_tx_size",
		Reason:     "exact value equality; a wrong value is the #3928 wedge",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_bh_size",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_block_header_size",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "key_deposit",
		Class:      CoverageExactMatch,
		DingoField: "pparams key_deposit",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "pool_deposit",
		Class:      CoverageExactMatch,
		DingoField: "pparams pool_deposit",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_epoch",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_epoch",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "optimal_pool_count",
		Class:      CoverageExactMatch,
		DingoField: "pparams n_opt",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "influence",
		Class:      CoverageExactMatch,
		DingoField: "pparams a0",
		Reason:     "rational equality: Koios publishes 0.3 for Dingo's 3/10",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "monetary_expand_rate",
		Class:      CoverageExactMatch,
		DingoField: "pparams rho",
		Reason:     "rational equality: Koios publishes 0.003 for Dingo's 3/1000",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "treasury_growth_rate",
		Class:      CoverageExactMatch,
		DingoField: "pparams tau",
		Reason:     "rational equality: Koios publishes 0.2 for Dingo's 1/5",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "protocol_major",
		Class:      CoverageExactMatch,
		DingoField: "pparams protocol_major",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "protocol_minor",
		Class:      CoverageExactMatch,
		DingoField: "pparams protocol_minor",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "min_pool_cost",
		Class:      CoverageExactMatch,
		DingoField: "pparams min_pool_cost",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "price_mem",
		Class:      CoverageExactMatch,
		DingoField: "pparams execution_costs.mem_price",
		Reason:     "rational equality: Koios publishes 0.0577 for Dingo's 577/10000",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "price_step",
		Class:      CoverageExactMatch,
		DingoField: "pparams execution_costs.step_price",
		Reason:     "rational equality: Koios publishes 7.21e-05 for Dingo's 721/10000000",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_tx_ex_mem",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_tx_ex_units.memory",
		Reason:     "gates phase-2 validation; a divergence is silent until a script tx fails",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_tx_ex_steps",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_tx_ex_units.steps",
		Reason:     "gates phase-2 validation; a divergence is silent until a script tx fails",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_block_ex_mem",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_block_ex_units.memory",
		Reason:     "gates phase-2 validation; a divergence is silent until a script tx fails",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_block_ex_steps",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_block_ex_units.steps",
		Reason:     "gates phase-2 validation; a divergence is silent until a script tx fails",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_val_size",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_value_size",
		Reason:     "exact value equality against the effective pparams row",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "collateral_percent",
		Class:      CoverageExactMatch,
		DingoField: "pparams collateral_percentage",
		Reason:     "gates phase-2 collateral validation",
	},
	{
		Endpoint:   "/epoch_params",
		Field:      "max_collateral_inputs",
		Class:      CoverageExactMatch,
		DingoField: "pparams max_collateral_inputs",
		Reason:     "gates phase-2 collateral validation",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "decentralisation",
		Class:    CoverageUnsupported,
		Reason:   "no Babbage or Conway parameter struct defines it, so no live era has a Dingo value; cached for reference",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "min_utxo_value",
		Class:    CoverageUnsupported,
		Reason:   "a Shelley-era parameter absent from every live era's struct; cached for reference",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "coins_per_utxo_size",
		Class:    CoverageUnsupported,
		Reason:   "Koios reports Alonzo's per-word figure where Dingo stores per-byte (34482 vs 4310 on preview epochs 0-2); cached for reference pending its own investigation",
	},

	// Documented /epoch_params fields this checker does not model at all.
	// Classified here so a report never implies they were checked.
	{
		Endpoint: "/epoch_params",
		Field:    "cost_models",
		Class:    CoverageUnsupported,
		Reason:   "Koios publishes named PlutusV1/V2 arrays against Dingo's map[uint][]int64; the operation ordering that makes them comparable is not established, so they are neither fetched nor compared",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "nonce",
		Class:    CoverageUnsupported,
		Reason:   "epoch identity, not a protocol parameter",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "block_hash",
		Class:    CoverageUnsupported,
		Reason:   "epoch identity, not a protocol parameter",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "extra_entropy",
		Class:    CoverageUnsupported,
		Reason:   "a Shelley-era nonce field absent from every live era's parameter struct",
	},

	// Conway governance and reference-script parameters. These are real
	// consensus-relevant values and worth covering, but the reference chain
	// available to verify their cross-side representation is Babbage-era
	// throughout, so they are left to follow-up work rather than compared on
	// an unverified assumption.
	{
		Endpoint: "/epoch_params",
		Field:    "pvt_motion_no_confidence",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "pvt_committee_normal",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "pvt_committee_no_confidence",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "pvt_hard_fork_initiation",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "pvtpp_security_group",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_motion_no_confidence",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_committee_normal",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_committee_no_confidence",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_update_to_constitution",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_hard_fork_initiation",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_p_p_network_group",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_p_p_economic_group",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_p_p_technical_group",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_p_p_gov_group",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "dvt_treasury_withdrawal",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "committee_min_size",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "committee_max_term_length",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "gov_action_lifetime",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "gov_action_deposit",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "drep_deposit",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "drep_activity",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},
	{
		Endpoint: "/epoch_params",
		Field:    "min_fee_ref_script_cost_per_byte",
		Class:    CoverageUnsupported,
		Reason:   "Conway governance parameter; cross-side representation not yet verified against a Conway-era reference chain",
	},

	// /account_reward_history (#3097): CompareAccountEpoch compares every
	// reference row against reward_account_output exactly, per
	// (stake_address, type) — member/leader rows only, since
	// koiosAccountRewardTypesOutOfScope filters out reward mechanisms
	// (treasury/reserves MIR, refund) Dingo does not currently produce.
	{
		Endpoint:   "/account_reward_history",
		Field:      "stake_address",
		Class:      CoverageExactMatch,
		DingoField: "reward_account_output staking key, decoded to a bech32 stake address",
		Reason:     "identifies the (stake_address, type) row pair CompareAccountEpoch matches on",
	},
	{
		Endpoint:   "/account_reward_history",
		Field:      "earned_epoch",
		Class:      CoverageExactMatch,
		DingoField: "reporting epoch",
		Reason:     "response identity must equal the requested epoch",
	},
	{
		Endpoint: "/account_reward_history",
		Field:    "spendable_epoch",
		Class:    CoverageUnsupported,
		Reason:   "stored for reference but not currently compared against Dingo's schema",
	},
	{
		Endpoint:   "/account_reward_history",
		Field:      "amount",
		Class:      CoverageExactMatch,
		DingoField: "reward_account_output.amount",
		Reason:     "exact integer lovelace equality via lovelaceEqual, no rounding/sampling/tolerance",
	},
	{
		Endpoint:   "/account_reward_history",
		Field:      "type",
		Class:      CoverageExactMatch,
		DingoField: "reward_account_output.reward_type",
		Reason:     "member/leader rows are matched exactly; treasury/reserves/refund rows are out of scope, see koiosAccountRewardTypesOutOfScope",
	},
	{
		Endpoint: "/account_reward_history",
		Field:    "pool_id_bech32",
		Class:    CoverageUnsupported,
		Reason:   "null for reward types with no associated pool; not part of the (stake_address, type) match key",
	},
}
