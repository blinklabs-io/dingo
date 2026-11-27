# Koios parity reference

The parity tracker compares closed-epoch ledger results with Koios. It runs
in the node as an epoch-boundary observer, enabled by `koiosParity` in the node
configuration; there is no separate binary. The observer reads consensus results from the LedgerDB and historical
reward-account outputs from the IndexerDB. An IndexerDB that has not reached
the comparison point cannot establish a mismatch.

## Epoch alignment

Koios reports pool performance under the epoch in which blocks were produced.
Dingo snapshots are indexed by their capture boundary. For Koios reporting
epoch K, the comparison reads:

| Data | Dingo epoch |
| --- | --- |
| Reward stake inputs, pool margin and fixed cost, reward pool outputs | K-1 |
| Pool block-production counts in captured reward inputs | K+1 |
| Effective protocol parameters and the epoch's era | K |
| Boundary ADA pots | K |

`koiosStakeEpoch` and `koiosParamEpoch` in `check.go` own these conversions.
Pre-staking epochs have no reward stake epoch and are excluded before
subtracting. Protocol parameters use the latest change at or before K for
K's recorded era; selecting an exact-epoch row or the last inserted row can
choose no parameters or the wrong era at a boundary.

## Coverage matrix

A PASS covers only the fields classified below. `coverage.go` owns the
machine-readable matrix; `internal/docsparity` checks this table against it.
An unsupported or intentionally incomparable field is not evidence of parity.

| Koios endpoint | Classification | Fields | Dingo mapping / reason |
|---|---|---|---|
| `/tip` | derived-match | `epoch_no` | `tip - 1` bounds the safely closed epoch range. |
| `/pool_list` | derived-match | `pool_id_bech32` | Decode to pool key hash and compare complete pool membership. |
| `/pool_updates` | derived-match | `pool_id_bech32`, `active_epoch_no` | Derive each pool's earliest possible history request. These control fetching, not value parity. |
| `/epoch_info` | exact-match | `epoch_no` | The filtered response must contain exactly the requested reporting epoch K. |
| `/epoch_info` | derived-match | `active_stake` | Exact lovelace match to `epoch_summary.total_active_stake` at K-1. |
| `/epoch_info` | derived-match | `end_time` | Establishes closure and the reference-lag window; not a Dingo value assertion. |
| `/epoch_info` | unsupported | `era`, `out_sum`, `fees`, `tx_count`, `blk_count`, `start_time`, `first_block_time`, `last_block_time`, `total_rewards`, `avg_blk_reward` | Dingo has no matching persisted per-epoch aggregate. In particular, raw `fees`/`total_rewards` are not AdaPots balances. `era` is the exception: Dingo does persist it, in `epoch.era_id`, and it is compared once under `/epoch_params` rather than twice. |
| `/epoch_info` | unsupported | `pool_cnt`, `delegator_cnt` | Documented fields are not returned by Koios and are omitted from the response projection. |
| `/totals` | exact-match | `epoch_no`, `treasury`, `reserves`, `fees` | Require the requested epoch and exact equality with K's `reward_ada_pots` boundary balances. |
| `/totals` | intentionally-incomparable | `reward` | Koios reports a lagged cumulative accumulator; Dingo stores a per-epoch flow. |
| `/totals` | unsupported | `circulation`, `supply`, `deposits_stake`, `deposits_drep`, `deposits_proposal`, `treasury_donation`, `treasury_withdrawal`, `reserves_withdrawal` | No matching persisted Dingo network aggregate. |
| `/pool_history` | exact-match | `epoch_no` | The filtered response must contain exactly the requested reporting epoch K. |
| `/pool_history` | derived-match | `pool_id_bech32` | Request identity is decoded to Dingo's pool key hash for set membership. |
| `/pool_history` | derived-match | `active_stake`, `delegator_cnt` | Exact values against `reward_pool_input` at stake epoch K-1. |
| `/pool_history` | derived-match | `block_cnt` | Exact value against `reward_pool_input` at parameter epoch K+1. |
| `/pool_history` | derived-match | `fixed_cost` | Exact value against `reward_pool_input` at stake epoch K-1: a mark snapshot records the pool parameters in force for the epoch it is the basis for. |
| `/pool_history` | derived-match | `margin` | K-1 values compared as equivalent rational numbers. |
| `/pool_history` | intentionally-incomparable | `member_rewards` | A cached per-pool total that can include rewards the ledger forfeited and differs between Koios instances; it never decides a verdict. A disagreement with Koios's own per-account sum for the pool and epoch is recorded as `koios_pool_history_inconsistent`. |
| `/pool_history` | intentionally-incomparable | `pool_fees`, `deleg_rewards` | Koios derives these from an approximation that omits the pledge/owner-stake bonus and rounds components. |
| `/pool_history` | unsupported | `active_stake_pct`, `saturation_pct`, `epoch_ros` | Dingo has no matching persisted pool aggregate. |
| `/account_reward_history` | exact-match | `stake_address`, `earned_epoch` | Identifies the `(stake_address, type)` aggregate `CompareAccountEpoch` matches on; response identity must equal the requested epoch. |
| `/account_reward_history` | exact-match | `amount`, `type` | Exact integer lovelace equality against `reward_account_output.amount`/`reward_type` (and, for `proposal_refund`, against the proposal-deposit credit in `account_reward_delta.amount`) for member/leader/proposal_refund rows, compared as per-`(stake_address, type)` totals summed across pool contributions, and then per pool contribution, so a disagreement that preserves the total (equal and opposite per-pool errors, or a total attributed to one of the pools the other side splits it between) is reported as `account_reward_pool_amount` rather than passing; repeated `proposal_refund` rows sum as separate deposits; a type that is neither modeled nor filtered by `koiosAccountRewardTypesOutOfScope` is `reward_type_unmodeled`, an ERROR rather than a parity failure, so the account check cannot pass. |
| `/account_reward_history` | derived-match | `pool_id_bech32` | Decoded to `reward_account_output.pool_key_hash`'s pool ID to identify each contribution to a shared reward account; distinct pools aggregate and are then compared per pool, a repeated same-pool row is `acct_duplicate`. A side that names no pool for a key makes no per-pool statement to disagree with, so the totals are the whole verdict there. Member rows are also summed per pool and compared exactly with Dingo's spendable member sum (`reward_account_output` member rows that are `spendable` and not `guarded`, at K-1), in both directions, once the epoch's account coverage is complete. |
| `/account_reward_history` | unsupported | `spendable_epoch` | Stored for reference only; not currently compared against Dingo's schema. |
| `/epoch_params` | exact-match | `epoch_no` | The filtered response must contain exactly the requested reporting epoch K. |
| `/epoch_params` | exact-match | `era` | Dingo's `epoch.era_id` name; the era decides which validation rules run at all. |
| `/epoch_params` | exact-match | `min_fee_a`, `min_fee_b`, `max_block_size`, `max_tx_size`, `max_bh_size`, `key_deposit`, `pool_deposit`, `max_epoch`, `optimal_pool_count`, `protocol_major`, `protocol_minor`, `min_pool_cost` | Exact values against the effective `pparams` row for K. A wrong `max_tx_size` can wedge replay on a transaction exactly `maxTxSize` bytes long. |
| `/epoch_params` | exact-match | `influence`, `monetary_expand_rate`, `treasury_growth_rate`, `price_mem`, `price_step` | Compared as rationals: Koios publishes `0.0577`/`7.21e-05` where Dingo stores `577/10000`/`721/10000000`. |
| `/epoch_params` | exact-match | `max_tx_ex_mem`, `max_tx_ex_steps`, `max_block_ex_mem`, `max_block_ex_steps`, `max_val_size`, `collateral_percent`, `max_collateral_inputs` | Exact values against the effective `pparams` row. These gate phase-2 validation, where a divergence is silent until a script transaction fails. |
| `/epoch_params` | exact-match | `coins_per_utxo_size` | Exact era-native value against the effective `pparams` row: per word in Alonzo and per byte from Babbage onward. |
| `/epoch_params` | exact-match | `cost_models` | Entry-for-entry equality per Plutus language. Dingo's numeric keys (`0`, `1`) map to Koios's `PlutusV1`/`PlutusV2` names and the positional arrays agree exactly (166 and 175 entries on preview). Findings name the language and first differing entry rather than dumping the array. |
| `/epoch_params` | unsupported | `decentralisation`, `min_utxo_value`, `extra_entropy` | Pre-Babbage parameters absent from every live era's parameter struct. |
| `/epoch_params` | unsupported | `nonce`, `block_hash` | Epoch identity, not protocol parameters. |
| `/epoch_params` | unsupported | `pvt_*`, `pvtpp_security_group`, `dvt_*`, `committee_min_size`, `committee_max_term_length`, `gov_action_lifetime`, `gov_action_deposit`, `drep_deposit`, `drep_activity`, `min_fee_ref_script_cost_per_byte` | Conway governance and reference-script parameters. Real and worth covering, but their cross-side representation is not yet verified against a Conway-era reference chain. |

## Per-account exact parity

Account comparison is optional and has its own reference-coverage checkpoint.
Pool-level cache completeness does not establish account completeness. Enabling
account comparison on an existing cache selects missing account coverage for
backfill and selects incomplete or refreshed coverage for another check.
Pre-staking epochs are excluded from both selections.

Stake addresses preserve the credential's key/script tag and network identity.
Reward rows compare their pool, type, earned epoch and amount; spendable epoch
is retained as reference data only.
Member and leader rewards are not interchangeable; proposal-refund rows may
repeat and are summed, while duplicate member or leader rows for the same pool
are rejected.
Treasury/reserves MIR and legacy refund types are outside the account reward
comparison; `koiosAccountRewardTypesOutOfScope` in `compare.go` owns that set.
Reward rows are read from the IndexerDB rather than fabricated from the live
account balance. The Dingo side is read directly through storage contracts;
an HTTP client must not substitute API responses for that committed state.

## Results and retries

A value difference is a failure. Missing data or a failed read is an error,
not a successful comparison. An epoch before staking began, or before the
node's earliest available ledger epoch, passes without a comparison; an epoch
whose reward state was pruned is not checked. Reference-lag grace applies only to the cases
classified as lag; it does not soften protocol-parameter mismatches.
Aggregate and account comparisons keep separate status and retry state.
The observer retries failed checks even when cached reference data has not
changed, and cancellation stops its workers.

The comparison and epoch-selection contracts live on their declarations in
`check.go`, `compare.go`, `cache.go` and `observer.go`. Configuration is
specified in `dingo.yaml.example`; this reference does not duplicate defaults.
