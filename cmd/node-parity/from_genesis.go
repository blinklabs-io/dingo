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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/spf13/cobra"
)

// koiosFlags are specific to the from-genesis command -- unlike
// globalFlags.network/dingoAddr, nothing else in this tool needs them.
var koiosFlags struct {
	apiKey            string
	baseURL           string
	allowInsecureHTTP bool
	verbose           bool
}

// splitStakeMismatches partitions mismatches into real Dingo/Koios
// divergences and KoiosFault entries (an unparseable koios active_stake
// value -- a comparison Koios's own data made untrustworthy, not a Dingo
// divergence; see StakeMismatch.KoiosFault's doc comment). Counting a fault
// toward a real mismatch total would page on Koios's own data quality, the
// same wrong outcome the protocol-params branch avoids via
// koiosparity.DetermineStatus.
func splitStakeMismatches(
	mismatches []nodeparity.StakeMismatch,
) (real, faults []nodeparity.StakeMismatch) {
	for _, m := range mismatches {
		if m.KoiosFault {
			faults = append(faults, m)
		} else {
			real = append(real, m)
		}
	}
	return real, faults
}

// fromGenesisCounters accumulates from-genesis's per-epoch verdicts across
// a whole run: recordEpoch is the report callback's actual logic, pulled
// out of that closure so a test can drive it directly with a synthetic
// EpochResult and assert on the resulting counts -- rather than only on
// splitStakeMismatches in isolation, which proves the partition is correct
// but not that this counting actually uses it (human review, Chris Guiney,
// dingo#4319: reverting recordEpoch's stake branch to the old
// "stakeMismatches++ for any non-empty StakeMismatches" shape left a
// helper-only test green).
//
// The six Incomplete counters matter as much as the three Mismatch ones:
// each increments whenever a check could not be trusted at all (a Koios
// fetch failure, a Dingo query error, or a Koios-side data fault) rather
// than confirming a real match. Without them, a run whose Koios side was
// degraded throughout reports 0 mismatches across the board and exits 0,
// indistinguishable from a run that genuinely verified everything (human
// review, Chris Guiney, dingo#4319).
type fromGenesisCounters struct {
	epochsChecked                                 int
	ppMismatches, stakeMismatches, utxoMismatches int
	ppIncomplete, stakeIncomplete, utxoIncomplete int
}

// recordEpoch is documented on fromGenesisCounters.
func (c *fromGenesisCounters) recordEpoch(
	r nodeparity.EpochResult,
	logger *slog.Logger,
) {
	c.epochsChecked++

	logger.Debug("epoch timing",
		"epoch", r.Epoch,
		"tx_info_flushes", r.TxInfoFlushCount,
		"tx_info_flush_elapsed", r.TxInfoFlushElapsed.String(),
		"protocol_params_and_stake_elapsed", r.ProtocolParamsAndStakeElapsed.String(),
		"utxo_elapsed", r.UTxOElapsed.String(),
	)

	// koiosparity.DetermineStatus distinguishes a real Dingo/Koios
	// disagreement (StatusFail) from a comparison that could not be
	// trusted at all -- most commonly a Koios fetch failure, which
	// CompareEpochProtocolParams reports as a CategoryDBError mismatch
	// entry rather than a Go error return (mirroring koios-parity's own
	// reporting convention exactly). Treating every non-empty
	// mismatches slice as a real mismatch would count "Koios's daily
	// quota is exhausted" the same as "Dingo answered the wrong value,"
	// which are very different things to page someone about.
	if r.ProtocolParamsErr != nil {
		c.ppIncomplete++
		logger.Warn("protocol params check did not run",
			"epoch", r.Epoch, "error", r.ProtocolParamsErr)
	} else {
		switch koiosparity.DetermineStatus(r.ProtocolParamsMismatches) {
		case koiosparity.StatusFail:
			c.ppMismatches++
			for _, m := range r.ProtocolParamsMismatches {
				logger.Warn("protocol params mismatch",
					"epoch", r.Epoch, "field", m.Field,
					"dingo", m.DingoValue, "koios", m.KoiosValue,
					"category", m.Category)
			}
		case koiosparity.StatusError:
			c.ppIncomplete++
			for _, m := range r.ProtocolParamsMismatches {
				logger.Warn("protocol params check incomplete",
					"epoch", r.Epoch, "field", m.Field,
					"dingo", m.DingoValue, "category", m.Category)
			}
		default:
			logger.Info("protocol params match", "epoch", r.Epoch)
		}
	}

	if r.StakeErr != nil {
		c.stakeIncomplete++
		logger.Warn("stake distribution check did not run",
			"epoch", r.Epoch, "error", r.StakeErr)
	} else {
		realMismatches, faults := splitStakeMismatches(r.StakeMismatches)
		if len(faults) > 0 {
			c.stakeIncomplete++
		}
		for _, m := range faults {
			logger.Warn("stake distribution check incomplete",
				"epoch", r.Epoch, "pool", m.PoolIDBech32,
				"dingo_stake", m.DingoStake, "koios_stake", m.KoiosStake,
				"reason", m.Reason)
		}
		if len(realMismatches) > 0 {
			c.stakeMismatches++
			for _, m := range realMismatches {
				logger.Warn("stake distribution mismatch",
					"epoch", r.Epoch, "pool", m.PoolIDBech32,
					"dingo_stake", m.DingoStake, "koios_stake", m.KoiosStake,
					"diff_lovelace", m.DiffLovelace, "reason", m.Reason)
			}
		} else if len(faults) == 0 {
			logger.Info("stake distribution match", "epoch", r.Epoch)
		}
	}

	if !r.UTxOAttempted {
		c.utxoIncomplete++
		logger.Debug("utxo check skipped (no genesis baseline)", "epoch", r.Epoch)
	} else if r.UTxOErr != nil {
		c.utxoIncomplete++
		logger.Warn("utxo check did not run",
			"epoch", r.Epoch, "error", r.UTxOErr)
	} else if len(r.UTxOMissing) > 0 || len(r.UTxOExtra) > 0 || len(r.UTxODiffers) > 0 {
		c.utxoMismatches++
		logger.Warn("utxo set mismatch",
			"epoch", r.Epoch, "missing", len(r.UTxOMissing),
			"extra", len(r.UTxOExtra), "differs", len(r.UTxODiffers),
			"dingo_ref_count", r.UTxORefCount)
		for _, d := range r.UTxODiffers {
			logger.Debug("utxo content differs", "epoch", r.Epoch, "detail", d)
		}
	} else {
		logger.Info("utxo set match",
			"epoch", r.Epoch, "ref_count", r.UTxORefCount)
	}
}

func fromGenesisCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "from-genesis",
		Short: "Validate a from-genesis Dingo replay against Koios instead of a reference cardano-node",
		Long: `Follows --dingo-addr's chain from genesis and, at every epoch boundary,
compares protocol parameters, stake distribution, and the UTxO set against
Koios -- instead of a reference cardano-node, which 'check'/'watch' use.

This exists specifically because a real cardano-node cannot fill the
reference role for a from-genesis replay: cardano-node's own replay races
ahead of a freshly-started Dingo fast enough that there is never a matching
historical block left to compare against by the time Dingo reaches it. Koios
has no such problem -- it retains full per-epoch history indefinitely.

Restricted to --network preview or preprod: Koios only ever serves these two
networks, and this command's UTxO-set check walks every transaction one at a
time from genesis, which is only tractable at their scale. For mainnet-scale
validation of an already-synced node, use 'check'/'watch' against a real
reference cardano-node instead -- both nodes bootstrapped near their live tip
(e.g. via a Mithril snapshot) avoids the from-genesis race this command
exists to work around in the first place.

Runs until interrupted (Ctrl-C) or the chain-sync session ends. Prints one
summary line per epoch and a running total on exit; exits nonzero if any
epoch found a real mismatch.

Pass --at-slot and --at-hash together to resume from an already-validated
point instead of genesis: a killed or restarted process has no on-disk
checkpoint of its own, so a caller that already trusts a prior run's epochs
up to some point (e.g. its logged final "run summary") passes that point's
slot/hash back in here to seed both the chain-sync start point and the UTxO
reconstruction baseline (captured fresh from Dingo at that point, the same
way genesis's own baseline is), skipping the epochs already covered instead
of re-deriving them from scratch.`,
		Args: cobra.NoArgs,
		RunE: fromGenesisRun,
	}
	cmd.Flags().StringVar(
		&koiosFlags.apiKey, "koios-api-key", "",
		"Koios API key (optional; raises the public host's rate limit)",
	)
	cmd.Flags().StringVar(
		&koiosFlags.baseURL, "koios-base-url", "",
		"Koios v1 API root override, e.g. for a self-hosted or mirrored instance (default: the public host for --network)",
	)
	cmd.Flags().BoolVar(
		&koiosFlags.allowInsecureHTTP, "koios-allow-insecure-http", false,
		"allow a plain-HTTP --koios-base-url (only for a trusted local/test instance)",
	)
	cmd.Flags().BoolVar(
		&koiosFlags.verbose, "verbose", false,
		"log each individual UTxO ref that differs (address/amount/assets/datum/scriptref), not just per-epoch counts",
	)
	return cmd
}

func fromGenesisRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}
	if globalFlags.dingoAddr == "" {
		return errors.New("--dingo-addr is required")
	}
	resumeFrom, err := requireAtPoint()
	if err != nil {
		return err
	}
	magic, err := networkMagic(network)
	if err != nil {
		return err
	}

	koios, err := nodeparity.NewKoiosClient(
		network, koiosFlags.apiKey, koiosFlags.baseURL, koiosFlags.allowInsecureHTTP,
	)
	if err != nil {
		return err
	}

	logLevel := slog.LevelInfo
	if koiosFlags.verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	var counters fromGenesisCounters
	report := func(r nodeparity.EpochResult) {
		counters.recordEpoch(r, logger)
	}

	logf := func(format string, args ...any) {
		logger.Info(fmt.Sprintf(format, args...))
	}

	err = nodeparity.RunFromGenesis(
		cmd.Context(), globalFlags.dingoAddr, network, magic, koios, report, logf,
		resumeFrom,
	)

	logger.Info("run summary",
		"epochs_checked", counters.epochsChecked,
		"protocol_param_mismatches", counters.ppMismatches,
		"stake_mismatches", counters.stakeMismatches,
		"utxo_mismatches", counters.utxoMismatches,
		"protocol_param_checks_incomplete", counters.ppIncomplete,
		"stake_checks_incomplete", counters.stakeIncomplete,
		"utxo_checks_incomplete", counters.utxoIncomplete,
	)

	// context.Canceled is this command's own documented normal way to
	// stop ("Runs until interrupted (Ctrl-C)...") -- returning it here
	// unconditionally made the mismatch check below unreachable on the
	// single most common way to end a run, so a clean run and one that
	// found real divergence exited identically. Treat it as a normal
	// stop and fall through to the mismatch check instead.
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if counters.ppMismatches > 0 || counters.stakeMismatches > 0 || counters.utxoMismatches > 0 {
		return fmt.Errorf(
			"ledger state diverged from Koios: %d protocol-param, %d stake, %d utxo mismatch epoch(s)",
			counters.ppMismatches, counters.stakeMismatches, counters.utxoMismatches,
		)
	}
	return nil
}
