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
epoch found a real mismatch.`,
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
	if err := rejectAtPointFlags("from-genesis"); err != nil {
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

	var (
		epochsChecked   int
		ppMismatchCount int
		stakeMismatches int
		utxoMismatches  int
	)
	report := func(r nodeparity.EpochResult) {
		epochsChecked++

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
			logger.Warn("protocol params check did not run",
				"epoch", r.Epoch, "error", r.ProtocolParamsErr)
		} else {
			switch koiosparity.DetermineStatus(r.ProtocolParamsMismatches) {
			case koiosparity.StatusFail:
				ppMismatchCount++
				for _, m := range r.ProtocolParamsMismatches {
					logger.Warn("protocol params mismatch",
						"epoch", r.Epoch, "field", m.Field,
						"dingo", m.DingoValue, "koios", m.KoiosValue,
						"category", m.Category)
				}
			case koiosparity.StatusError:
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
			logger.Warn("stake distribution check did not run",
				"epoch", r.Epoch, "error", r.StakeErr)
		} else if len(r.StakeMismatches) > 0 {
			stakeMismatches++
			for _, m := range r.StakeMismatches {
				logger.Warn("stake distribution mismatch",
					"epoch", r.Epoch, "pool", m.PoolIDBech32,
					"dingo_stake", m.DingoStake, "koios_stake", m.KoiosStake,
					"diff_lovelace", m.DiffLovelace, "reason", m.Reason)
			}
		} else {
			logger.Info("stake distribution match", "epoch", r.Epoch)
		}

		if !r.UTxOAttempted {
			logger.Debug("utxo check skipped (no genesis baseline)", "epoch", r.Epoch)
		} else if r.UTxOErr != nil {
			logger.Warn("utxo check did not run",
				"epoch", r.Epoch, "error", r.UTxOErr)
		} else if len(r.UTxOMissing) > 0 || len(r.UTxOExtra) > 0 || len(r.UTxODiffers) > 0 {
			utxoMismatches++
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

	logf := func(format string, args ...any) {
		logger.Info(fmt.Sprintf(format, args...))
	}

	err = nodeparity.RunFromGenesis(
		cmd.Context(), globalFlags.dingoAddr, network, magic, koios, report, logf,
	)

	logger.Info("run summary",
		"epochs_checked", epochsChecked,
		"protocol_param_mismatches", ppMismatchCount,
		"stake_mismatches", stakeMismatches,
		"utxo_mismatches", utxoMismatches,
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
	if ppMismatchCount > 0 || stakeMismatches > 0 || utxoMismatches > 0 {
		return fmt.Errorf(
			"ledger state diverged from Koios: %d protocol-param, %d stake, %d utxo mismatch epoch(s)",
			ppMismatchCount, stakeMismatches, utxoMismatches,
		)
	}
	return nil
}
