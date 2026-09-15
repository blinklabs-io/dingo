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
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/spf13/cobra"
)

func checkCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Run one ledger-state comparison cycle and print the result",
		Long: `Runs one comparison cycle against --dingo-addr and --cardano-addr: reads
both nodes' tips, and if they agree, compares protocol parameters, stake
distribution, and the whole UTxO set. Exits nonzero if the two diverged, or
if the cycle had to be discarded because the nodes never held a stable
common tip -- a caller must not read a discarded cycle as a clean match.

Pass --at-slot and --at-hash together to compare at an explicit historical
block instead of the live tip (blinklabs-io/dingo#382): neither node needs
to be near its own live tip for this, and a point neither can reconstruct
surfaces as a command error rather than a discarded cycle.`,
		Args: cobra.NoArgs,
		RunE: checkRun,
	}
}

func checkRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}
	if err := requireAddrs(); err != nil {
		return err
	}
	magic, err := networkMagic(network)
	if err != nil {
		return err
	}
	at, err := requireAtPoint()
	if err != nil {
		return err
	}

	result, err := nodeparity.Check(
		cmd.Context(),
		globalFlags.dingoAddr,
		globalFlags.cardanoAddr,
		magic,
		at,
	)
	if err != nil {
		return err
	}
	return reportResult(result)
}

// reportResult prints a check cycle's outcome and returns a non-nil error
// for a skipped cycle or a real divergence, so 'check' and the default
// action signal anything other than a clean match through the process exit
// code, matching cmd/koios-parity's checkResultErr convention.
func reportResult(result *nodeparity.CheckResult) error {
	if result.Skipped {
		fmt.Printf("check skipped: %s\n", result.SkipDetail)
		return fmt.Errorf("check skipped: %s", result.SkipDetail)
	}
	if result.Diff.Empty() {
		fmt.Printf(
			"check complete: matched at slot %d (block %d)\n",
			result.Tip.Slot, result.Tip.BlockNumber,
		)
		return nil
	}
	lines := result.Diff.Lines()
	count := result.Diff.Count()
	fmt.Printf(
		"check complete: DIVERGED at slot %d (block %d), %d difference(s):\n%s\n",
		result.Tip.Slot,
		result.Tip.BlockNumber,
		count,
		strings.Join(lines, "\n"),
	)
	return fmt.Errorf(
		"ledger state diverged at slot %d (block %d): %d difference(s)",
		result.Tip.Slot, result.Tip.BlockNumber, count,
	)
}
