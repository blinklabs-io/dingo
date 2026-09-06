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

package config

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// TestForgeEBCapsUnsetTakeTheDefault and its explicit-zero counterpart pin
// the distinction a plain uint64 cannot express: an operator who never
// mentioned the cap gets the backstop, and one who wrote 0 gets the cap
// switched off, as both the flag help and the ForgerConfig contract
// promise.
func TestForgeEBCapsUnsetTakeTheDefault(t *testing.T) {
	c := &Config{}
	c.ApplyDefaults()

	if c.ForgeEBMaxTxRefs == nil {
		t.Fatal("unset forgeEbMaxTxRefs must take the default, got nil")
	}
	if got := *c.ForgeEBMaxTxRefs; got != DefaultForgeEBMaxTxRefs {
		t.Fatalf("forgeEbMaxTxRefs = %d, want %d", got, DefaultForgeEBMaxTxRefs)
	}
	if c.ForgeEBMaxBytes == nil {
		t.Fatal("unset forgeEbMaxBytes must take the default, got nil")
	}
	if got := *c.ForgeEBMaxBytes; got != DefaultForgeEBMaxBytes {
		t.Fatalf("forgeEbMaxBytes = %d, want %d", got, DefaultForgeEBMaxBytes)
	}
}

func TestForgeEBCapsExplicitZeroDisablesThem(t *testing.T) {
	zero := uint64(0)
	c := &Config{ForgeEBMaxTxRefs: &zero, ForgeEBMaxBytes: &zero}
	c.ApplyDefaults()

	if c.ForgeEBMaxTxRefs == nil || *c.ForgeEBMaxTxRefs != 0 {
		t.Fatalf(
			"explicit zero forgeEbMaxTxRefs must survive ApplyDefaults, got %v",
			c.ForgeEBMaxTxRefs,
		)
	}
	if c.ForgeEBMaxBytes == nil || *c.ForgeEBMaxBytes != 0 {
		t.Fatalf(
			"explicit zero forgeEbMaxBytes must survive ApplyDefaults, got %v",
			c.ForgeEBMaxBytes,
		)
	}
}

// TestForgeEBCapDefaultsArePinned guards the copy of these numbers that
// ledger/forging keeps for the embedder path. internal/config cannot
// import ledger/forging (it would cycle), so the values are declared twice
// on purpose; ledger/forging asserts the same literals from its side.
func TestForgeEBCapDefaultsArePinned(t *testing.T) {
	if DefaultForgeEBMaxTxRefs != 20000 {
		t.Fatalf("forgeEbMaxTxRefs default drifted: %d", DefaultForgeEBMaxTxRefs)
	}
	if DefaultForgeEBMaxBytes != 25165824 {
		t.Fatalf("forgeEbMaxBytes default drifted: %d", DefaultForgeEBMaxBytes)
	}
}

// TestForgeEBSelectionReserveUnsetTakesTheDefault covers the third state
// this field can be in. Unlike the caps, a reserve of zero cannot mean
// "disabled": it would leave the ranking block no time at all, so zero can
// only mean the operator never mentioned it.
func TestForgeEBSelectionReserveUnsetTakesTheDefault(t *testing.T) {
	c := &Config{}
	c.ApplyDefaults()

	if c.ForgeEBSelectionReserve != DefaultForgeEBSelectionReserve {
		t.Fatalf(
			"forgeEbSelectionReserve = %s, want %s",
			c.ForgeEBSelectionReserve,
			DefaultForgeEBSelectionReserve,
		)
	}
}

func TestForgeEBSelectionReserveKeepsAConfiguredValue(t *testing.T) {
	c := &Config{ForgeEBSelectionReserve: 750 * time.Millisecond}
	c.ApplyDefaults()

	if c.ForgeEBSelectionReserve != 750*time.Millisecond {
		t.Fatalf(
			"configured forgeEbSelectionReserve must survive ApplyDefaults, got %s",
			c.ForgeEBSelectionReserve,
		)
	}
}

// TestForgeEBSelectionReserveDefaultIsPinned is the internal/config half
// of the same two-sided pin as TestForgeEBCapDefaultsArePinned: the
// forging package keeps its own copy of this number for embedders that
// never touch internal/config.
func TestForgeEBSelectionReserveDefaultIsPinned(t *testing.T) {
	if DefaultForgeEBSelectionReserve != 300*time.Millisecond {
		t.Fatalf(
			"forgeEbSelectionReserve default drifted: %s",
			DefaultForgeEBSelectionReserve,
		)
	}
}

// TestGetConfigSnapshotDoesNotAliasForgeEBCaps: GetConfig hands out a
// snapshot, and callers treat it as their own. Pointer fields make that
// promise easy to break -- writing through a snapshot's cap would change
// the process-wide configuration every later reader sees, and race with
// them while doing it.
func TestGetConfigSnapshotDoesNotAliasForgeEBCaps(t *testing.T) {
	resetGlobalConfig()

	snapshot := GetConfig()
	if snapshot.ForgeEBMaxTxRefs == nil || snapshot.ForgeEBMaxBytes == nil {
		t.Fatal("snapshot must carry the default caps")
	}
	*snapshot.ForgeEBMaxTxRefs = 1
	*snapshot.ForgeEBMaxBytes = 2

	fresh := GetConfig()
	if got := *fresh.ForgeEBMaxTxRefs; got != DefaultForgeEBMaxTxRefs {
		t.Fatalf("global forgeEbMaxTxRefs changed to %d", got)
	}
	if got := *fresh.ForgeEBMaxBytes; got != DefaultForgeEBMaxBytes {
		t.Fatalf("global forgeEbMaxBytes changed to %d", got)
	}
}

// TestForgeEBCapFlagDefaultsShowTheEffectiveValue: --help is where an
// operator learns what omitting a flag does. Registering 0 there said
// "uncapped" while omitting the flag actually applies the backstop, which
// is the opposite of the truth for the one number that bounds a forged
// endorser block.
func TestForgeEBCapFlagDefaultsShowTheEffectiveValue(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)

	for name, want := range map[string]string{
		"forge-eb-max-tx-refs":       "20000",
		"forge-eb-max-bytes":         "25165824",
		"forge-eb-selection-reserve": "300ms",
	} {
		flag := cmd.PersistentFlags().Lookup(name)
		if flag == nil {
			t.Fatalf("flag %q is not registered", name)
		}
		if flag.DefValue != want {
			t.Fatalf("flag %q default = %q, want %q", name, flag.DefValue, want)
		}
	}
}

// TestForgeEBCapFlagsPreserveAnExplicitZero: the effective-default help
// text must not cost the operator the ability to switch a cap off, which
// is what a zero on the command line means.
func TestForgeEBCapFlagsPreserveAnExplicitZero(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.PersistentFlags().Parse([]string{
		"--forge-eb-max-tx-refs=0",
		"--forge-eb-max-bytes=0",
	}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	cfg := &Config{}
	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("apply flags: %v", err)
	}
	cfg.ApplyDefaults()

	if cfg.ForgeEBMaxTxRefs == nil || *cfg.ForgeEBMaxTxRefs != 0 {
		t.Fatalf(
			"--forge-eb-max-tx-refs=0 must disable the cap, got %v",
			cfg.ForgeEBMaxTxRefs,
		)
	}
	if cfg.ForgeEBMaxBytes == nil || *cfg.ForgeEBMaxBytes != 0 {
		t.Fatalf(
			"--forge-eb-max-bytes=0 must disable the cap, got %v",
			cfg.ForgeEBMaxBytes,
		)
	}
}

// TestForgeEBSelectionReserveFlagIsApplied closes the CLI half of the
// reserve's path: flag -> Config -> (buildDingoConfig, covered in
// internal/node) -> forger.
func TestForgeEBSelectionReserveFlagIsApplied(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.PersistentFlags().Parse([]string{
		"--forge-eb-selection-reserve=750ms",
	}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	cfg := &Config{}
	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("apply flags: %v", err)
	}
	cfg.ApplyDefaults()

	if cfg.ForgeEBSelectionReserve != 750*time.Millisecond {
		t.Fatalf(
			"forgeEbSelectionReserve = %s, want 750ms",
			cfg.ForgeEBSelectionReserve,
		)
	}
}
