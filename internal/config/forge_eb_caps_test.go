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

import "testing"

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
