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

package koios_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/koios"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestKoiosParityCheck compares Dingo's closed-epoch reward state against the
// Koios public API for the configured network.
//
// This test is intentionally skipped in CI. Run it manually when validating
// reward state on a live preview or preprod node:
//
//	KOIOS_PARITY_TEST=1 \
//	DINGO_DATA_DIR=/path/to/dingo/data \
//	KOIOS_NETWORK=preview \
//	go test ./internal/test/koios/ -v -run TestKoiosParityCheck -timeout 60m
//
// Environment variables:
//
//	KOIOS_PARITY_TEST  required; any non-empty value enables the test
//	DINGO_DATA_DIR     path to the Dingo data directory (contains metadata.sqlite)
//	KOIOS_NETWORK      preview (default) or preprod
//	KOIOS_API_KEY      optional Koios Bearer token for higher rate limits
//	KOIOS_MAX_POOLS    max pool inputs sampled per epoch (default 10)
//	KOIOS_MAX_ACCOUNTS max stake accounts sampled for balance checks (default 50)
func TestKoiosParityCheck(t *testing.T) {
	if os.Getenv("KOIOS_PARITY_TEST") == "" {
		t.Skip(
			"set KOIOS_PARITY_TEST=1 to enable; " +
				"requires a running or recently synced Dingo node",
		)
	}

	dataDir := os.Getenv("DINGO_DATA_DIR")
	if dataDir == "" {
		t.Fatal("DINGO_DATA_DIR must point to the Dingo data directory")
	}

	network := os.Getenv("KOIOS_NETWORK")
	if network == "" {
		network = koios.NetworkPreview
	}
	if network != koios.NetworkPreview && network != koios.NetworkPreprod {
		t.Fatalf(
			"KOIOS_NETWORK must be %q or %q, got %q",
			koios.NetworkPreview, koios.NetworkPreprod, network,
		)
	}

	apiKey := os.Getenv("KOIOS_API_KEY") // optional

	maxPoolsPerEpoch := 10
	maxAccounts := 50
	if v := os.Getenv("KOIOS_MAX_POOLS"); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &maxPoolsPerEpoch); n != 1 || err != nil {
			t.Fatalf("invalid KOIOS_MAX_POOLS %q: %v", v, err)
		}
		if maxPoolsPerEpoch < 0 {
			t.Fatalf("KOIOS_MAX_POOLS must be >= 0, got %d", maxPoolsPerEpoch)
		}
	}
	if v := os.Getenv("KOIOS_MAX_ACCOUNTS"); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &maxAccounts); n != 1 || err != nil {
			t.Fatalf("invalid KOIOS_MAX_ACCOUNTS %q: %v", v, err)
		}
		if maxAccounts < 0 {
			t.Fatalf("KOIOS_MAX_ACCOUNTS must be >= 0, got %d", maxAccounts)
		}
	}

	dbPath := filepath.Join(dataDir, "metadata.sqlite")
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("cannot access Dingo database at %s: %v", dbPath, err)
	}

	// Open read-only so the parity test cannot mutate production state.
	db, err := gorm.Open(
		sqlite.Open("file:"+dbPath+"?mode=ro&_journal=WAL"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	if err != nil {
		t.Fatalf("open Dingo database: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sqlDB.Close() }()

	checker, err := koios.NewChecker(db, network, apiKey)
	if err != nil {
		t.Fatalf("create parity checker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	t.Logf("running Koios parity check: network=%s db=%s", network, dbPath)
	t.Logf("sampling: max_pools_per_epoch=%d max_accounts=%d", maxPoolsPerEpoch, maxAccounts)

	report, err := checker.Run(ctx, maxPoolsPerEpoch, maxAccounts)
	if err != nil {
		t.Fatalf("parity check error: %v", err)
	}

	t.Logf("epochs checked:   %d", report.EpochsChecked)
	t.Logf("pools checked:    %d", report.PoolsChecked)
	t.Logf("accounts checked: %d", report.AccountsChecked)
	t.Logf("total mismatches: %d", len(report.Mismatches))

	// Print all mismatches regardless of pass/fail so the caller can triage.
	for i, m := range report.Mismatches {
		t.Logf("mismatch[%d]: %s", i, m)
	}

	if len(report.Mismatches) > 0 {
		t.Errorf(
			"found %d reward-state mismatch(es) between Dingo and Koios — "+
				"see mismatch lines above for details",
			len(report.Mismatches),
		)
	}
}
