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

package models

import (
	"bytes"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestBackfillAccountCreatedSlotSkipsNullStakingKey(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&Account{}); err != nil {
		t.Fatalf("migrate account: %v", err)
	}
	if err := db.Exec(`
		CREATE TABLE stake_registration (
			credential_tag integer,
			staking_key blob,
			added_slot integer
		)`).Error; err != nil {
		t.Fatalf("create legacy registration table: %v", err)
	}
	if err := db.Exec(`
		INSERT INTO stake_registration
			(credential_tag, staking_key, added_slot)
		VALUES (0, NULL, 10)`).Error; err != nil {
		t.Fatalf("insert null staking key: %v", err)
	}

	queryCount := 0
	const callbackName = "test:bound_account_created_slot_scan"
	if err := db.Callback().Row().Before("gorm:row").Register(
		callbackName,
		func(tx *gorm.DB) {
			queryCount++
			if queryCount > 1 {
				tx.AddError(errors.New("registration page repeated"))
			}
		},
	); err != nil {
		t.Fatalf("register query callback: %v", err)
	}

	if _, err := backfillAccountCreatedSlotFromTable(
		db,
		"stake_registration",
		nil,
	); err != nil {
		t.Fatalf("backfill null staking key: %v", err)
	}
	if queryCount != 1 {
		t.Fatalf("registration queries = %d, want 1", queryCount)
	}
}

func TestAccount_String(t *testing.T) {
	tests := []struct {
		name         string
		wantErrMsg   string
		expectedHRP  string
		stakingKey   []byte
		wantErr      bool
		validateAddr bool
	}{
		{
			name: "valid staking key - 28 bytes",
			// Sample 28-byte staking key hash
			stakingKey: hexDecode(
				"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
			),
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "valid staking key - 32 bytes",
			// Sample 32-byte staking key hash
			stakingKey: hexDecode(
				"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b12345678",
			),
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name:       "empty staking key",
			stakingKey: []byte{},
			wantErr:    true,
			wantErrMsg: "staking key is empty",
		},
		{
			name:       "nil staking key",
			stakingKey: nil,
			wantErr:    true,
			wantErrMsg: "staking key is empty",
		},
		{
			name: "single byte staking key",
			// Edge case with minimal data
			stakingKey:   []byte{0x01},
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "all zeros staking key",
			// Edge case with all zeros
			stakingKey:   make([]byte, 28),
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "all ones staking key",
			// Edge case with all 0xFF
			stakingKey: []byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff,
			},
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Account{
				StakingKey: tt.stakingKey,
			}
			got, err := a.String()
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Account.String() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Errorf("Account.String() expected error but got none")
					return
				}
				if tt.wantErrMsg != "" &&
					!strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Errorf(
						"Account.String() error = %v, want error containing %q",
						err,
						tt.wantErrMsg,
					)
				}
				return
			}
			if err != nil {
				t.Errorf("Account.String() unexpected error: %v", err)
				return
			}
			if got == "" {
				t.Errorf(
					"Account.String() returned empty string for valid input",
				)
				return
			}
			if tt.validateAddr {
				// Validate the returned bech32 address
				hrp, data, err := bech32.Decode(got)
				if err != nil {
					t.Errorf(
						"Account.String() returned invalid bech32 address: %v",
						err,
					)
					return
				}
				if hrp != tt.expectedHRP {
					t.Errorf(
						"Account.String() HRP = %v, want %v",
						hrp,
						tt.expectedHRP,
					)
				}
				// Convert back and verify we get the original data
				converted, err := bech32.ConvertBits(data, 5, 8, false)
				if err != nil {
					t.Errorf("Failed to convert address back to bytes: %v", err)
					return
				}
				if !bytes.Equal(converted, tt.stakingKey) {
					t.Errorf(
						"Account.String() round-trip failed: got %x, want %x",
						converted,
						tt.stakingKey,
					)
				}
			}
		})
	}
}

func TestAccount_String_Idempotent(t *testing.T) {
	// Test that calling String() multiple times returns the same result
	stakingKey := hexDecode(
		"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)
	a := &Account{
		StakingKey: stakingKey,
	}

	result1, err := a.String()
	if err != nil {
		t.Fatalf("Account.String() first call error: %v", err)
	}

	result2, err := a.String()
	if err != nil {
		t.Fatalf("Account.String() second call error: %v", err)
	}

	if result1 != result2 {
		t.Errorf(
			"Account.String() not idempotent: first=%s, second=%s",
			result1,
			result2,
		)
	}
}

func TestAccount_String_DifferentKeys(t *testing.T) {
	// Test that different staking keys produce different addresses
	key1 := hexDecode(
		"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)
	key2 := hexDecode(
		"a1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)

	a1 := &Account{StakingKey: key1}
	a2 := &Account{StakingKey: key2}

	addr1, err := a1.String()
	if err != nil {
		t.Fatalf("Account.String() for key1 error: %v", err)
	}

	addr2, err := a2.String()
	if err != nil {
		t.Fatalf("Account.String() for key2 error: %v", err)
	}

	if addr1 == addr2 {
		t.Errorf(
			"Account.String() produced same address for different keys: %s",
			addr1,
		)
	}
}

func TestMigrateAccountRewardDeltaTxHashNormalization(t *testing.T) {
	tests := []struct {
		name               string
		createTableSQL     string
		createIndexSQL     string
		insertNullRowSQL   string
		insertNullAfterSQL string
		migrate            func(*gorm.DB) error
	}{
		{
			name: "credential tag migration",
			createTableSQL: `
				CREATE TABLE account_reward_delta (
					id integer primary key autoincrement,
					staking_key blob NOT NULL,
					tx_hash blob,
					amount text NOT NULL,
					previous_reward text,
					added_slot integer NOT NULL,
					withdrawal numeric NOT NULL DEFAULT false
				)`,
			createIndexSQL: `
				CREATE UNIQUE INDEX idx_account_reward_delta_w_tx_s
				ON account_reward_delta (withdrawal, tx_hash, staking_key)`,
			insertNullRowSQL: `
				INSERT INTO account_reward_delta
					(staking_key, tx_hash, amount, previous_reward, added_slot, withdrawal)
				VALUES (x'010203', NULL, '5', '0', 10, false)`,
			insertNullAfterSQL: `
				INSERT INTO account_reward_delta
					(staking_key, tx_hash, amount, previous_reward, added_slot, withdrawal)
				VALUES (x'040506', NULL, '7', '0', 11, false)`,
			migrate: func(db *gorm.DB) error {
				return MigrateAccountRewardDeltaCredentialTagIndex(db, nil)
			},
		},
		{
			name: "slot migration",
			createTableSQL: `
				CREATE TABLE account_reward_delta (
					id integer primary key autoincrement,
					staking_key blob NOT NULL,
					credential_tag integer NOT NULL DEFAULT 0,
					tx_hash blob,
					amount text NOT NULL,
					previous_reward text,
					added_slot integer NOT NULL,
					withdrawal numeric NOT NULL DEFAULT false
				)`,
			createIndexSQL: `
				CREATE UNIQUE INDEX idx_account_reward_delta_w_tx_s
				ON account_reward_delta
					(withdrawal, tx_hash, credential_tag, staking_key)`,
			insertNullRowSQL: `
				INSERT INTO account_reward_delta
					(staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot, withdrawal)
				VALUES (x'010203', 0, NULL, '5', '0', 10, false)`,
			insertNullAfterSQL: `
				INSERT INTO account_reward_delta
					(staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot, withdrawal)
				VALUES (x'040506', 0, NULL, '7', '0', 11, false)`,
			migrate: func(db *gorm.DB) error {
				return MigrateAccountRewardDeltaSlotIndex(db, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			if err != nil {
				t.Fatalf("open sqlite: %v", err)
			}
			if err := db.Exec(tt.createTableSQL).Error; err != nil {
				t.Fatalf("create legacy table: %v", err)
			}
			if err := db.Exec(tt.createIndexSQL).Error; err != nil {
				t.Fatalf("create legacy index: %v", err)
			}
			if err := db.Exec(tt.insertNullRowSQL).Error; err != nil {
				t.Fatalf("insert legacy null tx_hash row: %v", err)
			}

			if err := tt.migrate(db); err != nil {
				t.Fatalf("migrate: %v", err)
			}

			var nullCount int64
			if err := db.Table("account_reward_delta").
				Where("tx_hash IS NULL").
				Count(&nullCount).Error; err != nil {
				t.Fatalf("count null tx_hash rows: %v", err)
			}
			if nullCount != 0 {
				t.Fatalf("null tx_hash rows = %d, want 0", nullCount)
			}
			if err := db.Exec(tt.insertNullAfterSQL).Error; err == nil {
				t.Fatal("expected tx_hash NOT NULL constraint after migration")
			}
		})
	}
}

func TestAccount_String_Bech32Format(t *testing.T) {
	// Test that the output follows bech32 format conventions
	stakingKey := hexDecode(
		"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)
	a := &Account{
		StakingKey: stakingKey,
	}

	addr, err := a.String()
	if err != nil {
		t.Fatalf("Account.String() error: %v", err)
	}

	// Check that address starts with "stake"
	if !strings.HasPrefix(addr, "stake") {
		t.Errorf(
			"Account.String() address doesn't start with 'stake': %s",
			addr,
		)
	}

	// Check that address contains the separator '1'
	if !strings.Contains(addr, "1") {
		t.Errorf(
			"Account.String() address doesn't contain separator '1': %s",
			addr,
		)
	}

	// Check that address only contains valid bech32 characters (lowercase alphanumeric, no '1', 'b', 'i', 'o')
	_, datapart, _ := strings.Cut(addr, "1")
	validChars := "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
	for _, c := range datapart {
		if !strings.ContainsRune(validChars, c) {
			t.Errorf(
				"Account.String() address contains invalid bech32 character: %c in %s",
				c,
				addr,
			)
		}
	}
}

func TestAccountWithdrawalWitnessCredentialSlotIndex(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&AccountWithdrawalWitness{}); err != nil {
		t.Fatalf("migrate account withdrawal witness: %v", err)
	}

	indexes, err := db.Migrator().GetIndexes(&AccountWithdrawalWitness{})
	if err != nil {
		t.Fatalf("get account withdrawal witness indexes: %v", err)
	}
	for _, index := range indexes {
		if index.Name() != "idx_account_withdrawal_witness_credential_slot" {
			continue
		}
		columns := index.Columns()
		want := []string{"staking_key", "credential_tag", "added_slot"}
		if !slices.Equal(columns, want) {
			t.Fatalf(
				"credential-slot index columns = %v, want %v",
				columns,
				want,
			)
		}
		return
	}
	t.Fatal("credential-slot index missing after AutoMigrate")
}

func TestAccountInactivityActivationCompositeIdentity(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&AccountInactivityActivation{}); err != nil {
		t.Fatalf("migrate account inactivity activation: %v", err)
	}

	key := bytes.Repeat([]byte{0xA5}, 28)
	otherKey := bytes.Repeat([]byte{0x5A}, 28)
	if err := db.Create(&[]AccountInactivityActivation{
		{CredentialTag: 0, StakingKey: key},
		{CredentialTag: 1, StakingKey: key},
		{CredentialTag: 0, StakingKey: otherKey},
	}).Error; err != nil {
		t.Fatalf("insert composite-distinct activation members: %v", err)
	}
	if err := db.Create(&AccountInactivityActivation{
		CredentialTag: 0,
		StakingKey:    key,
	}).Error; err == nil {
		t.Fatal("duplicate composite activation member unexpectedly succeeded")
	}
}

// Helper functions

func hexDecode(s string) []byte {
	data, err := hex.DecodeString(s)
	if err != nil {
		panic("invalid hex in test data: " + err.Error())
	}
	return data
}
