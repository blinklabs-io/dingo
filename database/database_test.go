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

package database_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"gorm.io/gorm"
)

type TestTable struct {
	gorm.Model
}

var dbConfig = &database.Config{
	BlobCacheSize: 1 << 20,
	Logger:        nil,
	PromRegistry:  nil,
	DataDir:       "",
}

// TestInMemorySqliteMultipleTransaction tests that our sqlite connection allows multiple
// concurrent transactions when using in-memory mode. This requires special URI flags, and
// this is mostly making sure that we don't lose them
func TestInMemorySqliteMultipleTransaction(t *testing.T) {
	var db *database.Database
	doQuery := func(sleep time.Duration) error {
		txn := db.Metadata().Transaction()
		if result := txn.First(&TestTable{}); result.Error != nil {
			return result.Error
		}
		time.Sleep(sleep)
		if result := txn.Commit(); result.Error != nil {
			return result.Error
		}
		return nil
	}
	db, err := database.New(dbConfig)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := db.Metadata().DB().AutoMigrate(&TestTable{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if result := db.Metadata().DB().Create(&TestTable{}); result.Error != nil {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	// The linter calls us on the lack of error checking, but it's a goroutine...
	//nolint:errcheck
	go doQuery(5 * time.Second)
	time.Sleep(1 * time.Second)
	if err := doQuery(0); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}
