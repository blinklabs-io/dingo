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

package database

import (
	"testing"
)

// BenchmarkTransactionCreate benchmarks creating a read-only transaction
func BenchmarkTransactionCreate(b *testing.B) {
	// Create a temporary database
	config := &Config{
		DataDir: "", // In-memory
	}
	db, err := New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer() // Reset timer after setup
	for b.Loop() {
		txn := db.Transaction(false)
		if err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
