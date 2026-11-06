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
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/bigintsum"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/dgraph-io/badger/v4"
	_ "modernc.org/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	data := flag.String("data-dir", "", "offline node data directory")
	snapshot := flag.String(
		"snapshot",
		"",
		"verified signed ledger snapshot file",
	)
	epoch := flag.Uint64("epoch", 0, "single reward epoch")
	apply := flag.Bool(
		"apply",
		false,
		"commit validated canonical stake inputs",
	)
	flag.Parse()
	if *data == "" || *snapshot == "" || *epoch == 0 {
		return errors.New("data-dir, snapshot and epoch required")
	}
	lock, err := badger.Open(
		badger.DefaultOptions(filepath.Join(*data, "blob")).
			WithReadOnly(true).
			WithLogger(nil),
	)
	if err != nil {
		return fmt.Errorf("exclusive node-writer guard: %w", err)
	}
	defer lock.Close()
	mode := "ro"
	if *apply {
		mode = "rw"
	}
	db, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(
			*data,
			"metadata.sqlite",
		)+"?mode="+mode+"&_pragma=busy_timeout(1000)",
	)
	if err != nil {
		return err
	}
	defer db.Close()
	var credits int
	if err := db.QueryRow("SELECT COUNT(*) FROM reward_credit_round WHERE snapshot_epoch = ?", *epoch).Scan(&credits); err != nil {
		return fmt.Errorf("credit guard: %w", err)
	}
	if credits != 0 {
		return errors.New("refusing repair of already credited round")
	}
	store, err := sqlstore.New(
		sqlstore.Config{
			WriteDB:             db,
			Dialect:             sqlstore.SQLiteDialect(),
			MaintenanceInterval: 1000 * time.Hour,
		},
	)
	if err != nil {
		return err
	}
	if err := store.Start(context.Background()); err != nil {
		return err
	}
	defer store.Close()
	state, err := ledgerstate.ParseSnapshot(*snapshot)
	if err != nil {
		return err
	}
	txn := store.ReadTransaction(context.Background())
	if *apply {
		_ = txn.Rollback()
		txn = store.Transaction(context.Background())
	}
	defer func() { _ = txn.Rollback() }()
	report, err := ledgerstate.RepairRewardStakeInputs(
		store,
		txn,
		state,
		*epoch,
		*apply,
	)
	if err != nil {
		return err
	}
	if *apply {
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}
