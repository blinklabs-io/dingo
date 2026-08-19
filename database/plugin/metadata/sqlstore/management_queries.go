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

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	mysqlquery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/mysql"
	postgresquery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/postgres"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
)

type managementQueries interface {
	getCommitTimestamp(context.Context) (sql.NullInt64, error)
	setCommitTimestamp(context.Context, sql.NullInt64) error
	getNodeSettings(context.Context) (string, string, error)
	insertNodeSettings(context.Context, string, string) (int64, error)
	backfillNodeSettingsNetwork(
		context.Context,
		string,
		string,
	) (int64, error)
	getNodeSettingsGates(context.Context) (map[string]string, error)
	upsertNodeSettingsGate(
		context.Context,
		string,
		string,
		int64,
		int64,
	) error
	insertNodeSettingsGateIfAbsent(
		context.Context,
		string,
		string,
		int64,
		int64,
	) (int64, error)
}

func newManagementQueries(
	dialectName string,
	db queryer,
) (managementQueries, error) {
	switch dialectName {
	case "sqlite":
		return sqliteManagementQueries{sqlitequery.New(db)}, nil
	case "postgres":
		return postgresManagementQueries{postgresquery.New(db)}, nil
	case "mysql":
		return mysqlManagementQueries{mysqlquery.New(db)}, nil
	default:
		return nil, fmt.Errorf(
			"sqlstore: unsupported dialect %q",
			dialectName,
		)
	}
}

type sqliteManagementQueries struct {
	queries *sqlitequery.Queries
}

func (q sqliteManagementQueries) getCommitTimestamp(
	ctx context.Context,
) (sql.NullInt64, error) {
	return q.queries.GetCommitTimestamp(ctx)
}

func (q sqliteManagementQueries) setCommitTimestamp(
	ctx context.Context,
	timestamp sql.NullInt64,
) error {
	return q.queries.SetCommitTimestamp(ctx, timestamp)
}

func (q sqliteManagementQueries) getNodeSettings(
	ctx context.Context,
) (string, string, error) {
	row, err := q.queries.GetNodeSettings(ctx)
	return row.StorageMode, row.Network, err
}

func (q sqliteManagementQueries) insertNodeSettings(
	ctx context.Context,
	storageMode string,
	network string,
) (int64, error) {
	return q.queries.InsertNodeSettings(
		ctx,
		sqlitequery.InsertNodeSettingsParams{
			StorageMode: storageMode,
			Network:     network,
		},
	)
}

func (q sqliteManagementQueries) backfillNodeSettingsNetwork(
	ctx context.Context,
	network string,
	storageMode string,
) (int64, error) {
	return q.queries.BackfillNodeSettingsNetwork(
		ctx,
		sqlitequery.BackfillNodeSettingsNetworkParams{
			Network:     network,
			StorageMode: storageMode,
		},
	)
}

func (q sqliteManagementQueries) getNodeSettingsGates(
	ctx context.Context,
) (map[string]string, error) {
	rows, err := q.queries.GetNodeSettingsGates(ctx)
	if err != nil {
		return nil, err
	}
	gates := make(map[string]string, len(rows))
	for _, row := range rows {
		gates[row.Name] = row.Value
	}
	return gates, nil
}

func (q sqliteManagementQueries) upsertNodeSettingsGate(
	ctx context.Context,
	name string,
	value string,
	recordedEpoch int64,
	recordedSlot int64,
) error {
	return q.queries.UpsertNodeSettingsGate(
		ctx,
		sqlitequery.UpsertNodeSettingsGateParams{
			Name:          name,
			Value:         value,
			RecordedEpoch: recordedEpoch,
			RecordedSlot:  recordedSlot,
		},
	)
}

func (q sqliteManagementQueries) insertNodeSettingsGateIfAbsent(
	ctx context.Context,
	name string,
	value string,
	recordedEpoch int64,
	recordedSlot int64,
) (int64, error) {
	return q.queries.InsertNodeSettingsGateIfAbsent(
		ctx,
		sqlitequery.InsertNodeSettingsGateIfAbsentParams{
			Name:          name,
			Value:         value,
			RecordedEpoch: recordedEpoch,
			RecordedSlot:  recordedSlot,
		},
	)
}

type postgresManagementQueries struct {
	queries *postgresquery.Queries
}

func (q postgresManagementQueries) getCommitTimestamp(
	ctx context.Context,
) (sql.NullInt64, error) {
	return q.queries.GetCommitTimestamp(ctx)
}

func (q postgresManagementQueries) setCommitTimestamp(
	ctx context.Context,
	timestamp sql.NullInt64,
) error {
	return q.queries.SetCommitTimestamp(ctx, timestamp)
}

func (q postgresManagementQueries) getNodeSettings(
	ctx context.Context,
) (string, string, error) {
	row, err := q.queries.GetNodeSettings(ctx)
	return row.StorageMode, row.Network, err
}

func (q postgresManagementQueries) insertNodeSettings(
	ctx context.Context,
	storageMode string,
	network string,
) (int64, error) {
	return q.queries.InsertNodeSettings(
		ctx,
		postgresquery.InsertNodeSettingsParams{
			StorageMode: storageMode,
			Network:     network,
		},
	)
}

func (q postgresManagementQueries) backfillNodeSettingsNetwork(
	ctx context.Context,
	network string,
	storageMode string,
) (int64, error) {
	return q.queries.BackfillNodeSettingsNetwork(
		ctx,
		postgresquery.BackfillNodeSettingsNetworkParams{
			Network:     network,
			StorageMode: storageMode,
		},
	)
}

func (q postgresManagementQueries) getNodeSettingsGates(
	ctx context.Context,
) (map[string]string, error) {
	rows, err := q.queries.GetNodeSettingsGates(ctx)
	if err != nil {
		return nil, err
	}
	gates := make(map[string]string, len(rows))
	for _, row := range rows {
		gates[row.Name] = row.Value
	}
	return gates, nil
}

func (q postgresManagementQueries) upsertNodeSettingsGate(
	ctx context.Context,
	name string,
	value string,
	recordedEpoch int64,
	recordedSlot int64,
) error {
	return q.queries.UpsertNodeSettingsGate(
		ctx,
		postgresquery.UpsertNodeSettingsGateParams{
			Name:          name,
			Value:         value,
			RecordedEpoch: recordedEpoch,
			RecordedSlot:  recordedSlot,
		},
	)
}

func (q postgresManagementQueries) insertNodeSettingsGateIfAbsent(
	ctx context.Context,
	name string,
	value string,
	recordedEpoch int64,
	recordedSlot int64,
) (int64, error) {
	return q.queries.InsertNodeSettingsGateIfAbsent(
		ctx,
		postgresquery.InsertNodeSettingsGateIfAbsentParams{
			Name:          name,
			Value:         value,
			RecordedEpoch: recordedEpoch,
			RecordedSlot:  recordedSlot,
		},
	)
}

type mysqlManagementQueries struct {
	queries *mysqlquery.Queries
}

func (q mysqlManagementQueries) getCommitTimestamp(
	ctx context.Context,
) (sql.NullInt64, error) {
	return q.queries.GetCommitTimestamp(ctx)
}

func (q mysqlManagementQueries) setCommitTimestamp(
	ctx context.Context,
	timestamp sql.NullInt64,
) error {
	return q.queries.SetCommitTimestamp(ctx, timestamp)
}

func (q mysqlManagementQueries) getNodeSettings(
	ctx context.Context,
) (string, string, error) {
	row, err := q.queries.GetNodeSettings(ctx)
	return row.StorageMode, row.Network, err
}

func (q mysqlManagementQueries) insertNodeSettings(
	ctx context.Context,
	storageMode string,
	network string,
) (int64, error) {
	return q.queries.InsertNodeSettings(
		ctx,
		mysqlquery.InsertNodeSettingsParams{
			StorageMode: storageMode,
			Network:     network,
		},
	)
}

func (q mysqlManagementQueries) backfillNodeSettingsNetwork(
	ctx context.Context,
	network string,
	storageMode string,
) (int64, error) {
	return q.queries.BackfillNodeSettingsNetwork(
		ctx,
		mysqlquery.BackfillNodeSettingsNetworkParams{
			Network:     network,
			StorageMode: storageMode,
		},
	)
}

func (q mysqlManagementQueries) getNodeSettingsGates(
	ctx context.Context,
) (map[string]string, error) {
	rows, err := q.queries.GetNodeSettingsGates(ctx)
	if err != nil {
		return nil, err
	}
	gates := make(map[string]string, len(rows))
	for _, row := range rows {
		gates[row.Name] = row.Value
	}
	return gates, nil
}

func (q mysqlManagementQueries) upsertNodeSettingsGate(
	ctx context.Context,
	name string,
	value string,
	recordedEpoch int64,
	recordedSlot int64,
) error {
	return q.queries.UpsertNodeSettingsGate(
		ctx,
		mysqlquery.UpsertNodeSettingsGateParams{
			Name:          name,
			Value:         value,
			RecordedEpoch: recordedEpoch,
			RecordedSlot:  recordedSlot,
		},
	)
}

func (q mysqlManagementQueries) insertNodeSettingsGateIfAbsent(
	ctx context.Context,
	name string,
	value string,
	recordedEpoch int64,
	recordedSlot int64,
) (int64, error) {
	return q.queries.InsertNodeSettingsGateIfAbsent(
		ctx,
		mysqlquery.InsertNodeSettingsGateIfAbsentParams{
			Name:          name,
			Value:         value,
			RecordedEpoch: recordedEpoch,
			RecordedSlot:  recordedSlot,
		},
	)
}
