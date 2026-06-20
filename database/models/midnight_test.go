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

package models

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMidnightModelsAutoMigrate(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, db.AutoMigrate(
		&MidnightAssetCreate{},
		&MidnightAssetSpend{},
		&MidnightRegistration{},
		&MidnightDeregistration{},
		&MidnightGovernanceDatum{},
		&MidnightAriadneParams{},
		&MidnightEpochCandidates{},
	))

	migrator := db.Migrator()
	tests := []struct {
		model any
		table string
		index string
	}{
		{
			model: &MidnightAssetCreate{},
			table: "midnight_asset_creates",
			index: "idx_midnight_asset_creates_block_tx",
		},
		{
			model: &MidnightAssetSpend{},
			table: "midnight_asset_spends",
			index: "idx_midnight_asset_spends_block_tx",
		},
		{
			model: &MidnightRegistration{},
			table: "midnight_registrations",
			index: "idx_midnight_registrations_block_tx",
		},
		{
			model: &MidnightDeregistration{},
			table: "midnight_deregistrations",
			index: "idx_midnight_deregistrations_block_tx",
		},
		{
			model: &MidnightGovernanceDatum{},
			table: "midnight_governance_datums",
			index: "idx_midnight_governance_datums_latest",
		},
		{
			model: &MidnightAriadneParams{},
			table: "midnight_ariadne_params",
			index: "idx_midnight_ariadne_params_epoch",
		},
		{
			model: &MidnightEpochCandidates{},
			table: "midnight_epoch_candidates",
			index: "idx_midnight_epoch_candidates_epoch",
		},
	}

	for _, tt := range tests {
		require.True(t, migrator.HasTable(tt.model), "%s table missing", tt.table)
		require.True(
			t,
			migrator.HasIndex(tt.model, tt.index),
			"%s index missing",
			tt.index,
		)
	}
}

func TestMigrateModelsIncludesMidnightModels(t *testing.T) {
	models := map[reflect.Type]bool{}
	for _, model := range MigrateModels {
		models[reflect.TypeOf(model)] = true
	}

	require.True(t, models[reflect.TypeOf(&MidnightAssetCreate{})])
	require.True(t, models[reflect.TypeOf(&MidnightAssetSpend{})])
	require.True(t, models[reflect.TypeOf(&MidnightRegistration{})])
	require.True(t, models[reflect.TypeOf(&MidnightDeregistration{})])
	require.True(t, models[reflect.TypeOf(&MidnightGovernanceDatum{})])
	require.True(t, models[reflect.TypeOf(&MidnightAriadneParams{})])
	require.True(t, models[reflect.TypeOf(&MidnightEpochCandidates{})])
}
