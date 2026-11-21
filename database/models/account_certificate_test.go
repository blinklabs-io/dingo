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

package models_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestApplyAccountCertificate(t *testing.T) {
	t.Parallel()
	pool, pool2, drep := []byte{1}, []byte{2}, []byte{3}
	active := models.AccountDelegation{
		Pool: pool, Drep: drep, DrepType: models.DrepTypeAddrKeyHash, Active: true,
	}
	inactive := active
	inactive.Active = false
	for _, tc := range []struct {
		name string
		prev *models.AccountDelegation
		next models.AccountDelegation
		want models.AccountDelegation
	}{
		{"registration of a new credential", nil,
			models.AccountDelegation{Active: true},
			models.AccountDelegation{Active: true}},
		{"registration keeps an active account's delegations", &active,
			models.AccountDelegation{Active: true}, active},
		{"re-registration of an inactive account starts empty", &inactive,
			models.AccountDelegation{Active: true},
			models.AccountDelegation{Active: true}},
		{"deregistration clears delegations", &active,
			models.AccountDelegation{},
			models.AccountDelegation{}},
		{"pool delegation replaces the pool only", &active,
			models.AccountDelegation{Pool: pool2, Active: true},
			models.AccountDelegation{
				Pool: pool2, Drep: drep, DrepType: models.DrepTypeAddrKeyHash, Active: true,
			}},
		{"always-abstain replaces the DRep", &active,
			models.AccountDelegation{DrepType: models.DrepTypeAlwaysAbstain, Active: true},
			models.AccountDelegation{
				Pool: pool, DrepType: models.DrepTypeAlwaysAbstain, Active: true,
			}},
	} {
		require.Equal(t, tc.want, models.ApplyAccountCertificate(tc.prev, tc.next), tc.name)
	}
}
