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
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// SetRegistrationDrep saves a registration drep certificate
func (d *Database) SetRegistrationDrep(
	cert *lcommon.RegistrationDrepCertificate,
	slot uint64,
	deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetRegistrationDrep(
		cert,
		slot,
		types.Uint64(deposit),
		txn.Metadata(),
	)
}

// SetDeregistrationDrep saves a deregistration drep certificate
func (d *Database) SetDeregistrationDrep(
	cert *lcommon.DeregistrationDrepCertificate,
	slot uint64,
	deposit uint64,
	txn *Txn,
) error {
	return d.metadata.SetDeregistrationDrep(
		cert,
		slot,
		types.Uint64(deposit),
		txn.Metadata(),
	)
}

// SetUpdateDrep saves an update drep certificate
func (d *Database) SetUpdateDrep(
	cert *lcommon.UpdateDrepCertificate,
	slot uint64,
	txn *Txn,
) error {
	return d.metadata.SetUpdateDrep(
		cert,
		slot,
		txn.Metadata(),
	)
}
