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

import "github.com/blinklabs-io/dingo/database/models"

// SQL fragments over the credential and datum dictionaries. utxo and
// address_transaction name a credential by id; every reader that reports or
// compares the hash goes through these.
const (
	// credentialIDSubquery selects the id of the credential hash bound to its
	// placeholder.
	credentialIDSubquery = models.CredentialIDSubquery

	// utxoStakingKeyExpr, utxoPaymentKeyExpr and utxoDatumHashExpr project the
	// hashes of an output's credentials and datum from the dictionaries by id.
	utxoStakingKeyExpr = "(SELECT c.hash FROM credential AS c " +
		"WHERE c.id = utxo.staking_key_id)"
	utxoPaymentKeyExpr = "(SELECT c.hash FROM credential AS c " +
		"WHERE c.id = utxo.payment_key_id)"
	utxoDatumHashExpr = "(SELECT d.hash FROM datum AS d " +
		"WHERE d.id = utxo.datum_id)"
)

// uCredentialHash projects the hash of the credential that column of an
// output aliased u names.
func uCredentialHash(column string) string {
	return "(SELECT c.hash FROM credential AS c WHERE c.id = u." + column + ")"
}
