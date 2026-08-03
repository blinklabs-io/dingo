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

package drepquery

const sqliteUtxoStakingLiveAmountIndex = "idx_utxo_staking_deleted_amount"

type dialectSQL struct {
	castType         string
	active           string
	credentialParams string
	utxoJoin         string
}

func sqlDialect(name string) dialectSQL {
	switch name {
	case "postgres":
		return dialectSQL{"BIGINT", "true", "$1 AND ax.drep_type = $2", "JOIN utxo"}
	case "mysql":
		return dialectSQL{"UNSIGNED", "1", "? AND ax.drep_type = ?", "JOIN utxo"}
	default:
		return dialectSQL{
			"INTEGER",
			"1",
			"? AND ax.drep_type = ?",
			"JOIN utxo INDEXED BY " + sqliteUtxoStakingLiveAmountIndex,
		}
	}
}

// expiryClauses returns the paired CIP-0163 predicates. The positional form is
// used only by PostgreSQL's single-credential query, which deliberately reuses
// $3 in both query scopes.
func expiryClauses(expiryEpoch uint64, positional bool) (string, string) {
	if expiryEpoch == 0 {
		return "", ""
	}
	placeholder := "?"
	if positional {
		placeholder = "$3"
	}
	return " AND (ax.expiration_epoch = 0 OR ax.expiration_epoch >= " + placeholder + ")",
		" AND (a.expiration_epoch = 0 OR a.expiration_epoch >= " + placeholder + ")"
}

// VotingPowerSQL builds the single-credential voting-power query and its bind
// arguments in textual order.
func VotingPowerSQL(
	dialectName string,
	credentialTag uint8,
	drepCredential []byte,
	expiryEpoch uint64,
) (string, []any) {
	dialect := sqlDialect(dialectName)
	positional := dialectName == "postgres"
	innerExpiry, outerExpiry := expiryClauses(expiryEpoch, positional)
	outerParams := "? AND a.drep_type = ?"
	args := []any{drepCredential, credentialTag}
	if positional {
		outerParams = "$1 AND a.drep_type = $2"
		if expiryEpoch > 0 {
			args = append(args, expiryEpoch)
		}
	} else {
		if expiryEpoch > 0 {
			args = append(args, expiryEpoch)
		}
		args = append(args, drepCredential, credentialTag)
		if expiryEpoch > 0 {
			args = append(args, expiryEpoch)
		}
	}
	return `
		SELECT COALESCE(SUM(
				   COALESCE(u.utxo_sum, 0)
				   + COALESCE(CAST(a.reward AS ` + dialect.castType + `), 0)
			   ), 0)
		FROM account a
		LEFT JOIN (
			SELECT credential_tag, staking_key,
				   COALESCE(SUM(CAST(amount AS ` + dialect.castType + `)), 0) AS utxo_sum
			FROM utxo
			WHERE deleted_slot = 0
			  AND EXISTS (
				  SELECT 1 FROM account ax
				  WHERE ax.credential_tag = utxo.credential_tag
				    AND ax.staking_key = utxo.staking_key
				    AND ax.drep = ` + dialect.credentialParams + ` AND ax.active = ` + dialect.active + innerExpiry + `
			  )
			GROUP BY credential_tag, staking_key
		) u ON u.credential_tag = a.credential_tag
			AND u.staking_key = a.staking_key
		WHERE a.drep = ` + outerParams + ` AND a.active = ` + dialect.active + outerExpiry + `
	`, args
}

func collectionExpiry(expiryEpoch uint64) (string, string) {
	inner, outer := expiryClauses(expiryEpoch, false)
	if inner != "" {
		inner = inner[1:] + " "
		outer = outer[1:] + " "
	}
	return inner, outer
}

// CollectionArgs returns the bind order shared by batch and by-type queries:
// inner expiry, inner collection, outer expiry, outer collection.
func CollectionArgs(values any, expiryEpoch uint64) []any {
	args := make([]any, 0, 4)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, values)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	return append(args, values)
}

// VotingPowerBatchSQL builds the aggregate credential voting-power query.
func VotingPowerBatchSQL(dialectName string, expiryEpoch uint64) string {
	dialect := sqlDialect(dialectName)
	innerExpiry, outerExpiry := collectionExpiry(expiryEpoch)
	return `
	SELECT a.drep AS drep, a.drep_type AS credential_tag,
		   COALESCE(SUM(
			   COALESCE(u.utxo_sum, 0)
			   + COALESCE(CAST(a.reward AS ` + dialect.castType + `), 0)
		   ), 0) AS stake
	FROM account a
	LEFT JOIN (
			SELECT ax.drep_type, ax.credential_tag, ax.staking_key,
				   COALESCE(SUM(CAST(utxo.amount AS ` + dialect.castType + `)), 0) AS utxo_sum
			FROM account ax
			` + dialect.utxoJoin + `
			         ON utxo.credential_tag = ax.credential_tag
			         AND utxo.staking_key = ax.staking_key
			         AND utxo.deleted_slot = 0
		WHERE ax.active = ` + dialect.active + ` ` + innerExpiry + `AND ax.drep IN ?
		GROUP BY ax.drep_type, ax.credential_tag, ax.staking_key
	) u ON u.credential_tag = a.credential_tag
		AND u.staking_key = a.staking_key
		AND u.drep_type = a.drep_type
	WHERE a.active = ` + dialect.active + ` ` + outerExpiry + `AND a.drep IN ?
	GROUP BY a.drep, a.drep_type
`
}

// VotingPowerByTypeSQL builds the predefined-DRep voting-power query.
func VotingPowerByTypeSQL(dialectName string, expiryEpoch uint64) string {
	dialect := sqlDialect(dialectName)
	innerExpiry, outerExpiry := collectionExpiry(expiryEpoch)
	return `
		SELECT a.drep_type AS drep_type,
			   COALESCE(SUM(
				   COALESCE(u.utxo_sum, 0)
				   + COALESCE(CAST(a.reward AS ` + dialect.castType + `), 0)
			   ), 0) AS stake
		FROM account a
		LEFT JOIN (
			SELECT credential_tag, staking_key,
				   COALESCE(SUM(CAST(amount AS ` + dialect.castType + `)), 0) AS utxo_sum
			FROM utxo
			WHERE deleted_slot = 0
			  AND EXISTS (
				  SELECT 1 FROM account ax
				  WHERE ax.credential_tag = utxo.credential_tag
				    AND ax.staking_key = utxo.staking_key
				    AND ax.active = ` + dialect.active + ` ` + innerExpiry + `AND ax.drep_type IN ?
			  )
			GROUP BY credential_tag, staking_key
		) u ON u.credential_tag = a.credential_tag
			AND u.staking_key = a.staking_key
		WHERE a.active = ` + dialect.active + ` ` + outerExpiry + `AND a.drep_type IN ?
		GROUP BY a.drep_type
	`
}
