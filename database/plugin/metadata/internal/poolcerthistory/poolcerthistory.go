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

// Package poolcerthistory centralizes the pool-certificate-history query
// shared by the sqlite, postgres, and mysql metadata store backends. The
// three backends differ only in how the "transaction" table is quoted
// (sqldialect.TransactionTableName), so keeping the query itself in one
// place prevents that single quoting difference from drifting into three
// independently-maintained copies.
package poolcerthistory

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/sqldialect"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetPoolCertificateHistory returns the transaction hashes of a pool's
// registration and retirement certificates, ordered chronologically
// (added_slot, block_index, cert_index ascending). The inner joins to certs
// and transaction naturally exclude rows with no linked transaction —
// certificates synthesized by the Mithril ledger-state import carry
// certificate_id = 0, which matches no certs row — since they have no
// originating transaction to report. Both pool_registration.pool_key_hash
// and pool_retirement.pool_key_hash are indexed, so each query is a small,
// indexed lookup rather than a table scan.
func GetPoolCertificateHistory(
	db *gorm.DB,
	pkh lcommon.PoolKeyHash,
) ([][]byte, [][]byte, error) {
	transactionTable := sqldialect.TransactionTableName(db)

	type certTxRow struct {
		TxHash []byte
	}

	var regRows []certTxRow
	if err := db.Table("pool_registration").
		Select(transactionTable+".hash AS tx_hash").
		Joins("JOIN certs ON certs.id = pool_registration.certificate_id").
		Joins("JOIN "+transactionTable+" ON "+transactionTable+".id = certs.transaction_id").
		Where("pool_registration.pool_key_hash = ?", pkh.Bytes()).
		Order(
			"pool_registration.added_slot ASC, " +
				transactionTable + ".block_index ASC, " +
				"certs.cert_index ASC",
		).
		Find(&regRows).Error; err != nil {
		return nil, nil, fmt.Errorf(
			"GetPoolCertificateHistory: query registrations: %w", err,
		)
	}

	var retRows []certTxRow
	if err := db.Table("pool_retirement").
		Select(transactionTable+".hash AS tx_hash").
		Joins("JOIN certs ON certs.id = pool_retirement.certificate_id").
		Joins("JOIN "+transactionTable+" ON "+transactionTable+".id = certs.transaction_id").
		Where("pool_retirement.pool_key_hash = ?", pkh.Bytes()).
		Order(
			"pool_retirement.added_slot ASC, " +
				transactionTable + ".block_index ASC, " +
				"certs.cert_index ASC",
		).
		Find(&retRows).Error; err != nil {
		return nil, nil, fmt.Errorf(
			"GetPoolCertificateHistory: query retirements: %w", err,
		)
	}

	registrationTxHashes := make([][]byte, 0, len(regRows))
	for _, row := range regRows {
		registrationTxHashes = append(registrationTxHashes, row.TxHash)
	}
	retirementTxHashes := make([][]byte, 0, len(retRows))
	for _, row := range retRows {
		retirementTxHashes = append(retirementTxHashes, row.TxHash)
	}
	return registrationTxHashes, retirementTxHashes, nil
}
