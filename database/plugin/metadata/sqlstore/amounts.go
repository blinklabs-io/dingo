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
	"errors"
	"fmt"
	"math/big"
	"strconv"
)

// sumUint64Rows sums a decimal column in Go instead of asking SQL to coerce
// it to a signed integer. Metadata amounts are persisted as decimal text and
// may occupy the full uint64 range, while SUM/INTEGER is signed (and differs
// across SQLite, PostgreSQL, and MySQL).
func sumUint64Rows(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) (uint64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total uint64
	for rows.Next() {
		var raw any
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		text, err := decimalAmountText(raw)
		if err != nil {
			return 0, err
		}
		if text == "" {
			continue
		}
		value, err := parseUint64("amount", text)
		if err != nil {
			return 0, err
		}
		if ^uint64(0)-total < value {
			return 0, errors.New("amount sum overflow")
		}
		total += value
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return total, nil
}

// decimalAmountText normalizes the values returned by database/sql drivers
// for decimal amount columns. SQLite/MySQL may expose an INTEGER column as
// int64 while text columns are returned as string/[]byte; all represent the
// same non-negative decimal domain to the metadata API.
//
// The refusal to format a negative value is deliberate and is kept: every
// column this reaches holds coin, which the wire format types as uint, so a
// negative there is corrupt state rather than a representable value. Columns
// whose domain is delta_coin use signedAmountText instead.
func decimalAmountText(value any) (string, error) {
	switch value := value.(type) {
	case nil:
		return "", nil
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case int64:
		if value < 0 {
			return "", fmt.Errorf("negative amount %d", value)
		}
		return strconv.FormatInt(value, 10), nil
	case int:
		if value < 0 {
			return "", fmt.Errorf("negative amount %d", value)
		}
		return strconv.Itoa(value), nil
	case uint64:
		return strconv.FormatUint(value, 10), nil
	case uint:
		return strconv.FormatUint(uint64(value), 10), nil
	default:
		return "", fmt.Errorf("unsupported amount type %T", value)
	}
}

// signedAmountText normalizes driver values for decimal amount columns whose
// domain includes negative values. MIR reward deltas are delta_coin, which the
// reference decodes as a signed unbounded integer, so decimalAmountText's
// non-negative guard would reject a valid row here.
func signedAmountText(value any) (string, error) {
	switch value := value.(type) {
	case nil:
		return "", nil
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case int64:
		return strconv.FormatInt(value, 10), nil
	case int:
		return strconv.Itoa(value), nil
	case uint64:
		return strconv.FormatUint(value, 10), nil
	case uint:
		return strconv.FormatUint(uint64(value), 10), nil
	default:
		return "", fmt.Errorf("unsupported amount type %T", value)
	}
}

// sumSignedRows sums a signed decimal column in Go instead of asking SQL to
// coerce it. It is the delta_coin counterpart of sumUint64Rows: the column is
// persisted as decimal text carrying its own sign, and the total is
// accumulated without a width bound so a row larger than any pot is summed
// rather than silently truncated. The returned total is never nil.
func sumSignedRows(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) (*big.Int, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	total := new(big.Int)
	for rows.Next() {
		var raw any
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		text, err := signedAmountText(raw)
		if err != nil {
			return nil, err
		}
		if text == "" {
			continue
		}
		value, err := parseBigInt("amount", text)
		if err != nil {
			return nil, err
		}
		total.Add(total, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return total, nil
}

// signedDecimal renders a signed amount for a decimal text column. A nil value
// is a programming error at the call site rather than a representable amount,
// so it is reported instead of being written as zero.
func signedDecimal(description string, value *big.Int) (string, error) {
	if value == nil {
		return "", fmt.Errorf("encode %s: no value", description)
	}
	return value.String(), nil
}

// parseBigInt decodes a signed decimal text column. Unlike parseUint64 it
// imposes no width bound: the column's domain is delta_coin, and a value the
// ledger rules would never accept still has to read back as what was written
// rather than as a truncated or rejected row.
func parseBigInt(description, value string) (*big.Int, error) {
	ret, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return nil, fmt.Errorf(
			"decode %s: invalid decimal %q",
			description,
			value,
		)
	}
	return ret, nil
}

// parseNullBigInt decodes a nullable signed decimal text column, treating NULL
// and the empty string as zero. The result is never nil.
func parseNullBigInt(
	description string,
	value sql.NullString,
) (*big.Int, error) {
	if !value.Valid || value.String == "" {
		return new(big.Int), nil
	}
	return parseBigInt(description, value.String)
}
