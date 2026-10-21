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
	"database/sql/driver"
	"fmt"
	"math/big"

	driversqlite "modernc.org/sqlite"
)

// sqliteBigintSumFunc is the SQLite aggregate behind wideSumExpr. SQLite's
// SUM raises an integer-overflow error past 64 bits and TOTAL is a float, so
// neither is exact for the signed, unbounded delta_coin columns.
const sqliteBigintSumFunc = "dingo_bigint_sum"

type bigintSum struct {
	total big.Int
}

func bigintSumArg(arg driver.Value) (*big.Int, error) {
	switch value := arg.(type) {
	case nil:
		return nil, nil
	case int64:
		return big.NewInt(value), nil
	case string:
		return parseBigInt("amount", value)
	case []byte:
		return parseBigInt("amount", string(value))
	default:
		return nil, fmt.Errorf("bigint sum: unsupported amount type %T", arg)
	}
}

func (s *bigintSum) Step(
	_ *driversqlite.FunctionContext,
	args []driver.Value,
) error {
	item, err := bigintSumArg(args[0])
	if err != nil || item == nil {
		return err
	}
	s.total.Add(&s.total, item)
	return nil
}

func (s *bigintSum) WindowInverse(
	_ *driversqlite.FunctionContext,
	args []driver.Value,
) error {
	item, err := bigintSumArg(args[0])
	if err != nil || item == nil {
		return err
	}
	s.total.Sub(&s.total, item)
	return nil
}

func (s *bigintSum) WindowValue(
	_ *driversqlite.FunctionContext,
) (driver.Value, error) {
	return s.total.String(), nil
}

func (*bigintSum) Final(_ *driversqlite.FunctionContext) {}

func init() {
	driversqlite.MustRegisterFunction(
		sqliteBigintSumFunc,
		&driversqlite.FunctionImpl{
			NArgs:         1,
			Deterministic: true,
			MakeAggregate: func(
				driversqlite.FunctionContext,
			) (driversqlite.AggregateFunction, error) {
				return &bigintSum{}, nil
			},
		},
	)
}
