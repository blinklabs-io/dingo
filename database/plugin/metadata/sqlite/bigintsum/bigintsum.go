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

// Package bigintsum registers the dingo_bigint_sum SQLite aggregate with the
// pure-Go SQLite driver. Importing it is enough: registration happens once at
// package initialization and applies to every connection the driver opens
// afterwards.
//
// The aggregate sums signed decimal text exactly. SQLite's own SUM raises an
// integer-overflow error past 64 bits and TOTAL is a float, so neither is
// exact for the unbounded delta_coin columns. It ignores NULL inputs and
// returns the total as decimal text, "0" when no row contributed.
//
// It lives beside the SQLite provider rather than in the shared sqlstore
// package so that the PostgreSQL and MySQL providers do not link the SQLite
// driver.
package bigintsum

import (
	"database/sql/driver"
	"fmt"
	"math/big"

	driversqlite "modernc.org/sqlite"
)

// Name is the SQL name of the aggregate.
const Name = "dingo_bigint_sum"

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
		return parseDecimal(value)
	case []byte:
		return parseDecimal(string(value))
	default:
		return nil, fmt.Errorf("%s: unsupported amount type %T", Name, arg)
	}
}

func parseDecimal(value string) (*big.Int, error) {
	ret, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return nil, fmt.Errorf("%s: invalid decimal %q", Name, value)
	}
	return ret, nil
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
		Name,
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
