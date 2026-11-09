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

// Package coinsum registers the dingo_coin_sum SQLite aggregate. The SQLite
// provider imports it so shared SQL queries can sum coin without linking the
// driver into sqlstore. Each input and the running total must fit uint64;
// NULL and empty amounts contribute zero. Results are decimal text.
package coinsum

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"

	driversqlite "modernc.org/sqlite"
)

// Name is the SQL name of the aggregate.
const Name = "dingo_coin_sum"

type coinSum struct {
	total uint64
}

func coinValue(arg driver.Value) (uint64, error) {
	var text string
	switch value := arg.(type) {
	case nil:
		return 0, nil
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("negative amount %d", value)
		}
		return uint64(value), nil
	case string:
		text = value
	case []byte:
		text = string(value)
	default:
		return 0, fmt.Errorf("unsupported amount type %T", arg)
	}
	if text == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		if number, ok := errors.AsType[*strconv.NumError](err); ok {
			owned := *number
			// Volatile text belongs to SQLite only until this callback returns.
			owned.Num = strings.Clone(number.Num)
			err = &owned
		}
		return 0, fmt.Errorf("decode amount: %w", err)
	}
	return value, nil
}

func (s *coinSum) Step(
	_ *driversqlite.FunctionContext,
	args []driver.Value,
) error {
	value, err := coinValue(args[0])
	if err != nil {
		return err
	}
	if ^uint64(0)-s.total < value {
		return errors.New("amount sum overflow")
	}
	s.total += value
	return nil
}

func (s *coinSum) WindowInverse(
	_ *driversqlite.FunctionContext,
	args []driver.Value,
) error {
	value, err := coinValue(args[0])
	if err != nil {
		return err
	}
	if value > s.total {
		return errors.New("amount sum underflow")
	}
	s.total -= value
	return nil
}

func (s *coinSum) WindowValue(
	_ *driversqlite.FunctionContext,
) (driver.Value, error) {
	return strconv.FormatUint(s.total, 10), nil
}

func (*coinSum) Final(_ *driversqlite.FunctionContext) {}

func init() {
	driversqlite.MustRegisterFunction(Name, &driversqlite.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		// Callbacks retain only the numeric total and owned error text.
		VolatileArgs: true,
		MakeAggregate: func(driversqlite.FunctionContext) (driversqlite.AggregateFunction, error) {
			return &coinSum{}, nil
		},
	})
}
