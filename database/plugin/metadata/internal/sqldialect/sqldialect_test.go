// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sqldialect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type namedDialector string

func (d namedDialector) Name() string                                 { return string(d) }
func (namedDialector) Initialize(*gorm.DB) error                      { return nil }
func (namedDialector) Migrator(*gorm.DB) gorm.Migrator                { return nil }
func (namedDialector) DataTypeOf(*schema.Field) string                { return "" }
func (namedDialector) DefaultValueOf(*schema.Field) clause.Expression { return nil }
func (namedDialector) BindVarTo(clause.Writer, *gorm.Statement, any)  {}
func (namedDialector) QuoteTo(clause.Writer, string)                  {}
func (namedDialector) Explain(string, ...any) string                  { return "" }

func dialectDB(name string) *gorm.DB {
	return &gorm.DB{Config: &gorm.Config{Dialector: namedDialector(name)}}
}

func TestExactUint64ArithmeticDialects(t *testing.T) {
	tests := []struct {
		name       string
		castType   string
		arithmetic string
	}{
		{name: "sqlite", castType: "INTEGER", arithmetic: "?"},
		{name: "mysql", castType: "UNSIGNED", arithmetic: "CAST(? AS UNSIGNED)"},
		{name: "postgres", castType: "NUMERIC", arithmetic: "CAST(? AS NUMERIC)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := dialectDB(test.name)
			require.Equal(t, test.castType, IntegerCastType(db))
			require.Equal(t, test.arithmetic, ArithmeticParam(db))
		})
	}
}
