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
	"database/sql/driver"
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// OpenGuardedDB opens a database whose every statement is vetted by guard
// before a connection prepares or runs it. With a nil guard it is OpenDB.
func OpenGuardedDB(
	driverName, dataSourceName, systemName string,
	tracingEnabled bool,
	guard metadata.StatementGuard,
) (*sql.DB, error) {
	if guard == nil {
		return OpenDB(driverName, dataSourceName, systemName, tracingEnabled)
	}
	probe, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	drv := probe.Driver()
	if err := probe.Close(); err != nil {
		return nil, err
	}
	var inner driver.Connector = dsnConnector{drv: drv, dsn: dataSourceName}
	if dc, ok := drv.(driver.DriverContext); ok {
		inner, err = dc.OpenConnector(dataSourceName)
		if err != nil {
			return nil, fmt.Errorf("open connector: %w", err)
		}
	}
	return sql.OpenDB(guardConnector{inner: inner, guard: guard}), nil
}

type dsnConnector struct {
	drv driver.Driver
	dsn string
}

func (c dsnConnector) Connect(context.Context) (driver.Conn, error) {
	return c.drv.Open(c.dsn)
}

func (c dsnConnector) Driver() driver.Driver { return c.drv }

type guardConnector struct {
	inner driver.Connector
	guard metadata.StatementGuard
}

func (c guardConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &guardConn{Conn: conn, guard: c.guard}, nil
}

func (c guardConnector) Driver() driver.Driver { return c.inner.Driver() }

// guardConn vets statement text on every path a statement can take to the
// driver: prepare, and the direct query and exec shortcuts.
type guardConn struct {
	driver.Conn
	guard metadata.StatementGuard
}

func (c *guardConn) PrepareContext(
	ctx context.Context,
	query string,
) (driver.Stmt, error) {
	if err := c.guard.CheckStatement(query); err != nil {
		return nil, err
	}
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

func (c *guardConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *guardConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	if err := c.guard.CheckStatement(query); err != nil {
		return nil, err
	}
	q, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return q.QueryContext(ctx, query, args)
}

func (c *guardConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	if err := c.guard.CheckStatement(query); err != nil {
		return nil, err
	}
	e, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return e.ExecContext(ctx, query, args)
}

func (c *guardConn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions,
) (driver.Tx, error) {
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, opts)
	}
	return c.Conn.Begin() //nolint:staticcheck
}

func (c *guardConn) Ping(ctx context.Context) error {
	if p, ok := c.Conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *guardConn) ResetSession(ctx context.Context) error {
	if r, ok := c.Conn.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

func (c *guardConn) IsValid() bool {
	if v, ok := c.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (c *guardConn) CheckNamedValue(nv *driver.NamedValue) error {
	if n, ok := c.Conn.(driver.NamedValueChecker); ok {
		return n.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}
