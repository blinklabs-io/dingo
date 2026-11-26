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
	rtrace "runtime/trace"
)

// phaseRegions marks consecutive phases of one function as runtime/trace
// regions: next ends the running phase and starts the named one. With no
// execution trace running every call returns after one atomic load.
type phaseRegions struct {
	region *rtrace.Region
}

func startPhases(ctx context.Context, name string) *phaseRegions {
	p := &phaseRegions{}
	p.next(ctx, name)
	return p
}

func (p *phaseRegions) next(ctx context.Context, name string) {
	if p.region != nil {
		p.region.End()
		p.region = nil
	}
	if rtrace.IsEnabled() {
		p.region = rtrace.StartRegion(ctx, name)
	}
}

func (p *phaseRegions) end() {
	if p.region != nil {
		p.region.End()
		p.region = nil
	}
}

// statementRegion starts a region named for one SQL statement, so a trace
// attributes the writer's time per statement rather than by CPU sampling.
// The statement is classified only while a trace runs.
func statementRegion(ctx context.Context, query string) func() {
	if !rtrace.IsEnabled() {
		return noopRegionEnd
	}
	op, name := classifySQLStatement(query)
	if name == "" || name == "unknown" {
		name = op
	}
	return rtrace.StartRegion(ctx, "dingo.sql."+name).End
}

func noopRegionEnd() {}

// inStatementRegion runs a call on a cached statement inside its statement
// region, ending the region even when the call panics. Only the cache hit is
// wrapped: a miss falls back to the queryer, where countingQueryer (present
// when metrics are configured) records the region, and wrapping the whole
// helper would record that miss as two nested regions of the same name.
func inStatementRegion[T any](
	ctx context.Context,
	query string,
	run func() (T, error),
) (T, error) {
	defer statementRegion(ctx, query)()
	return run()
}

// queryRowInStatementRegion is inStatementRegion for QueryRowContext. It is
// not routed through the generic helper because nilaway cannot see that a
// *sql.Row returned through T is non-nil, and callers Scan it unguarded.
func queryRowInStatementRegion(
	ctx context.Context,
	query string,
	stmt *sql.Stmt,
	args ...any,
) *sql.Row {
	defer statementRegion(ctx, query)()
	return stmt.QueryRowContext(ctx, args...)
}
