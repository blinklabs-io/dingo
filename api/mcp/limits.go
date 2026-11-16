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

package mcp

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
)

var errReadsBusy = errors.New("too many concurrent SQLite queries")

var errOffsetTooLarge = errors.New("offset exceeds the configured maximum")

// readLimits holds optional resource limits shared by the MCP tools that
// run database reads. A zero or nil field disables that limit.
type readLimits struct {
	slots       chan struct{}
	callerSlots chan struct{}
	parseSlots  chan struct{}
	// maxAssetRows is the most live asset rows one get_asset_info call may
	// scan; zero means unlimited.
	maxAssetRows int
	// maxOffset is the largest OFFSET a paginated tool accepts; zero means
	// unlimited.
	maxOffset int
	// drained reports the ledger-state families whose SQL rows are deleted;
	// nil means none is.
	drained func() nodesettings.FamilyMask
}

// ErrDrainedFamily is wrapped by the error a tool returns for a read of SQL
// tables whose family the metadata store no longer holds.
var ErrDrainedFamily = errors.New(
	"the metadata database no longer holds this ledger-state family",
)

func (l *readLimits) drainedMask() nodesettings.FamilyMask {
	if l == nil || l.drained == nil {
		return 0
	}
	return l.drained()
}

// refuseDrainedTables refuses a caller's SQL that names a table of a drained
// family: the table is empty, and an empty answer would read as the truth.
func (l *readLimits) refuseDrainedTables(query string) error {
	family, table, ok := statedrain.DrainedTableIn(l.drainedMask(), query)
	if !ok {
		return nil
	}
	return fmt.Errorf(
		"%w: table %s belongs to family %s; use a tool that reads the ledger state",
		ErrDrainedFamily,
		table,
		family,
	)
}

// refuseDrainedFamilies refuses a tool path that reads the SQL tables of any
// of families directly.
func (l *readLimits) refuseDrainedFamilies(
	families ...nodesettings.Family,
) error {
	mask := l.drainedMask()
	for _, family := range families {
		if mask.Has(family) {
			return fmt.Errorf(
				"%w: family %s; the tool needs the ledger state",
				ErrDrainedFamily, family,
			)
		}
	}
	return nil
}

func newReadLimits() *readLimits {
	return &readLimits{}
}

func (l *readLimits) admit() error {
	if !l.acquire() {
		return errReadsBusy
	}
	return nil
}

// admitCallerSQL takes a caller-SQL slot and a shared query slot. A
// successful call must be paired with releaseCallerSQL.
func (l *readLimits) admitCallerSQL() error {
	if l.callerSlots != nil {
		select {
		case l.callerSlots <- struct{}{}:
		default:
			return errReadsBusy
		}
	}
	if !l.acquire() {
		if l.callerSlots != nil {
			<-l.callerSlots
		}
		return errReadsBusy
	}
	return nil
}

func (l *readLimits) releaseCallerSQL() {
	l.release()
	if l.callerSlots != nil {
		<-l.callerSlots
	}
}

// acquire takes a query slot without blocking and reports whether it got one.
// With no slot budget configured it always succeeds.
func (l *readLimits) acquire() bool {
	if l.slots == nil {
		return true
	}
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *readLimits) release() {
	if l.slots != nil {
		<-l.slots
	}
}

// offsetLimitNote returns a notice for a full page whose next offset would
// exceed maxOffset, or "" otherwise. Without it, a caller paging until a
// short page cannot tell a complete result from one cut off by the bound.
func offsetLimitNote(offset, limit, returned, maxOffset int) string {
	if maxOffset <= 0 || returned < limit || offset+limit <= maxOffset {
		return ""
	}
	return fmt.Sprintf(
		"**Offset limit reached**: the next page would start past "+
			"offset %d, which this tool rejects. Matching rows beyond "+
			"this page are not retrievable here; treat the result as "+
			"truncated.\n\n",
		maxOffset,
	)
}

// assetScanLimit returns the SQL LIMIT for the asset-row scan: one row past
// maxAssetRows so an over-cap policy is detected, or -1 (no limit).
func (l *readLimits) assetScanLimit() int {
	if l.maxAssetRows <= 0 {
		return -1
	}
	return l.maxAssetRows + 1
}
