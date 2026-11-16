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

package database

import "github.com/blinklabs-io/dingo/database/nodesettings"

// checkNotDrained returns an error wrapping ErrFamilyDrained when the SQL
// rows of f are deleted, so a read cannot return the empty table as an answer
// and a write cannot repopulate it.
func (d *Database) checkNotDrained(f nodesettings.Family) error {
	if d.FamilyDrained(f) {
		return drainedWrite(f)
	}
	return nil
}

// checkCommitteeLive is checkNotDrained for the committee and constitution
// family.
func (d *Database) checkCommitteeLive() error {
	return d.checkNotDrained(nodesettings.FamilyCommitteeConstitution)
}
