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

package types

import "time"

const (
	// PlannerStatsTriggerStartup labels the run made before block processing.
	PlannerStatsTriggerStartup = "startup"
	// PlannerStatsTriggerEpoch labels a run made after an epoch rollover.
	PlannerStatsTriggerEpoch = "epoch"
)

// PlannerStatsResult reports one incremental planner-statistics run.
type PlannerStatsResult struct {
	// Supported is false when the backend has no incremental statistics
	// maintenance; nothing was run.
	Supported bool
	// Skipped is true when a supported backend declined to run, for example
	// during a bulk load.
	Skipped bool
	// Changed is true when the stored statistics differ after the run.
	Changed bool
	// Stat1Rows is the number of rows in sqlite_stat1 after the run.
	Stat1Rows int
	// Duration is the wall time of the run, including any connection and
	// statement refresh.
	Duration time.Duration
}
