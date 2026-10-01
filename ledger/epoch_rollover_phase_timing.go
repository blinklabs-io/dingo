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

package ledger

import "time"

// timeRolloverPhase runs one processEpochRollover phase and records its
// duration, on failure as well as success, so a phase that errors after doing
// real work still reports what it cost.
func (ls *LedgerState) timeRolloverPhase(
	epoch uint64,
	phase string,
	fn func() error,
) error {
	start := time.Now()
	err := fn()
	ls.observeRolloverPhase(epoch, phase, time.Since(start), err)
	return err
}

func (ls *LedgerState) observeRolloverPhase(
	epoch uint64,
	phase string,
	d time.Duration,
	err error,
) {
	if ls.metrics.epochRolloverPhaseDuration != nil {
		ls.metrics.epochRolloverPhaseDuration.WithLabelValues(phase).
			Observe(d.Seconds())
	}
	if ls.config.Logger == nil {
		return
	}
	args := []any{
		"component", "ledger",
		"epoch", epoch,
		"phase", phase,
		"duration_seconds", d.Seconds(),
	}
	if err != nil {
		args = append(args, "error", err)
	}
	ls.config.Logger.Debug("epoch rollover phase", args...)
}
