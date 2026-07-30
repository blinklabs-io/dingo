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

package migrations

import "errors"

var (
	ErrEmptyRegistry   = errors.New("metadata migration registry is empty")
	ErrInvalidRegistry = errors.New("invalid metadata migration registry")
	ErrChecksumDrift   = errors.New("metadata migration checksum changed")
	ErrNewerSchema     = errors.New("metadata schema is newer than this binary")
	ErrLegacySchema    = errors.New("unsupported unversioned metadata schema")
)

// Phase identifies the durable point at which an interrupted upgrade resumes.
type Phase string

const (
	PhaseExpand   Phase = "expand"
	PhaseBackfill Phase = "backfill"
	PhaseContract Phase = "contract"
	PhaseComplete Phase = "complete"
)

// UpgradeError identifies the exact migration phase that blocked readiness.
type UpgradeError struct {
	Version int
	Name    string
	Phase   Phase
	Err     error
}

func (e *UpgradeError) Error() string {
	return "metadata migration " + e.Name + " (version " +
		itoa(e.Version) + ") failed in " + string(e.Phase) + ": " +
		e.Err.Error()
}

func (e *UpgradeError) Unwrap() error {
	return e.Err
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	position := len(buffer)
	for value > 0 {
		position--
		buffer[position] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[position:])
}
