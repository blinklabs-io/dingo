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

import (
	"errors"
	"fmt"
)

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
	return fmt.Sprintf(
		"metadata migration %s (version %d) failed in %s: %v",
		e.Name,
		e.Version,
		e.Phase,
		e.Err,
	)
}

func (e *UpgradeError) Unwrap() error {
	return e.Err
}
