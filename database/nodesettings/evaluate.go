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

package nodesettings

import (
	"fmt"
	"slices"
	"strings"
)

// Values maps a gate name to its encoded value.
type Values map[string]string

// Mismatch is one fatal disagreement between the database and the
// configuration.
type Mismatch struct {
	Gate       string
	Label      string
	Persisted  string
	Configured string
	Reason     string
}

// String renders the operator-facing message. It uses the gate's Label so
// the existing wording in database/storage_mode_test.go keeps matching.
func (m Mismatch) String() string {
	return fmt.Sprintf(
		"%s was %q but configured as %q (%s)",
		m.Label, m.Persisted, m.Configured, m.Reason,
	)
}

// Result is the outcome of an evaluation.
type Result struct {
	// Effective is the value the caller should actually use for each gate
	// present in configured, after any override.
	Effective Values
	// Writes are the values to persist. Empty when nothing changed.
	Writes Values
	// Mismatches is non-empty when startup must fail.
	Mismatches []Mismatch
}

// Evaluate applies the registry to a persisted and configured value set.
//
// explicit reports, per gate, whether the configured value came from an
// operator (a flag, an environment variable, or a config file) rather than a
// built-in default. It is the only thing separating "resume what this database
// already is" from "the operator asked for something incompatible", so callers
// that genuinely mean every value they pass, such as database.New, mark them
// all explicit and get strict validation with no override behavior.
//
// A gate absent from configured is skipped: the caller does not know that
// value yet, which is normal for tool paths that never load a cardano config.
func Evaluate(
	persisted Values,
	configured Values,
	explicit map[string]bool,
) Result {
	result := Result{
		Effective: make(Values, len(configured)),
		Writes:    make(Values),
	}
	for _, gate := range Gates() {
		configuredValue, ok := configured[gate.Name]
		if !ok {
			continue
		}
		persistedValue, hasPersisted := persisted[gate.Name]

		// An override replaces the effective value and short-circuits the
		// class rules, so a default never looks like a deliberate change.
		if gate.OverrideEligible &&
			!explicit[gate.Name] &&
			hasPersisted &&
			persistedValue != "" {
			result.Effective[gate.Name] = persistedValue
			continue
		}

		result.Effective[gate.Name] = configuredValue
		if !hasPersisted {
			// First start for this gate: record whatever is configured.
			if gate.Class == FrozenFillOnce && configuredValue == "" {
				continue
			}
			result.Writes[gate.Name] = configuredValue
			continue
		}
		if mismatch, write, keep := gate.apply(
			persistedValue, configuredValue,
		); mismatch != nil {
			result.Mismatches = append(result.Mismatches, *mismatch)
		} else if write {
			result.Writes[gate.Name] = configuredValue
		} else if keep != "" {
			result.Effective[gate.Name] = keep
		}
	}
	if len(result.Writes) == 0 {
		result.Writes = nil
	}
	return result
}

// apply returns a mismatch, or whether to write the configured value, or a
// value that overrides the effective one (used by Taint, where a set bit wins
// over a tightened configuration).
func (g Gate) apply(
	persisted, configured string,
) (*Mismatch, bool, string) {
	mismatch := func(reason string) (*Mismatch, bool, string) {
		return &Mismatch{
			Gate:       g.Name,
			Label:      g.label(),
			Persisted:  persisted,
			Configured: configured,
			Reason:     reason,
		}, false, ""
	}
	switch g.Class {
	case Frozen:
		if persisted != configured {
			return mismatch(g.remedy())
		}
	case FrozenFillOnce:
		switch {
		case configured == "":
			// Not known on this path; keep what the database has.
			return nil, false, persisted
		case persisted == "":
			return nil, true, ""
		case persisted != configured:
			return mismatch(g.remedy())
		}
	case LatchEnum:
		if persisted == configured {
			return nil, false, ""
		}
		from := slices.Index(g.Ordered, persisted)
		to := slices.Index(g.Ordered, configured)
		if from < 0 || to < 0 {
			return mismatch("not a recognised value for this setting")
		}
		if to < from {
			return mismatch(
				"this setting only moves " +
					strings.Join(g.Ordered, " to ") +
					", never back",
			)
		}
		return nil, true, ""
	case LatchBool:
		if persisted == configured {
			return nil, false, ""
		}
		if persisted == LatchOff {
			return nil, true, ""
		}
		if configured == LatchOff {
			return mismatch(
				"it cannot be turned off once the database has run with it on",
			)
		}
		return mismatch(
			"its value is frozen while it is enabled",
		)
	case Taint:
		if persisted == LatchOn {
			// Sticky: tightening is allowed but cannot clear the record.
			return nil, false, LatchOn
		}
		if configured == LatchOn {
			return mismatch(
				"relaxing it would mix unverified data into a database whose " +
					"existing range was verified",
			)
		}
	}
	return nil, false, ""
}
