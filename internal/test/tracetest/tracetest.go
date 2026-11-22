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

// Package tracetest records a runtime/trace execution trace around a test
// body and reports the regions it holds, parsed the way go tool trace does.
package tracetest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	rtrace "runtime/trace"
	"testing"

	"github.com/stretchr/testify/require"
	xtrace "golang.org/x/exp/trace"
)

// Regions summarizes the runtime/trace regions in one execution trace.
type Regions struct {
	// Begins counts RegionBegin events by region type.
	Begins map[string]int
	// Ends counts RegionEnd events by region type.
	Ends map[string]int
	// Misuse lists every region end that did not close the innermost region
	// open on its goroutine. go tool trace rejects such an end and drops the
	// regions after it.
	Misuse []string
	// Open lists the regions still open when the trace stopped.
	Open []string
}

// Record runs fn under an execution trace and returns the regions it
// recorded. Only one trace runs per process, so a test calling Record must
// not run in parallel with another that does.
func Record(t testing.TB, fn func()) Regions {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, rtrace.Start(&buf))
	func() {
		defer rtrace.Stop()
		fn()
	}()
	regions, err := parse(&buf)
	require.NoError(t, err)
	return regions
}

func parse(r io.Reader) (Regions, error) {
	reader, err := xtrace.NewReader(r)
	if err != nil {
		return Regions{}, fmt.Errorf("open execution trace: %w", err)
	}
	regions := Regions{Begins: map[string]int{}, Ends: map[string]int{}}
	stacks := map[xtrace.GoID][]string{}
	for {
		ev, err := reader.ReadEvent()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Regions{}, fmt.Errorf("read execution trace: %w", err)
		}
		kind := ev.Kind()
		if kind != xtrace.EventRegionBegin && kind != xtrace.EventRegionEnd {
			continue
		}
		name, g := ev.Region().Type, ev.Goroutine()
		if kind == xtrace.EventRegionBegin {
			regions.Begins[name]++
			stacks[g] = append(stacks[g], name)
			continue
		}
		regions.Ends[name]++
		stack := stacks[g]
		if len(stack) == 0 || stack[len(stack)-1] != name {
			regions.Misuse = append(
				regions.Misuse,
				fmt.Sprintf("end %q with open regions %q", name, stack),
			)
			continue
		}
		stacks[g] = stack[:len(stack)-1]
	}
	for _, stack := range stacks {
		regions.Open = append(regions.Open, stack...)
	}
	return regions, nil
}
