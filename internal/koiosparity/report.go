// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// StatusSummary is the data model for the `status` command output.
type StatusSummary struct {
	Network      string
	MinFetched   uint64
	MaxFetched   uint64
	TotalFetched int
	PassCount    int
	FailCount    int
	ErrorCount   int
	UncheckedMin uint64
	UncheckedMax uint64
	UncheckedCnt int
	CheckedMin   uint64
	CheckedMax   uint64
	FailEpochs   []uint64
	ErrorEpochs  []uint64
	LastCheck    time.Time
}

// BuildStatusSummary computes a StatusSummary from cache data.
func BuildStatusSummary(
	network string,
	fetchedEpochs []uint64,
	statuses []CheckEpochStatus,
) StatusSummary {
	s := StatusSummary{
		Network:      network,
		TotalFetched: len(fetchedEpochs),
	}
	if len(fetchedEpochs) > 0 {
		s.MinFetched = fetchedEpochs[0]
		s.MaxFetched = fetchedEpochs[len(fetchedEpochs)-1]
	}

	checkedSet := make(map[uint64]bool, len(statuses))
	for _, st := range statuses {
		checkedSet[st.Epoch] = true
		switch st.Status {
		case StatusPass:
			s.PassCount++
		case StatusFail:
			s.FailCount++
			s.FailEpochs = append(s.FailEpochs, st.Epoch)
		case StatusError:
			s.ErrorCount++
			s.ErrorEpochs = append(s.ErrorEpochs, st.Epoch)
		}
		if s.CheckedMin == 0 || st.Epoch < s.CheckedMin {
			s.CheckedMin = st.Epoch
		}
		if st.Epoch > s.CheckedMax {
			s.CheckedMax = st.Epoch
		}
		if st.LastCheckedAt.After(s.LastCheck) {
			s.LastCheck = st.LastCheckedAt
		}
	}

	for _, e := range fetchedEpochs {
		if !checkedSet[e] {
			if s.UncheckedCnt == 0 {
				s.UncheckedMin = e
			}
			s.UncheckedMax = e
			s.UncheckedCnt++
		}
	}

	sort.Slice(s.FailEpochs, func(i, j int) bool { return s.FailEpochs[i] < s.FailEpochs[j] })
	sort.Slice(s.ErrorEpochs, func(i, j int) bool { return s.ErrorEpochs[i] < s.ErrorEpochs[j] })
	return s
}

// PrintStatus writes a human-readable status summary to w.
func PrintStatus(w io.Writer, s StatusSummary, verbose bool, statuses []CheckEpochStatus) {
	fmt.Fprintf(w, "%s parity status\n", s.Network)
	if s.TotalFetched > 0 {
		fmt.Fprintf(w, "  cache:    epochs %d–%d fetched (%d total)\n",
			s.MinFetched, s.MaxFetched, s.TotalFetched)
	} else {
		fmt.Fprintf(w, "  cache:    no epochs fetched\n")
	}

	checked := s.PassCount + s.FailCount + s.ErrorCount
	if checked > 0 {
		lastCheck := "(unknown)"
		if !s.LastCheck.IsZero() {
			lastCheck = s.LastCheck.Format("2006-01-02")
		}
		fmt.Fprintf(w, "  checked:  epochs %d–%d (PASS %d / FAIL %d / ERROR %d, last check %s)\n",
			s.CheckedMin, s.CheckedMax,
			s.PassCount, s.FailCount, s.ErrorCount, lastCheck)
	} else {
		fmt.Fprintf(w, "  checked:  none\n")
	}

	if len(s.FailEpochs) > 0 {
		fmt.Fprintf(w, "  failing:  %s\n", joinUint64s(s.FailEpochs))
	}
	if len(s.ErrorEpochs) > 0 {
		fmt.Fprintf(w, "  errors:   %s\n", joinUint64s(s.ErrorEpochs))
	}
	if s.UncheckedCnt > 0 {
		fmt.Fprintf(w, "  pending:  %d–%d (in cache, not checked)\n",
			s.UncheckedMin, s.UncheckedMax)
	}

	if verbose {
		for _, st := range statuses {
			if st.Status != StatusFail {
				continue
			}
			fmt.Fprintf(w, "  epoch %d: %d mismatches (only_koios=%d only_dingo=%d)\n",
				st.Epoch, st.MismatchCount,
				len(UnmarshalPoolList(st.OnlyKoiosPools)),
				len(UnmarshalPoolList(st.OnlyDingoPools)),
			)
		}
	}
}

// JSONReport is the machine-readable report format written by `run`.
type JSONReport struct {
	Network     string            `json:"network"`
	GeneratedAt string            `json:"generated_at"`
	Summary     JSONReportSummary `json:"summary"`
	FailEpochs  []JSONEpochEntry  `json:"fail_epochs,omitempty"`
	ErrorEpochs []JSONEpochEntry  `json:"error_epochs,omitempty"`
}

// JSONReportSummary holds aggregate counts.
type JSONReportSummary struct {
	TotalFetched int `json:"total_fetched"`
	TotalChecked int `json:"total_checked"`
	PassCount    int `json:"pass"`
	FailCount    int `json:"fail"`
	ErrorCount   int `json:"error"`
}

// JSONEpochEntry is a failing or erroring epoch in the JSON report.
type JSONEpochEntry struct {
	Epoch         uint64         `json:"epoch"`
	Status        string         `json:"status"`
	MismatchCount int            `json:"mismatch_count"`
	Mismatches    []JSONMismatch `json:"mismatches,omitempty"`
}

// JSONMismatch is a single field-level mismatch in the JSON report.
type JSONMismatch struct {
	Pool       string `json:"pool,omitempty"`
	Field      string `json:"field"`
	DingoValue string `json:"dingo_value"`
	KoiosValue string `json:"koios_value"`
	Category   string `json:"category"`
}

// BuildJSONReport constructs a JSONReport from status and mismatch data.
func BuildJSONReport(
	network string,
	generatedAt string,
	fetchedEpochs []uint64,
	statuses []CheckEpochStatus,
	getMismatches func(epoch uint64) ([]CheckMismatch, error),
) (*JSONReport, error) {
	report := &JSONReport{
		Network:     network,
		GeneratedAt: generatedAt,
		Summary: JSONReportSummary{
			TotalFetched: len(fetchedEpochs),
		},
	}

	for _, st := range statuses {
		report.Summary.TotalChecked++
		switch st.Status {
		case StatusPass:
			report.Summary.PassCount++
		case StatusFail:
			report.Summary.FailCount++
		case StatusError:
			report.Summary.ErrorCount++
		}

		if st.Status == StatusPass {
			continue
		}

		entry := JSONEpochEntry{
			Epoch:         st.Epoch,
			Status:        st.Status,
			MismatchCount: st.MismatchCount,
		}
		if getMismatches != nil {
			mismatches, err := getMismatches(st.Epoch)
			if err == nil {
				for _, m := range mismatches {
					entry.Mismatches = append(entry.Mismatches, JSONMismatch{
						Pool:       m.PoolBech32,
						Field:      m.Field,
						DingoValue: m.DingoValue,
						KoiosValue: m.KoiosValue,
						Category:   m.Category,
					})
				}
			}
		}

		if st.Status == StatusFail {
			report.FailEpochs = append(report.FailEpochs, entry)
		} else {
			report.ErrorEpochs = append(report.ErrorEpochs, entry)
		}
	}

	return report, nil
}

// WriteJSONReport serialises the report and writes it to w.
func WriteJSONReport(w io.Writer, report *JSONReport) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

// PrintExplain writes a human-readable per-epoch mismatch breakdown to w.
func PrintExplain(w io.Writer, network string, epoch uint64, mismatches []CheckMismatch, poolFilter string) {
	if poolFilter != "" {
		fmt.Fprintf(w, "%s epoch %d — pool %s\n", network, epoch, poolFilter)
	} else {
		fmt.Fprintf(w, "%s epoch %d\n", network, epoch)
	}
	if len(mismatches) == 0 {
		fmt.Fprintln(w, "  no mismatches recorded")
		return
	}
	byCat := make(map[string][]CheckMismatch)
	for _, m := range mismatches {
		byCat[m.Category] = append(byCat[m.Category], m)
	}
	cats := make([]string, 0, len(byCat))
	for c := range byCat {
		cats = append(cats, c)
	}
	sort.Strings(cats)
	for _, cat := range cats {
		items := byCat[cat]
		fmt.Fprintf(w, "  [%s] %d\n", cat, len(items))
		for _, m := range items {
			pool := m.PoolBech32
			if pool == "" {
				pool = "(epoch)"
			}
			fmt.Fprintf(w, "    pool=%-55s field=%-22s dingo=%-20s koios=%s\n",
				pool, m.Field, m.DingoValue, m.KoiosValue)
		}
	}
}

func joinUint64s(ns []uint64) string {
	ss := make([]string, len(ns))
	for i, n := range ns {
		ss[i] = fmt.Sprintf("%d", n)
	}
	return strings.Join(ss, ", ")
}
