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

package soak

import (
	"bufio"
	"encoding/json"
	"io"
	"regexp"
	"sort"
	"strings"
)

// Repeated is one normalised WARN or ERROR message and how often it occurred.
type Repeated struct {
	Level   string
	Message string
	Count   int
}

var (
	// Variable fragments are masked so one message logged with different
	// hashes, slots, peers or durations collapses to a single key.
	hexRe   = regexp.MustCompile(`\b[0-9a-fA-F]{16,}\b`)
	numRe   = regexp.MustCompile(`\b\d+(?:\.\d+)?(?:ms|s|m|h|µs|ns|B|KiB|MiB|GiB)?\b`)
	addrRe  = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)
	textMsg = regexp.MustCompile(`\bmsg=(?:"((?:[^"\\]|\\.)*)"|(\S+))`)
	textLvl = regexp.MustCompile(`\blevel=(\w+)`)
)

func normalise(msg string) string {
	msg = hexRe.ReplaceAllString(msg, "<hex>")
	msg = addrRe.ReplaceAllString(msg, "<addr>")
	return numRe.ReplaceAllString(msg, "<n>")
}

// SummariseLog reads slog JSON or text lines and returns WARN and ERROR
// messages seen at least minCount times, most frequent first. Lines that are
// neither format are ignored.
func SummariseLog(r io.Reader, minCount int) ([]Repeated, error) {
	type key struct{ level, msg string }
	counts := map[key]int{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		level, msg, ok := parseLogLine(sc.Text())
		if !ok || (level != "WARN" && level != "ERROR") {
			continue
		}
		counts[key{level, normalise(msg)}]++
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	var out []Repeated
	for k, c := range counts {
		if c >= minCount {
			out = append(out, Repeated{Level: k.level, Message: k.msg, Count: c})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Message < out[j].Message
	})
	return out, nil
}

func parseLogLine(line string) (level, msg string, ok bool) {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "{") {
		var rec struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Msg == "" {
			return "", "", false
		}
		return strings.ToUpper(rec.Level), rec.Msg, true
	}
	lm := textLvl.FindStringSubmatch(line)
	mm := textMsg.FindStringSubmatch(line)
	if lm == nil || mm == nil {
		return "", "", false
	}
	msg = mm[1]
	if msg == "" {
		msg = mm[2]
	}
	return strings.ToUpper(lm[1]), msg, true
}
