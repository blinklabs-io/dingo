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

// Package utxocond builds fixed-shape "(tx_id = ? AND output_idx = ?)" OR-list
// conditions for the UTxO block-apply UPDATEs (consume, collateral, reference
// inputs) used by the shared metadata store.
//
// The block-apply path builds one such UPDATE per transaction, matching that
// transaction's consumed/collateral/reference inputs. Building the WHERE clause
// as a variable-length list of OR-ed placeholder pairs made the SQL text vary
// with the input count, so prepared statements could not be reused and
// thrashed on parse/prepare under dense sync (issue #2943).
//
// Chunks pads each chunk's term count up to the next power of two by repeating
// the chunk's last ref. Because these UPDATEs match rows by an OR of composite
// keys, repeating a ref is idempotent: it does not change which rows match or
// how many rows are affected. Padding to powers of two bounds the number of
// distinct SQL shapes to about log2(maxTerms)+1 regardless of input count, so
// the prepared-statement cache can reuse them.
package utxocond

import (
	"math/bits"
	"strings"
)

// Ref is a (tx_id, output_idx) UTxO reference.
type Ref struct {
	TxID []byte
	Idx  uint32
}

// Chunk is one fixed-shape OR-list condition and its bound arguments.
type Chunk struct {
	// Condition is the parenthesized OR-list, e.g.
	// "(tx_id = ? AND output_idx = ?) OR (tx_id = ? AND output_idx = ?)".
	// Its term count is always a power of two and never exceeds the maxTerms
	// passed to Chunks, so only a handful of distinct Condition strings are
	// ever produced.
	Condition string
	// Args holds TxID, Idx pairs for every term including padding, in order:
	// len(Args) == 2 * (padded term count).
	Args []any
	// Real is the number of leading terms that correspond to distinct input
	// refs (the remainder are idempotent padding). Callers that verify an
	// affected-row count should sum Real across chunks.
	Real int
}

// DefaultMaxTerms bounds the OR-terms per statement. Each term binds two
// parameters, so 256 terms is 512 parameters, comfortably under driver
// parameter limits (SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 999, newer
// builds 32766; postgres/mysql are far higher). It is a power of two so a full
// chunk reuses the single largest SQL shape.
const DefaultMaxTerms = 256

const term = "(tx_id = ? AND output_idx = ?)"

// Chunks splits refs into chunks and pads each chunk's term count up to the
// next power of two (repeating the chunk's last ref). It returns nil for an
// empty input.
//
// maxTerms is an upper bound on the padded term count, and so on the bound
// parameter count (two per term) of the statements callers build from a Chunk.
// Every padded term count is a power of two and must not exceed maxTerms, so
// the effective bound is maxTerms rounded down to a power of two: rounding down
// (never up) is what keeps the parameter count inside the caller's limit. The
// split boundary uses that same rounded value, so a chunk is never larger than
// the term count it is padded to. maxTerms values below 1 fall back to
// DefaultMaxTerms, which is already a power of two.
func Chunks(refs []Ref, maxTerms int) []Chunk {
	if len(refs) == 0 {
		return nil
	}
	if maxTerms < 1 {
		maxTerms = DefaultMaxTerms
	}
	// Round the caller's bound down to a power of two so that padded term
	// counts are always powers of two (bounding the number of distinct SQL
	// shapes) and always <= maxTerms (respecting driver parameter limits).
	maxTerms = floorPow2(maxTerms)
	var chunks []Chunk
	for start := 0; start < len(refs); start += maxTerms {
		end := min(start+maxTerms, len(refs))
		group := refs[start:end]
		padded := nextPow2Capped(len(group), maxTerms)
		args := make([]any, 0, padded*2)
		for _, r := range group {
			args = append(args, r.TxID, r.Idx)
		}
		last := group[len(group)-1]
		for i := len(group); i < padded; i++ {
			args = append(args, last.TxID, last.Idx)
		}
		chunks = append(chunks, Chunk{
			Condition: condition(padded),
			Args:      args,
			Real:      len(group),
		})
	}
	return chunks
}

// condition returns n OR-ed terms.
func condition(n int) string {
	if n <= 1 {
		return term
	}
	var b strings.Builder
	b.Grow(len(term)*n + 4*(n-1))
	for i := range n {
		if i > 0 {
			b.WriteString(" OR ")
		}
		b.WriteString(term)
	}
	return b.String()
}

// nextPow2Capped returns the smallest power of two >= n, capped at limit. n is
// assumed to be in [1, limit] and limit is assumed to be a power of two (Chunks
// guarantees both), so the result is always a power of two and never exceeds
// limit. The cap is defensive: it cannot trigger while those preconditions hold.
func nextPow2Capped(n, limit int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	if p > limit {
		return limit
	}
	return p
}

// floorPow2 returns the largest power of two <= n. n is assumed to be >= 1.
func floorPow2(n int) int {
	return 1 << (bits.Len(uint(n)) - 1)
}
