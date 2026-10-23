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

package mithril

import (
	"context"
	"fmt"
	"io"
)

const (
	// Normal entries need one header and one padded data block beyond payload.
	// tar.Reader consumes metadata internally, so the stream limit also allows
	// a fixed amount for those headers and their payloads.
	maxTarStreamMetadataBytes int64 = 64 << 20
	tarEntryFramingBytes      int64 = 2 * 512
)

func archiveStreamLimit(payloadBytes int64, entries int) int64 {
	const maxInt64 = int64(1<<63 - 1)
	if entries < 0 {
		entries = 0
	}
	entryCount := int64(entries)
	if entryCount > (maxInt64-maxTarStreamMetadataBytes)/tarEntryFramingBytes {
		return maxInt64
	}
	overhead := entryCount*tarEntryFramingBytes + maxTarStreamMetadataBytes
	if payloadBytes > maxInt64-overhead {
		return maxInt64
	}
	return payloadBytes + overhead
}

type archiveStreamReader struct {
	ctx      context.Context
	reader   io.Reader
	budget   *bootstrapExtractionBudget
	maxBytes int64
	read     int64
}

func (r *archiveStreamReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}

	remaining := r.maxBytes - r.read
	if remaining < 0 {
		return 0, fmt.Errorf(
			"%w: decompressed tar stream limit reached",
			ErrExtractLimitExceeded,
		)
	}
	if remaining == 0 {
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if n > 0 {
			return 0, fmt.Errorf(
				"%w: decompressed tar stream exceeds %d bytes",
				ErrExtractLimitExceeded, r.maxBytes,
			)
		}
		return 0, err
	}
	if remaining < int64(len(p)) {
		p = p[:int(remaining)]
	}
	n, err := r.reader.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	if n > 0 && r.budget != nil {
		if budgetErr := r.budget.reserveStreamBytes(int64(n)); budgetErr != nil {
			return 0, budgetErr
		}
	}
	r.read += int64(n)
	return n, err
}
