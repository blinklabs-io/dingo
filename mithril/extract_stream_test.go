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
	"archive/tar"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancelOnRead struct {
	reader io.Reader
	cancel context.CancelFunc
	read   int
	at     int
}

func (r *cancelOnRead) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read++
	if r.read == r.at {
		r.cancel()
	}
	return n, err
}

func paxTarBytes(t *testing.T) []byte {
	t.Helper()

	var archive bytes.Buffer
	w := tar.NewWriter(&archive)
	require.NoError(t, w.WriteHeader(&tar.Header{
		Name:     "file",
		Typeflag: tar.TypeReg,
		Mode:     0o600,
		Size:     1,
		Format:   tar.FormatPAX,
		PAXRecords: map[string]string{
			"comment": "metadata",
		},
	}))
	_, err := w.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return archive.Bytes()
}

func TestArchiveStreamReaderBoundsMetadataConsumedByTarNext(t *testing.T) {
	t.Parallel()

	reader := &archiveStreamReader{
		ctx:      context.Background(),
		reader:   bytes.NewReader(paxTarBytes(t)),
		maxBytes: 512,
	}
	_, err := tar.NewReader(reader).Next()
	require.ErrorIs(t, err, ErrExtractLimitExceeded)
}

func TestArchiveStreamReaderChecksCancellationDuringTarNext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &archiveStreamReader{
		ctx: ctx,
		reader: &cancelOnRead{
			reader: bytes.NewReader(paxTarBytes(t)),
			cancel: cancel,
			at:     2,
		},
		maxBytes: 1 << 20,
	}
	_, err := tar.NewReader(reader).Next()
	require.ErrorIs(t, err, context.Canceled)
}

func TestArchiveStreamReaderChargesGlobalBudget(t *testing.T) {
	t.Parallel()

	budget := &bootstrapExtractionBudget{maxStreamBytes: 4}
	reader := &archiveStreamReader{
		ctx:      context.Background(),
		reader:   strings.NewReader("hello"),
		budget:   budget,
		maxBytes: 10,
	}
	var buf [4]byte
	n, err := reader.Read(buf[:])
	require.NoError(t, err)
	require.Equal(t, 4, n)
	n, err = reader.Read(buf[:])
	require.ErrorIs(t, err, ErrExtractLimitExceeded)
	require.Zero(t, n)
	require.EqualValues(t, 4, budget.streamBytes)
}
