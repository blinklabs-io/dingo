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

package compensate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type undoCall struct {
	key     string
	value   string
	deleted bool
}

func newTestLog(t *testing.T) *Log {
	t.Helper()
	log, err := NewLog("dingo-compensate-test-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	return log
}

// Undo replays in reverse order so a later change is reversed before an earlier
// one, and it distinguishes restore-a-prior-value from delete-what-we-created.
func TestUndoReversesRecordedChanges(t *testing.T) {
	t.Parallel()

	log := newTestLog(t)
	require.NoError(t, log.RecordValue("existing", []byte("prior")))
	log.RecordMissing("created")
	require.NoError(t, log.RecordValue("other", []byte("other-prior")))
	assert.Equal(t, 3, log.Len())

	var calls []undoCall
	require.NoError(t, log.Undo(
		log.Len(),
		func(key string, value *io.SectionReader, size int64) error {
			got, err := io.ReadAll(value)
			if err != nil {
				return err
			}
			if int64(len(got)) != size {
				return fmt.Errorf(
					"size %d does not match %d bytes read",
					size,
					len(got),
				)
			}
			calls = append(calls, undoCall{key: key, value: string(got)})
			return nil
		},
		func(key string) error {
			calls = append(calls, undoCall{key: key, deleted: true})
			return nil
		},
	))

	assert.Equal(t, []undoCall{
		{key: "other", value: "other-prior"},
		{key: "created", deleted: true},
		{key: "existing", value: "prior"},
	}, calls)
}

// Undo(n) reverses only the first n applied changes, so a commit that failed on
// key i does not try to undo keys it never touched.
func TestUndoOnlyReversesAppliedPrefix(t *testing.T) {
	t.Parallel()

	log := newTestLog(t)
	log.RecordMissing("a")
	log.RecordMissing("b")
	log.RecordMissing("c")

	var deleted []string
	require.NoError(t, log.Undo(
		2,
		func(string, *io.SectionReader, int64) error { return nil },
		func(key string) error {
			deleted = append(deleted, key)
			return nil
		},
	))
	assert.Equal(t, []string{"b", "a"}, deleted)
}

// A single unreachable key must not abandon the rest of the compensation, and
// every failure has to surface so the caller can report a partial commit.
func TestUndoContinuesAfterFailureAndJoinsErrors(t *testing.T) {
	t.Parallel()

	log := newTestLog(t)
	require.NoError(t, log.RecordValue("first", []byte("1")))
	log.RecordMissing("second")
	require.NoError(t, log.RecordValue("third", []byte("3")))

	putErr := errors.New("put failed")
	delErr := errors.New("delete failed")
	var attempted []string
	err := log.Undo(
		log.Len(),
		func(key string, _ *io.SectionReader, _ int64) error {
			attempted = append(attempted, key)
			if key == "third" {
				return putErr
			}
			return nil
		},
		func(key string) error {
			attempted = append(attempted, key)
			return delErr
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, putErr)
	assert.ErrorIs(t, err, delErr)
	assert.Equal(
		t,
		[]string{"third", "second", "first"},
		attempted,
		"every entry is attempted even after a failure",
	)
}

// Prior values live on disk, not in memory: the spool file grows with recorded
// values and is removed on Close.
func TestRecordValueSpoolsToDiskAndCloseRemovesIt(t *testing.T) {
	t.Parallel()

	log, err := NewLog("dingo-compensate-test-")
	require.NoError(t, err)
	name := log.file.Name()

	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	require.NoError(t, log.RecordValue("big", payload))

	info, err := os.Stat(name)
	require.NoError(t, err)
	assert.EqualValues(t, len(payload), info.Size(),
		"prior value should be spooled to the file, not held in memory")

	// The spooled bytes round-trip exactly.
	var restored []byte
	require.NoError(t, log.Undo(
		1,
		func(_ string, value *io.SectionReader, _ int64) error {
			got, err := io.ReadAll(value)
			restored = got
			return err
		},
		func(string) error { return nil },
	))
	assert.Equal(t, payload, restored)

	require.NoError(t, log.Close())
	_, err = os.Stat(name)
	assert.True(t, os.IsNotExist(err), "Close should remove the spool file")
	// Close is idempotent.
	require.NoError(t, log.Close())
}

// RecordMissing uses no spool space, so the common append-only commit keeps the
// compensation log empty on disk.
func TestRecordMissingUsesNoSpoolSpace(t *testing.T) {
	t.Parallel()

	log, err := NewLog("dingo-compensate-test-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	name := log.file.Name()

	for _, key := range []string{"a", "b", "c"} {
		log.RecordMissing(key)
	}

	info, err := os.Stat(name)
	require.NoError(t, err)
	assert.Zero(t, info.Size())
	assert.Equal(t, 3, log.Len())
}

// Undo clamps n rather than panicking on an out-of-range prefix.
func TestUndoClampsOversizedPrefix(t *testing.T) {
	t.Parallel()

	log := newTestLog(t)
	log.RecordMissing("only")

	var deleted []string
	require.NoError(t, log.Undo(
		99,
		func(string, *io.SectionReader, int64) error { return nil },
		func(key string) error {
			deleted = append(deleted, key)
			return nil
		},
	))
	assert.Equal(t, []string{"only"}, deleted)
}

// A closed log cannot spool or restore, and says so instead of silently
// reporting a clean compensation.
func TestClosedLogReportsErrors(t *testing.T) {
	t.Parallel()

	log, err := NewLog("dingo-compensate-test-")
	require.NoError(t, err)
	require.NoError(t, log.RecordValue("key", []byte("value")))
	require.NoError(t, log.Close())

	assert.Error(t, log.RecordValue("other", []byte("value")))
	assert.Error(t, log.Undo(
		1,
		func(string, *io.SectionReader, int64) error { return nil },
		func(string) error { return nil },
	))
}

// RecordValueFrom streams without buffering and without a size cap, so an
// object larger than the plugins' 256 MiB read limit can still be compensated —
// which is what makes it possible to overwrite or delete such an object inside a
// transaction.
func TestRecordValueFromStreamsWithoutSizeCap(t *testing.T) {
	t.Parallel()

	log := newTestLog(t)

	// The log has no size cap of its own, so a few MiB (many times the 64 KiB
	// copy and read chunks) exercises streaming without spooling hundreds of
	// MiB; a repeating reader of known length stands in for a large object.
	const size = int64(4<<20) + 1024
	require.NoError(
		t,
		log.RecordValueFrom("huge", &patternReader{remaining: size}),
	)
	assert.Equal(t, 1, log.Len())

	info, err := os.Stat(log.file.Name())
	require.NoError(t, err)
	assert.Equal(t, size, info.Size())

	// The spooled bytes are restored intact.
	var restoredLen int64
	var mismatch bool
	require.NoError(t, log.Undo(
		1,
		func(_ string, value *io.SectionReader, size int64) error {
			// A SectionReader also satisfies io.ReadSeeker, which the AWS SDK
			// needs in order to re-sign or retry a request body.
			var _ io.ReadSeeker = value
			// Read in chunks: the point of the streaming signature is that a
			// restore never has to materialize the whole object.
			buf := make([]byte, 64*1024)
			var off int64
			for {
				n, readErr := value.Read(buf)
				for i := range n {
					if buf[i] != byte((off+int64(i))%251) {
						mismatch = true
					}
				}
				off += int64(n)
				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					return readErr
				}
			}
			restoredLen = off
			if off != size {
				return fmt.Errorf(
					"size %d does not match %d bytes read",
					size,
					off,
				)
			}
			return nil
		},
		func(string) error { return nil },
	))
	assert.Equal(t, size, restoredLen)
	assert.False(t, mismatch, "spooled bytes must round-trip unchanged")
}

// Mixing buffered and streamed entries must not overlap their spool regions.
func TestRecordValueAndRecordValueFromShareSpoolCorrectly(t *testing.T) {
	t.Parallel()

	log := newTestLog(t)
	require.NoError(t, log.RecordValue("a", []byte("first")))
	require.NoError(t, log.RecordValueFrom("b", strings.NewReader("second")))
	require.NoError(t, log.RecordValue("c", []byte("third")))

	got := map[string]string{}
	require.NoError(t, log.Undo(
		3,
		func(key string, value *io.SectionReader, _ int64) error {
			b, err := io.ReadAll(value)
			got[key] = string(b)
			return err
		},
		func(string) error { return nil },
	))
	assert.Equal(
		t,
		map[string]string{"a": "first", "b": "second", "c": "third"},
		got,
	)
}

// patternReader yields `remaining` bytes of a repeating pattern without holding
// them in memory, standing in for a very large cloud object.
type patternReader struct {
	remaining int64
	pos       int64
}

func (r *patternReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.remaining)
	for i := range n {
		p[i] = byte((r.pos + i) % 251)
	}
	r.pos += n
	r.remaining -= n
	return int(n), nil
}
