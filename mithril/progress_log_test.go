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
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogDownloadProgressFormatsFields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	logDownloadProgress(logger, "preview", "immutable_archives", DownloadProgress{
		BytesDownloaded: 10251151472,
		TotalBytes:      15511564579,
		Percent:         66.08715336090792,
		BytesPerSecond:  9.107229714665115e+06,
	})

	out := buf.String()
	assert.Contains(t, out, "bytes_per_second=9107229 ")
	assert.Contains(t, out, "percent=66.09 ")
	assert.Contains(t, out, `rate="9.1 MB/s"`)
	assert.Contains(t, out, `downloaded="10 GB / 16 GB"`)
	assert.Contains(t, out, "bytes_downloaded=10251151472")
	assert.Contains(t, out, "total_bytes=15511564579")
	assert.NotContains(t, out, "e+0")
}

func TestFormatSIBytes(t *testing.T) {
	t.Parallel()
	cases := map[int64]string{
		-5:                  "0 B",
		0:                   "0 B",
		999:                 "999 B",
		1000:                "1.0 kB",
		9107229:             "9.1 MB",
		9949999:             "9.9 MB",
		9950000:             "10 MB",
		15511564579:         "16 GB",
		10251151472:         "10 GB",
		9223372036854775807: "9.2 EB",
	}
	for in, want := range cases {
		assert.Equal(t, want, formatSIBytes(in), "input %d", in)
	}
}

func TestRoundAndWholeRate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 66.09, roundPercent(66.08715336090792))
	assert.Equal(t, 0.0, roundPercent(-0.0+0))
	assert.Equal(t, int64(9107229), wholeBytesPerSecond(9.107229714665115e+06))
	assert.Equal(t, int64(0), wholeBytesPerSecond(-1))
	assert.Equal(t, int64(0), wholeBytesPerSecond(0.5))
}

func TestFormatSIBytesCarriesToNextUnit(t *testing.T) {
	t.Parallel()
	cases := map[int64]string{
		999_499:       "999 kB",
		999_500:       "1.0 MB",
		999_999:       "1.0 MB",
		999_999_999:   "1.0 GB",
		1_000_000_000: "1.0 GB",
	}
	for in, want := range cases {
		assert.Equal(t, want, formatSIBytes(in), "input %d", in)
	}
}

func TestImmutableArchiveProgressLogsRoundedPercent(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	cfg := BootstrapConfig{
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	}
	onDone := newImmutableProgressWithContext(cfg, 3, 0, "", "")
	onDone(1)

	out := buf.String()
	assert.Contains(t, out, "immutable archives: progress")
	assert.Contains(t, out, "percent=33.33\n")
}
