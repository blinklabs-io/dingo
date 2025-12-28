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

package gcs

import (
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestWithLogger(t *testing.T) {
	b := &BlobStoreGCS{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	option := WithLogger(logger)

	option(b)

	if b.logger == nil {
		t.Errorf("Expected logger to be set")
	}
}

func TestWithPromRegistry(t *testing.T) {
	b := &BlobStoreGCS{}
	registry := prometheus.NewRegistry()
	option := WithPromRegistry(registry)

	option(b)

	if b.promRegistry != registry {
		t.Errorf("Expected promRegistry to be set correctly")
	}
}

func TestWithBucket(t *testing.T) {
	b := &BlobStoreGCS{}
	option := WithBucket("test-bucket")

	option(b)

	if b.bucketName != "test-bucket" {
		t.Errorf(
			"Expected bucketName to be 'test-bucket', got '%s'",
			b.bucketName,
		)
	}
}
