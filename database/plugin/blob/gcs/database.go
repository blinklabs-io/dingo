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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/prometheus/client_golang/prometheus"
)

// Register plugin
func init() {
	plugin.Register(
		plugin.PluginEntry{
			Type: plugin.PluginTypeBlob,
			Name: "gcs",
		},
	)
}

// BlobStoreGCS stores data in a Google Cloud Storage bucket.
type BlobStoreGCS struct {
	logger        *GcsLogger
	promRegistry  prometheus.Registerer
	client        *storage.Client
	bucket        *storage.BucketHandle
	bucketName    string
	startupCtx    context.Context
	startupCancel context.CancelFunc
}

// New creates a new GCS-backed blob store.
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*BlobStoreGCS, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	const prefix = "gcs://"
	var bucketName string
	if strings.HasPrefix(dataDir, prefix) {
		bucketName = strings.TrimPrefix(dataDir, prefix)
	}
	if bucketName == "" {
		cancel()
		return nil, errors.New("gcs blob: bucket not set (expected dataDir='gcs://<bucket>')")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("gcs blob: failed in creating storage client: %w", err)
	}

	db := &BlobStoreGCS{
		logger:        NewGcsLogger(logger),
		promRegistry:  promRegistry,
		client:        client,
		bucket:        client.Bucket(bucketName),
		bucketName:    bucketName,
		startupCtx:    ctx,
		startupCancel: cancel,
	}

	if err := db.init(); err != nil {
		return db, err
	}
	return db, nil
}

func (d *BlobStoreGCS) init() error {
	// Configure metrics
	if d.promRegistry != nil {
		d.registerBlobMetrics()
	}

	// Close the startup context so that initialization will succeed.
	if d.startupCancel != nil {
		d.startupCancel()
		d.startupCancel = nil
	}
	d.startupCtx = context.Background()
	return nil
}

// closes the GCS client.
func (d *BlobStoreGCS) Close() error {
	return d.client.Close()
}

// Returns the GCS client.
func (d *BlobStoreGCS) Client() *storage.Client {
	return d.client
}

// Returns the bucket handle.
func (d *BlobStoreGCS) Bucket() *storage.BucketHandle {
	return d.bucket
}
