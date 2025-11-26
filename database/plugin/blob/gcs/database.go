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
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/option"
)

// BlobStoreGCS stores data in a Google Cloud Storage bucket.
type BlobStoreGCS struct {
	promRegistry    prometheus.Registerer
	startupCtx      context.Context
	logger          *GcsLogger
	client          *storage.Client
	bucket          *storage.BucketHandle
	startupCancel   context.CancelFunc
	bucketName      string
	credentialsFile string
}

// New creates a new GCS-backed blob store.
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*BlobStoreGCS, error) {
	const prefix = "gcs://"
	var bucketName string
	if after, ok := strings.CutPrefix(dataDir, prefix); ok {
		bucketName = after
	}
	if bucketName == "" {
		return nil, errors.New(
			"gcs blob: bucket not set (expected dataDir='gcs://<bucket>')",
		)
	}

	return NewWithOptions(
		WithBucket(bucketName),
		WithLogger(logger),
		WithPromRegistry(promRegistry),
	)
}

// NewWithOptions creates a new GCS-backed blob store using options.
func NewWithOptions(opts ...BlobStoreGCSOptionFunc) (*BlobStoreGCS, error) {
	db := &BlobStoreGCS{}

	// Apply options
	for _, opt := range opts {
		opt(db)
	}

	// Set defaults
	if db.logger == nil {
		db.logger = NewGcsLogger(slog.New(slog.NewJSONHandler(io.Discard, nil)))
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

// Close closes the GCS client.
func (d *BlobStoreGCS) Close() error {
	if d.client == nil {
		return nil
	}
	err := d.client.Close()
	d.client = nil
	return err
}

// Returns the GCS client.
func (d *BlobStoreGCS) Client() *storage.Client {
	return d.client
}

// Returns the bucket handle.
func (d *BlobStoreGCS) Bucket() *storage.BucketHandle {
	return d.bucket
}

// Start implements the plugin.Plugin interface.
func (d *BlobStoreGCS) Start() error {
	// Validate required fields
	if d.bucketName == "" {
		return errors.New("gcs blob: bucket not set")
	}

	// Validate credentials file if specified
	if d.credentialsFile != "" {
		if err := validateCredentials(d.credentialsFile); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	var clientOpts []option.ClientOption
	clientOpts = append(clientOpts, storage.WithDisabledClientMetrics())
	if d.credentialsFile != "" {
		clientOpts = append(
			clientOpts,
			option.WithCredentialsFile(d.credentialsFile),
		)
	}

	client, err := storage.NewGRPCClient(
		ctx,
		clientOpts...,
	)
	if err != nil {
		cancel()
		return fmt.Errorf(
			"gcs blob: failed in creating storage client: %w",
			err,
		)
	}

	d.client = client
	d.bucket = client.Bucket(d.bucketName)
	d.startupCtx = ctx
	d.startupCancel = cancel

	if err := d.init(); err != nil {
		// Clean up resources on init failure
		d.Close()
		return err
	}
	return nil
}

// Stop implements the plugin.Plugin interface.
func (d *BlobStoreGCS) Stop() error {
	return d.Close()
}
