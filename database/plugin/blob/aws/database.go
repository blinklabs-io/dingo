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

package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/prometheus/client_golang/prometheus"
)

// Register plugin
func init() {
	plugin.Register(
		plugin.PluginEntry{
			Type: plugin.PluginTypeBlob,
			Name: "s3",
		},
	)
}

// BlobStoreS3 stores data in an AWS S3 bucket
type BlobStoreS3 struct {
	promRegistry  prometheus.Registerer
	startupCtx    context.Context
	logger        *S3Logger
	client        *s3.Client
	bucket        string
	prefix        string
	startupCancel context.CancelFunc
}

// New creates a new S3-backed blob store and dataDir must be "s3://bucket" or "s3://bucket/prefix"
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*BlobStoreS3, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	const prefix = "s3://"
	if !strings.HasPrefix(strings.ToLower(dataDir), prefix) {
		cancel()
		return nil, errors.New("s3 blob: expected dataDir='s3://<bucket>[/prefix]'")
	}

	path := strings.TrimPrefix(strings.ToLower(dataDir), prefix)
	if path == "" {
		cancel()
		return nil, errors.New("s3 blob: bucket not set")
	}

	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		cancel()
		return nil, errors.New("s3 blob: invalid S3 path (missing bucket)")
	}

	bucket := parts[0]
	keyPrefix := ""
	if len(parts) > 1 && parts[1] != "" {
		keyPrefix = strings.TrimSuffix(parts[1], "/")
		if keyPrefix != "" {
			keyPrefix += "/"
		}
	}

	// Loads all the aws credentials.
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("s3 blob: load default AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg)

	db := &BlobStoreS3{
		logger:        NewS3Logger(logger),
		promRegistry:  promRegistry,
		client:        client,
		bucket:        bucket,
		prefix:        keyPrefix,
		startupCtx:    ctx,
		startupCancel: cancel,
	}
	if err := db.init(); err != nil {
		cancel()
		return nil, err
	}
	return db, nil
}

func (d *BlobStoreS3) init() error {
	// Configure metrics
	if d.promRegistry != nil {
		d.registerBlobMetrics()
	}

	// Close the startup context so that initialization will succeed.
	if d.startupCancel != nil {
		d.startupCancel()
		d.startupCancel = nil
	}
	return nil
}

// Returns the S3 client.
func (d *BlobStoreS3) Client() *s3.Client {
	return d.client
}

// Returns the bucket handle.
func (d *BlobStoreS3) Bucket() string {
	return d.bucket
}

// Returns the S3 key with an optional prefix.
func (d *BlobStoreS3) fullKey(key string) string {
	return d.prefix + key
}

func awsString(s string) *string {
	return &s
}

// Get reads the value at key.
func (d *BlobStoreS3) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    awsString(d.fullKey(key)),
	})
	if err != nil {
		d.logger.Errorf("s3 get %q failed: %v", key, err)
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		d.logger.Errorf("s3 read %q failed: %v", key, err)
		return nil, err
	}
	d.logger.Infof("s3 get %q ok (%d bytes)", key, len(data))
	return data, nil
}

// Put writes a value to key.
func (d *BlobStoreS3) Put(ctx context.Context, key string, value []byte) error {
	_, err := d.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &d.bucket,
		Key:    awsString(d.fullKey(key)),
		Body:   bytes.NewReader(value),
	})
	if err != nil {
		d.logger.Errorf("s3 put %q failed: %v", key, err)
		return err
	}
	d.logger.Infof("s3 put %q ok (%d bytes)", key, len(value))
	return nil
}
