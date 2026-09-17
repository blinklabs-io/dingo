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

//go:build dingo_extra_plugins

package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestIsS3NotFoundErrorMatchesNoSuchKey verifies the case AWS's own S3
// actually returns from GetObject on a missing key.
func TestIsS3NotFoundErrorMatchesNoSuchKey(t *testing.T) {
	t.Parallel()

	require.True(t, isS3NotFoundError(&types.NoSuchKey{}))
}

// TestIsS3NotFoundErrorMatchesTypedNotFound guards the defensive
// s3types.NotFound check, kept even though AWS's own GetObject never
// actually constructs one.
func TestIsS3NotFoundErrorMatchesTypedNotFound(t *testing.T) {
	t.Parallel()

	require.True(t, isS3NotFoundError(&types.NotFound{}))
}

// TestIsS3NotFoundErrorMatchesGenericNotFoundCode guards against
// a real gap: an S3-compatible (non-AWS) endpoint can
// report a missing key as a generic, untyped API error whose ErrorCode()
// is "NotFound" without the SDK ever deserializing it into the strongly-
// typed s3types.NotFound struct -- errors.As for that concrete type alone
// would miss this, silently treating a confirmed-absent snapshot as a
// real communication failure instead.
func TestIsS3NotFoundErrorMatchesGenericNotFoundCode(t *testing.T) {
	t.Parallel()

	err := &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}
	require.True(t, isS3NotFoundError(err))
}

// TestIsS3NotFoundErrorRejectsOtherErrors verifies real failures (auth,
// throttling, a generic error code that isn't "NotFound") are not
// misclassified as a confirmed-absent object.
func TestIsS3NotFoundErrorRejectsOtherErrors(t *testing.T) {
	t.Parallel()

	require.False(t, isS3NotFoundError(errors.New("connection reset")))
	require.False(t, isS3NotFoundError(
		&smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"},
	))
}

var _ smithy.APIError = (*smithy.GenericAPIError)(nil)

type manifestRoundTripper func(*http.Request) (*http.Response, error)

func (f manifestRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestS3ManifestByteLimits(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, WriteManifest(dir, Manifest{Network: "preview"}))
	data, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	require.NoError(t, err)
	for _, oversized := range []bool{false, true} {
		t.Run(fmt.Sprintf("oversized=%t", oversized), func(t *testing.T) {
			body := bytes.NewReader(data)
			client := s3.NewFromConfig(aws.Config{
				Region: "test", Credentials: aws.AnonymousCredentials{},
				HTTPClient: &http.Client{Transport: manifestRoundTripper(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Body: io.NopCloser(body), ContentLength: -1, Header: make(http.Header)}, nil
				})},
			})
			d := s3Destination{client: client, bucket: "test", prefix: "snapshot"}
			limit := int64(len(data))
			if oversized {
				limit = 3
			}
			_, err := d.FetchManifestWithOptions(context.Background(), WithManifestMaxBytes(limit))
			if oversized {
				require.ErrorContains(t, err, "size exceeds maximum")
				require.ErrorIs(t, err, ErrManifestTooLarge)
				require.False(t, errors.Is(err, ErrCloudSnapshotNotFound))
				require.Equal(t, 4, len(data)-body.Len(), "cloud read must stop at limit plus probe")
			} else {
				require.NoError(t, err)
				require.Zero(t, body.Len())
			}
		})
	}
}
