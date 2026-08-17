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

package aws

import (
	"errors"
	"testing"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestIsS3NotFoundRecognizesHeadObjectAndGetObjectVariants guards a real
// bug found via a live MinIO run: GetObject and HeadObject disagree on
// which error type/code they report for the identical "key does not
// exist" condition -- GetObject returns NoSuchKey, but HeadObject (which
// objectExists uses, and both Delete and Commit's per-key existence probe
// depend on) returns the differently-coded NotFound instead, since a HEAD
// response has no body to parse a specific error out of. Missing the
// NotFound case made every existence probe against a genuinely-absent key
// fail with a hard error instead of correctly reporting "not found".
func TestIsS3NotFoundRecognizesHeadObjectAndGetObjectVariants(t *testing.T) {
	require.True(t, isS3NotFound(&s3types.NoSuchKey{}))
	require.True(t, isS3NotFound(&s3types.NotFound{}))
	require.False(t, isS3NotFound(errors.New("some other failure")))
	require.False(t, isS3NotFound(nil))
}
