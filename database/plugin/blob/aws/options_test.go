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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithPrefixPreservesObjectKeyLayout(t *testing.T) {
	testCases := []struct {
		name   string
		prefix string
	}{
		{name: "empty", prefix: ""},
		{name: "without trailing slash", prefix: "foo"},
		{name: "with trailing slash", prefix: "foo/"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store, err := NewWithOptions(WithPrefix(testCase.prefix))
			require.NoError(t, err)
			require.Equal(t, testCase.prefix+"6b6579", store.fullKey("key"))
		})
	}
}

func TestNewNormalizesDataDirPrefix(t *testing.T) {
	testCases := []struct {
		name     string
		dataDir  string
		expected string
	}{
		{name: "without prefix", dataDir: "s3://bucket", expected: "6b6579"},
		{
			name:     "without trailing slash",
			dataDir:  "s3://bucket/foo",
			expected: "foo/6b6579",
		},
		{
			name:     "with trailing slash",
			dataDir:  "s3://bucket/foo/",
			expected: "foo/6b6579",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store, err := New(testCase.dataDir, nil, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, store.fullKey("key"))
		})
	}
}

func TestWithPrefixOptionCanBeReusedConcurrently(t *testing.T) {
	const workerCount = 16

	option := WithPrefix("foo")
	stores := make([]BlobStoreS3, workerCount)
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			option(&stores[i])
		}()
	}
	wg.Wait()

	for i := range workerCount {
		require.Equal(t, "foo6b6579", stores[i].fullKey("key"))
	}
}
