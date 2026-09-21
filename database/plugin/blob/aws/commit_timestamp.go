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

//go:build dingo_extra_plugins

package aws

import (
	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/committimestamp"
	"github.com/blinklabs-io/dingo/database/types"
)

func (b *BlobStoreS3) GetCommitTimestamp() (int64, error) {
	return committimestamp.GetEncrypted(b, b.logger, "S3")
}

func (b *BlobStoreS3) SetCommitTimestamp(
	ts int64,
	txn types.Txn,
) error {
	return committimestamp.SetEncrypted(b, b.logger, "S3", ts, txn)
}
