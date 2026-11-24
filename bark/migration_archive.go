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

package bark

import (
	"net/http"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/storageadopt"
)

// The split-layout migration fetches blocks that expired locally through the
// archive the configuration names, with the wrapper the node reads through.
func init() {
	storageadopt.ArchiveStore = func(
		cfg *config.Config,
		upstream blob.BlobStore,
	) (blob.BlobStore, error) {
		return NewBarkBlobStore(BlobStoreBarkConfig{
			BaseUrl:                            cfg.BarkBaseUrl,
			BlockDownloadAllowedHosts:          cfg.BarkBlockDownloadHosts,
			BlockDownloadAllowPrivateAddresses: cfg.BarkBlockDownloadAllowPrivateAddresses,
			HTTPClient:                         &http.Client{Timeout: 30 * time.Second},
		}, upstream)
	}
}
