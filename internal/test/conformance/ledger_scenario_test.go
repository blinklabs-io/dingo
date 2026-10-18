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

package conformance

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
)

func TestCommitteeCredentialKeyMatchesProviderKey(t *testing.T) {
	t.Parallel()
	credential := common.Credential{
		CredType:   common.CredentialTypeScriptHash,
		Credential: common.CredentialHash{1, 2, 3},
	}
	want := string(
		[]byte{uint8(credential.CredType)},
	) + string(
		credential.Credential[:],
	)
	if got := committeeCredentialKey(credential); got != want {
		t.Fatalf("committee credential key = %q, want %q", got, want)
	}
}
