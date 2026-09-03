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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlstore

import (
	"context"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestCommitteeCertificateRejectsUnsupportedCredentialTag proves committee
// certificates validate their credential tags before writing, like every
// other credential-backed certificate path.
//
// Credential.CredType is decoded from CBOR without a range check. Storing it
// raw would write a tag no validated uint8 writer can ever match, so the
// member would silently drop out of the active committee, or the row would
// fail to scan back into the uint8 model field. The tag conversion runs before
// any statement, so a nil queryer is never reached on the rejection path.
func TestCommitteeCertificateRejectsUnsupportedCredentialTag(t *testing.T) {
	t.Parallel()

	var hash lcommon.Blake2b224
	hash[0] = 0xe1
	const unsupportedTag = uint(7)

	store := &Store{}
	for _, test := range []struct {
		name        string
		certificate lcommon.Certificate
	}{
		{
			name: "auth committee hot unsupported cold tag",
			certificate: &lcommon.AuthCommitteeHotCertificate{
				CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: lcommon.Credential{
					CredType:   unsupportedTag,
					Credential: hash,
				},
				HotCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeAddrKeyHash,
					Credential: hash,
				},
			},
		},
		{
			name: "auth committee hot unsupported hot tag",
			certificate: &lcommon.AuthCommitteeHotCertificate{
				CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeAddrKeyHash,
					Credential: hash,
				},
				HotCredential: lcommon.Credential{
					CredType:   unsupportedTag,
					Credential: hash,
				},
			},
		},
		{
			name: "resign committee cold unsupported cold tag",
			certificate: &lcommon.ResignCommitteeColdCertificate{
				CertType: uint(lcommon.CertificateTypeResignCommitteeCold),
				ColdCredential: lcommon.Credential{
					CredType:   unsupportedTag,
					Credential: hash,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := store.applySpecializedCertificate(
				context.Background(),
				nil,
				test.certificate,
				1,
				100,
				0,
				0,
				0,
			)
			require.ErrorContains(t, err, "unsupported stake credential tag")
		})
	}
}
