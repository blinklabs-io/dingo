//go:build linux && devnet && !devnet_conformance

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

package scenarios

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func TestStakeAddressInfoQueries(t *testing.T) {
	cred := olsq.StakeCredential{
		Tag:   0,
		Bytes: lcommon.NewBlake2b224([]byte("stake-address-info")),
	}
	for name, addr := range devnet.DingoNtcAddrs() {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, devnet.QueryStakeAddressInfoByNtc(
				addr,
				devnet.DefaultNetworkMagic,
				cred,
			))
		})
	}
}
