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

package chainselection

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func assertConsensusProviderSurface(config ChainSelectorConfig) {
	_ = config.Consensus.Name
	_ = config.Consensus.NetworkIdentity
	_ = config.Consensus.DatabaseIdentity
	_ = config.Consensus.ScheduleIdentity
	_ = config.Consensus.ValidateHeader
	_ = config.Consensus.CompareTips
	_ = config.Consensus.NewLeaderScheduler
}

func TestChainSelectorConfigConsensusPreservesProviderAPI(t *testing.T) {
	t.Parallel()

	configType := reflect.TypeFor[ChainSelectorConfig]()
	field, ok := configType.FieldByName("Consensus")
	require.True(t, ok)
	methods := []string{
		"CompareTips",
		"DatabaseIdentity",
		"Name",
		"NetworkIdentity",
		"NewLeaderScheduler",
		"ScheduleIdentity",
		"ValidateHeader",
	}
	for _, method := range methods {
		_, found := field.Type.MethodByName(method)
		require.Truef(
			t,
			found,
			"ChainSelectorConfig.Consensus lost Provider.%s",
			method,
		)
	}
	require.Equal(
		t,
		"github.com/blinklabs-io/dingo/consensus",
		field.Type.PkgPath(),
	)
	require.Equal(
		t,
		"Provider",
		field.Type.Name(),
		"ChainSelectorConfig.Consensus must retain consensus.Provider identity",
	)
}
