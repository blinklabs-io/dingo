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

package ouroboros

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/stretchr/testify/assert"
)

func TestRequestableTxIdsWithinHeadroom(t *testing.T) {
	tx1 := txsubmission.TxId{EraId: 6, TxId: [32]byte{1}}
	tx2 := txsubmission.TxId{EraId: 6, TxId: [32]byte{2}}
	tx3 := txsubmission.TxId{EraId: 6, TxId: [32]byte{3}}

	tests := []struct {
		name           string
		availableBytes int64
		txIds          []txsubmission.TxIdAndSize
		want           []txsubmission.TxId
	}{
		{
			name:           "takes full prefix that fits",
			availableBytes: 300,
			txIds:          []txsubmission.TxIdAndSize{{TxId: tx1, Size: 100}, {TxId: tx2, Size: 150}, {TxId: tx3, Size: 75}},
			want:           []txsubmission.TxId{tx1, tx2},
		},
		{
			name:           "skips all when first tx is too large",
			availableBytes: 90,
			txIds:          []txsubmission.TxIdAndSize{{TxId: tx1, Size: 100}, {TxId: tx2, Size: 50}},
			want:           []txsubmission.TxId{},
		},
		{
			name:           "stops at first tx that would overflow remaining headroom",
			availableBytes: 160,
			txIds:          []txsubmission.TxIdAndSize{{TxId: tx1, Size: 100}, {TxId: tx2, Size: 70}, {TxId: tx3, Size: 20}},
			want:           []txsubmission.TxId{tx1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(
				t,
				test.want,
				requestableTxIdsWithinHeadroom(test.txIds, test.availableBytes),
			)
		})
	}
}
