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

package models

import (
	"errors"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

var ErrBlockNotFound = errors.New("block not found")

type Block struct {
	Hash     []byte
	PrevHash []byte
	Cbor     []byte
	ID       uint64
	Slot     uint64
	Number   uint64
	Type     uint
}

// Decode decodes b.Cbor as a block of b.Type. config is forwarded to
// gouroboros for every type except Conway, which always routes through
// DecodeConwayBlock (Musashi/Leios's extended header has nothing config
// would toggle); at most one config is meaningful, matching
// ledger.NewBlockFromCbor's own variadic convention. Omitting it preserves
// every existing caller's behavior unchanged.
func (b Block) Decode(config ...common.VerifyConfig) (ledger.Block, error) {
	// Conway blocks may carry the Musashi/Leios extended header; route them
	// through the Leios-aware decoder, which falls back to reconstructing the
	// block only when gouroboros' strict Conway decode fails.
	if b.Type == ledger.BlockTypeConway {
		return DecodeConwayBlock(b.Cbor)
	}
	return ledger.NewBlockFromCbor(b.Type, b.Cbor, config...)
}
