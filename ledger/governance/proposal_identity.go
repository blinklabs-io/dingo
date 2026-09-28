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

package governance

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/blinklabs-io/dingo/database/models"
)

const proposalRewardSourcePrefix = "dingo:governance-proposal:"

func proposalRewardSourceHash(proposal *models.GovernanceProposal) []byte {
	if proposal == nil {
		return nil
	}
	buf := make(
		[]byte,
		0,
		len(proposalRewardSourcePrefix)+len(proposal.TxHash)+4,
	)
	buf = append(buf, proposalRewardSourcePrefix...)
	buf = append(buf, proposal.TxHash...)
	buf = binary.BigEndian.AppendUint32(buf, proposal.ActionIndex)
	sum := sha256.Sum256(buf)
	return sum[:]
}
