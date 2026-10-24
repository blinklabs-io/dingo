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

package consensus

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"

	gconsensus "github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

const tachysCommitmentDomain = "dingo/tachys-distribution/v1"

// TachysDistribution is an immutable, canonically ordered election snapshot.
// It carries the whole pool distribution because a local stake fraction is not
// enough to reproduce a CIP-0177 schedule.
type TachysDistribution struct {
	pools      []gconsensus.TachysPool
	totalStake uint64
	commitment common.Blake2b256
	selector   *gconsensus.TachysLeaderSelector
}

// NewTachysDistribution validates and copies a full election distribution.
func NewTachysDistribution(
	pools []gconsensus.TachysPool,
	totalStake uint64,
) (*TachysDistribution, error) {
	ordered := append([]gconsensus.TachysPool(nil), pools...)
	slices.SortFunc(ordered, func(a, b gconsensus.TachysPool) int {
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	selector, err := gconsensus.NewTachysLeaderSelector(
		ordered,
		totalStake,
	)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, len(tachysCommitmentDomain)+16+len(ordered)*36)
	buf = append(buf, tachysCommitmentDomain...)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], totalStake)
	buf = append(buf, encoded[:]...)
	binary.BigEndian.PutUint64(encoded[:], uint64(len(ordered)))
	buf = append(buf, encoded[:]...)
	for _, pool := range ordered {
		buf = append(buf, pool.ID[:]...)
		binary.BigEndian.PutUint64(encoded[:], pool.Stake)
		buf = append(buf, encoded[:]...)
	}
	return &TachysDistribution{
		pools:      ordered,
		totalStake: totalStake,
		commitment: common.Blake2b256Hash(buf),
		selector:   selector,
	}, nil
}

// Pools returns a copy of the canonical pool distribution.
func (d *TachysDistribution) Pools() []gconsensus.TachysPool {
	if d == nil {
		return nil
	}
	return append([]gconsensus.TachysPool(nil), d.pools...)
}

// TotalStake returns the exact sum of the pool distribution.
func (d *TachysDistribution) TotalStake() uint64 {
	if d == nil {
		return 0
	}
	return d.totalStake
}

// Commitment returns the full-distribution commitment used by persisted
// schedules.
func (d *TachysDistribution) Commitment() common.Blake2b256 {
	if d == nil {
		return common.Blake2b256{}
	}
	return d.commitment
}

// Selector returns the shared immutable exact-integer selector for this snapshot.
func (d *TachysDistribution) Selector() (*gconsensus.TachysLeaderSelector, error) {
	if d == nil || d.selector == nil {
		return nil, errors.New("tachys distribution is nil")
	}
	return d.selector, nil
}
