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

package blockfrost

import (
	"context"
	"encoding/json"
	"net/http/httptest"
)

// pageStub is a canned answer for a paginated node method that also records
// the arguments of its last call.
type pageStub[T any] struct {
	items  []T
	total  int
	err    error
	params PaginationParams
	key    string
	number uint64
}

func (p *pageStub[T]) respond(
	number uint64,
	key string,
	params PaginationParams,
) ([]T, int, error) {
	p.number = number
	p.key = key
	p.params = params
	return p.items, p.total, p.err
}

// valueStub is a canned answer for a single-value node method.
type valueStub[T any] struct {
	value  T
	err    error
	number uint64
	key    string
}

func (v *valueStub[T]) respond(number uint64, key string) (T, error) {
	v.number = number
	v.key = key
	return v.value, v.err
}

// historyMock supplies the historical-query half of BlockfrostNode for
// mockNode.
type historyMock struct {
	poolHistoryMock
	accountHistoryMock
	epochByNumber   valueStub[EpochInfo]
	epochsAfter     pageStub[EpochInfo]
	epochsBefore    pageStub[EpochInfo]
	epochStakes     pageStub[EpochStakeInfo]
	epochPoolStakes pageStub[EpochPoolStakeInfo]
	epochBlocks     pageStub[string]
	epochPoolBlocks pageStub[string]
}

func (m *historyMock) Epoch(
	_ context.Context,
	epoch uint64,
) (EpochInfo, error) {
	return m.epochByNumber.respond(epoch, "")
}

func (m *historyMock) EpochsAfter(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochInfo, int, error) {
	return m.epochsAfter.respond(epoch, "", params)
}

func (m *historyMock) EpochsBefore(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochInfo, int, error) {
	return m.epochsBefore.respond(epoch, "", params)
}

func (m *historyMock) EpochStakes(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochStakeInfo, int, error) {
	return m.epochStakes.respond(epoch, "", params)
}

func (m *historyMock) EpochPoolStakes(
	_ context.Context,
	epoch uint64,
	poolID string,
	params PaginationParams,
) ([]EpochPoolStakeInfo, int, error) {
	return m.epochPoolStakes.respond(epoch, poolID, params)
}

func (m *historyMock) EpochBlocks(
	_ context.Context,
	epoch uint64,
	params PaginationParams,
) ([]string, int, error) {
	return m.epochBlocks.respond(epoch, "", params)
}

func (m *historyMock) EpochPoolBlocks(
	_ context.Context,
	epoch uint64,
	poolID string,
	params PaginationParams,
) ([]string, int, error) {
	return m.epochPoolBlocks.respond(epoch, poolID, params)
}

func jsonDecode(recorder *httptest.ResponseRecorder, into any) error {
	return json.Unmarshal(recorder.Body.Bytes(), into)
}

// poolHistoryMock supplies the pool half of historyMock.
type poolHistoryMock struct {
	poolsRetired   pageStub[PoolRetiringInfo]
	poolHistory    pageStub[PoolHistoryInfo]
	poolRelays     valueStub[[]PoolRelayInfo]
	poolDelegators pageStub[PoolDelegatorInfo]
	poolBlocks     pageStub[string]
	poolUpdates    pageStub[PoolUpdateInfo]
}

func (m *poolHistoryMock) PoolsRetired(
	_ context.Context,
	params PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	return m.poolsRetired.respond(0, "", params)
}

func (m *poolHistoryMock) PoolHistory(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolHistoryInfo, int, error) {
	return m.poolHistory.respond(0, poolID, params)
}

func (m *poolHistoryMock) PoolRelays(
	_ context.Context,
	poolID string,
) ([]PoolRelayInfo, error) {
	return m.poolRelays.respond(0, poolID)
}

func (m *poolHistoryMock) PoolDelegators(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolDelegatorInfo, int, error) {
	return m.poolDelegators.respond(0, poolID, params)
}

func (m *poolHistoryMock) PoolBlocks(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]string, int, error) {
	return m.poolBlocks.respond(0, poolID, params)
}

func (m *poolHistoryMock) PoolUpdates(
	_ context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolUpdateInfo, int, error) {
	return m.poolUpdates.respond(0, poolID, params)
}

// accountHistoryMock supplies the account half of historyMock.
type accountHistoryMock struct {
	accountStakeHistory pageStub[AccountStakeHistoryInfo]
	accountMIRs         pageStub[AccountMIRInfo]
	accountAssets       pageStub[AddressAmountInfo]
	accountTotal        valueStub[AccountAddressTotalInfo]
}

func (m *accountHistoryMock) AccountStakeHistory(
	_ context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AccountStakeHistoryInfo, int, error) {
	return m.accountStakeHistory.respond(0, stakeAddress, params)
}

func (m *accountHistoryMock) AccountMIRs(
	_ context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AccountMIRInfo, int, error) {
	return m.accountMIRs.respond(0, stakeAddress, params)
}

func (m *accountHistoryMock) AccountAddressAssets(
	_ context.Context,
	stakeAddress string,
	params PaginationParams,
) ([]AddressAmountInfo, int, error) {
	return m.accountAssets.respond(0, stakeAddress, params)
}

func (m *accountHistoryMock) AccountAddressTotal(
	_ context.Context,
	stakeAddress string,
) (AccountAddressTotalInfo, error) {
	return m.accountTotal.respond(0, stakeAddress)
}
