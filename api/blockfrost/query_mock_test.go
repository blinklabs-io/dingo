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
	"sync"
)

// queryMock holds the canned results of the block, address, asset, script and
// mempool query surfaces. mockNode embeds it so those methods are promoted.
// Each method records the arguments of its last call under mu, since handler
// tests run the router concurrently.
type queryMock struct {
	mu sync.Mutex

	navBlocks        []BlockInfo
	navBlock         BlockInfo
	navHashes        []string
	navTxCBOR        []BlockTransactionCBORInfo
	navAddresses     []BlockAddressInfo
	navErr           error
	lastBlockID      string
	lastBlockParams  PaginationParams
	lastBlockSlot    uint64
	lastBlockEpoch   uint64
	lastBlockEpSlot  uint64
	lastBlockCalls   int
	addressExtended  AddressExtendedInfo
	addressTotal     AddressTotalInfo
	addressAssetUTXO []AddressUTXOInfo
	addressTotalN    int
	addressErr       error
	lastAddress      string
	lastAssetPolicy  string
	lastAssetName    []byte
	lastAddressPage  PaginationParams

	assetList       []AssetListItemInfo
	assetHistory    []AssetHistoryItemInfo
	assetTxs        []AssetTransactionInfo
	assetUTXOs      []AssetUTXOInfo
	assetTotal      int
	assetListErr    error
	lastAssetRange  TransactionRangeParams
	lastAssetParams PaginationParams
	lastPolicyID    string

	scripts         []string
	scriptInfo      ScriptInfo
	scriptJSON      json.RawMessage
	scriptCBOR      *string
	scriptRedeemers []ScriptRedeemerInfo
	scriptUTXOs     []ScriptUTXOInfo
	datumJSON       json.RawMessage
	datumCBOR       string
	scriptTotal     int
	scriptErr       error
	lastScriptHash  []byte
	lastScriptPage  PaginationParams

	mempoolHashes []string
	mempoolTx     MempoolTransactionInfo
	mempoolTotal  int
	mempoolErr    error
	lastMempool   PaginationParams
	lastMempoolTx []byte
}

func (m *queryMock) recordBlock(id string, params PaginationParams) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastBlockID = id
	m.lastBlockParams = params
	m.lastBlockCalls++
}

func (m *queryMock) BlockNext(
	_ context.Context,
	id string,
	params PaginationParams,
) ([]BlockInfo, error) {
	m.recordBlock(id, params)
	return m.navBlocks, m.navErr
}

func (m *queryMock) BlockPrevious(
	_ context.Context,
	id string,
	params PaginationParams,
) ([]BlockInfo, error) {
	m.recordBlock(id, params)
	return m.navBlocks, m.navErr
}

func (m *queryMock) BlockBySlot(
	_ context.Context,
	slot uint64,
) (BlockInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastBlockSlot = slot
	return m.navBlock, m.navErr
}

func (m *queryMock) BlockByEpochSlot(
	_ context.Context,
	epoch uint64,
	epochSlot uint64,
) (BlockInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastBlockEpoch = epoch
	m.lastBlockEpSlot = epochSlot
	return m.navBlock, m.navErr
}

func (m *queryMock) BlockTransactionHashes(
	_ context.Context,
	id string,
) ([]string, error) {
	m.recordBlock(id, PaginationParams{})
	return m.navHashes, m.navErr
}

func (m *queryMock) BlockTransactionCBOR(
	_ context.Context,
	id string,
) ([]BlockTransactionCBORInfo, error) {
	m.recordBlock(id, PaginationParams{})
	return m.navTxCBOR, m.navErr
}

func (m *queryMock) LatestBlockTransactionCBOR(
	_ context.Context,
) ([]BlockTransactionCBORInfo, error) {
	m.recordBlock("latest", PaginationParams{})
	return m.navTxCBOR, m.navErr
}

func (m *queryMock) BlockAddresses(
	_ context.Context,
	id string,
) ([]BlockAddressInfo, error) {
	m.recordBlock(id, PaginationParams{})
	return m.navAddresses, m.navErr
}

func (m *queryMock) AddressExtended(
	_ context.Context,
	address string,
) (AddressExtendedInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAddress = address
	return m.addressExtended, m.addressErr
}

func (m *queryMock) AddressTotal(
	_ context.Context,
	address string,
) (AddressTotalInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAddress = address
	return m.addressTotal, m.addressErr
}

func (m *queryMock) AddressAssetUTXOs(
	_ context.Context,
	address string,
	policyID string,
	assetName []byte,
	params PaginationParams,
) ([]AddressUTXOInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAddress = address
	m.lastAssetPolicy = policyID
	m.lastAssetName = assetName
	m.lastAddressPage = params
	return m.addressAssetUTXO, m.addressTotalN, m.addressErr
}

func (m *queryMock) Assets(
	_ context.Context,
	params PaginationParams,
) ([]AssetListItemInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAssetParams = params
	return m.assetList, m.assetTotal, m.assetListErr
}

func (m *queryMock) AssetsByPolicy(
	_ context.Context,
	policyID string,
	params PaginationParams,
) ([]AssetListItemInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastPolicyID = policyID
	m.lastAssetParams = params
	return m.assetList, m.assetTotal, m.assetListErr
}

func (m *queryMock) AssetHistory(
	_ context.Context,
	policyID string,
	assetName []byte,
	params PaginationParams,
) ([]AssetHistoryItemInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAssetPolicy = policyID
	m.lastAssetName = assetName
	m.lastAssetParams = params
	return m.assetHistory, m.assetTotal, m.assetListErr
}

func (m *queryMock) AssetTransactions(
	_ context.Context,
	policyID string,
	assetName []byte,
	params TransactionRangeParams,
) ([]AssetTransactionInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAssetPolicy = policyID
	m.lastAssetName = assetName
	m.lastAssetRange = params
	return m.assetTxs, m.assetTotal, m.assetListErr
}

func (m *queryMock) AssetUTXOs(
	_ context.Context,
	policyID string,
	assetName []byte,
	params TransactionRangeParams,
) ([]AssetUTXOInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAssetPolicy = policyID
	m.lastAssetName = assetName
	m.lastAssetRange = params
	return m.assetUTXOs, m.assetTotal, m.assetListErr
}

func (m *queryMock) Scripts(
	_ context.Context,
	params PaginationParams,
) ([]string, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptPage = params
	return m.scripts, m.scriptTotal, m.scriptErr
}

func (m *queryMock) Script(
	_ context.Context,
	hash []byte,
) (ScriptInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptHash = hash
	return m.scriptInfo, m.scriptErr
}

func (m *queryMock) ScriptJSON(
	_ context.Context,
	hash []byte,
) (json.RawMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptHash = hash
	return m.scriptJSON, m.scriptErr
}

func (m *queryMock) ScriptCBOR(
	_ context.Context,
	hash []byte,
) (*string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptHash = hash
	return m.scriptCBOR, m.scriptErr
}

func (m *queryMock) ScriptRedeemers(
	_ context.Context,
	hash []byte,
	params PaginationParams,
) ([]ScriptRedeemerInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptHash = hash
	m.lastScriptPage = params
	return m.scriptRedeemers, m.scriptTotal, m.scriptErr
}

func (m *queryMock) ScriptUTXOs(
	_ context.Context,
	hash []byte,
	params PaginationParams,
) ([]ScriptUTXOInfo, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptHash = hash
	m.lastScriptPage = params
	return m.scriptUTXOs, m.scriptTotal, m.scriptErr
}

func (m *queryMock) DatumJSON(
	_ context.Context,
	hash []byte,
) (json.RawMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptHash = hash
	return m.datumJSON, m.scriptErr
}

func (m *queryMock) DatumCBOR(
	_ context.Context,
	hash []byte,
) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastScriptHash = hash
	return m.datumCBOR, m.scriptErr
}

func (m *queryMock) MempoolTransactions(
	_ context.Context,
	params PaginationParams,
) ([]string, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastMempool = params
	return m.mempoolHashes, m.mempoolTotal, m.mempoolErr
}

func (m *queryMock) MempoolTransaction(
	_ context.Context,
	hash []byte,
) (MempoolTransactionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastMempoolTx = hash
	return m.mempoolTx, m.mempoolErr
}

func (m *queryMock) MempoolAddressTransactions(
	_ context.Context,
	address string,
	params PaginationParams,
) ([]string, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAddress = address
	m.lastMempool = params
	return m.mempoolHashes, m.mempoolTotal, m.mempoolErr
}
