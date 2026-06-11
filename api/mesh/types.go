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

package mesh

// Mesh API types following the Coinbase Mesh specification v1.4.15.
// These types are defined manually to avoid the broken mesh-sdk-go
// module path issue. They implement the JSON wire format expected by
// mesh-cli and other Mesh-compatible tooling.

// NetworkIdentifier identifies a blockchain network.
type NetworkIdentifier struct {
	Blockchain string `json:"blockchain"`
	Network    string `json:"network"`
}

// BlockIdentifier uniquely identifies a block.
type BlockIdentifier struct {
	Index int64  `json:"index"`
	Hash  string `json:"hash"`
}

// PartialBlockIdentifier is used in requests
// where either index or hash may be specified.
type PartialBlockIdentifier struct {
	Index *int64  `json:"index,omitempty"`
	Hash  *string `json:"hash,omitempty"`
}

// TransactionIdentifier uniquely identifies a transaction.
type TransactionIdentifier struct {
	Hash string `json:"hash"`
}

// AccountIdentifier uniquely identifies an account.
type AccountIdentifier struct {
	Address    string                `json:"address"`
	SubAccount *SubAccountIdentifier `json:"sub_account,omitempty"`
}

// SubAccountIdentifier identifies a sub-account.
type SubAccountIdentifier struct {
	Address string `json:"address"`
}

// CoinIdentifier uniquely identifies a UTXO coin.
type CoinIdentifier struct {
	Identifier string `json:"identifier"`
}

// Currency represents a currency with symbol and decimals.
type Currency struct {
	Symbol   string         `json:"symbol"`
	Decimals int32          `json:"decimals"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Amount represents a value in a specific currency.
type Amount struct {
	Value    string    `json:"value"`
	Currency *Currency `json:"currency"`
}

// Operation describes a single mutation to state.
type Operation struct {
	OperationIdentifier *OperationIdentifier   `json:"operation_identifier"`
	RelatedOperations   []*OperationIdentifier `json:"related_operations,omitempty"`
	Type                string                 `json:"type"`
	Status              *string                `json:"status,omitempty"`
	Account             *AccountIdentifier     `json:"account,omitempty"`
	Amount              *Amount                `json:"amount,omitempty"`
	CoinChange          *CoinChange            `json:"coin_change,omitempty"`
	Metadata            map[string]any         `json:"metadata,omitempty"`
}

// OperationIdentifier uniquely identifies an operation.
type OperationIdentifier struct {
	Index        int64  `json:"index"`
	NetworkIndex *int64 `json:"network_index,omitempty"`
}

// CoinChange describes the change to a coin (created or spent).
type CoinChange struct {
	CoinIdentifier *CoinIdentifier `json:"coin_identifier"`
	CoinAction     string          `json:"coin_action"`
}

// Coin represents a UTXO.
type Coin struct {
	CoinIdentifier *CoinIdentifier `json:"coin_identifier"`
	Amount         *Amount         `json:"amount"`
}

// Transaction represents a blockchain transaction.
type Transaction struct {
	TransactionIdentifier *TransactionIdentifier `json:"transaction_identifier"`
	Operations            []*Operation           `json:"operations"`
	RelatedTransactions   []*RelatedTransaction  `json:"related_transactions,omitempty"`
	Metadata              map[string]any         `json:"metadata,omitempty"`
}

// RelatedTransaction identifies a related transaction.
type RelatedTransaction struct {
	NetworkIdentifier     *NetworkIdentifier     `json:"network_identifier,omitempty"`
	TransactionIdentifier *TransactionIdentifier `json:"transaction_identifier"`
	Direction             string                 `json:"direction"`
}

// Block represents a blockchain block.
type Block struct {
	BlockIdentifier       *BlockIdentifier `json:"block_identifier"`
	ParentBlockIdentifier *BlockIdentifier `json:"parent_block_identifier"`
	Timestamp             int64            `json:"timestamp"`
	Transactions          []*Transaction   `json:"transactions"`
	Metadata              map[string]any   `json:"metadata,omitempty"`
}

// Peer represents a network peer.
type Peer struct {
	PeerID   string         `json:"peer_id"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// SyncStatus describes the sync state.
type SyncStatus struct {
	CurrentIndex *int64  `json:"current_index,omitempty"`
	TargetIndex  *int64  `json:"target_index,omitempty"`
	Stage        *string `json:"stage,omitempty"`
	Synced       *bool   `json:"synced,omitempty"`
}

// Version describes software version info.
type Version struct {
	RosettaVersion    string         `json:"rosetta_version"`
	NodeVersion       string         `json:"node_version"`
	MiddlewareVersion *string        `json:"middleware_version,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
}

// Allow specifies what the server supports.
type Allow struct {
	OperationStatuses       []*OperationStatus  `json:"operation_statuses"`
	OperationTypes          []string            `json:"operation_types"`
	Errors                  []*Error            `json:"errors"`
	HistoricalBalanceLookup bool                `json:"historical_balance_lookup"`
	CallMethods             []string            `json:"call_methods,omitempty"`
	BalanceExemptions       []*BalanceExemption `json:"balance_exemptions,omitempty"`
	MempoolCoins            bool                `json:"mempool_coins"`
	BlockHashCase           *string             `json:"block_hash_case,omitempty"`
	TransactionHashCase     *string             `json:"transaction_hash_case,omitempty"`
}

// OperationStatus describes the status of an operation.
type OperationStatus struct {
	Status     string `json:"status"`
	Successful bool   `json:"successful"`
}

// BalanceExemption allows operations to change
// balances without affecting account balances.
type BalanceExemption struct {
	SubAccountAddress string    `json:"sub_account_address,omitempty"`
	Currency          *Currency `json:"currency,omitempty"`
	ExemptionType     string    `json:"exemption_type,omitempty"`
}

// SigningPayload is the payload to be signed.
type SigningPayload struct {
	AccountIdentifier *AccountIdentifier `json:"account_identifier,omitempty"`
	HexBytes          string             `json:"hex_bytes"`
	SignatureType     string             `json:"signature_type,omitempty"`
}

// Signature is a signed payload.
type Signature struct {
	SigningPayload *SigningPayload `json:"signing_payload"`
	PublicKey      *PublicKey      `json:"public_key"`
	SignatureType  string          `json:"signature_type"`
	HexBytes       string          `json:"hex_bytes"`
}

// PublicKey represents a public key.
type PublicKey struct {
	HexBytes  string `json:"hex_bytes"`
	CurveType string `json:"curve_type"`
}

// CoinAction constants.
const (
	CoinCreated = "coin_created"
	CoinSpent   = "coin_spent"
)

// Signature type constants.
const (
	Ed25519 = "ed25519"
)

// Curve type constants.
const (
	Edwards25519 = "edwards25519"
)

// --- Request types ---

// MetadataRequest is used for network list.
type MetadataRequest struct {
	Metadata map[string]any `json:"metadata,omitempty"`
}

// networkIdentifierField is embedded by request types
// that carry a NetworkIdentifier, satisfying the
// networkRequest interface.
type networkIdentifierField struct {
	NetworkIdentifier *NetworkIdentifier `json:"network_identifier"`
}

func (f *networkIdentifierField) networkID() *NetworkIdentifier {
	return f.NetworkIdentifier
}

// NetworkRequest identifies the target network.
type NetworkRequest struct {
	networkIdentifierField
	Metadata map[string]any `json:"metadata,omitempty"`
}

// BlockRequest identifies a block to fetch.
type BlockRequest struct {
	networkIdentifierField
	BlockIdentifier *PartialBlockIdentifier `json:"block_identifier"`
}

// BlockTransactionRequest identifies a tx in a block.
type BlockTransactionRequest struct {
	networkIdentifierField
	BlockIdentifier       *BlockIdentifier       `json:"block_identifier"`
	TransactionIdentifier *TransactionIdentifier `json:"transaction_identifier"`
}

// AccountBalanceRequest is for balance lookups.
type AccountBalanceRequest struct {
	networkIdentifierField
	AccountIdentifier *AccountIdentifier      `json:"account_identifier"`
	BlockIdentifier   *PartialBlockIdentifier `json:"block_identifier,omitempty"`
	Currencies        []*Currency             `json:"currencies,omitempty"`
}

// AccountCoinsRequest is for UTXO lookups.
type AccountCoinsRequest struct {
	networkIdentifierField
	AccountIdentifier *AccountIdentifier `json:"account_identifier"`
	IncludeMempool    bool               `json:"include_mempool"`
	Currencies        []*Currency        `json:"currencies,omitempty"`
}

// MempoolTransactionRequest identifies a mempool tx.
type MempoolTransactionRequest struct {
	networkIdentifierField
	TransactionIdentifier *TransactionIdentifier `json:"transaction_identifier"`
}

// ConstructionDeriveRequest derives an address.
type ConstructionDeriveRequest struct {
	networkIdentifierField
	PublicKey *PublicKey     `json:"public_key"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// ConstructionPreprocessRequest preprocesses operations.
type ConstructionPreprocessRequest struct {
	networkIdentifierField
	Operations []*Operation   `json:"operations"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// ConstructionMetadataRequest fetches tx metadata.
type ConstructionMetadataRequest struct {
	networkIdentifierField
	Options    map[string]any `json:"options,omitempty"`
	PublicKeys []*PublicKey   `json:"public_keys,omitempty"`
}

// ConstructionPayloadsRequest creates unsigned tx.
type ConstructionPayloadsRequest struct {
	networkIdentifierField
	Operations []*Operation   `json:"operations"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	PublicKeys []*PublicKey   `json:"public_keys,omitempty"`
}

// ConstructionCombineRequest combines unsigned tx + sigs.
type ConstructionCombineRequest struct {
	networkIdentifierField
	UnsignedTransaction string       `json:"unsigned_transaction"`
	Signatures          []*Signature `json:"signatures"`
}

// ConstructionParseRequest parses a transaction.
type ConstructionParseRequest struct {
	networkIdentifierField
	Signed      bool   `json:"signed"`
	Transaction string `json:"transaction"`
}

// ConstructionHashRequest computes a tx hash.
type ConstructionHashRequest struct {
	networkIdentifierField
	SignedTransaction string `json:"signed_transaction"`
}

// ConstructionSubmitRequest submits a signed tx.
type ConstructionSubmitRequest struct {
	networkIdentifierField
	SignedTransaction string `json:"signed_transaction"`
}

// --- Response types ---

// NetworkListResponse lists supported networks.
type NetworkListResponse struct {
	NetworkIdentifiers []*NetworkIdentifier `json:"network_identifiers"`
}

// NetworkOptionsResponse describes network options.
type NetworkOptionsResponse struct {
	Version *Version `json:"version"`
	Allow   *Allow   `json:"allow"`
}

// NetworkStatusResponse describes network status.
type NetworkStatusResponse struct {
	CurrentBlockIdentifier *BlockIdentifier `json:"current_block_identifier"`
	CurrentBlockTimestamp  int64            `json:"current_block_timestamp"`
	GenesisBlockIdentifier *BlockIdentifier `json:"genesis_block_identifier"`
	OldestBlockIdentifier  *BlockIdentifier `json:"oldest_block_identifier,omitempty"`
	SyncStatus             *SyncStatus      `json:"sync_status,omitempty"`
	Peers                  []*Peer          `json:"peers"`
}

// BlockResponse returns a block.
type BlockResponse struct {
	Block             *Block                   `json:"block,omitempty"`
	OtherTransactions []*TransactionIdentifier `json:"other_transactions,omitempty"`
}

// BlockTransactionResponse returns a transaction.
type BlockTransactionResponse struct {
	Transaction *Transaction `json:"transaction"`
}

// AccountBalanceResponse returns balances.
type AccountBalanceResponse struct {
	BlockIdentifier *BlockIdentifier `json:"block_identifier"`
	Balances        []*Amount        `json:"balances"`
	Metadata        map[string]any   `json:"metadata,omitempty"`
}

// AccountCoinsResponse returns UTXOs.
type AccountCoinsResponse struct {
	BlockIdentifier *BlockIdentifier `json:"block_identifier"`
	Coins           []*Coin          `json:"coins"`
	Metadata        map[string]any   `json:"metadata,omitempty"`
}

// MempoolResponse lists mempool transactions.
type MempoolResponse struct {
	TransactionIdentifiers []*TransactionIdentifier `json:"transaction_identifiers"`
}

// MempoolTransactionResponse returns a mempool tx.
type MempoolTransactionResponse struct {
	Transaction *Transaction   `json:"transaction"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// ConstructionDeriveResponse returns a derived address.
type ConstructionDeriveResponse struct {
	AccountIdentifier *AccountIdentifier `json:"account_identifier,omitempty"`
	Metadata          map[string]any     `json:"metadata,omitempty"`
}

// ConstructionPreprocessResponse returns options.
type ConstructionPreprocessResponse struct {
	Options            map[string]any       `json:"options,omitempty"`
	RequiredPublicKeys []*AccountIdentifier `json:"required_public_keys,omitempty"`
}

// ConstructionMetadataResponse returns tx metadata.
type ConstructionMetadataResponse struct {
	Metadata     map[string]any `json:"metadata"`
	SuggestedFee []*Amount      `json:"suggested_fee,omitempty"`
}

// ConstructionPayloadsResponse returns unsigned tx.
type ConstructionPayloadsResponse struct {
	UnsignedTransaction string            `json:"unsigned_transaction"`
	Payloads            []*SigningPayload `json:"payloads"`
}

// ConstructionCombineResponse returns signed tx.
type ConstructionCombineResponse struct {
	SignedTransaction string `json:"signed_transaction"`
}

// ConstructionParseResponse returns parsed operations.
type ConstructionParseResponse struct {
	Operations               []*Operation         `json:"operations"`
	AccountIdentifierSigners []*AccountIdentifier `json:"account_identifier_signers,omitempty"`
	Metadata                 map[string]any       `json:"metadata,omitempty"`
}

// ConstructionHashResponse returns a tx hash.
type ConstructionHashResponse struct {
	TransactionIdentifier *TransactionIdentifier `json:"transaction_identifier"`
	Metadata              map[string]any         `json:"metadata,omitempty"`
}

// ConstructionSubmitResponse returns submit result.
type ConstructionSubmitResponse struct {
	TransactionIdentifier *TransactionIdentifier `json:"transaction_identifier"`
	Metadata              map[string]any         `json:"metadata,omitempty"`
}
