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

package blockfrost

import "encoding/json"

// BlockfrostNode is the interface that the Blockfrost API
// server uses to query the node for blockchain data. This
// decouples the HTTP server from the concrete Node struct
// and enables testing with mock implementations.
type BlockfrostNode interface {
	// ChainTip returns the current chain tip info.
	ChainTip() (ChainTipInfo, error)

	// LatestBlock returns information about the latest
	// block on the chain.
	LatestBlock() (BlockInfo, error)

	// BlockByHashOrNumber returns information about a block
	// identified by a 64-character hex hash or decimal height.
	BlockByHashOrNumber(id string) (BlockInfo, error)

	// LatestBlockTxHashes returns the transaction hashes
	// for the latest block.
	LatestBlockTxHashes() ([]string, error)

	// CurrentEpoch returns information about the current
	// epoch.
	CurrentEpoch() (EpochInfo, error)

	// CurrentProtocolParams returns the current protocol
	// parameters.
	CurrentProtocolParams() (ProtocolParamsInfo, error)

	// EpochProtocolParams returns protocol parameters for a
	// specific epoch.
	EpochProtocolParams(epoch uint64) (ProtocolParamsInfo, error)

	// Network returns current network supply and stake information.
	Network() (NetworkInfo, error)

	// NetworkEras returns hard-fork era summaries.
	NetworkEras() ([]NetworkEraInfo, error)

	// Genesis returns Shelley genesis configuration values.
	Genesis() (GenesisInfo, error)

	// PoolsExtended returns the current active pools with
	// extended details.
	PoolsExtended() ([]PoolExtendedInfo, error)

	// Address returns summary information for an address,
	// including balances aggregated across its live UTxOs.
	Address(address string) (AddressInfo, error)

	// PoolsRetiring returns the paginated list of pools with a
	// pending retirement announcement, along with the total number
	// of matching results before pagination.
	PoolsRetiring(PaginationParams) ([]PoolRetiringInfo, int, error)

	// PoolMetadata returns the registered metadata for a pool.
	PoolMetadata(poolID string) (PoolMetadataInfo, error)

	// PoolDetail returns the OpenAPI pool detail object for the requested
	// pool (bech32 or hex ID), computing epoch-sensitive aggregates
	// (blocks_epoch, live/active stake, saturation) as of the current
	// epoch.
	PoolDetail(poolID string) (PoolDetailInfo, error)

	// AddressUTXOs returns the paginated current UTxOs for
	// an address along with the total number of matching
	// results before pagination.
	AddressUTXOs(
		address string,
		params PaginationParams,
	) ([]AddressUTXOInfo, int, error)

	// AddressTransactions returns the paginated transaction
	// history for an address along with the total number of
	// matching results before pagination.
	AddressTransactions(
		address string,
		params PaginationParams,
	) ([]AddressTransactionInfo, int, error)

	// MetadataTransactions returns the paginated transactions
	// containing the requested metadata label, with JSON values.
	MetadataTransactions(
		label uint64,
		params PaginationParams,
	) ([]MetadataTransactionJSONInfo, int, error)

	// MetadataTransactionsCBOR returns the paginated transactions
	// containing the requested metadata label, with CBOR values.
	MetadataTransactionsCBOR(
		label uint64,
		params PaginationParams,
	) ([]MetadataTransactionCBORInfo, int, error)

	// Transaction returns summary details for a transaction hash.
	Transaction(hash []byte) (TransactionInfo, error)

	// TransactionSubmit submits raw signed transaction CBOR to the mempool.
	TransactionSubmit(txCbor []byte) (string, error)

	// TransactionCBOR returns raw signed transaction CBOR bytes.
	TransactionCBOR(hash []byte) ([]byte, error)

	// TransactionMetadata returns transaction metadata labels as JSON values.
	TransactionMetadata(hash []byte) ([]TransactionMetadataInfo, error)

	// TransactionMetadataCBOR returns transaction metadata labels as CBOR values.
	TransactionMetadataCBOR(hash []byte) ([]TransactionMetadataCBORInfo, error)

	// TransactionUTXOs returns transaction inputs and outputs.
	TransactionUTXOs(hash []byte) (TransactionUTXOsInfo, error)

	// TransactionDelegations returns delegation certificates in a transaction.
	TransactionDelegations(hash []byte) ([]TransactionDelegationInfo, error)

	// TransactionStakeAddresses returns stake address certificates in a transaction.
	TransactionStakeAddresses(hash []byte) ([]TransactionStakeAddressInfo, error)

	// TransactionWithdrawals returns reward withdrawals in a transaction.
	TransactionWithdrawals(hash []byte) ([]TransactionWithdrawalInfo, error)

	// TransactionMIRs returns MIR certificates in a transaction.
	TransactionMIRs(hash []byte) ([]TransactionMIRInfo, error)

	// TransactionPoolUpdates returns pool registration certificates in a transaction.
	TransactionPoolUpdates(hash []byte) ([]TransactionPoolUpdateInfo, error)

	// TransactionPoolRetires returns pool retirement certificates in a transaction.
	TransactionPoolRetires(hash []byte) ([]TransactionPoolRetireInfo, error)

	// TransactionRedeemers returns Plutus redeemers in a transaction.
	TransactionRedeemers(hash []byte) ([]TransactionRedeemerInfo, error)

	// TransactionRequiredSigners returns required signing key hashes in a transaction.
	TransactionRequiredSigners(hash []byte) ([]TransactionRequiredSignerInfo, error)

	// Asset returns native asset information for a
	// concatenated hex asset ID ({policy_id}{asset_name}).
	Asset(
		policyID string,
		assetName []byte,
	) (AssetInfo, error)

	// AssetAddresses returns paginated addresses currently holding the given
	// asset, along with the total holder count before pagination.
	AssetAddresses(
		policyID string,
		assetName []byte,
		params PaginationParams,
	) ([]AssetHolderInfo, int, error)

	// DRep returns governance DRep information for a parsed
	// DRep credential.
	DRep(DRepCredential) (DRepInfo, error)

	// DReps returns the paginated DRep list along with the total
	// number of matching results before pagination.
	DReps(DRepListParams) ([]DRepListItemInfo, int, error)

	// Account returns stake-account information for the
	// requested stake address.
	Account(string) (AccountInfo, error)

	// AccountAssociatedAddresses returns payment addresses
	// associated with the requested stake address.
	AccountAssociatedAddresses(
		string,
		PaginationParams,
	) ([]AccountAssociatedAddressInfo, int, error)

	// AccountDelegationHistory returns delegation history
	// rows for the requested stake address.
	AccountDelegationHistory(
		string,
		PaginationParams,
	) ([]AccountDelegationHistoryInfo, int, error)

	// AccountRegistrationHistory returns registration
	// history rows for the requested stake address.
	AccountRegistrationHistory(
		string,
		PaginationParams,
	) ([]AccountRegistrationHistoryInfo, int, error)

	// AccountRewardHistory returns reward history rows for
	// the requested stake address.
	AccountRewardHistory(
		string,
		PaginationParams,
	) ([]AccountRewardHistoryInfo, int, error)

	// AccountUTXOs returns the current UTxOs controlled by the
	// stake credential behind the requested stake address.
	AccountUTXOs(
		string,
		PaginationParams,
	) ([]AccountUTXOInfo, int, error)

	// AccountWithdrawals returns withdrawal history rows for
	// the requested stake address.
	AccountWithdrawals(
		string,
		PaginationParams,
	) ([]AccountWithdrawalInfo, int, error)

	// AccountTransactions returns transactions associated with
	// addresses controlled by the stake credential behind the
	// requested stake address, optionally filtered by an
	// inclusive from/to block-range position.
	AccountTransactions(
		string,
		AccountTransactionsParams,
	) ([]AccountTransactionInfo, int, error)
}

// ChainTipInfo holds chain tip data needed by the API.
type ChainTipInfo struct {
	BlockHash   string
	Slot        uint64
	BlockNumber uint64
}

// BlockInfo holds block data needed by the API.
type BlockInfo struct {
	Hash          string
	Slot          uint64
	Epoch         uint64
	EpochSlot     uint64
	Height        uint64
	Time          int64
	Size          uint64
	TxCount       int
	SlotLeader    string
	PreviousBlock string
	Confirmations uint64
	// Output is the total lovelace output of the block's transactions.
	Output string
	// Fees is the total lovelace fees of the block's transactions.
	Fees string
	// BlockVRF is the bech32-encoded (vrf_vk) VRF verification key from the
	// Praos/TPraos header, or nil for Byron/unknown headers.
	BlockVRF *string
	// OPCert is the hex-encoded operational certificate hot vkey, or nil for
	// Byron/unknown headers.
	OPCert *string
	// OPCertCounter is the operational certificate issue counter as a decimal
	// string, or nil for Byron/unknown headers.
	OPCertCounter *string
	// NextBlock is the hash of the block that builds on this one, or nil when
	// this block is the chain tip.
	NextBlock *string
}

// EpochInfo holds epoch data needed by the API.
type EpochInfo struct {
	Epoch          uint64
	StartTime      int64
	EndTime        int64
	FirstBlockTime int64
	LastBlockTime  int64
	BlockCount     int
	TxCount        int
}

// ProtocolParamsInfo holds protocol parameter data needed
// by the API.
type ProtocolParamsInfo struct {
	Epoch               uint64
	MinFeeA             int
	MinFeeB             int
	MaxBlockSize        int
	MaxTxSize           int
	MaxBlockHeaderSize  int
	KeyDeposit          string
	PoolDeposit         string
	EMax                int
	NOpt                int
	A0                  float64
	Rho                 float64
	Tau                 float64
	ProtocolMajorVer    int
	ProtocolMinorVer    int
	MinPoolCost         string
	CoinsPerUtxoSize    string
	PriceMem            float64
	PriceStep           float64
	MaxTxExMem          string
	MaxTxExSteps        string
	MaxBlockExMem       string
	MaxBlockExSteps     string
	MaxValSize          string
	CollateralPercent   int
	MaxCollateralInputs int
	// Conway-era governance and reference-script parameters. These are nil
	// for pre-Conway eras where they do not apply.
	PvtMotionNoConfidence      *float64
	PvtCommitteeNormal         *float64
	PvtCommitteeNoConfidence   *float64
	PvtHardForkInitiation      *float64
	PvtPPSecurityGroup         *float64
	DvtMotionNoConfidence      *float64
	DvtCommitteeNormal         *float64
	DvtCommitteeNoConfidence   *float64
	DvtUpdateToConstitution    *float64
	DvtHardForkInitiation      *float64
	DvtPPNetworkGroup          *float64
	DvtPPEconomicGroup         *float64
	DvtPPTechnicalGroup        *float64
	DvtPPGovGroup              *float64
	DvtTreasuryWithdrawal      *float64
	CommitteeMinSize           *string
	CommitteeMaxTermLength     *string
	GovActionLifetime          *string
	GovActionDeposit           *string
	DRepDeposit                *string
	DRepActivity               *string
	MinFeeRefScriptCostPerByte *float64
	// CostModelsRaw holds cost models keyed by Plutus version name with raw
	// integer arrays, matching the Blockfrost cost_models_raw field. nil
	// when the era carries no cost models.
	CostModelsRaw *any
}

// NetworkInfo holds network supply and stake data needed by the API.
type NetworkInfo struct {
	Supply NetworkSupplyInfo
	Stake  NetworkStakeInfo
}

// NetworkSupplyInfo holds network supply data needed by the API.
type NetworkSupplyInfo struct {
	Max         string
	Total       string
	Circulating string
	Locked      string
	Treasury    string
	Reserves    string
}

// NetworkStakeInfo holds network stake data needed by the API.
type NetworkStakeInfo struct {
	Live   string
	Active string
}

// NetworkEraInfo holds a Blockfrost hard-fork era summary.
type NetworkEraInfo struct {
	Era    string
	Start  NetworkEraBoundInfo
	End    *NetworkEraBoundInfo
	Params NetworkEraParamsInfo
}

// NetworkEraBoundInfo holds an era boundary.
type NetworkEraBoundInfo struct {
	Time  int64
	Slot  uint64
	Epoch uint64
}

// NetworkEraParamsInfo holds era timing parameters.
type NetworkEraParamsInfo struct {
	EpochLength uint64
	SlotLength  uint64
	SafeZone    uint64
}

// GenesisInfo holds Shelley genesis configuration values.
type GenesisInfo struct {
	ActiveSlotsCoefficient float32
	UpdateQuorum           int
	MaxLovelaceSupply      string
	NetworkMagic           int
	EpochLength            int
	SystemStart            int
	SlotsPerKESPeriod      int
	SlotLength             int
	MaxKESEvolutions       int
	SecurityParam          int
}

// PoolRetiringInfo is one entry of the retiring pools list.
type PoolRetiringInfo struct {
	PoolID string
	Epoch  uint64
}

// PoolMetadataInfo holds a pool's registered metadata: the on-chain
// anchor (URL and hash) plus the off-chain document fields when the
// document has been fetched and validated. Error carries the fetch
// failure when the document could not be retrieved or validated.
type PoolMetadataInfo struct {
	PoolID      string
	Hex         string
	URL         *string
	Hash        *string
	Ticker      *string
	Name        *string
	Description *string
	Homepage    *string
	Error       *OffchainFetchErrorInfo
}

// OffchainFetchErrorInfo describes a failed off-chain metadata fetch in
// Blockfrost's error-object form.
type OffchainFetchErrorInfo struct {
	Code    string
	Message string
}

// PoolExtendedInfo holds pool data needed by the /pools/extended endpoint,
// following the Blockfrost OpenAPI 0.1.90 pool_list_extended schema.
// Neither vrf_key nor relays is part of that schema (vrf_key belongs to the
// pool-detail schema instead; see PoolDetailInfo), so this type omits both.
type PoolExtendedInfo struct {
	PoolID         string
	Hex            string
	ActiveStake    string
	LiveStake      string
	BlocksMinted   uint64
	LiveSaturation float64
	DeclaredPledge string
	MarginCost     float64
	FixedCost      string
	Metadata       *PoolExtendedMetadataInfo
}

// PoolExtendedMetadataInfo holds the nullable metadata object nested in
// pool_list_extended: the on-chain anchor (URL/hash) plus the off-chain
// document fields when fetched and validated, or Error when the fetch or
// validation failed. A nil *PoolExtendedMetadataInfo means the pool has no
// registered metadata anchor at all (metadata: null); this mirrors
// PoolMetadataInfo's fields but omits PoolID/Hex, which pool_list_extended
// carries at the parent object level instead of inside metadata.
type PoolExtendedMetadataInfo struct {
	URL         *string
	Hash        *string
	Ticker      *string
	Name        *string
	Description *string
	Homepage    *string
	Error       *OffchainFetchErrorInfo
}

// PoolDetailInfo holds pool data needed by the /pools/{pool_id} endpoint,
// following the Blockfrost OpenAPI 0.1.90 pool schema.
type PoolDetailInfo struct {
	PoolID         string
	Hex            string
	VrfKey         string
	LiveStake      string
	ActiveStake    string
	DeclaredPledge string
	LivePledge     string
	FixedCost      string
	RewardAccount  string
	Owners         []string
	// Registration and Retirement are the transaction hashes of the pool's
	// registration and retirement certificates, oldest first. Certificates
	// with no linked transaction (rows synthesized by a Mithril
	// ledger-state import) are excluded since they have no originating
	// transaction to report.
	Registration   []string
	Retirement     []string
	CalidusKey     *PoolCalidusKeyInfo
	BlocksMinted   uint64
	BlocksEpoch    uint64
	LiveDelegators uint64
	LiveSize       float64
	LiveSaturation float64
	ActiveSize     float64
	MarginCost     float64
}

// PoolCalidusKeyInfo holds a pool's latest valid CIP-0088 Calidus key
// registration (Blockfrost's calidus_key object). dingo does not currently
// ingest Calidus key registrations from any source, so every
// BlockfrostNode implementation returns a nil *PoolCalidusKeyInfo here; the
// type exists so PoolDetailInfo mirrors the OpenAPI schema exactly.
type PoolCalidusKeyInfo struct {
	ID          string
	PubKey      string
	Nonce       uint64
	TxHash      string
	BlockHeight uint64
	BlockTime   int64
	Epoch       uint64
}

// AddressAmountInfo holds amount data needed by address
// UTxO responses.
type AddressAmountInfo struct {
	Unit     string
	Quantity string
}

// AddressInfo holds address summary data needed by the API.
type AddressInfo struct {
	Address      string
	Amount       []AddressAmountInfo
	StakeAddress *string
	Type         string
	Script       bool
}

// AddressUTXOInfo holds address UTxO data needed by the
// API.
type AddressUTXOInfo struct {
	Address string
	TxHash  string
	// TxIndex is the deprecated Blockfrost alias for OutputIndex (the UTxO's
	// index within its producing transaction); it carries the same value.
	TxIndex             uint32
	OutputIndex         uint32
	Amount              []AddressAmountInfo
	Block               string
	DataHash            *string
	InlineDatum         *string
	ReferenceScriptHash *string
}

// AddressTransactionInfo holds address transaction data
// needed by the API.
type AddressTransactionInfo struct {
	TxHash      string
	TxIndex     uint32
	BlockHeight uint64
	BlockTime   int64
}

// MetadataTransactionJSONInfo holds metadata label query
// data for the JSON endpoint.
type MetadataTransactionJSONInfo struct {
	TxHash       string
	JSONMetadata json.RawMessage
}

// MetadataTransactionCBORInfo holds metadata label query
// data for the CBOR endpoint.
type MetadataTransactionCBORInfo struct {
	TxHash   string
	Metadata string
}

// TransactionInfo holds transaction summary data needed by the API.
type TransactionInfo struct {
	InvalidBefore      *string
	InvalidHereafter   *string
	OutputAmount       []AddressAmountInfo
	Hash               string
	Block              string
	Deposit            string
	Fees               string
	TreasuryDonation   string
	Slot               uint64
	BlockHeight        uint64
	BlockTime          int64
	Size               int
	Index              uint32
	UtxoCount          int
	WithdrawalCount    int
	MirCertCount       int
	DelegationCount    int
	StakeCertCount     int
	PoolUpdateCount    int
	PoolRetireCount    int
	AssetMintBurnCount int
	RedeemerCount      int
	ValidContract      bool
}

// TransactionMetadataInfo holds a transaction metadata label and JSON value.
type TransactionMetadataInfo struct {
	JSONMetadata json.RawMessage
	Label        string
}

// TransactionMetadataCBORInfo holds a transaction metadata label and CBOR value.
type TransactionMetadataCBORInfo struct {
	CBORMetadata string
	Label        string
}

// TransactionUTXOsInfo holds transaction inputs and outputs.
type TransactionUTXOsInfo struct {
	Hash    string
	Inputs  []TransactionInputInfo
	Outputs []TransactionOutputInfo
}

// TransactionInputInfo holds one transaction input.
type TransactionInputInfo struct {
	ReferenceScriptHash *string
	InlineDatum         *string
	DataHash            *string
	Address             string
	TxHash              string
	Amount              []AddressAmountInfo
	OutputIndex         uint32
	Collateral          bool
	Reference           *bool
}

// TransactionOutputInfo holds one transaction output.
type TransactionOutputInfo struct {
	ReferenceScriptHash *string
	InlineDatum         *string
	DataHash            *string
	ConsumedByTx        *string
	Address             string
	Amount              []AddressAmountInfo
	OutputIndex         uint32
	Collateral          bool
}

// TransactionDelegationInfo holds one delegation certificate.
type TransactionDelegationInfo struct {
	Address     string
	PoolID      string
	Index       int
	CertIndex   int
	ActiveEpoch uint64
}

// TransactionStakeAddressInfo holds one stake registration/deregistration certificate.
type TransactionStakeAddressInfo struct {
	Address      string
	CertIndex    int
	Registration bool
}

// TransactionWithdrawalInfo holds one reward withdrawal.
type TransactionWithdrawalInfo struct {
	Address string
	Amount  string
}

// TransactionMIRInfo holds one MIR target.
type TransactionMIRInfo struct {
	Address   string
	Amount    string
	CertIndex int
	Pot       string
}

// TransactionPoolUpdateInfo holds one pool registration certificate.
type TransactionPoolUpdateInfo struct {
	Metadata      *TransactionPoolMetadataInfo
	Owners        []string
	Relays        []TransactionPoolRelayInfo
	ActiveEpoch   uint64
	CertIndex     int
	FixedCost     string
	MarginCost    float64
	Pledge        string
	PoolID        string
	RewardAccount string
	VrfKey        string
}

// TransactionPoolMetadataInfo holds on-chain pool metadata pointer data.
type TransactionPoolMetadataInfo struct {
	URL  string
	Hash string
}

// TransactionPoolRelayInfo holds one pool relay from a pool registration cert.
type TransactionPoolRelayInfo struct {
	DNS    string
	DNSSrv string
	IPv4   string
	IPv6   string
	Port   *int
}

// TransactionPoolRetireInfo holds one pool retirement certificate.
type TransactionPoolRetireInfo struct {
	PoolID        string
	CertIndex     int
	RetiringEpoch uint64
}

// TransactionRedeemerInfo holds one Plutus redeemer.
type TransactionRedeemerInfo struct {
	DatumHash        *string
	TxIndex          int
	Purpose          string
	ScriptHash       string
	RedeemerDataHash string
	UnitMem          string
	UnitSteps        string
	Fee              string
}

// TransactionRequiredSignerInfo holds one required signing key hash.
type TransactionRequiredSignerInfo struct {
	WitnessHash string
}

// AssetInfo holds native asset data needed by the API.
type AssetInfo struct {
	OnchainMetadata         *any
	OnchainMetadataStandard *string
	OnchainMetadataExtra    *string
	Metadata                *any
	Asset                   string
	PolicyID                string
	AssetName               string
	AssetNameASCII          string
	Fingerprint             string
	Quantity                string
	InitialMintTxHash       string
	MintOrBurnCount         int
}

// AssetHolderInfo holds address and quantity data for a single asset holder.
type AssetHolderInfo struct {
	Address  string
	Quantity string
}

// DRepCredential holds a parsed governance DRep identifier.
type DRepCredential struct {
	ID                 string
	Hash               []byte
	HasScript          bool
	CredentialTagKnown bool
	// Predefined is set for the special DReps
	// (drep_always_abstain, drep_always_no_confidence), which carry
	// no credential hash. The value is a models.DrepType* constant.
	Predefined *uint64
}

// DRepInfo holds DRep data needed by the governance API,
// following the Blockfrost OpenAPI 0.1.90 drep schema.
type DRepInfo struct {
	DRepID          string
	Hex             string
	Amount          string
	Active          bool
	ActiveEpoch     *uint64
	HasScript       bool
	Retired         bool
	Expired         bool
	LastActiveEpoch *uint64
}

// DRepListParams holds query parameters for the DRep list endpoint.
type DRepListParams struct {
	Pagination    PaginationParams
	OrderByAmount bool
	Retired       *bool
	Expired       *bool
}

// DRepListItemInfo is one entry of the DRep list endpoint. Unlike
// DRepInfo it always uses CIP-129 identifiers and carries the
// resolved anchor metadata.
type DRepListItemInfo struct {
	DRepID          string
	Hex             string
	Amount          string
	HasScript       bool
	Retired         bool
	Expired         bool
	LastActiveEpoch *uint64
	Metadata        *DRepMetadataInfo
}

// DRepMetadataInfo is the resolved CIP-119 anchor document for a DRep.
type DRepMetadataInfo struct {
	URL          string
	Hash         string
	JSONMetadata json.RawMessage
	Bytes        string
}

// AccountInfo holds stake-account data needed by the API.
type AccountInfo struct {
	StakeAddress       string
	Active             bool
	ActiveEpoch        *int64
	ControlledAmount   string
	RewardsSum         string
	WithdrawalsSum     string
	ReservesSum        string
	TreasurySum        string
	WithdrawableAmount string
	PoolID             *string
	DrepID             *string
	Registered         bool
}

// AccountAssociatedAddressInfo holds a payment address
// associated with a stake key.
type AccountAssociatedAddressInfo struct {
	Address string
}

// AccountDelegationHistoryInfo holds a stake-account
// delegation history row.
type AccountDelegationHistoryInfo struct {
	ActiveEpoch int32
	TxHash      string
	Amount      string
	PoolID      string
	TxSlot      int64
	BlockTime   int64
	BlockHeight int64
}

// AccountRegistrationHistoryInfo holds a stake-account
// registration history row.
type AccountRegistrationHistoryInfo struct {
	TxHash      string
	Action      string
	Deposit     string
	TxSlot      int64
	BlockTime   int64
	BlockHeight int64
}

// AccountRewardHistoryInfo holds a stake-account reward
// history row.
type AccountRewardHistoryInfo struct {
	Epoch  int32
	Amount string
	PoolID string
	// Type is the Blockfrost reward-type enum spelling (leader, member,
	// pool_deposit_refund) mapped from models.RewardAccountOutput.RewardType.
	Type string
}

// AccountUTXOInfo holds a stake-account UTxO row.
type AccountUTXOInfo struct {
	Address string
	TxHash  string
	// TxIndex is the deprecated Blockfrost alias for OutputIndex (the UTxO's
	// index within its producing transaction); it carries the same value.
	TxIndex             uint32
	OutputIndex         uint32
	Amount              []AddressAmountInfo
	Block               string
	DataHash            *string
	InlineDatum         *string
	ReferenceScriptHash *string
}

// AccountWithdrawalInfo holds a stake-account withdrawal
// history row.
type AccountWithdrawalInfo struct {
	TxHash      string
	Amount      string
	TxSlot      int64
	BlockTime   int64
	BlockHeight int64
}

// BlockRangePosition holds a parsed Blockfrost account-transactions
// from/to query value: a block number and an optional transaction
// index within that block (the "block:index" form).
type BlockRangePosition struct {
	Block uint64
	Index *uint32
}

// AccountTransactionsParams holds query parameters for the account
// transactions endpoint: standard pagination plus the optional
// inclusive from/to block-range filter.
type AccountTransactionsParams struct {
	Pagination PaginationParams
	From       *BlockRangePosition
	To         *BlockRangePosition
}

// AccountTransactionInfo holds a stake-account transaction row: one
// entry per distinct address (sharing the stake credential) involved in
// the transaction.
type AccountTransactionInfo struct {
	Address     string
	TxHash      string
	TxIndex     uint32
	BlockHeight uint64
	BlockTime   int64
}
