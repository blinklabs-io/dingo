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

// RootResponse is returned by GET /.
type RootResponse struct {
	URL     string `json:"url"`
	Version string `json:"version"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	IsHealthy bool `json:"is_healthy"`
}

// BlockResponse represents a Blockfrost block object.
type BlockResponse struct {
	Time          int64   `json:"time"`
	Height        uint64  `json:"height"`
	Hash          string  `json:"hash"`
	Slot          uint64  `json:"slot"`
	Epoch         uint64  `json:"epoch"`
	EpochSlot     uint64  `json:"epoch_slot"`
	SlotLeader    string  `json:"slot_leader"`
	Size          uint64  `json:"size"`
	TxCount       int     `json:"tx_count"`
	Output        *string `json:"output"`
	Fees          *string `json:"fees"`
	BlockVRF      *string `json:"block_vrf"`
	OPCert        *string `json:"op_cert"`
	OPCertCounter *string `json:"op_cert_counter"`
	PreviousBlock string  `json:"previous_block"`
	NextBlock     *string `json:"next_block"`
	Confirmations uint64  `json:"confirmations"`
}

// EpochResponse represents a Blockfrost epoch object.
type EpochResponse struct {
	Epoch          uint64  `json:"epoch"`
	StartTime      int64   `json:"start_time"`
	EndTime        int64   `json:"end_time"`
	FirstBlockTime int64   `json:"first_block_time"`
	LastBlockTime  int64   `json:"last_block_time"`
	BlockCount     int     `json:"block_count"`
	TxCount        int     `json:"tx_count"`
	Output         string  `json:"output"`
	Fees           string  `json:"fees"`
	ActiveStake    *string `json:"active_stake"`
}

// ProtocolParamsResponse represents Blockfrost protocol
// parameters.
type ProtocolParamsResponse struct {
	Epoch              uint64  `json:"epoch"`
	MinFeeA            int     `json:"min_fee_a"`
	MinFeeB            int     `json:"min_fee_b"`
	MaxBlockSize       int     `json:"max_block_size"`
	MaxTxSize          int     `json:"max_tx_size"`
	MaxBlockHeaderSize int     `json:"max_block_header_size"`
	KeyDeposit         string  `json:"key_deposit"`
	PoolDeposit        string  `json:"pool_deposit"`
	EMax               int     `json:"e_max"`
	NOpt               int     `json:"n_opt"`
	A0                 float64 `json:"a0"`
	Rho                float64 `json:"rho"`
	Tau                float64 `json:"tau"`
	//nolint:tagliatelle
	DecentralisationParam float64         `json:"decentralisation_param"`
	ExtraEntropy          *map[string]any `json:"extra_entropy"`
	ProtocolMajorVer      int             `json:"protocol_major_ver"`
	ProtocolMinorVer      int             `json:"protocol_minor_ver"`
	MinUtxo               string          `json:"min_utxo"`
	MinPoolCost           string          `json:"min_pool_cost"`
	Nonce                 string          `json:"nonce"`
	CoinsPerUtxoSize      *string         `json:"coins_per_utxo_size"`
	CoinsPerUtxoWord      string          `json:"coins_per_utxo_word"`
	CostModels            *any            `json:"cost_models"`
	PriceMem              *float64        `json:"price_mem"`
	PriceStep             *float64        `json:"price_step"`
	MaxTxExMem            *string         `json:"max_tx_ex_mem"`
	MaxTxExSteps          *string         `json:"max_tx_ex_steps"`
	MaxBlockExMem         *string         `json:"max_block_ex_mem"`
	MaxBlockExSteps       *string         `json:"max_block_ex_steps"`
	MaxValSize            *string         `json:"max_val_size"`
	CollateralPercent     *int            `json:"collateral_percent"`
	MaxCollateralInputs   *int            `json:"max_collateral_inputs"`
	// Conway-era governance and reference-script parameters. These are
	// null for pre-Conway eras where they do not apply.
	PvtMotionNoConfidence    *float64 `json:"pvt_motion_no_confidence"`
	PvtCommitteeNormal       *float64 `json:"pvt_committee_normal"`
	PvtCommitteeNoConfidence *float64 `json:"pvt_committee_no_confidence"`
	PvtHardForkInitiation    *float64 `json:"pvt_hard_fork_initiation"`
	DvtMotionNoConfidence    *float64 `json:"dvt_motion_no_confidence"`
	DvtCommitteeNormal       *float64 `json:"dvt_committee_normal"`
	DvtCommitteeNoConfidence *float64 `json:"dvt_committee_no_confidence"`
	DvtUpdateToConstitution  *float64 `json:"dvt_update_to_constitution"`
	DvtHardForkInitiation    *float64 `json:"dvt_hard_fork_initiation"`
	//nolint:tagliatelle
	DvtPPNetworkGroup *float64 `json:"dvt_p_p_network_group"`
	//nolint:tagliatelle
	DvtPPEconomicGroup *float64 `json:"dvt_p_p_economic_group"`
	//nolint:tagliatelle
	DvtPPTechnicalGroup *float64 `json:"dvt_p_p_technical_group"`
	//nolint:tagliatelle
	DvtPPGovGroup          *float64 `json:"dvt_p_p_gov_group"`
	DvtTreasuryWithdrawal  *float64 `json:"dvt_treasury_withdrawal"`
	CommitteeMinSize       *string  `json:"committee_min_size"`
	CommitteeMaxTermLength *string  `json:"committee_max_term_length"`
	GovActionLifetime      *string  `json:"gov_action_lifetime"`
	GovActionDeposit       *string  `json:"gov_action_deposit"`
	DrepDeposit            *string  `json:"drep_deposit"`
	DrepActivity           *string  `json:"drep_activity"`
	PvtppSecurityGroup     *float64 `json:"pvtpp_security_group"`
	//nolint:tagliatelle
	PvtPPSecurityGroup         *float64 `json:"pvt_p_p_security_group"`
	MinFeeRefScriptCostPerByte *float64 `json:"min_fee_ref_script_cost_per_byte"`
	CostModelsRaw              *any     `json:"cost_models_raw"`
}

// NetworkResponse represents Blockfrost network info.
type NetworkResponse struct {
	Supply NetworkSupply `json:"supply"`
	Stake  NetworkStake  `json:"stake"`
}

// NetworkSupply holds supply information.
type NetworkSupply struct {
	Max         string `json:"max"`
	Total       string `json:"total"`
	Circulating string `json:"circulating"`
	Locked      string `json:"locked"`
	Treasury    string `json:"treasury"`
	Reserves    string `json:"reserves"`
}

// NetworkStake holds stake information.
type NetworkStake struct {
	Live   string `json:"live"`
	Active string `json:"active"`
}

// NetworkEraResponse represents a Blockfrost era summary. Per Blockfrost
// OpenAPI 0.1.88 (schema network-eras) each item carries only start, end,
// and parameters; the human-readable era name is intentionally omitted to
// keep the response shape compatible with the published schema.
type NetworkEraResponse struct {
	Start      NetworkEraBound      `json:"start"`
	End        *NetworkEraBound     `json:"end"`
	Parameters NetworkEraParameters `json:"parameters"`
}

// NetworkEraBound represents an era boundary.
type NetworkEraBound struct {
	Time  int64  `json:"time"`
	Slot  uint64 `json:"slot"`
	Epoch uint64 `json:"epoch"`
}

// NetworkEraParameters represents era timing parameters.
type NetworkEraParameters struct {
	EpochLength uint64 `json:"epoch_length"`
	SlotLength  uint64 `json:"slot_length"`
	SafeZone    uint64 `json:"safe_zone"`
}

// GenesisResponse represents Blockfrost genesis info.
type GenesisResponse struct {
	ActiveSlotsCoefficient float32 `json:"active_slots_coefficient"`
	UpdateQuorum           int     `json:"update_quorum"`
	MaxLovelaceSupply      string  `json:"max_lovelace_supply"`
	NetworkMagic           int     `json:"network_magic"`
	EpochLength            int     `json:"epoch_length"`
	SystemStart            int     `json:"system_start"`
	SlotsPerKESPeriod      int     `json:"slots_per_kes_period"`
	SlotLength             int     `json:"slot_length"`
	MaxKESEvolutions       int     `json:"max_kes_evolutions"`
	SecurityParam          int     `json:"security_param"`
}

// AddressAmountResponse represents a Blockfrost address
// amount object.
type AddressAmountResponse struct {
	Unit     string `json:"unit"`
	Quantity string `json:"quantity"`
}

// AddressUTXOResponse represents a Blockfrost address
// UTxO object.
type AddressUTXOResponse struct {
	Address string `json:"address"`
	TxHash  string `json:"tx_hash"`
	// TxIndex is a deprecated Blockfrost alias for OutputIndex ("UTXO index
	// in the transaction"), kept for schema compatibility.
	TxIndex             int                     `json:"tx_index"`
	OutputIndex         int                     `json:"output_index"`
	Amount              []AddressAmountResponse `json:"amount"`
	Block               string                  `json:"block"`
	DataHash            *string                 `json:"data_hash"`
	InlineDatum         *string                 `json:"inline_datum"`
	ReferenceScriptHash *string                 `json:"reference_script_hash"`
}

// AddressTransactionResponse represents a Blockfrost
// address transaction object.
type AddressTransactionResponse struct {
	TxHash      string `json:"tx_hash"`
	TxIndex     int    `json:"tx_index"`
	BlockHeight uint64 `json:"block_height"`
	BlockTime   int    `json:"block_time"`
}

// AssetAddressResponse represents one entry in the
// GET /assets/{asset}/addresses response.
type AssetAddressResponse struct {
	Address  string `json:"address"`
	Quantity string `json:"quantity"`
}

// AssetResponse represents a Blockfrost native asset
// object.
type AssetResponse struct {
	Asset                   string  `json:"asset"`
	PolicyID                string  `json:"policy_id"`
	AssetName               string  `json:"asset_name"`
	AssetNameASCII          string  `json:"asset_name_ascii"`
	Fingerprint             string  `json:"fingerprint"`
	Quantity                string  `json:"quantity"`
	InitialMintTxHash       string  `json:"initial_mint_tx_hash"`
	MintOrBurnCount         int     `json:"mint_or_burn_count"`
	OnchainMetadata         *any    `json:"onchain_metadata"`
	OnchainMetadataStandard *string `json:"onchain_metadata_standard"`
	OnchainMetadataExtra    *string `json:"onchain_metadata_extra"`
	Metadata                *any    `json:"metadata"`
}

// DRepResponse represents a Blockfrost governance DRep object.
type DRepResponse struct {
	DRepID      string `json:"drep_id"`
	Hex         string `json:"hex"`
	HasScript   bool   `json:"has_script"`
	Registered  bool   `json:"registered"`
	Epoch       uint64 `json:"epoch"`
	Amount      string `json:"amount"`
	Active      bool   `json:"active"`
	ActiveEpoch uint64 `json:"active_epoch"`
	LiveStake   string `json:"live_stake"`
}

// ErrorResponse represents a Blockfrost error response.
type ErrorResponse struct {
	StatusCode int    `json:"status_code"`
	Error      string `json:"error"`
	Message    string `json:"message"`
}

// PoolRelayResponse represents a stake pool relay.
type PoolRelayResponse struct {
	IPv4 *string `json:"ipv4"`
	IPv6 *string `json:"ipv6"`
	DNS  *string `json:"dns"`
	Port *int    `json:"port"`
}

// PoolExtendedResponse represents an extended stake pool
// list item.
type PoolExtendedResponse struct {
	PoolID         string              `json:"pool_id"`
	Hex            string              `json:"hex"`
	VrfKey         string              `json:"vrf_key"`
	ActiveStake    string              `json:"active_stake"`
	LiveStake      string              `json:"live_stake"`
	DeclaredPledge string              `json:"declared_pledge"`
	FixedCost      string              `json:"fixed_cost"`
	MarginCost     float64             `json:"margin_cost"`
	Relays         []PoolRelayResponse `json:"relays"`
}

// MetadataTransactionJSONResponse represents a Blockfrost
// metadata label transaction item with JSON content.
type MetadataTransactionJSONResponse struct {
	TxHash       string          `json:"tx_hash"`
	JSONMetadata json.RawMessage `json:"json_metadata"`
}

// MetadataTransactionCBORResponse represents a Blockfrost
// metadata label transaction item with CBOR content.
type MetadataTransactionCBORResponse struct {
	TxHash       string  `json:"tx_hash"`
	CborMetadata *string `json:"cbor_metadata"`
	Metadata     string  `json:"metadata"`
}

// TransactionResponse represents a Blockfrost transaction
// content object.
type TransactionResponse struct {
	InvalidBefore      *string                 `json:"invalid_before"`
	InvalidHereafter   *string                 `json:"invalid_hereafter"`
	OutputAmount       []AddressAmountResponse `json:"output_amount"`
	Hash               string                  `json:"hash"`
	Block              string                  `json:"block"`
	Deposit            string                  `json:"deposit"`
	Fees               string                  `json:"fees"`
	TreasuryDonation   string                  `json:"treasury_donation"`
	Slot               uint64                  `json:"slot"`
	BlockHeight        uint64                  `json:"block_height"`
	BlockTime          int64                   `json:"block_time"`
	Size               int                     `json:"size"`
	Index              int                     `json:"index"`
	UtxoCount          int                     `json:"utxo_count"`
	WithdrawalCount    int                     `json:"withdrawal_count"`
	MirCertCount       int                     `json:"mir_cert_count"`
	DelegationCount    int                     `json:"delegation_count"`
	StakeCertCount     int                     `json:"stake_cert_count"`
	PoolUpdateCount    int                     `json:"pool_update_count"`
	PoolRetireCount    int                     `json:"pool_retire_count"`
	AssetMintBurnCount int                     `json:"asset_mint_or_burn_count"`
	RedeemerCount      int                     `json:"redeemer_count"`
	ValidContract      bool                    `json:"valid_contract"`
}

// TransactionCBORResponse represents transaction CBOR in
// hex form.
type TransactionCBORResponse struct {
	CBOR string `json:"cbor"`
}

// TransactionMetadataResponse represents one transaction metadata label in
// JSON form.
type TransactionMetadataResponse struct {
	JSONMetadata json.RawMessage `json:"json_metadata"`
	Label        string          `json:"label"`
}

// TransactionMetadataCBORResponse represents one transaction metadata label in
// CBOR form.
type TransactionMetadataCBORResponse struct {
	CborMetadata *string `json:"cbor_metadata"`
	Metadata     string  `json:"metadata"`
	Label        string  `json:"label"`
}

// TransactionUTXOsResponse represents transaction inputs and outputs.
type TransactionUTXOsResponse struct {
	Hash    string                      `json:"hash"`
	Inputs  []TransactionInputResponse  `json:"inputs"`
	Outputs []TransactionOutputResponse `json:"outputs"`
}

// TransactionInputResponse represents one transaction input.
type TransactionInputResponse struct {
	ReferenceScriptHash *string                 `json:"reference_script_hash"`
	InlineDatum         *string                 `json:"inline_datum"`
	DataHash            *string                 `json:"data_hash"`
	Reference           *bool                   `json:"reference"`
	Address             string                  `json:"address"`
	TxHash              string                  `json:"tx_hash"`
	Amount              []AddressAmountResponse `json:"amount"`
	OutputIndex         int                     `json:"output_index"`
	Collateral          bool                    `json:"collateral"`
}

// TransactionOutputResponse represents one transaction output.
type TransactionOutputResponse struct {
	ReferenceScriptHash *string                 `json:"reference_script_hash"`
	InlineDatum         *string                 `json:"inline_datum"`
	DataHash            *string                 `json:"data_hash"`
	ConsumedByTx        *string                 `json:"consumed_by_tx"`
	Address             string                  `json:"address"`
	Amount              []AddressAmountResponse `json:"amount"`
	OutputIndex         int                     `json:"output_index"`
	Collateral          bool                    `json:"collateral"`
}

// TransactionDelegationResponse represents one delegation certificate.
type TransactionDelegationResponse struct {
	Address     string `json:"address"`
	PoolID      string `json:"pool_id"`
	Index       int    `json:"index"`
	CertIndex   int    `json:"cert_index"`
	ActiveEpoch uint64 `json:"active_epoch"`
}

// TransactionStakeAddressResponse represents one stake address certificate.
type TransactionStakeAddressResponse struct {
	Address      string `json:"address"`
	CertIndex    int    `json:"cert_index"`
	Registration bool   `json:"registration"`
}

// TransactionWithdrawalResponse represents one reward withdrawal.
type TransactionWithdrawalResponse struct {
	Address string `json:"address"`
	Amount  string `json:"amount"`
}

// TransactionMIRResponse represents one MIR certificate target.
type TransactionMIRResponse struct {
	Address   string `json:"address"`
	Amount    string `json:"amount"`
	CertIndex int    `json:"cert_index"`
	Pot       string `json:"pot"`
}

// TransactionPoolUpdateResponse represents one pool registration certificate.
type TransactionPoolUpdateResponse struct {
	Metadata      *TransactionPoolMetadataResponse `json:"metadata"`
	Owners        []string                         `json:"owners"`
	Relays        []TransactionPoolRelayResponse   `json:"relays"`
	ActiveEpoch   uint64                           `json:"active_epoch"`
	CertIndex     int                              `json:"cert_index"`
	FixedCost     string                           `json:"fixed_cost"`
	MarginCost    float64                          `json:"margin_cost"`
	Pledge        string                           `json:"pledge"`
	PoolID        string                           `json:"pool_id"`
	RewardAccount string                           `json:"reward_account"`
	VrfKey        string                           `json:"vrf_key"`
}

// TransactionPoolMetadataResponse represents pool metadata pointer data.
type TransactionPoolMetadataResponse struct {
	Hash string `json:"hash"`
	URL  string `json:"url"`
}

// TransactionPoolRelayResponse represents one relay in a pool registration cert.
type TransactionPoolRelayResponse struct {
	DNS    *string `json:"dns"`
	DNSSrv *string `json:"dns_srv"`
	IPv4   *string `json:"ipv4"`
	IPv6   *string `json:"ipv6"`
	Port   *int    `json:"port"`
}

// TransactionPoolRetireResponse represents one pool retirement certificate.
type TransactionPoolRetireResponse struct {
	PoolID        string `json:"pool_id"`
	CertIndex     int    `json:"cert_index"`
	RetiringEpoch uint64 `json:"retiring_epoch"`
}

// TransactionRedeemerResponse represents one Plutus redeemer.
type TransactionRedeemerResponse struct {
	DatumHash        *string `json:"datum_hash"`
	TxIndex          int     `json:"tx_index"`
	Purpose          string  `json:"purpose"`
	ScriptHash       string  `json:"script_hash"`
	RedeemerDataHash string  `json:"redeemer_data_hash"`
	UnitMem          string  `json:"unit_mem"`
	UnitSteps        string  `json:"unit_steps"`
	Fee              string  `json:"fee"`
}

// TransactionRequiredSignerResponse represents one required signing key hash.
type TransactionRequiredSignerResponse struct {
	WitnessHash string `json:"witness_hash"`
}

// AccountResponse represents a Blockfrost stake account.
type AccountResponse struct {
	StakeAddress       string  `json:"stake_address"`
	Active             bool    `json:"active"`
	ActiveEpoch        *int64  `json:"active_epoch"`
	ControlledAmount   string  `json:"controlled_amount"`
	RewardsSum         string  `json:"rewards_sum"`
	WithdrawalsSum     string  `json:"withdrawals_sum"`
	ReservesSum        string  `json:"reserves_sum"`
	TreasurySum        string  `json:"treasury_sum"`
	WithdrawableAmount string  `json:"withdrawable_amount"`
	PoolID             *string `json:"pool_id"`
	DrepID             *string `json:"drep_id"`
	Registered         bool    `json:"registered"`
}

// AccountAssociatedAddressResponse represents a stake
// account associated payment address.
type AccountAssociatedAddressResponse struct {
	Address string `json:"address"`
}

// AccountDelegationHistoryResponse represents a
// stake-account delegation history row.
type AccountDelegationHistoryResponse struct {
	ActiveEpoch int32  `json:"active_epoch"`
	TxHash      string `json:"tx_hash"`
	Amount      string `json:"amount"`
	PoolID      string `json:"pool_id"`
	TxSlot      int64  `json:"tx_slot"`
	BlockTime   int64  `json:"block_time"`
	BlockHeight int64  `json:"block_height"`
}

// AccountRegistrationHistoryResponse represents a
// stake-account registration history row.
type AccountRegistrationHistoryResponse struct {
	TxHash      string `json:"tx_hash"`
	Action      string `json:"action"`
	Deposit     string `json:"deposit"`
	TxSlot      int64  `json:"tx_slot"`
	BlockTime   int64  `json:"block_time"`
	BlockHeight int64  `json:"block_height"`
}

// AccountRewardHistoryResponse represents a stake-account
// reward history row.
type AccountRewardHistoryResponse struct {
	Epoch  int32  `json:"epoch"`
	Amount string `json:"amount"`
	PoolID string `json:"pool_id"`
}
