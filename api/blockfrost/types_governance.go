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

import "encoding/json"

// GovActionRef identifies a governance action by its proposing transaction
// and its position inside it.
type GovActionRef struct {
	TxHash []byte
	Index  uint32
}

// CommitteeCredentialRef is a committee credential with its key or script
// tag.
type CommitteeCredentialRef struct {
	Hash      []byte
	HasScript bool
}

// GovernanceProposalInfo is one entry of the proposal list.
type GovernanceProposalInfo struct {
	ID             string
	TxHash         string
	CertIndex      int
	GovernanceType string
}

// GovernanceProposalDetailInfo is a governance proposal. Description is the
// governance action rendered as JSON.
type GovernanceProposalDetailInfo struct {
	GovernanceProposalInfo
	Description   json.RawMessage
	Deposit       string
	ReturnAddress string
	RatifiedEpoch *uint64
	EnactedEpoch  *uint64
	DroppedEpoch  *uint64
	ExpiredEpoch  *uint64
	Expiration    uint64
}

// GovernanceProposalParametersInfo is the protocol parameter update a
// parameter change proposal carries.
type GovernanceProposalParametersInfo struct {
	ID         string
	TxHash     string
	CertIndex  int
	Parameters ProposalParametersResponse
}

// GovernanceWithdrawalInfo is one treasury withdrawal of a proposal.
type GovernanceWithdrawalInfo struct {
	StakeAddress string
	Amount       string
}

// GovernanceProposalVoteInfo is one vote cast on a proposal.
type GovernanceProposalVoteInfo struct {
	TxHash    string
	CertIndex int
	// VoterRole is constitutional_committee, drep or spo.
	VoterRole string
	Voter     string
	// Vote is yes, no or abstain.
	Vote string
	// Counted is false for a vote invalidated by its DRep deregistering.
	Counted bool
}

// GovernanceProposalMetadataInfo is the off-chain document behind a
// proposal's anchor.
type GovernanceProposalMetadataInfo struct {
	ID           string
	TxHash       string
	CertIndex    int
	URL          string
	Hash         string
	JSONMetadata json.RawMessage
	Bytes        string
}

// DRepDelegatorInfo is one delegator of a DRep.
type DRepDelegatorInfo struct {
	Address string
	Amount  string
}

// DRepAnchorInfo is the off-chain document behind a DRep's anchor. Error is
// set when the document could not be fetched or validated.
type DRepAnchorInfo struct {
	DRepID       string
	Hex          string
	URL          string
	Hash         string
	JSONMetadata json.RawMessage
	Bytes        *string
	Error        *OffchainFetchErrorInfo
}

// DRepUpdateInfo is one registration, update or deregistration certificate
// of a DRep.
type DRepUpdateInfo struct {
	TxHash    string
	CertIndex int
	// Action is registered, deregistered or updated.
	Action  string
	Deposit *string
}

// DRepVoteInfo is one vote a DRep cast.
type DRepVoteInfo struct {
	TxHash            string
	CertIndex         int
	ProposalID        string
	ProposalTxHash    string
	ProposalCertIndex int
	Vote              string
}

// PoolVoteInfo is one vote a pool cast.
type PoolVoteInfo struct {
	TxHash    string
	CertIndex int
	Vote      string
}

// CommitteeVoteInfo is one vote a committee member cast.
type CommitteeVoteInfo struct {
	TxHash         string
	VoterHotID     string
	ProposalID     string
	ProposalTxHash string
	ProposalIndex  int
	GovernanceType string
	Vote           string
	MetadataURL    *string
	MetadataHash   *string
	BlockHeight    uint64
	BlockTime      int64
}

// CommitteeMemberInfo is one member of the constitutional committee.
type CommitteeMemberInfo struct {
	ColdID        string
	ColdHex       string
	ColdHasScript bool
	HotID         *string
	HotHex        *string
	HotHasScript  *bool
	// Status is authorized, not_authorized or resigned.
	Status          string
	ExpirationEpoch uint64
}

// CommitteeInfo is the constitutional committee.
type CommitteeInfo struct {
	// GovActionID, ProposalTxHash and ProposalIndex name the enacted action
	// that last changed the committee; nil for the genesis committee.
	GovActionID    *string
	ProposalTxHash *string
	ProposalIndex  *int
	IsDissolved    bool
	QuorumNum      uint64
	QuorumDenom    uint64
	Members        []CommitteeMemberInfo
}

// GovernanceProposalListResponse is one entry of the Blockfrost proposal
// list.
type GovernanceProposalListResponse struct {
	ID             string `json:"id"`
	TxHash         string `json:"tx_hash"`
	CertIndex      int    `json:"cert_index"`
	GovernanceType string `json:"governance_type"`
}

// GovernanceProposalResponse is a Blockfrost governance proposal.
type GovernanceProposalResponse struct {
	ID                    string          `json:"id"`
	TxHash                string          `json:"tx_hash"`
	CertIndex             int             `json:"cert_index"`
	GovernanceType        string          `json:"governance_type"`
	GovernanceDescription json.RawMessage `json:"governance_description"`
	Deposit               string          `json:"deposit"`
	ReturnAddress         string          `json:"return_address"`
	RatifiedEpoch         *uint64         `json:"ratified_epoch"`
	EnactedEpoch          *uint64         `json:"enacted_epoch"`
	DroppedEpoch          *uint64         `json:"dropped_epoch"`
	ExpiredEpoch          *uint64         `json:"expired_epoch"`
	Expiration            uint64          `json:"expiration"`
}

// ProposalParametersResponse is the protocol parameter update of a
// parameter change proposal. Parameters the proposal leaves unchanged are
// null.
type ProposalParametersResponse struct {
	Epoch                      *uint64         `json:"epoch"`
	MinFeeA                    *uint64         `json:"min_fee_a"`
	MinFeeB                    *uint64         `json:"min_fee_b"`
	MaxBlockSize               *uint64         `json:"max_block_size"`
	MaxTxSize                  *uint64         `json:"max_tx_size"`
	MaxBlockHeaderSize         *uint64         `json:"max_block_header_size"`
	KeyDeposit                 *string         `json:"key_deposit"`
	PoolDeposit                *string         `json:"pool_deposit"`
	EMax                       *uint64         `json:"e_max"`
	NOpt                       *uint64         `json:"n_opt"`
	A0                         *float64        `json:"a0"`
	Rho                        *float64        `json:"rho"`
	Tau                        *float64        `json:"tau"`
	DecentralisationParam      *float64        `json:"decentralisation_param"`
	ExtraEntropy               *string         `json:"extra_entropy"`
	ProtocolMajorVer           *uint64         `json:"protocol_major_ver"`
	ProtocolMinorVer           *uint64         `json:"protocol_minor_ver"`
	MinUtxo                    *string         `json:"min_utxo"`
	MinPoolCost                *string         `json:"min_pool_cost"`
	CostModels                 json.RawMessage `json:"cost_models"`
	PriceMem                   *float64        `json:"price_mem"`
	PriceStep                  *float64        `json:"price_step"`
	MaxTxExMem                 *string         `json:"max_tx_ex_mem"`
	MaxTxExSteps               *string         `json:"max_tx_ex_steps"`
	MaxBlockExMem              *string         `json:"max_block_ex_mem"`
	MaxBlockExSteps            *string         `json:"max_block_ex_steps"`
	MaxValSize                 *string         `json:"max_val_size"`
	CollateralPercent          *uint64         `json:"collateral_percent"`
	MaxCollateralInputs        *uint64         `json:"max_collateral_inputs"`
	CoinsPerUtxoSize           *string         `json:"coins_per_utxo_size"`
	CoinsPerUtxoWord           *string         `json:"coins_per_utxo_word"`
	PvtMotionNoConfidence      *float64        `json:"pvt_motion_no_confidence"`
	PvtCommitteeNormal         *float64        `json:"pvt_committee_normal"`
	PvtCommitteeNoConfidence   *float64        `json:"pvt_committee_no_confidence"`
	PvtHardForkInitiation      *float64        `json:"pvt_hard_fork_initiation"`
	DvtMotionNoConfidence      *float64        `json:"dvt_motion_no_confidence"`
	DvtCommitteeNormal         *float64        `json:"dvt_committee_normal"`
	DvtCommitteeNoConfidence   *float64        `json:"dvt_committee_no_confidence"`
	DvtUpdateToConstitution    *float64        `json:"dvt_update_to_constitution"`
	DvtHardForkInitiation      *float64        `json:"dvt_hard_fork_initiation"`
	DvtPPNetworkGroup          *float64        `json:"dvt_p_p_network_group"`
	DvtPPEconomicGroup         *float64        `json:"dvt_p_p_economic_group"`
	DvtPPTechnicalGroup        *float64        `json:"dvt_p_p_technical_group"`
	DvtPPGovGroup              *float64        `json:"dvt_p_p_gov_group"`
	DvtTreasuryWithdrawal      *float64        `json:"dvt_treasury_withdrawal"`
	CommitteeMinSize           *string         `json:"committee_min_size"`
	CommitteeMaxTermLength     *string         `json:"committee_max_term_length"`
	GovActionLifetime          *string         `json:"gov_action_lifetime"`
	GovActionDeposit           *string         `json:"gov_action_deposit"`
	DRepDeposit                *string         `json:"drep_deposit"`
	DRepActivity               *string         `json:"drep_activity"`
	PvtppSecurityGroup         *float64        `json:"pvtpp_security_group"`
	PvtPPSecurityGroup         *float64        `json:"pvt_p_p_security_group"`
	MinFeeRefScriptCostPerByte *float64        `json:"min_fee_ref_script_cost_per_byte"`
}

// GovernanceProposalParametersResponse is the parameter update of a
// proposal.
type GovernanceProposalParametersResponse struct {
	ID         string                     `json:"id"`
	TxHash     string                     `json:"tx_hash"`
	CertIndex  int                        `json:"cert_index"`
	Parameters ProposalParametersResponse `json:"parameters"`
}

// GovernanceWithdrawalResponse is one treasury withdrawal of a proposal.
type GovernanceWithdrawalResponse struct {
	StakeAddress string `json:"stake_address"`
	Amount       string `json:"amount"`
}

// GovernanceProposalVoteResponse is one vote cast on a proposal.
type GovernanceProposalVoteResponse struct {
	TxHash    string `json:"tx_hash"`
	CertIndex int    `json:"cert_index"`
	VoterRole string `json:"voter_role"`
	Voter     string `json:"voter"`
	Vote      string `json:"vote"`
	Counted   bool   `json:"counted"`
}

// GovernanceProposalMetadataResponse is the off-chain document behind a
// proposal's anchor.
type GovernanceProposalMetadataResponse struct {
	ID           string          `json:"id"`
	TxHash       string          `json:"tx_hash"`
	CertIndex    int             `json:"cert_index"`
	URL          string          `json:"url"`
	Hash         string          `json:"hash"`
	JSONMetadata json.RawMessage `json:"json_metadata"`
	Bytes        string          `json:"bytes"`
}

// DRepDelegatorResponse is one delegator of a DRep.
type DRepDelegatorResponse struct {
	Address string `json:"address"`
	Amount  string `json:"amount"`
}

// DRepAnchorResponse is the off-chain document behind a DRep's anchor.
type DRepAnchorResponse struct {
	DRepID       string                      `json:"drep_id"`
	Hex          string                      `json:"hex"`
	URL          string                      `json:"url"`
	Hash         string                      `json:"hash"`
	JSONMetadata json.RawMessage             `json:"json_metadata"`
	Bytes        *string                     `json:"bytes"`
	Error        *OffchainFetchErrorResponse `json:"error,omitempty"`
}

// DRepUpdateResponse is one certificate of a DRep.
type DRepUpdateResponse struct {
	TxHash    string  `json:"tx_hash"`
	CertIndex int     `json:"cert_index"`
	Action    string  `json:"action"`
	Deposit   *string `json:"deposit"`
}

// DRepVoteResponse is one vote a DRep cast.
type DRepVoteResponse struct {
	TxHash            string `json:"tx_hash"`
	CertIndex         int    `json:"cert_index"`
	ProposalID        string `json:"proposal_id"`
	ProposalTxHash    string `json:"proposal_tx_hash"`
	ProposalCertIndex int    `json:"proposal_cert_index"`
	Vote              string `json:"vote"`
}

// PoolVoteResponse is one vote a pool cast.
type PoolVoteResponse struct {
	TxHash    string `json:"tx_hash"`
	CertIndex int    `json:"cert_index"`
	Vote      string `json:"vote"`
}

// CommitteeVoteResponse is one vote a committee member cast.
type CommitteeVoteResponse struct {
	TxHash         string  `json:"tx_hash"`
	VoterHotID     string  `json:"voter_hot_id"`
	ProposalID     string  `json:"proposal_id"`
	ProposalTxHash string  `json:"proposal_tx_hash"`
	ProposalIndex  int     `json:"proposal_index"`
	GovernanceType string  `json:"governance_type"`
	Vote           string  `json:"vote"`
	MetadataURL    *string `json:"metadata_url"`
	MetadataHash   *string `json:"metadata_hash"`
	BlockHeight    uint64  `json:"block_height"`
	BlockTime      int64   `json:"block_time"`
}

// CommitteeQuorumResponse is the committee quorum threshold.
type CommitteeQuorumResponse struct {
	Numerator   uint64 `json:"numerator"`
	Denominator uint64 `json:"denominator"`
}

// CommitteeMemberResponse is one member of the constitutional committee.
type CommitteeMemberResponse struct {
	CCColdID        string  `json:"cc_cold_id"`
	CCColdHex       string  `json:"cc_cold_hex"`
	CCColdHasScript bool    `json:"cc_cold_has_script"`
	CCHotID         *string `json:"cc_hot_id"`
	CCHotHex        *string `json:"cc_hot_hex"`
	CCHotHasScript  *bool   `json:"cc_hot_has_script"`
	Status          string  `json:"status"`
	ExpirationEpoch uint64  `json:"expiration_epoch"`
}

// CommitteeResponse is the constitutional committee.
type CommitteeResponse struct {
	GovActionID    *string                   `json:"gov_action_id"`
	ProposalTxHash *string                   `json:"proposal_tx_hash"`
	ProposalIndex  *int                      `json:"proposal_index"`
	IsDissolved    bool                      `json:"is_dissolved"`
	Quorum         CommitteeQuorumResponse   `json:"quorum"`
	Members        []CommitteeMemberResponse `json:"members"`
}
