// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

var conwayRules = []string{
	"%constr<plutus_data>",
	"%multiasset<positive_coin>",
	"addr_keyhash",
	"address",
	"alonzo_transaction_output",
	"anchor",
	"asset_name",
	"auxiliary_data",
	"auxiliary_data_array",
	"auxiliary_data_hash",
	"auxiliary_data_map",
	"auxiliary_scripts",
	"babbage_transaction_output",
	"big_int",
	"block",
	"block_number",
	"bootstrap_witness",
	"bounded_bytes",
	"certificate",
	"certificates",
	"coin",
	"committee_cold_credential",
	"committee_hot_credential",
	"constitution",
	"cost_models",
	"credential",
	"data",
	"datum_option",
	"dns_name",
	"drep",
	"drep_credential",
	"drep_voting_thresholds",
	"epoch",
	"ex_unit_prices",
	"ex_units",
	"gov_action",
	"gov_action_id",
	"guardrails_script_hash",
	"hash28",
	"hash32",
	"header",
	"header_body",
	"ipv4",
	"ipv6",
	"kes_period",
	"kes_vkey",
	"language",
	"major_protocol_version",
	"metadata",
	"metadatum",
	"mint",
	"native_script",
	"network_id",
	"nonnegative_interval",
	"nonzero_int64",
	"operational_cert",
	"plutus_data",
	"plutus_v1_script",
	"plutus_v2_script",
	"plutus_v3_script",
	"policy_id",
	"pool_keyhash",
	"pool_metadata",
	"pool_voting_thresholds",
	"positive_coin",
	"proposal_procedure",
	"proposal_procedures",
	"protocol_param_update",
	"protocol_version",
	"redeemer",
	"redeemers",
	"relay",
	"reward_account",
	"script",
	"script_data_hash",
	"script_hash",
	"signature",
	"signkey_kes",
	"slot",
	"stake_credential",
	"transaction",
	"transaction_body",
	"transaction_id",
	"transaction_index",
	"transaction_input",
	"transaction_output",
	"transaction_witness_set",
	"unit_interval",
	"url",
	"value",
	"vkey",
	"vkeywitness",
	"vote",
	"voter",
	"voting_procedure",
	"voting_procedures",
	"vrf_keyhash",
	"vrf_vkey",
	"withdrawals",
}

var byteExactRules = map[string]struct{}{
	"auxiliary_data":          {},
	"header":                  {},
	"native_script":           {},
	"plutus_data":             {},
	"redeemers":               {},
	"transaction_body":        {},
	"transaction_witness_set": {},
}

func ruleNames() []string {
	return append([]string(nil), conwayRules...)
}

func isByteExactRule(rule string) bool {
	_, ok := byteExactRules[rule]
	return ok
}

type storedCBOR interface {
	Cbor() []byte
}

type cborValue struct {
	cbor.StructAsArray
	Type   uint
	Native common.NativeScript
	Script []byte
}

type rawVotingProcedures common.VotingProcedures

type conwayConstitution struct {
	cbor.StructAsArray
	Anchor     common.GovAnchor
	ScriptHash []byte
}

func (s *cborValue) UnmarshalCBOR(data []byte) error {
	var items []cbor.RawMessage
	if n, err := cbor.Decode(data, &items); err != nil {
		return err
	} else if n != len(data) {
		return fmt.Errorf("%d trailing bytes after script", len(data)-n)
	}
	if len(items) != 2 {
		return fmt.Errorf("script must contain two array items, got %d", len(items))
	}
	if _, err := cbor.Decode(items[0], &s.Type); err != nil {
		return fmt.Errorf("decode script type: %w", err)
	}
	s.Native = common.NativeScript{}
	s.Script = nil
	switch s.Type {
	case common.ScriptRefTypeNativeScript:
		if err := validateConwayNativeScriptCBOR(items[1]); err != nil {
			return fmt.Errorf("validate Conway native script shape: %w", err)
		}
		if _, err := cbor.Decode(items[1], &s.Native); err != nil {
			return fmt.Errorf("decode native script: %w", err)
		}
		if err := common.ValidateNativeScriptConstructors(
			[]common.NativeScript{s.Native}, 5,
		); err != nil {
			return fmt.Errorf("validate Conway native script: %w", err)
		}
	case common.ScriptRefTypePlutusV1, common.ScriptRefTypePlutusV2, common.ScriptRefTypePlutusV3:
		if _, err := cbor.Decode(items[1], &s.Script); err != nil {
			return fmt.Errorf("decode Plutus script bytes: %w", err)
		}
	default:
		return fmt.Errorf("unsupported Conway script type %d", s.Type)
	}
	return nil
}

func (s *cborValue) MarshalCBOR() ([]byte, error) {
	var payload any
	switch s.Type {
	case common.ScriptRefTypeNativeScript:
		payload = s.Native
	case common.ScriptRefTypePlutusV1, common.ScriptRefTypePlutusV2, common.ScriptRefTypePlutusV3:
		payload = s.Script
	default:
		return nil, fmt.Errorf("unsupported Conway script type %d", s.Type)
	}
	return cbor.Encode([]any{s.Type, payload})
}

func reencode(rule string, raw []byte) ([]byte, error) {
	if _, err := cbor.DecodeExact(raw, new(cbor.RawMessage)); err != nil {
		return nil, err
	}
	if len(raw) == 0 || raw[0] == 0xf6 || raw[0] == 0xf7 {
		return nil, errors.New("CBOR null and undefined are not valid Conway rule values")
	}
	switch rule {
	case "auxiliary_data":
		decoded, err := common.DecodeAuxiliaryDataForEra(
			raw,
			common.AuxiliaryDataEraConway,
		)
		if err != nil {
			return nil, err
		}
		metadata, err := decoded.Metadata()
		if err != nil {
			return nil, fmt.Errorf("decode Conway auxiliary-data metadata: %w", err)
		}
		if err := shelley.ValidateMetadataContent(metadata); err != nil {
			return nil, fmt.Errorf("validate Conway auxiliary-data metadata: %w", err)
		}
		return storedEncoding(decoded)
	case "auxiliary_data_array":
		return reencodeConwayAuxiliaryData(raw, "array")
	case "auxiliary_data_hash", "hash32", "script_data_hash", "transaction_id":
		return reencodeBytes(raw, 32, 32)
	case "auxiliary_data_map":
		return reencodeConwayAuxiliaryData(raw, "map")
	case "auxiliary_scripts":
		var rawScripts []cbor.RawMessage
		if _, err := cbor.DecodeExact(raw, &rawScripts); err != nil {
			return nil, err
		}
		for index, script := range rawScripts {
			if err := validateConwayNativeScriptCBOR(script); err != nil {
				return nil, fmt.Errorf("auxiliary native script %d: %w", index, err)
			}
		}
		return reencodeChecked[[]common.NativeScript](raw, func(scripts []common.NativeScript) error {
			return common.ValidateNativeScriptConstructors(scripts, 5)
		})
	case "addr_keyhash", "guardrails_script_hash", "hash28", "policy_id", "pool_keyhash", "script_hash":
		return reencodeBytes(raw, 28, 28)
	case "address":
		if err := requireCBORDefiniteMajorType(raw, 2); err != nil {
			return nil, err
		}
		return reencodeChecked[common.Address](raw, func(decoded common.Address) error {
			return common.CheckAddressPointerInRange(decoded)
		})
	case "alonzo_transaction_output":
		return reencodeTyped[alonzo.AlonzoTransactionOutput](raw)
	case "anchor":
		return reencodeTyped[common.GovAnchor](raw)
	case "asset_name":
		return reencodeBytes(raw, 0, 32)
	case "big_int":
		if err := validateConwayBigIntCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeTyped[big.Int](raw)
	case "block":
		if err := validateConwayBlockCBOR(raw); err != nil {
			return nil, err
		}
		block, err := ledger.NewBlockFromCbor(
			ledger.BlockTypeConway,
			raw,
			common.VerifyConfig{SkipBodyHashValidation: true},
		)
		if err != nil {
			return nil, err
		}
		return cbor.Encode(block)
	case "block_number", "coin", "epoch", "kes_period", "slot":
		if err := requireCBORMajorType(raw, 0); err != nil {
			return nil, err
		}
		return reencodeTyped[uint64](raw)
	case "bootstrap_witness":
		return reencodeTyped[common.BootstrapWitness](raw)
	case "bounded_bytes":
		return reencodeByteString(raw, 0, 64, false)
	case "certificate":
		if err := requireCBORMajorType(raw, 4); err != nil {
			return nil, err
		}
		var decoded common.CertificateWrapper
		if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
			return nil, err
		}
		if err := common.ValidatePoolRegistrationOwners(
			[]common.CertificateWrapper{decoded},
		); err != nil {
			return nil, err
		}
		clearPoolRegistrationCertificateCBOR(&decoded)
		return cbor.Encode(&decoded)
	case "certificates":
		var decoded cbor.SetType[common.CertificateWrapper]
		if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
			return nil, err
		}
		certificates := decoded.Items()
		if err := common.ValidateCertificateSet(certificates); err != nil {
			return nil, err
		}
		for index := range certificates {
			clearPoolRegistrationCertificateCBOR(&certificates[index])
		}
		canonical := cbor.NewSetType(certificates, true)
		return cbor.Encode(&canonical)
	case "committee_cold_credential", "committee_hot_credential", "drep_credential", "stake_credential":
		return reencodeTyped[common.Credential](raw)
	case "constitution":
		if err := validateConwayConstitutionCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[conwayConstitution](raw, func(decoded conwayConstitution) error {
			if decoded.ScriptHash != nil && len(decoded.ScriptHash) != 28 {
				return fmt.Errorf("constitution script hash must be 28 bytes, got %d", len(decoded.ScriptHash))
			}
			return nil
		})
	case "cost_models":
		if err := requireCBORMajorType(raw, 5); err != nil {
			return nil, err
		}
		if err := validateConwayCostModelParameterTypes(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[map[uint][]int64](raw, validateConwayCostModelLanguageIDs)
	case "credential":
		return reencodeTyped[common.Credential](raw)
	case "data":
		return reencodeWrappedPlutusData(raw)
	case "%constr<plutus_data>":
		return reencodeStored[common.Datum](raw)
	case "datum_option":
		return reencodeTyped[babbage.BabbageTransactionOutputDatumOption](raw)
	case "dns_name":
		return reencodeText(raw, 0, 128)
	case "drep":
		return reencodeTyped[common.Drep](raw)
	case "drep_voting_thresholds":
		if err := common.ValidateNonNegativeBoundedRatArrayCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[conway.DRepVotingThresholds](raw, func(value conway.DRepVotingThresholds) error {
			return validateUnitIntervals(
				value.MotionNoConfidence,
				value.CommitteeNormal,
				value.CommitteeNoConfidence,
				value.UpdateToConstitution,
				value.HardForkInitiation,
				value.PpNetworkGroup,
				value.PpEconomicGroup,
				value.PpTechnicalGroup,
				value.PpGovGroup,
				value.TreasuryWithdrawal,
			)
		})
	case "ex_unit_prices":
		if err := common.ValidateNonNegativeBoundedRatArrayCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[common.ExUnitPrice](raw, func(value common.ExUnitPrice) error {
			if err := common.ValidateNonNegativeInterval(value.MemPrice, false); err != nil {
				return fmt.Errorf("memory price: %w", err)
			}
			if err := common.ValidateNonNegativeInterval(value.StepPrice, false); err != nil {
				return fmt.Errorf("step price: %w", err)
			}
			return nil
		})
	case "ex_units":
		if err := validateUnsignedArrayFields(raw, "execution units", 2); err != nil {
			return nil, err
		}
		return reencodeTyped[common.ExUnits](raw)
	case "gov_action":
		if err := validateConwayHardForkVersion(raw); err != nil {
			return nil, err
		}
		return reencodeTyped[conway.ConwayGovAction](raw)
	case "gov_action_id":
		return reencodeTyped[common.GovActionId](raw)
	case "header":
		header, err := ledger.NewBlockHeaderFromCbor(ledger.BlockTypeConway, raw)
		if err != nil {
			return nil, err
		}
		return storedEncoding(header)
	case "header_body":
		if err := validateConwayHeaderBodyCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeStored[babbage.BabbageBlockHeaderBody](raw)
	case "ipv4":
		return reencodeBytes(raw, 4, 4)
	case "ipv6":
		return reencodeBytes(raw, 16, 16)
	case "kes_vkey", "vrf_vkey", "vkey":
		return reencodeBytes(raw, 32, 32)
	case "language":
		if err := requireCBORMajorType(raw, 0); err != nil {
			return nil, err
		}
		return reencodeChecked[uint8](raw, func(value uint8) error {
			if value > 2 {
				return fmt.Errorf("unsupported Plutus language %d", value)
			}
			return nil
		})
	case "major_protocol_version":
		if err := requireCBORMajorType(raw, 0); err != nil {
			return nil, err
		}
		return reencodeChecked[uint8](raw, func(value uint8) error {
			if value > maxConwayMajorProtocolVersion {
				return fmt.Errorf("major protocol version exceeds %d: %d", maxConwayMajorProtocolVersion, value)
			}
			return nil
		})
	case "metadata":
		if err := requireCBORMajorType(raw, 5); err != nil {
			return nil, err
		}
		if err := validateMetadataMapKeys(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[common.ShelleyAuxiliaryData](raw, func(decoded common.ShelleyAuxiliaryData) error {
			metadata, err := decoded.Metadata()
			if err != nil {
				return err
			}
			return shelley.ValidateMetadataContent(metadata)
		})
	case "metadatum":
		decoded, err := common.DecodeMetadatumRaw(raw)
		if err != nil {
			return nil, err
		}
		if err := shelley.ValidateMetadataContent(decoded); err != nil {
			return nil, err
		}
		return decoded.Cbor(), nil
	case "mint":
		if err := requireCBORMajorType(raw, 5); err != nil {
			return nil, err
		}
		if err := requireDefiniteCBORStrings(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[common.MultiAsset[*big.Int]](
			raw,
			func(decoded common.MultiAsset[*big.Int]) error {
				if err := decoded.CheckForDuplicateKeys(); err != nil {
					return err
				}
				if err := decoded.CheckForZeroAssets(); err != nil {
					return err
				}
				return decoded.ValidateMintQuantities()
			},
		)
	case "native_script":
		return reencodeConwayNativeScript(raw)
	case "network_id":
		if err := requireCBORMajorType(raw, 0); err != nil {
			return nil, err
		}
		return reencodeChecked[uint8](raw, func(value uint8) error {
			if value > 1 {
				return fmt.Errorf("network id must be 0 or 1, got %d", value)
			}
			return nil
		})
	case "nonnegative_interval":
		return reencodeInterval(raw, false)
	case "nonzero_int64":
		return reencodeChecked[int64](raw, func(value int64) error {
			if value == 0 {
				return errors.New("integer must be nonzero")
			}
			return nil
		})
	case "operational_cert":
		if err := validateOperationalCertCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[babbage.BabbageOpCert](raw, func(value babbage.BabbageOpCert) error {
			if len(value.HotVkey) != 32 {
				return fmt.Errorf("operational certificate hot key must be 32 bytes, got %d", len(value.HotVkey))
			}
			if len(value.Signature) != 64 {
				return fmt.Errorf("operational certificate signature must be 64 bytes, got %d", len(value.Signature))
			}
			return nil
		})
	case "plutus_data":
		return reencodeStored[common.Datum](raw)
	case "plutus_v1_script":
		return reencodeTyped[common.PlutusV1Script](raw)
	case "plutus_v2_script":
		return reencodeTyped[common.PlutusV2Script](raw)
	case "plutus_v3_script":
		return reencodeTyped[common.PlutusV3Script](raw)
	case "positive_coin":
		if err := requireCBORMajorType(raw, 0); err != nil {
			return nil, err
		}
		return reencodeChecked[uint64](raw, func(value uint64) error {
			if value == 0 {
				return errors.New("coin must be positive")
			}
			return nil
		})
	case "pool_metadata":
		if err := requireCBORMajorType(raw, 4); err != nil {
			return nil, err
		}
		if err := validatePoolMetadataCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[common.PoolMetadata](raw, func(value common.PoolMetadata) error {
			return common.ValidatePoolMetadata(&value)
		})
	case "pool_voting_thresholds":
		if err := common.ValidateNonNegativeBoundedRatArrayCBOR(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[conway.PoolVotingThresholds](raw, func(value conway.PoolVotingThresholds) error {
			return validateUnitIntervals(
				value.MotionNoConfidence,
				value.CommitteeNormal,
				value.CommitteeNoConfidence,
				value.HardForkInitiation,
				value.PpSecurityGroup,
			)
		})
	case "proposal_procedure":
		return reencodeTyped[conway.ConwayProposalProcedure](raw)
	case "proposal_procedures":
		var decoded cbor.SetType[conway.ConwayProposalProcedure]
		if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
			return nil, err
		}
		if err := decoded.CheckForDuplicatesAlways(); err != nil {
			return nil, err
		}
		canonical := cbor.NewSetType(decoded.Items(), true)
		return cbor.Encode(&canonical)
	case "protocol_param_update":
		return reencodeTyped[conway.ConwayProtocolParameterUpdate](raw)
	case "protocol_version":
		return reencodeProtocolVersion(raw)
	case "redeemer":
		return reencodeConwayRedeemer(raw)
	case "redeemers":
		return reencodeConwayRedeemers(raw)
	case "relay":
		return reencodeTyped[common.PoolRelay](raw)
	case "reward_account":
		return reencodeChecked[common.Address](raw, common.CheckAccountAddress)
	case "script":
		return reencodeTyped[cborValue](raw)
	case "signature", "signkey_kes":
		return reencodeBytes(raw, 64, 64)
	case "transaction":
		tx, err := ledger.NewTransactionFromCbor(ledger.TxTypeConway, raw)
		if err != nil {
			return nil, err
		}
		return cbor.Encode(tx)
	case "transaction_body":
		if err := validateConwayTransactionBodyMap(raw); err != nil {
			return nil, err
		}
		return reencodeStored[conway.ConwayTransactionBody](raw)
	case "transaction_input":
		return reencodeTyped[shelley.ShelleyTransactionInput](raw)
	case "transaction_index":
		if err := requireCBORMajorType(raw, 0); err != nil {
			return nil, err
		}
		return reencodeTyped[uint16](raw)
	case "transaction_output":
		if len(raw) > 0 && raw[0]>>5 == 5 {
			if err := validateBabbageTransactionOutputMap(raw); err != nil {
				return nil, err
			}
		}
		var decoded babbage.BabbageTransactionOutput
		if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
			return nil, err
		}
		if err := common.CheckAddressPointerInRange(decoded.OutputAddress); err != nil {
			return nil, err
		}
		if err := decoded.OutputAmount.Assets.CheckForDuplicateKeys(); err != nil {
			return nil, err
		}
		if err := decoded.OutputAmount.Assets.CheckForZeroAssets(); err != nil {
			return nil, err
		}
		return cbor.Encode(&decoded)
	case "transaction_witness_set":
		return reencodeStored[conway.ConwayTransactionWitnessSet](raw)
	case "unit_interval":
		return reencodeInterval(raw, true)
	case "url":
		return reencodeText(raw, 0, 128)
	case "value":
		var decoded mary.MaryTransactionOutputValue
		if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
			return nil, err
		}
		if err := decoded.Assets.CheckForDuplicateKeys(); err != nil {
			return nil, err
		}
		if err := decoded.Assets.CheckForZeroAssets(); err != nil {
			return nil, err
		}
		return cbor.Encode(&decoded)
	case "voting_procedure":
		return reencodeTyped[conway.ConwayVotingProcedure](raw)
	case "vote":
		if err := requireCBORMajorType(raw, 0); err != nil {
			return nil, err
		}
		return reencodeChecked[common.Vote](raw, func(value common.Vote) error {
			if value > common.Vote(common.GovVoteAbstain) {
				return fmt.Errorf("invalid vote %d", value)
			}
			return nil
		})
	case "voter":
		return reencodeTyped[common.Voter](raw)
	case "voting_procedures":
		return reencodeConwayVotingProcedures(raw)
	case "vrf_keyhash":
		return reencodeBytes(raw, 32, 32)
	case "withdrawals":
		if err := requireCBORMajorType(raw, 5); err != nil {
			return nil, err
		}
		if err := validateWithdrawalAmounts(raw); err != nil {
			return nil, err
		}
		return reencodeWithdrawals(raw)
	case "%multiasset<positive_coin>":
		if err := requireCBORMajorType(raw, 5); err != nil {
			return nil, err
		}
		if err := requireDefiniteCBORStrings(raw); err != nil {
			return nil, err
		}
		return reencodeChecked[common.MultiAsset[uint64]](
			raw,
			func(decoded common.MultiAsset[uint64]) error {
				if err := decoded.CheckForDuplicateKeys(); err != nil {
					return err
				}
				if err := decoded.CheckForZeroAssets(); err != nil {
					return err
				}
				return decoded.ValidateOutputQuantities()
			},
		)
	case "babbage_transaction_output":
		if len(raw) > 0 && raw[0]>>5 == 5 {
			if err := validateBabbageTransactionOutputMap(raw); err != nil {
				return nil, err
			}
		}
		return reencodeBabbageTransactionOutput(raw)
	case "vkeywitness":
		return reencodeTyped[common.VkeyWitness](raw)
	default:
		return nil, fmt.Errorf("unknown Conway rule %q", rule)
	}
}

func clearPoolRegistrationCertificateCBOR(certificate *common.CertificateWrapper) {
	if poolRegistration, ok := certificate.Certificate.(*common.PoolRegistrationCertificate); ok {
		poolRegistration.SetCbor(nil)
	}
}

func reencodeBabbageTransactionOutput(raw []byte) ([]byte, error) {
	var decoded babbage.BabbageTransactionOutput
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if err := common.CheckAddressPointerInRange(decoded.OutputAddress); err != nil {
		return nil, err
	}
	if err := decoded.OutputAmount.Assets.CheckForDuplicateKeys(); err != nil {
		return nil, err
	}
	if err := decoded.OutputAmount.Assets.CheckForZeroAssets(); err != nil {
		return nil, err
	}
	if decoded.TxOutScriptRef == nil {
		if decoded.DatumOption != nil && decoded.DatumOption.IsDatumHash() {
			datumHash := decoded.DatumHash()
			return cbor.Encode([]any{
				decoded.OutputAddress,
				decoded.OutputAmount,
				datumHash,
			})
		}
		if decoded.DatumOption == nil {
			return cbor.Encode([]any{
				decoded.OutputAddress,
				decoded.OutputAmount,
			})
		}
	}
	return cbor.Encode(&decoded)
}

func requireDefiniteCBORStrings(raw []byte) error {
	root, err := cbor.ParseDiagnostic(raw)
	if err != nil {
		return err
	}
	var visit func(*cbor.DiagnosticNode) error
	visit = func(node *cbor.DiagnosticNode) error {
		if node.Indefinite && (node.Type == cbor.DiagTypeBytes || node.Type == cbor.DiagTypeText) {
			return fmt.Errorf("indefinite-length byte or text string at offset %d", node.Offset)
		}
		for index := range node.Children {
			if err := visit(&node.Children[index]); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

func validateMetadataMapKeys(raw []byte) error {
	var metadata map[uint64]cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &metadata); err != nil {
		return fmt.Errorf("decode metadata labels: %w", err)
	}
	return nil
}

func validateConwayBlockCBOR(raw []byte) error {
	var fields []cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 4 && len(fields) != 5 {
		return fmt.Errorf("conway block must contain four or five fields, got %d", len(fields))
	}
	for index, majorType := range []byte{4, 4, 4, 5} {
		if err := requireCBORMajorType(fields[index], majorType); err != nil {
			return fmt.Errorf("conway block field %d: %w", index, err)
		}
	}
	if len(fields) == 5 {
		invalidTransactions := fields[4]
		if len(invalidTransactions) > 0 && invalidTransactions[0]>>5 == 6 {
			var set cbor.RawTag
			if _, err := cbor.DecodeExact(invalidTransactions, &set); err != nil {
				return fmt.Errorf("conway block invalid transaction set: %w", err)
			}
			if set.Number != cbor.CborTagSet {
				return fmt.Errorf("conway block invalid transaction set has tag %d", set.Number)
			}
			invalidTransactions = set.Content
		}
		if err := requireCBORMajorType(invalidTransactions, 4); err != nil {
			return fmt.Errorf("conway block field 4: %w", err)
		}
		var indexes []uint64
		if _, err := cbor.DecodeExact(invalidTransactions, &indexes); err != nil {
			return fmt.Errorf("decode Conway block invalid transaction indexes: %w", err)
		}
		for index := 1; index < len(indexes); index++ {
			if indexes[index] <= indexes[index-1] {
				return errors.New("conway block invalid transaction indexes must be strictly increasing")
			}
		}
	}
	return nil
}

func validateConwayBigIntCBOR(raw []byte) error {
	node, err := cbor.ParseDiagnostic(raw)
	if err != nil {
		return err
	}
	if node.Type == cbor.DiagTypeUint || node.Type == cbor.DiagTypeNint {
		return nil
	}
	if node.Type != cbor.DiagTypeTag || node.Tag == nil ||
		(*node.Tag != 2 && *node.Tag != 3) || len(node.Children) != 1 ||
		node.Children[0].Type != cbor.DiagTypeBytes {
		return errors.New("big integer must be an integer or a CBOR bignum")
	}
	value, ok := node.Children[0].Value.([]byte)
	if !ok {
		return errors.New("CBOR bignum does not contain a byte string")
	}
	if len(value) > 64 {
		return fmt.Errorf("CBOR bignum magnitude is %d bytes, maximum is 64", len(value))
	}
	return nil
}

func validateUnsignedArrayFields(raw []byte, name string, count int) error {
	var fields []cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return err
	}
	if len(fields) != count {
		return fmt.Errorf("%s must contain %d fields, got %d", name, count, len(fields))
	}
	for index, field := range fields {
		if err := requireCBORMajorType(field, 0); err != nil {
			return fmt.Errorf("%s field %d must be an unsigned integer: %w", name, index, err)
		}
	}
	return nil
}

func validateConwayHeaderBodyCBOR(raw []byte) error {
	var fields []cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 10 {
		return fmt.Errorf("conway header body must contain 10 fields, got %d", len(fields))
	}
	if err := requireCBORDefiniteMajorType(fields[5], 4); err != nil {
		return fmt.Errorf("conway header body VRF result: %w", err)
	}
	return nil
}

func validateConwayTransactionBodyMap(raw []byte) error {
	if err := requireCBORMajorType(raw, 5); err != nil {
		return err
	}
	var fields map[uint]cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return fmt.Errorf("decode Conway transaction-body fields: %w", err)
	}
	for key, value := range fields {
		if len(value) == 0 || value[0] == 0xf6 || value[0] == 0xf7 {
			return fmt.Errorf("transaction-body field %d cannot be null or undefined", key)
		}
	}
	return nil
}

func reencodeConwayNativeScript(raw []byte) ([]byte, error) {
	if err := validateConwayNativeScriptCBOR(raw); err != nil {
		return nil, err
	}
	var decoded common.NativeScript
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if err := common.ValidateNativeScriptConstructors(
		[]common.NativeScript{decoded}, 5,
	); err != nil {
		return nil, err
	}
	return storedEncoding(decoded)
}

func validateConwayNativeScriptCBOR(raw []byte) error {
	node, err := cbor.ParseDiagnostic(raw)
	if err != nil {
		return err
	}
	return validateConwayNativeScriptNode(node)
}

func validateConwayNativeScriptNode(node *cbor.DiagnosticNode) error {
	if node.Type != cbor.DiagTypeArray || len(node.Children) == 0 ||
		node.Children[0].Type != cbor.DiagTypeUint {
		return errors.New("native script must begin with an unsigned constructor")
	}
	constructor, ok := node.Children[0].Value.(uint64)
	if !ok {
		return errors.New("native script constructor is not uint64")
	}
	if constructor == 3 {
		if len(node.Children) != 3 ||
			(node.Children[1].Type != cbor.DiagTypeUint &&
				node.Children[1].Type != cbor.DiagTypeNint) {
			return errors.New("native script n-of-k threshold must be an integer")
		}
	}
	if constructor < 1 || constructor > 3 {
		return nil
	}
	childrenIndex := 1
	if constructor == 3 {
		childrenIndex = 2
	}
	if len(node.Children) <= childrenIndex || node.Children[childrenIndex].Type != cbor.DiagTypeArray {
		return errors.New("native script children must be an array")
	}
	for index := range node.Children[childrenIndex].Children {
		if err := validateConwayNativeScriptNode(&node.Children[childrenIndex].Children[index]); err != nil {
			return fmt.Errorf("native script child %d: %w", index, err)
		}
	}
	return nil
}

func reencodeWrappedPlutusData(raw []byte) ([]byte, error) {
	var tag cbor.RawTag
	if _, err := cbor.DecodeExact(raw, &tag); err != nil {
		return nil, err
	}
	if tag.Number != cbor.CborTagCbor {
		return nil, fmt.Errorf("expected CBOR tag %d around Plutus data", cbor.CborTagCbor)
	}
	if err := requireCBORDefiniteMajorType(tag.Content, 2); err != nil {
		return nil, fmt.Errorf("wrapped Plutus data must use a definite byte string: %w", err)
	}
	var wrapped cbor.WrappedCbor
	if _, err := cbor.DecodeExact(raw, &wrapped); err != nil {
		return nil, err
	}
	var datum common.Datum
	if _, err := cbor.DecodeExact(wrapped.Bytes(), &datum); err != nil {
		return nil, err
	}
	return cbor.Encode(cbor.WrappedCbor(datum.Cbor()))
}

func validatePoolMetadataCBOR(raw []byte) error {
	var fields []cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 2 {
		return fmt.Errorf("pool metadata must contain two fields, got %d", len(fields))
	}
	if err := requireCBORDefiniteMajorType(fields[0], 3); err != nil {
		return fmt.Errorf("pool metadata URL: %w", err)
	}
	if err := requireCBORDefiniteMajorType(fields[1], 2); err != nil {
		return fmt.Errorf("pool metadata hash: %w", err)
	}
	return nil
}

func validateOperationalCertCBOR(raw []byte) error {
	var fields []cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 4 {
		return fmt.Errorf("operational certificate must contain four fields, got %d", len(fields))
	}
	for _, index := range []int{0, 3} {
		if err := requireCBORDefiniteMajorType(fields[index], 2); err != nil {
			return fmt.Errorf("operational certificate byte field %d: %w", index, err)
		}
	}
	return nil
}

func validateConwayConstitutionCBOR(raw []byte) error {
	if err := common.ValidateDefiniteCBORArrayLength(raw, 2, "constitution"); err != nil {
		return err
	}
	var fields []cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return err
	}
	if len(fields) > 1 && len(fields[1]) > 0 && fields[1][0] != 0xf6 {
		if err := requireCBORDefiniteMajorType(fields[1], 2); err != nil {
			return fmt.Errorf("constitution script hash: %w", err)
		}
	}
	return nil
}

func reencodeConwayAuxiliaryData(raw []byte, want string) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty auxiliary data")
	}
	wantMajor := byte(4)
	if want == "map" {
		wantMajor = 6
	}
	if raw[0]>>5 != wantMajor {
		return nil, fmt.Errorf("expected Conway auxiliary-data %s form", want)
	}
	decoded, err := common.DecodeAuxiliaryDataForEra(raw, common.AuxiliaryDataEraConway)
	if err != nil {
		return nil, err
	}
	switch want {
	case "array":
		if _, ok := decoded.(*common.ShelleyMaAuxiliaryData); !ok {
			return nil, errors.New("conway auxiliary-data array has an unexpected decoder type")
		}
	case "map":
		if _, ok := decoded.(*common.AlonzoAuxiliaryData); !ok {
			return nil, errors.New("conway auxiliary-data map has an unexpected decoder type")
		}
		if err := validateConwayAuxiliaryDataScripts(raw); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown Conway auxiliary-data form %q", want)
	}
	metadata, err := decoded.Metadata()
	if err != nil {
		return nil, err
	}
	if err := shelley.ValidateMetadataContent(metadata); err != nil {
		return nil, err
	}
	return storedEncoding(decoded)
}

func reencodeConwayVotingProcedures(raw []byte) ([]byte, error) {
	if len(raw) == 0 || raw[0]>>5 != 5 {
		return nil, errors.New("voting procedures must be a CBOR map")
	}
	var loose rawVotingProcedures
	if _, err := cbor.DecodeExact(raw, &loose); err != nil {
		return nil, err
	}
	if len(loose) == 0 {
		return cbor.Encode(loose)
	}
	var decoded common.VotingProcedures
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	return cbor.Encode(decoded)
}

func reencodeProtocolVersion(raw []byte) ([]byte, error) {
	if err := requireCBORMajorType(raw, 4); err != nil {
		return nil, err
	}
	var values []uint64
	if _, err := cbor.DecodeExact(raw, &values); err != nil {
		return nil, err
	}
	if len(values) != 2 {
		return nil, fmt.Errorf("protocol version must contain two fields, got %d", len(values))
	}
	if err := validateConwayProtocolVersion(values[0], values[1]); err != nil {
		return nil, err
	}
	return cbor.Encode(values)
}

const maxConwayMajorProtocolVersion uint8 = 13

func validateConwayProtocolVersion(major, minor uint64) error {
	if major > uint64(maxConwayMajorProtocolVersion) {
		return fmt.Errorf(
			"major protocol version exceeds %d: %d",
			maxConwayMajorProtocolVersion,
			major,
		)
	}
	if minor > uint64(^uint32(0)) {
		return fmt.Errorf("minor protocol version exceeds uint32: %d", minor)
	}
	return nil
}

func validateConwayHardForkVersion(raw []byte) error {
	var action []cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &action); err != nil {
		return err
	}
	if len(action) == 0 {
		return errors.New("empty Conway governance action")
	}
	var actionType uint64
	if _, err := cbor.DecodeExact(action[0], &actionType); err != nil {
		return err
	}
	if actionType != uint64(common.GovActionTypeHardForkInitiation) {
		return nil
	}
	if len(action) != 3 {
		return fmt.Errorf("hard-fork initiation action must contain 3 fields, got %d", len(action))
	}
	var version []uint64
	if _, err := cbor.DecodeExact(action[2], &version); err != nil {
		return err
	}
	if len(version) != 2 {
		return fmt.Errorf("protocol version must contain two fields, got %d", len(version))
	}
	return validateConwayProtocolVersion(version[0], version[1])
}

func validateConwayCostModelParameterTypes(raw []byte) error {
	var models map[uint]cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &models); err != nil {
		return err
	}
	for language, rawParameters := range models {
		if err := requireCBORMajorType(rawParameters, 4); err != nil {
			return fmt.Errorf("cost-model language %d parameters: %w", language, err)
		}
		var parameters []cbor.RawMessage
		if _, err := cbor.DecodeExact(rawParameters, &parameters); err != nil {
			return fmt.Errorf("cost-model language %d parameters: %w", language, err)
		}
		for index, rawParameter := range parameters {
			if len(rawParameter) == 0 || (rawParameter[0]>>5 != 0 && rawParameter[0]>>5 != 1) {
				return fmt.Errorf(
					"cost-model language %d parameter %d must be an integer",
					language,
					index,
				)
			}
			var parameter int64
			if _, err := cbor.DecodeExact(rawParameter, &parameter); err != nil {
				return fmt.Errorf(
					"cost-model language %d parameter %d is not int64: %w",
					language,
					index,
					err,
				)
			}
		}
	}
	return nil
}

func validateWithdrawalAmounts(raw []byte) error {
	var withdrawals map[*common.Address]cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &withdrawals); err != nil {
		return err
	}
	for _, amount := range withdrawals {
		if err := requireCBORMajorType(amount, 0); err != nil {
			return fmt.Errorf("withdrawal amount must be an unsigned integer: %w", err)
		}
		var value uint64
		if _, err := cbor.DecodeExact(amount, &value); err != nil {
			return fmt.Errorf("withdrawal amount must be uint64: %w", err)
		}
	}
	return nil
}

func reencodeWithdrawals(raw []byte) ([]byte, error) {
	var withdrawals map[*common.Address]uint64
	if _, err := cbor.DecodeExact(raw, &withdrawals); err != nil {
		return nil, err
	}
	if err := common.ValidateWithdrawalAddresses(withdrawals); err != nil {
		return nil, err
	}
	return encodeRewardAccountMap(withdrawals)
}

// encodeRewardAccountMap preserves Cardano's ledger ordering for reward accounts.
func encodeRewardAccountMap(withdrawals map[*common.Address]uint64) ([]byte, error) {
	encoded := appendCBORMapHeader(nil, uint64(len(withdrawals)))
	for _, address := range common.SortRewardAccountAddresses(withdrawals) {
		encodedAddress, err := cbor.Encode(address)
		if err != nil {
			return nil, fmt.Errorf("encode withdrawal reward account: %w", err)
		}
		encodedAmount, err := cbor.Encode(withdrawals[address])
		if err != nil {
			return nil, fmt.Errorf("encode withdrawal amount: %w", err)
		}
		encoded = append(encoded, encodedAddress...)
		encoded = append(encoded, encodedAmount...)
	}
	return encoded, nil
}

func appendCBORMapHeader(dst []byte, length uint64) []byte {
	const mapMajorType = byte(5 << 5)
	switch {
	case length < 24:
		return append(dst, mapMajorType|byte(length))
	case length <= 0xff:
		return append(dst, mapMajorType|24, byte(length))
	case length <= 0xffff:
		return append(
			dst,
			mapMajorType|25,
			byte((length>>8)&0xff),
			byte(length&0xff),
		)
	case length <= 0xffffffff:
		return append(
			dst,
			mapMajorType|26,
			byte((length>>24)&0xff),
			byte((length>>16)&0xff),
			byte((length>>8)&0xff),
			byte(length&0xff),
		)
	default:
		dst = append(dst, mapMajorType|27)
		for shift := 56; shift >= 0; shift -= 8 {
			dst = append(dst, byte((length>>shift)&0xff))
		}
		return dst
	}
}

func validateConwayAuxiliaryDataScripts(raw []byte) error {
	var tagged cbor.RawTag
	if _, err := cbor.DecodeExact(raw, &tagged); err != nil {
		return err
	}
	if tagged.Number != cbor.CborTagMap {
		return fmt.Errorf("expected CBOR tag %d for Conway auxiliary data", cbor.CborTagMap)
	}
	var fields map[uint]cbor.RawMessage
	if _, err := cbor.DecodeExact(tagged.Content, &fields); err != nil {
		return err
	}
	for key := uint(1); key <= 5; key++ {
		rawScripts, ok := fields[key]
		if !ok {
			continue
		}
		if err := requireCBORMajorType(rawScripts, 4); err != nil {
			return fmt.Errorf("auxiliary-data script field %d: %w", key, err)
		}
		var scripts []cbor.RawMessage
		if _, err := cbor.DecodeExact(rawScripts, &scripts); err != nil {
			return fmt.Errorf("auxiliary-data script field %d: %w", key, err)
		}
		for index, script := range scripts {
			if key == 1 {
				if _, err := reencodeConwayNativeScript(script); err != nil {
					return fmt.Errorf("auxiliary-data native script %d: %w", index, err)
				}
				continue
			}
			if err := requireCBORMajorType(script, 2); err != nil {
				return fmt.Errorf("auxiliary-data Plutus script %d: %w", index, err)
			}
			var bytes []byte
			if _, err := cbor.DecodeExact(script, &bytes); err != nil {
				return fmt.Errorf("auxiliary-data Plutus script %d: %w", index, err)
			}
		}
	}
	return nil
}

func validateBabbageTransactionOutputMap(raw []byte) error {
	var fields map[uint]cbor.RawMessage
	if _, err := cbor.DecodeExact(raw, &fields); err != nil {
		return err
	}
	for _, key := range []uint{0, 1} {
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("babbage transaction output map is missing required field %d", key)
		}
	}
	return nil
}

func validateConwayCostModelLanguageIDs(models map[uint][]int64) error {
	for language, parameters := range models {
		if language > 255 {
			return fmt.Errorf("conway cost-model language id exceeds uint8: %d", language)
		}
		if parameters == nil {
			return fmt.Errorf("conway cost-model language %d parameters must be an array", language)
		}
	}
	return nil
}

func validateUnitIntervals(values ...cbor.Rat) error {
	for index := range values {
		if err := common.ValidateNonNegativeInterval(&values[index], true); err != nil {
			return fmt.Errorf("unit interval %d: %w", index, err)
		}
	}
	return nil
}

func reencodeConwayRedeemer(raw []byte) ([]byte, error) {
	if err := validateConwayRedeemerNode(raw); err != nil {
		return nil, err
	}
	var decoded alonzo.AlonzoRedeemer
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if decoded.Tag > common.RedeemerTagProposing {
		return nil, fmt.Errorf("unsupported Conway redeemer tag %d", decoded.Tag)
	}
	return cbor.Encode(&decoded)
}

func reencodeConwayRedeemers(raw []byte) ([]byte, error) {
	if err := validateConwayRedeemersCBOR(raw); err != nil {
		return nil, err
	}
	var decoded conway.ConwayRedeemers
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if err := common.ValidateRedeemerTagLimit(
		&decoded,
		common.RedeemerTagProposing,
	); err != nil {
		return nil, err
	}
	return storedEncoding(&decoded)
}

func validateConwayRedeemerNode(raw []byte) error {
	node, err := cbor.ParseDiagnostic(raw)
	if err != nil {
		return err
	}
	if node.Type != cbor.DiagTypeArray || len(node.Children) != 4 {
		return errors.New("conway redeemer must contain four array fields")
	}
	if node.Children[0].Type != cbor.DiagTypeUint {
		return errors.New("conway redeemer tag must be an unsigned integer")
	}
	return validateConwayExUnitsNode(&node.Children[3])
}

func validateConwayExUnitsNode(node *cbor.DiagnosticNode) error {
	if node.Type != cbor.DiagTypeArray || len(node.Children) != 2 ||
		node.Children[0].Type != cbor.DiagTypeUint ||
		node.Children[1].Type != cbor.DiagTypeUint {
		return errors.New("conway execution units must contain two unsigned integers")
	}
	return nil
}

func validateConwayRedeemersCBOR(raw []byte) error {
	node, err := cbor.ParseDiagnostic(raw)
	if err != nil {
		return err
	}
	switch node.Type {
	case cbor.DiagTypeArray:
		for index := range node.Children {
			redeemer := &node.Children[index]
			if redeemer.Type != cbor.DiagTypeArray ||
				len(redeemer.Children) != 4 ||
				redeemer.Children[0].Type != cbor.DiagTypeUint {
				return fmt.Errorf("conway redeemer %d tag must be an unsigned integer", index)
			}
			if err := validateConwayExUnitsNode(&redeemer.Children[3]); err != nil {
				return fmt.Errorf("conway redeemer %d: %w", index, err)
			}
		}
	case cbor.DiagTypeMap:
		if len(node.Children)%2 != 0 {
			return errors.New("conway redeemer map must contain key-value pairs")
		}
		for index := 0; index < len(node.Children); index += 2 {
			key := &node.Children[index]
			if key.Type != cbor.DiagTypeArray || len(key.Children) != 2 ||
				key.Children[0].Type != cbor.DiagTypeUint ||
				key.Children[1].Type != cbor.DiagTypeUint {
				return errors.New("conway redeemer map key must contain an unsigned tag and index")
			}
			value := &node.Children[index+1]
			if value.Type != cbor.DiagTypeArray || len(value.Children) != 2 {
				return errors.New("conway redeemer map value must contain data and execution units")
			}
			if err := validateConwayExUnitsNode(&value.Children[1]); err != nil {
				return fmt.Errorf("conway redeemer map value: %w", err)
			}
		}
	case cbor.DiagTypeUint,
		cbor.DiagTypeNint,
		cbor.DiagTypeBytes,
		cbor.DiagTypeText,
		cbor.DiagTypeTag,
		cbor.DiagTypeSimple,
		cbor.DiagTypeFloat:
		return errors.New("conway redeemers must be an array or map")
	}
	return nil
}

func reencodeTyped[T any](raw []byte) ([]byte, error) {
	var decoded T
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	return cbor.Encode(&decoded)
}

func reencodeChecked[T any](raw []byte, validate func(T) error) ([]byte, error) {
	var decoded T
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if err := validate(decoded); err != nil {
		return nil, err
	}
	return cbor.Encode(&decoded)
}

func reencodeBytes(raw []byte, min, max int) ([]byte, error) {
	return reencodeByteString(raw, min, max, true)
}

func reencodeByteString(raw []byte, min, max int, definite bool) ([]byte, error) {
	var err error
	if definite {
		err = requireCBORDefiniteMajorType(raw, 2)
	} else {
		err = requireCBORMajorType(raw, 2)
	}
	if err != nil {
		return nil, err
	}
	var decoded []byte
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if len(decoded) < min || len(decoded) > max {
		return nil, fmt.Errorf("byte string length %d is outside %d..%d", len(decoded), min, max)
	}
	return cbor.Encode(decoded)
}

func reencodeText(raw []byte, min, max int) ([]byte, error) {
	if err := requireCBORDefiniteMajorType(raw, 3); err != nil {
		return nil, err
	}
	var decoded string
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if len(decoded) < min || len(decoded) > max {
		return nil, fmt.Errorf("text string length %d is outside %d..%d", len(decoded), min, max)
	}
	return cbor.Encode(decoded)
}

func reencodeInterval(raw []byte, unit bool) ([]byte, error) {
	if err := common.ValidateNonNegativeBoundedRatCBOR(raw); err != nil {
		return nil, err
	}
	var decoded cbor.Rat
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	if err := common.ValidateNonNegativeInterval(&decoded, unit); err != nil {
		return nil, err
	}
	return cbor.Encode(&decoded)
}

func requireCBORMajorType(raw []byte, major byte) error {
	if len(raw) == 0 {
		return errors.New("empty CBOR value")
	}
	if raw[0]>>5 != major {
		return fmt.Errorf("expected CBOR major type %d, got %d", major, raw[0]>>5)
	}
	return nil
}

func requireCBORDefiniteMajorType(raw []byte, major byte) error {
	if err := requireCBORMajorType(raw, major); err != nil {
		return err
	}
	if major >= 2 && major <= 5 && raw[0]&0x1f == 31 {
		return errors.New("indefinite-length CBOR item is not valid for this rule")
	}
	return nil
}

func reencodeStored[T any](raw []byte) ([]byte, error) {
	var decoded T
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	return storedEncoding(&decoded)
}

func storedEncoding(value any) ([]byte, error) {
	stored, ok := value.(storedCBOR)
	if !ok {
		return nil, fmt.Errorf("%T does not preserve its decoded CBOR", value)
	}
	encoded := stored.Cbor()
	if len(encoded) == 0 {
		return nil, errors.New("decoder returned no stored CBOR")
	}
	return encoded, nil
}
