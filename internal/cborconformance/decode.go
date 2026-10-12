// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"errors"
	"fmt"

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
	"auxiliary_data",
	"block",
	"certificate",
	"cost_models",
	"credential",
	"datum_option",
	"drep",
	"gov_action",
	"header",
	"header_body",
	"native_script",
	"plutus_data",
	"proposal_procedure",
	"protocol_param_update",
	"redeemer",
	"redeemers",
	"relay",
	"script",
	"transaction",
	"transaction_body",
	"transaction_input",
	"transaction_output",
	"transaction_witness_set",
	"value",
	"voting_procedure",
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
	case "block":
		block, err := ledger.NewBlockFromCbor(ledger.BlockTypeConway, raw)
		if err != nil {
			return nil, err
		}
		return cbor.Encode(block)
	case "certificate":
		return reencodeTyped[common.CertificateWrapper](raw)
	case "cost_models":
		return reencodeTyped[map[uint][]int64](raw)
	case "credential":
		return reencodeTyped[common.Credential](raw)
	case "datum_option":
		return reencodeTyped[babbage.BabbageTransactionOutputDatumOption](raw)
	case "drep":
		return reencodeTyped[common.Drep](raw)
	case "gov_action":
		return reencodeTyped[conway.ConwayGovAction](raw)
	case "header":
		header, err := ledger.NewBlockHeaderFromCbor(ledger.BlockTypeConway, raw)
		if err != nil {
			return nil, err
		}
		return storedEncoding(header)
	case "header_body":
		return reencodeTyped[babbage.BabbageBlockHeaderBody](raw)
	case "native_script":
		return reencodeConwayNativeScript(raw)
	case "plutus_data":
		return reencodeStored[common.Datum](raw)
	case "proposal_procedure":
		return reencodeTyped[conway.ConwayProposalProcedure](raw)
	case "protocol_param_update":
		return reencodeTyped[conway.ConwayProtocolParameterUpdate](raw)
	case "redeemer":
		return reencodeConwayRedeemer(raw)
	case "redeemers":
		return reencodeConwayRedeemers(raw)
	case "relay":
		return reencodeTyped[common.PoolRelay](raw)
	case "script":
		return reencodeTyped[cborValue](raw)
	case "transaction":
		tx, err := ledger.NewTransactionFromCbor(ledger.TxTypeConway, raw)
		if err != nil {
			return nil, err
		}
		return cbor.Encode(tx)
	case "transaction_body":
		return reencodeStored[conway.ConwayTransactionBody](raw)
	case "transaction_input":
		return reencodeTyped[shelley.ShelleyTransactionInput](raw)
	case "transaction_output":
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
	default:
		return nil, fmt.Errorf("unknown Conway rule %q", rule)
	}
}

func reencodeConwayNativeScript(raw []byte) ([]byte, error) {
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

func reencodeConwayRedeemer(raw []byte) ([]byte, error) {
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

func reencodeTyped[T any](raw []byte) ([]byte, error) {
	var decoded T
	if _, err := cbor.DecodeExact(raw, &decoded); err != nil {
		return nil, err
	}
	return cbor.Encode(&decoded)
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
