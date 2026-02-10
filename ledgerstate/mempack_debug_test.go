package ledgerstate

import (
	"os"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
)

func TestDebugFirstTwoEntries(t *testing.T) {
	tvarPath := "/tmp/dingo-mithril-download/ancillary/" +
		"ledger/104173430/tables/tvar"
	if _, err := os.Stat(tvarPath); os.IsNotExist(err) {
		t.Skip("tvar file not available")
	}

	data, err := os.ReadFile(tvarPath)
	if err != nil {
		t.Fatal(err)
	}

	// Parse outer array header
	decoder, err := cbor.NewStreamDecoder(data)
	if err != nil {
		t.Fatal(err)
	}

	arrCount, _, _, err := decoder.DecodeArrayHeader()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Array count: %d", arrCount)

	mapStart := decoder.Position()
	mapData := data[mapStart:]
	t.Logf(
		"Map starts at %d, first byte: 0x%02x",
		mapStart, mapData[0],
	)

	if mapData[0] != 0xbf {
		t.Fatal("expected indefinite map")
	}

	// Use a new decoder on the map data (after 0xbf)
	innerDecoder, err := cbor.NewStreamDecoder(mapData[1:])
	if err != nil {
		t.Fatal(err)
	}

	for i := range 5 {
		pos := innerDecoder.Position()
		if 1+pos >= len(mapData) || mapData[1+pos] == 0xff {
			t.Log("break byte reached")
			break
		}

		// Decode key
		var keyDummy any
		_, keyRaw, err := innerDecoder.DecodeRaw(&keyDummy)
		if err != nil {
			t.Fatalf("key %d: %v", i, err)
		}

		// Extract TxIn
		var keyBytes []byte
		if _, err := cbor.Decode(keyRaw, &keyBytes); err != nil {
			t.Fatalf("decoding key %d: %v", i, err)
		}
		t.Logf("\n--- Entry %d ---", i)
		t.Logf(
			"TxIn key (%d raw, %d unwrapped bytes): %x",
			len(keyRaw), len(keyBytes), keyBytes,
		)

		// Decode value
		var valDummy any
		_, valRaw, err := innerDecoder.DecodeRaw(&valDummy)
		if err != nil {
			t.Fatalf("val %d: %v", i, err)
		}

		var valBytes []byte
		if _, err := cbor.Decode(valRaw, &valBytes); err != nil {
			t.Fatalf("decoding val %d: %v", i, err)
		}
		t.Logf(
			"TxOut val (%d raw, %d unwrapped bytes)",
			len(valRaw), len(valBytes),
		)

		if len(valBytes) < 50 {
			t.Logf("TxOut hex: %x", valBytes)
		} else {
			t.Logf("TxOut hex[:50]: %x", valBytes[:50])
			t.Logf("TxOut hex[50:]: %x", valBytes[50:])
		}

		// Use the corrected decoder
		if len(valBytes) > 0 {
			tag := valBytes[0]
			t.Logf("MemPack tag: %d", tag)

			decoded, err := decodeMempackTxOut(valBytes)
			if err != nil {
				t.Logf(
					"DECODE ERROR: %v", err,
				)
			} else {
				t.Logf(
					"Address (%d bytes): %x",
					len(decoded.Address),
					decoded.Address,
				)
				if len(decoded.Address) > 0 {
					t.Logf(
						"Address header: 0x%02x",
						decoded.Address[0],
					)
				}
				t.Logf(
					"Lovelace: %d (~%.2f ADA)",
					decoded.Lovelace,
					float64(decoded.Lovelace)/1_000_000,
				)
				t.Logf(
					"Assets: %d", len(decoded.Assets),
				)
				for j, a := range decoded.Assets {
					t.Logf(
						"  Asset[%d]: %x / %s = %d",
						j, a.PolicyId, a.Name,
						a.Amount,
					)
				}
				if decoded.DatumHash != nil {
					t.Logf(
						"DatumHash: %x",
						decoded.DatumHash,
					)
				}
				if decoded.Datum != nil {
					t.Logf(
						"Datum: %d bytes",
						len(decoded.Datum),
					)
				}
			}
		}
	}
}
