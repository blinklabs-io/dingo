import type { UtxoRpcBytes } from "./utxorpcTypes";

export function bytesToHex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function hexToBytes(hex: string): UtxoRpcBytes {
  if (hex.length % 2 !== 0) {
    throw new Error("Hex string must have an even number of characters.");
  }

  const bytes = new Uint8Array(hex.length / 2) as UtxoRpcBytes;
  for (let offset = 0; offset < hex.length; offset += 2) {
    const byte = Number.parseInt(hex.slice(offset, offset + 2), 16);
    if (Number.isNaN(byte)) {
      throw new Error("Hex string contains invalid characters.");
    }
    bytes[offset / 2] = byte;
  }
  return bytes;
}
