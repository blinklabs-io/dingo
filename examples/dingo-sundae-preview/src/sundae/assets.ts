import { ADA_METADATA, type IPoolDataAsset } from "@sundaeswap/core";

export type AssetHint = {
  label?: string;
  decimals?: number;
};

export function assetIdFromTuple(tuple: [string, string]): string {
  const [policyId, assetName] = tuple;
  if (policyId === "" && assetName === "") {
    return ADA_METADATA.assetId;
  }
  return `${policyId}.${assetName}`;
}

export function unitFromAssetId(assetId: string): string {
  return assetId === ADA_METADATA.assetId ? "lovelace" : assetId.replace(".", "");
}

export function metadataFor(assetId: string, hint?: AssetHint): IPoolDataAsset {
  if (assetId === ADA_METADATA.assetId) {
    return ADA_METADATA;
  }

  return {
    assetId,
    decimals: hint?.decimals ?? 0,
    ticker: hint?.label ?? printableAssetName(assetId),
  } as IPoolDataAsset;
}

export function formatAssetAmount(amount: bigint, decimals = 0): string {
  const sign = amount < 0n ? "-" : "";
  const absAmount = amount < 0n ? -amount : amount;

  if (decimals === 0) {
    return `${sign}${absAmount}`;
  }

  const divisor = 10n ** BigInt(decimals);
  const whole = absAmount / divisor;
  const fraction = (absAmount % divisor).toString().padStart(decimals, "0");
  return `${sign}${whole}.${fraction.replace(/0+$/, "") || "0"}`;
}

export function parseAdaToLovelace(value: string): bigint {
  return parseAssetAmount(value, 6, "ADA");
}

export function parseAssetAmount(value: string, decimals: number, label = "asset"): bigint {
  const trimmed = value.trim();
  if (decimals < 0 || !Number.isInteger(decimals)) {
    throw new Error(`Invalid decimal count for ${label}.`);
  }

  if (decimals === 0) {
    if (!/^\d+$/.test(trimmed)) {
      throw new Error(`Enter a whole-number ${label} amount.`);
    }
    return BigInt(trimmed);
  }

  const pattern = new RegExp(`^\\d+(\\.\\d{0,${decimals}})?$`);
  if (!pattern.test(trimmed)) {
    const unit = decimals === 1 ? "decimal place" : "decimal places";
    throw new Error(`Enter a ${label} amount with up to ${decimals} ${unit}.`);
  }

  const [whole, fraction = ""] = trimmed.split(".");
  return BigInt(whole) * 10n ** BigInt(decimals) + BigInt(fraction.padEnd(decimals, "0"));
}

function printableAssetName(assetId: string): string | undefined {
  const assetName = assetId.includes(".") ? assetId.split(".")[1] : assetId.slice(56);
  if (!assetName || assetName.length % 2 !== 0 || !/^[0-9a-f]+$/i.test(assetName)) {
    return undefined;
  }

  const bytes = assetName.match(/.{2}/g)?.map((part) => Number.parseInt(part, 16)) ?? [];
  if (bytes.length === 0 || bytes.some((byte) => byte < 0x20 || byte > 0x7e)) {
    return undefined;
  }

  return String.fromCharCode(...bytes);
}
