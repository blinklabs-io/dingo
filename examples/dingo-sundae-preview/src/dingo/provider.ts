import { Core } from "@blaze-cardano/sdk";
import { U5C } from "@utxorpc/blaze-provider";
import type { CardanoQueryClient } from "@utxorpc/sdk";

export function createDingoProvider(): U5C {
  const url = import.meta.env.VITE_UTXORPC_URL || window.location.origin;
  const provider = new U5C({
    url,
    network: Core.NetworkId.Testnet,
  });

  provider.networkName = "cardano-preview";
  installDingoAssetSearchCompatibility(provider);
  return provider;
}

export async function assertDingoReady(provider: U5C): Promise<string> {
  const params = await provider.getParameters();
  return `Protocol ${params.protocolVersion.major}.${params.protocolVersion.minor}, max tx ${params.maxTxSize} bytes`;
}

type QueryClientHost = {
  queryClient?: CardanoQueryClient;
};

type SearchByAsset = CardanoQueryClient["searchUtxosByAsset"];
type SearchByAddressWithAsset = CardanoQueryClient["searchUtxosByAddressWithAsset"];
type AssetPattern =
  | { kind: "coin" }
  | { kind: "native"; policyId: NonNullable<Parameters<SearchByAsset>[0]>; assetName?: Parameters<SearchByAsset>[1] };

const POLICY_ID_BYTES = 28;
const MAX_ASSET_NAME_BYTES = 32;

function installDingoAssetSearchCompatibility(provider: U5C): void {
  const queryClient = (provider as unknown as QueryClientHost).queryClient;
  if (!queryClient) {
    return;
  }

  const searchUtxosByAsset = queryClient.searchUtxosByAsset.bind(queryClient);
  const searchUtxosByAddress = queryClient.searchUtxosByAddress.bind(queryClient);
  const searchUtxosByAddressWithAsset = queryClient.searchUtxosByAddressWithAsset.bind(queryClient);

  queryClient.searchUtxosByAsset = ((policyId, name) => {
    const asset = normalizeAssetPattern(policyId, name);
    if (asset.kind === "coin") {
      throw new Error("Cannot search ADA as a native asset. Use address UTxO search for lovelace.");
    }
    return searchUtxosByAsset(asset.policyId, asset.assetName);
  }) as SearchByAsset;

  queryClient.searchUtxosByAddressWithAsset = ((address, policyId, name) => {
    const asset = normalizeAssetPattern(policyId, name);
    if (asset.kind === "coin") {
      return searchUtxosByAddress(address);
    }
    return searchUtxosByAddressWithAsset(address, asset.policyId, asset.assetName);
  }) as SearchByAddressWithAsset;
}

function normalizeAssetPattern(policyId?: Uint8Array<ArrayBuffer>, name?: Uint8Array<ArrayBuffer>): AssetPattern {
  if (policyId && policyId.length > 0) {
    if (policyId.length !== POLICY_ID_BYTES) {
      throw new Error(`Native asset policy ID must be ${POLICY_ID_BYTES} bytes.`);
    }
    if (name && name.length > MAX_ASSET_NAME_BYTES) {
      throw new Error(`Native asset name must be at most ${MAX_ASSET_NAME_BYTES} bytes.`);
    }
    return { kind: "native", policyId, assetName: name };
  }

  if (!name || name.length === 0) {
    return { kind: "coin" };
  }
  if (name.length < POLICY_ID_BYTES) {
    throw new Error(`Native asset ID must include a ${POLICY_ID_BYTES}-byte policy ID.`);
  }
  if (name.length > POLICY_ID_BYTES + MAX_ASSET_NAME_BYTES) {
    throw new Error(
      `Native asset ID must be at most ${POLICY_ID_BYTES + MAX_ASSET_NAME_BYTES} bytes.`,
    );
  }

  return {
    kind: "native",
    policyId: name.slice(0, POLICY_ID_BYTES),
    assetName: name.slice(POLICY_ID_BYTES),
  };
}
