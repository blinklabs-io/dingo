import { Blaze, Core, type Wallet } from "@blaze-cardano/sdk";
import { AssetAmount } from "@sundaeswap/asset";
import {
  ADA_METADATA,
  EDatumType,
  ESwapType,
  TxBuilderV3,
  type IPoolData,
  type IPoolDataAsset,
} from "@sundaeswap/core";
import type { U5C } from "@utxorpc/blaze-provider";
import { parseAssetAmount } from "./assets";
import type { DingoSundaeQueryProvider } from "./dingoQueryProvider";

export type SwapDirection = "adaToToken" | "tokenToAda";

export type BuildSwapArgs = {
  blaze: Blaze<U5C, Wallet>;
  queryProvider: DingoSundaeQueryProvider;
  pool: IPoolData;
  amount: string;
  direction: SwapDirection;
  slippagePercent: string;
};

export type BuiltSwap = {
  unsignedCbor: string;
  signedCbor?: string;
  txFee: bigint;
  deposit: bigint;
  scooperFee: bigint;
  signAndSubmit(): Promise<string>;
};

export async function buildSwapOrder({
  blaze,
  queryProvider,
  pool,
  amount,
  direction,
  slippagePercent,
}: BuildSwapArgs): Promise<BuiltSwap> {
  const changeAddress = (await blaze.wallet.getChangeAddress()).toBech32();
  const suppliedAsset = assetForDirection(pool, direction);
  const suppliedAmount = parseAssetAmount(amount, suppliedAsset.decimals ?? 0, labelForAsset(suppliedAsset));
  if (suppliedAmount <= 0n) {
    throw new Error(`Enter a positive ${labelForAsset(suppliedAsset)} amount.`);
  }

  const slippage = Number(slippagePercent) / 100;
  if (!Number.isFinite(slippage) || slippage < 0 || slippage > 0.5) {
    throw new Error("Slippage must be between 0 and 50 percent.");
  }

  const builder = new TxBuilderV3(blaze, queryProvider);
  const composed = await builder.swap({
    pool,
    suppliedAsset: new AssetAmount(suppliedAmount, suppliedAsset),
    swapType: {
      type: ESwapType.MARKET,
      slippage,
    },
    ownerAddress: changeAddress,
    orderAddresses: {
      DestinationAddress: {
        address: changeAddress,
        datum: {
          type: EDatumType.NONE,
        },
      },
    },
  });

  const built = await composed.build();
  const txFee = BigInt(built.builtTx.body().fee()?.toString() ?? "0");

  return {
    unsignedCbor: built.cbor,
    txFee,
    deposit: composed.fees.deposit.amount,
    scooperFee: composed.fees.scooperFee.amount,
    async signAndSubmit() {
      const signed = await built.sign();
      const tx = Core.Transaction.fromCbor(Core.TxCBOR(signed.cbor));
      const txId = await blaze.provider.postTransactionToChain(tx);
      return txId.toString();
    },
  };
}

function assetForDirection(pool: IPoolData, direction: SwapDirection): IPoolDataAsset {
  if (!poolHasAda(pool)) {
    throw new Error("The selected pool is not an ADA pair.");
  }

  if (direction === "adaToToken") {
    return ADA_METADATA;
  }

  return pool.assetA.assetId === ADA_METADATA.assetId ? pool.assetB : pool.assetA;
}

function poolHasAda(pool: IPoolData): boolean {
  return pool.assetA.assetId === ADA_METADATA.assetId || pool.assetB.assetId === ADA_METADATA.assetId;
}

function labelForAsset(asset: IPoolDataAsset): string {
  if (asset.assetId === ADA_METADATA.assetId) {
    return "ADA";
  }
  return "token";
}
