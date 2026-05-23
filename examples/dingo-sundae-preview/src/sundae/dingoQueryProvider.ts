import { Core } from "@blaze-cardano/sdk";
import { Buffer } from "buffer";
import {
  DatumBuilderV3,
  EContractVersion,
  type IPoolByAssetQuery,
  type IPoolByIdentQuery,
  type IPoolByPairQuery,
  type IPoolBySearchTermQuery,
  QueryProviderSundaeSwap,
  type IPoolData,
  type IPoolDataAsset,
  type ISundaeProtocolParams,
  type ISundaeProtocolParamsFull,
  type TUTXO,
} from "@sundaeswap/core";
import type { U5C } from "@utxorpc/blaze-provider";
import type { CardanoQueryClient, CardanoUtxo } from "@utxorpc/sdk";
import { assetIdFromTuple, metadataFor, unitFromAssetId, type AssetHint } from "./assets";
import { SUNDAE_V3_PROTOCOL } from "./protocol";

const POOL_NFT_NAME_PREFIX = "000de140";

export class DingoSundaeQueryProvider extends QueryProviderSundaeSwap {
  readonly protocol: ISundaeProtocolParamsFull;
  private readonly datumBuilder = new DatumBuilderV3("preview");
  private readonly assetHints = new Map<string, AssetHint>();

  constructor(private readonly provider: U5C) {
    super("preview");
    this.baseUrl = "dingo://utxorpc";
    this.protocol = SUNDAE_V3_PROTOCOL;
  }

  setAssetHint(assetId: string, hint: AssetHint): void {
    this.assetHints.set(assetId, hint);
  }

  async validateProtocolReferences(): Promise<string> {
    const references = this.protocol.references.map(
      ({ txIn }) =>
        new Core.TransactionInput(Core.TransactionId(txIn.hash), BigInt(txIn.index)),
    );
    const resolved = await this.provider.resolveUnspentOutputs(references);
    const missing = this.protocol.references.filter((reference) => {
      return !resolved.some((utxo) => {
        return (
          utxo.input().transactionId().toString() === reference.txIn.hash &&
          Number(utxo.input().index()) === reference.txIn.index
        );
      });
    });

    if (missing.length > 0) {
      throw new Error(
        `Sundae V3 reference UTxO(s) missing from Dingo: ${missing
          .map(({ key }) => key)
          .join(", ")}`,
      );
    }

    return `${resolved.length} Sundae V3 reference inputs resolved through Dingo`;
  }

  async getProtocolParamsWithScripts(version: undefined): Promise<ISundaeProtocolParamsFull[]>;
  async getProtocolParamsWithScripts(version: EContractVersion): Promise<ISundaeProtocolParamsFull>;
  async getProtocolParamsWithScripts(
    version?: EContractVersion,
  ): Promise<ISundaeProtocolParamsFull | ISundaeProtocolParamsFull[]> {
    this.assertSupportedVersion(version);
    return version ? this.protocol : [this.protocol];
  }

  async getProtocolParamsWithScriptHashes(version: undefined): Promise<ISundaeProtocolParams[]>;
  async getProtocolParamsWithScriptHashes(version: EContractVersion): Promise<ISundaeProtocolParams>;
  async getProtocolParamsWithScriptHashes(
    version?: EContractVersion,
  ): Promise<ISundaeProtocolParams | ISundaeProtocolParams[]> {
    this.assertSupportedVersion(version);
    const stripped: ISundaeProtocolParams = {
      ...this.protocol,
      blueprint: {
        validators: this.protocol.blueprint.validators.map(({ title, hash }) => ({
          title,
          hash,
        })),
      },
    };
    return version ? stripped : [stripped];
  }

  private assertSupportedVersion(version?: EContractVersion): void {
    if (version !== undefined && version !== EContractVersion.V3) {
      throw new Error(`Only Sundae V3 is configured for this example, got ${version}`);
    }
  }

  async findPoolData(identArgs: IPoolByIdentQuery): Promise<IPoolData>;
  async findPoolData(assetArgs: IPoolByAssetQuery): Promise<IPoolData[]>;
  async findPoolData(assetPairArgs: IPoolByPairQuery): Promise<IPoolData[]>;
  async findPoolData(searchArgs: IPoolBySearchTermQuery): Promise<IPoolData[]>;
  async findPoolData(
    args: IPoolByIdentQuery | IPoolByAssetQuery | IPoolByPairQuery | IPoolBySearchTermQuery,
  ): Promise<IPoolData | IPoolData[]> {
    if (!("ident" in args)) {
      throw new Error("This Dingo-only query provider supports pool lookup by V3 ident.");
    }
    return this.findPoolDataByIdent({ ident: args.ident });
  }

  async findPoolDataByIdent({ ident }: { ident: string }): Promise<IPoolData> {
    const poolPolicy = this.validatorHash("pool.mint");
    const poolUtxo = await this.findUtxoByAsset(
      poolPolicy,
      DatumBuilderV3.computePoolNftName(ident),
      `pool NFT ${ident}`,
    );
    return this.poolDataFromUtxo(ident, poolUtxo);
  }

  async discoverPools(limit = 100): Promise<IPoolData[]> {
    const poolPolicy = this.validatorHash("pool.mint");
    const queryClient = this.getQueryClient();
    const rpcUtxos = await queryClient.searchUtxosByAsset(this.hexBytes(poolPolicy));
    const candidates = new Map<string, Core.TransactionInput>();

    for (const rpcUtxo of rpcUtxos) {
      const ident = this.poolIdentFromRpcUtxo(rpcUtxo);
      if (!ident || candidates.has(ident)) {
        continue;
      }

      candidates.set(
        ident,
        this.inputFromRpcUtxo(rpcUtxo),
      );

      if (candidates.size >= limit) {
        break;
      }
    }

    const resolved = await this.provider.resolveUnspentOutputs([...candidates.values()]);
    const pools: IPoolData[] = [];

    for (const utxo of resolved) {
      const ident = this.poolIdentFromCoreUtxo(utxo);
      if (!ident) {
        continue;
      }

      try {
        pools.push(await this.poolDataFromUtxo(ident, utxo));
      } catch (error) {
        if (!isNonPoolDecodeError(error)) {
          throw error;
        }
        // Ignore non-pool outputs under the same mint policy.
      }
    }

    return pools.sort((a, b) => compareBigints(b.liquidity.aReserve, a.liquidity.aReserve));
  }

  private async findUtxoByAsset(
    policyId: string,
    assetName: string,
    label: string,
  ): Promise<Core.TransactionUnspentOutput> {
    const queryClient = this.getQueryClient();
    const [rpcUtxo] = await queryClient.searchUtxosByAsset(
      this.hexBytes(policyId),
      this.hexBytes(assetName),
    );

    if (!rpcUtxo) {
      throw new Error(`${label} was not found in Dingo.`);
    }

    const [resolved] = await this.provider.resolveUnspentOutputs([this.inputFromRpcUtxo(rpcUtxo)]);
    if (!resolved) {
      throw new Error(`${label} was found by Dingo but could not be resolved.`);
    }
    return resolved;
  }

  private async poolDataFromUtxo(
    ident: string,
    poolUtxo: Core.TransactionUnspentOutput,
  ): Promise<IPoolData> {
    const poolPolicy = this.validatorHash("pool.mint");
    const datum = this.datumBuilder.decodeDatum(await this.poolDatum(poolUtxo, ident));
    if (datum.identifier !== ident) {
      throw new Error(`Pool datum identifier ${datum.identifier} did not match requested ${ident}.`);
    }

    const [assetATuple, assetBTuple] = datum.assets as [[string, string], [string, string]];
    const assetA = metadataFor(assetIdFromTuple(assetATuple), this.assetHints.get(assetIdFromTuple(assetATuple)));
    const assetB = metadataFor(assetIdFromTuple(assetBTuple), this.assetHints.get(assetIdFromTuple(assetBTuple)));

    return {
      ident,
      version: EContractVersion.V3,
      assetA,
      assetB,
      assetLP: {
        assetId: `${poolPolicy}.${DatumBuilderV3.computePoolLqName(ident)}`,
        decimals: 0,
      },
      currentFee: Number(datum.askFeesPer_10Thousand) / 10_000,
      protocolFee: 0,
      linearAmplificationFactor: 0n,
      liquidity: {
        aReserve: this.amountInOutput(poolUtxo, assetA),
        bReserve: this.amountInOutput(poolUtxo, assetB),
        lpTotal: datum.circulatingLp,
      },
    };
  }

  private async poolDatum(
    poolUtxo: Core.TransactionUnspentOutput,
    ident: string,
  ): Promise<Core.PlutusData> {
    const datum = poolUtxo.output().datum();
    const inlineDatum = datum?.asInlineData();
    if (inlineDatum) {
      return inlineDatum;
    }

    const datumHash = datum?.asDataHash()?.toString();
    if (!datumHash) {
      throw new Error(`Pool ${ident} was found but has no datum.`);
    }

    const response = await this.getQueryClient().inner.readData({
      keys: [this.hexBytes(datumHash)],
    });
    const resolved = response.values.find((value) => {
      return Buffer.from(value.key).toString("hex") === datumHash;
    });
    if (!resolved || resolved.nativeBytes.length === 0) {
      throw new Error(`Pool ${ident} datum ${datumHash} was not found in Dingo.`);
    }

    return Core.PlutusData.fromCbor(Core.HexBlob.fromBytes(resolved.nativeBytes));
  }

  async findOpenOrderDatum(utxoRef: TUTXO): Promise<{ datum: string; datumHash: string }> {
    const [utxo] = await this.provider.resolveUnspentOutputs([
      new Core.TransactionInput(Core.TransactionId(utxoRef.hash), BigInt(utxoRef.index)),
    ]);
    if (!utxo) {
      throw new Error(`Open order UTxO ${utxoRef.hash}#${utxoRef.index} was not found in Dingo.`);
    }

    const datum = utxo.output().datum();
    const inlineDatum = datum?.asInlineData();
    const datumHash = datum?.asDataHash()?.toString() ?? inlineDatum?.hash().toString();
    if (!inlineDatum || !datumHash) {
      throw new Error(`Open order UTxO ${utxoRef.hash}#${utxoRef.index} has no inline datum.`);
    }

    return {
      datum: inlineDatum.toCbor(),
      datumHash,
    };
  }

  private validatorHash(title: string): string {
    const validator = this.protocol.blueprint.validators.find((item) => item.title === title);
    if (!validator) {
      throw new Error(`Sundae V3 validator ${title} is not configured.`);
    }
    return validator.hash;
  }

  private amountInOutput(utxo: Core.TransactionUnspentOutput, asset: IPoolDataAsset): bigint {
    const value = utxo.output().amount().toCore();
    if (asset.assetId === "ada.lovelace") {
      return value.coins;
    }

    const unit = unitFromAssetId(asset.assetId);
    for (const [assetId, amount] of value.assets ?? new Map()) {
      if (assetId.toString() === unit) {
        return amount;
      }
    }
    return 0n;
  }

  private getQueryClient(): CardanoQueryClient {
    const providerWithClient = this.provider as unknown as { queryClient?: CardanoQueryClient };
    if (!providerWithClient.queryClient) {
      throw new Error("Blaze U5C query client is not available for pool discovery.");
    }
    return providerWithClient.queryClient;
  }

  private inputFromRpcUtxo(utxo: CardanoUtxo): Core.TransactionInput {
    return new Core.TransactionInput(
      Core.TransactionId(Buffer.from(utxo.txoRef.hash).toString("hex")),
      BigInt(utxo.txoRef.index),
    );
  }

  private poolIdentFromRpcUtxo(utxo: CardanoUtxo): string | undefined {
    const poolPolicy = this.validatorHash("pool.mint");
    for (const multiAsset of utxo.parsedValued?.assets ?? []) {
      if (Buffer.from(multiAsset.policyId).toString("hex") !== poolPolicy) {
        continue;
      }

      for (const asset of multiAsset.assets) {
        const name = Buffer.from(asset.name).toString("hex");
        const ident = identFromPoolNftName(name);
        if (ident) {
          return ident;
        }
      }
    }
    return undefined;
  }

  private poolIdentFromCoreUtxo(utxo: Core.TransactionUnspentOutput): string | undefined {
    const poolPolicy = this.validatorHash("pool.mint");
    const prefix = `${poolPolicy}${POOL_NFT_NAME_PREFIX}`;
    const assets = utxo.output().amount().toCore().assets ?? new Map();
    for (const assetId of assets.keys()) {
      const unit = assetId.toString();
      if (unit.startsWith(prefix) && unit.length === prefix.length + 56) {
        return unit.slice(prefix.length);
      }
    }
    return undefined;
  }

  private hexBytes(hex: string): Uint8Array<ArrayBuffer> {
    return new Uint8Array(Buffer.from(hex, "hex")) as Uint8Array<ArrayBuffer>;
  }
}

function identFromPoolNftName(assetName: string): string | undefined {
  if (
    assetName.startsWith(POOL_NFT_NAME_PREFIX) &&
    assetName.length === POOL_NFT_NAME_PREFIX.length + 56
  ) {
    return assetName.slice(POOL_NFT_NAME_PREFIX.length);
  }
  return undefined;
}

function compareBigints(a: bigint, b: bigint): number {
  if (a > b) {
    return 1;
  }
  if (a < b) {
    return -1;
  }
  return 0;
}

function isNonPoolDecodeError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return (
    /^Invalid (type|literal|map|tuple|constr) at /.test(message) ||
    /^Pool datum identifier .+ did not match requested /.test(message) ||
    /^Pool .+ was found but has no datum\.$/.test(message)
  );
}
