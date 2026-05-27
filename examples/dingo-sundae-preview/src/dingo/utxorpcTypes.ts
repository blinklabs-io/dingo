export type UtxoRpcBytes = Uint8Array<ArrayBuffer>;

export type DingoUtxo = {
  txoRef: {
    hash: Uint8Array;
    index: bigint | number | string;
  };
  parsedValued?: {
    assets?: Array<{
      policyId: Uint8Array;
      assets: Array<{
        name: Uint8Array;
      }>;
    }>;
  };
};

export type DingoQueryClient = {
  inner: {
    readData(request: { keys: UtxoRpcBytes[] }): Promise<{
      values: Array<{
        key: Uint8Array;
        nativeBytes: Uint8Array;
      }>;
    }>;
  };
  searchUtxosByAddress(address: UtxoRpcBytes): Promise<DingoUtxo[]>;
  searchUtxosByAsset(policyId?: UtxoRpcBytes, name?: UtxoRpcBytes): Promise<DingoUtxo[]>;
  searchUtxosByAddressWithAsset(address: UtxoRpcBytes, policyId?: UtxoRpcBytes, name?: UtxoRpcBytes): Promise<DingoUtxo[]>;
};
