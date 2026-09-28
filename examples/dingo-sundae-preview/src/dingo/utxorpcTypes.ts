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

// DingoGenesis is the subset of the UTxO RPC Query.ReadGenesis Cardano config
// this example reads. Dingo answers it from the Shelley genesis it loaded.
export type DingoGenesis = {
  networkMagic: number;
  networkId: string;
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
  readGenesis(): Promise<DingoGenesis>;
  // Dingo matches an address search on the complete serialized address bytes.
  searchUtxosByAddress(address: UtxoRpcBytes): Promise<DingoUtxo[]>;
  // A delegation part is a 28-byte stake credential, not an address. Dingo
  // matches it across every address form that shares the credential.
  searchUtxosByDelegationPart(delegationPart: UtxoRpcBytes): Promise<DingoUtxo[]>;
  searchUtxosByAsset(policyId?: UtxoRpcBytes, name?: UtxoRpcBytes): Promise<DingoUtxo[]>;
  searchUtxosByAddressWithAsset(address: UtxoRpcBytes, policyId?: UtxoRpcBytes, name?: UtxoRpcBytes): Promise<DingoUtxo[]>;
};
