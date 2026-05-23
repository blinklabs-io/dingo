type Cip30Cbor = string;

type Cip30Paginate = {
  page: number;
  limit: number;
};

type Cip30CollateralParams = {
  amount: Cip30Cbor;
};

export interface Cip30WalletApi {
  getNetworkId(): Promise<number>;
  getUtxos(amount?: Cip30Cbor, paginate?: Cip30Paginate): Promise<string[] | null>;
  getBalance(): Promise<string>;
  getUsedAddresses(paginate?: Cip30Paginate): Promise<string[]>;
  getUnusedAddresses(): Promise<string[]>;
  getChangeAddress(): Promise<string>;
  getRewardAddresses(): Promise<string[]>;
  signTx(tx: string, partialSign: boolean): Promise<string>;
  signData(
    address: string,
    payload: string,
  ): Promise<{ signature: string; key: string }>;
  submitTx(tx: string): Promise<string>;
  getCollateral(params?: Cip30CollateralParams): Promise<string[] | null>;
}

export interface Cip30Wallet {
  name?: string;
  icon?: string;
  apiVersion?: string;
  enable(): Promise<Cip30WalletApi>;
  isEnabled?(): Promise<boolean>;
}

declare global {
  interface Window {
    cardano?: Record<string, Cip30Wallet | unknown>;
  }
}
