import { EContractVersion, type ISundaeProtocolParamsFull } from "@sundaeswap/core";

export const SUNDAE_V3_PROTOCOL: ISundaeProtocolParamsFull = {
  version: EContractVersion.V3,
  blueprint: {
    validators: [
      {
        title: "order.spend",
        hash: "cfad1914b599d18bffd14d2bbd696019c2899cbdd6a03325cdf680bc",
        compiledCode: "",
      },
      {
        title: "pool.spend",
        hash: "44a1eb2d9f58add4eb1932bd0048e6a1947e85e3fe4f32956a110414",
        compiledCode: "",
      },
      {
        title: "pool.mint",
        hash: "44a1eb2d9f58add4eb1932bd0048e6a1947e85e3fe4f32956a110414",
        compiledCode: "",
      },
      {
        title: "pool_stake.stake",
        hash: "cc27980a8557fe9db2c9ac0a2677f4d1306dbf10689983758f0b8dbe",
        compiledCode: "",
      },
      {
        title: "settings.spend",
        hash: "85ed0c7060ccd4700927d8b60f0160abe2b3c30446fc0a9ac83b6b76",
        compiledCode: "",
      },
      {
        title: "settings.mint",
        hash: "85ed0c7060ccd4700927d8b60f0160abe2b3c30446fc0a9ac83b6b76",
        compiledCode: "",
      },
      {
        title: "stake.stake",
        hash: "ca3073f5df02c60404a1d5a0a4416f0d0288dce99ea887a5ebec18a7",
        compiledCode: "",
      },
    ],
  },
  references: [
    {
      key: "order.spend",
      txIn: {
        hash: "92ec2274938de291d3837b7facf9eddfaed57cd6ff97e26af57cb7a9978e3887",
        index: 0,
      },
    },
    {
      key: "pool.spend",
      txIn: {
        hash: "8036a88a61427262aba964a42d0b9924739ffc3214de9a07c54b5a09af7f0d7d",
        index: 0,
      },
    },
  ],
};

export type PoolPreset = {
  ident: string;
  label: string;
  assetBLabel: string;
  assetBAssetId: string;
  assetBDecimals: number;
};

export const POOL_PRESETS: PoolPreset[] = [
  {
    ident: "35a34996f515c5a28c8df9eada81f03f4f2756d92e7f73cde1f4e593",
    label: "ADA / USDM",
    assetBLabel: "USDM",
    assetBAssetId: "d8906ca5c7ba124a0407a32dab37b2c82b13b3dcd9111e42940dcea4.0014df105553444d",
    assetBDecimals: 6,
  },
  {
    ident: "377b4c93d252c2b5ec8d38a44368d840071f4f95762d1cf567fe90d4",
    label: "ADA / USDR",
    assetBLabel: "USDR",
    assetBAssetId: "45df5f274b8950b512b08d10656864958659c4ecf3ffad092ef63024.55534472",
    assetBDecimals: 6,
  },
  {
    ident: "2e74e6af9739616dd021f547bca1f68c937b566bb6ca2e4782e76001",
    label: "ADA / TINDY",
    assetBLabel: "TINDY",
    assetBAssetId: "fa3eff2047fdf9293c5feef4dc85ce58097ea1c6da4845a351535183.74494e4459",
    assetBDecimals: 0,
  },
  {
    ident: "bd9437d9ec1a559d053f05dc38d337c32d1d0746952ea961f874c39a",
    label: "ADA / RBERRY",
    assetBLabel: "RBERRY",
    assetBAssetId: "99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e15.524245525259",
    assetBDecimals: 0,
  },
  {
    ident: "803b3add42ec1c5301cc1c684e61bfc5eb374df32dd995a298858b8d",
    label: "ADA / KOIOS",
    assetBLabel: "KOIOS",
    assetBAssetId: "68dabebfd9b41b53a0ce2bb9c702b5d197f2d5bfbd965fddd89741f3.0014df104b6f696f73",
    assetBDecimals: 6,
  },
  {
    ident: "32c43f096fa05626da1ead9383793ccd7bba6a1b259e77597766aee8",
    label: "ADA / MyUSD",
    assetBLabel: "MyUSD",
    assetBAssetId: "91d4f382273f442f15e9da48cb23349ba275f8818e4c7ac5d1004a16.4d79555344",
    assetBDecimals: 6,
  },
  {
    ident: "bbcbc14e34d6f81f9cf6dc130c5a933f0165453cf6ee3b9165f8560d",
    label: "ADA / USDC",
    assetBLabel: "USDC",
    assetBAssetId: "99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e15.55534443",
    assetBDecimals: 0,
  },
  {
    ident: "64367985d26aad53b4e03fbe3052a48c365f6df7fe1a9d4876a94c84",
    label: "ADA / LQ",
    assetBLabel: "LQ",
    assetBAssetId: "63f9a5fc96d4f87026e97af4569975016b50eef092a46859b61898e5.0014df104c51",
    assetBDecimals: 0,
  },
];

export const DEFAULT_POOL = POOL_PRESETS[0];
