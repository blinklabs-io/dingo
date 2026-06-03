import "./styles.css";
import { Blaze, Core, WebWallet, type Wallet } from "@blaze-cardano/sdk";
import { ADA_METADATA, type IPoolData, type IPoolDataAsset } from "@sundaeswap/core";
import type { U5C } from "@utxorpc/blaze-provider";
import { createDingoProvider, assertDingoReady } from "./dingo/provider";
import { formatAssetAmount, metadataFor, parseAssetAmount, unitFromAssetId } from "./sundae/assets";
import { DingoSundaeQueryProvider } from "./sundae/dingoQueryProvider";
import { DEFAULT_POOL, POOL_PRESETS, type PoolPreset } from "./sundae/protocol";
import { buildSwapOrder, type SwapDirection } from "./sundae/swap";
import type { Cip30Wallet, Cip30WalletApi } from "./types/cip30";

const originalFetch = window.fetch.bind(window);
window.fetch = (input, init) => {
  const target = new URL(
    typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
    window.location.href,
  );
  if (target.origin !== window.location.origin) {
    throw new Error(`Blocked non-Dingo external request: ${target.href}`);
  }
  return originalFetch(input, init);
};

const DEFAULT_ADA_SWAP_RESERVE_LOVELACE = 7_000_000n;
const ADA_SWAP_RESERVE_LOVELACE = parseAdaSwapReserve();

type AppState = {
  provider: U5C;
  queryProvider: DingoSundaeQueryProvider;
  blaze?: Blaze<U5C, Wallet>;
  webWallet?: WebWallet;
  walletBalance?: Core.Value;
  walletApi?: Cip30WalletApi;
  poolOptions: PoolOption[];
  pool?: IPoolData;
  swapDirection: SwapDirection;
};

type PoolOption = PoolPreset & {
  source: "preset" | "dingo";
  pool?: IPoolData;
  priceLabel?: string;
  inversePriceLabel?: string;
};

const state: AppState = {
  provider: createDingoProvider(),
  queryProvider: undefined as unknown as DingoSundaeQueryProvider,
  poolOptions: POOL_PRESETS.map((preset) => ({ ...preset, source: "preset" })),
  swapDirection: "adaToToken",
};
state.queryProvider = new DingoSundaeQueryProvider(state.provider);

let poolLoadRequestId = 0;

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("Missing app root.");
}

document.title = "DingoSwap";

app.innerHTML = `
  <section class="shell">
    <header class="topbar">
      <div>
        <h1>DingoSwap</h1>
        <p>SundaeSwap V3 order construction using Dingo UTxO RPC only.</p>
      </div>
      <div class="header-actions">
        <div
          class="status ready network-status"
          id="networkStatus"
          title="Dingo provider network: cardano-preview"
        >
          Network: Preview
        </div>
        <div class="status" id="dingoStatus">Dingo: connecting</div>
        <details class="wallet-dropdown" id="walletDetails">
          <summary id="walletSummary">Connect</summary>
          <div class="wallet-menu-panel">
            <label>
              Wallet
              <select id="walletSelect" aria-label="Wallet"></select>
            </label>
            <button id="connectWallet">Connect Wallet</button>
            <dl class="facts wallet-facts">
              <dt>Network</dt><dd id="walletNetwork">Not connected</dd>
              <dt>Change address</dt><dd id="changeAddress">-</dd>
              <dt>Balance</dt><dd id="walletBalance">-</dd>
            </dl>
          </div>
        </details>
      </div>
    </header>

    <section class="trade-layout">
      <article class="panel chart-panel">
        <div class="panel-heading">
          <h2>Price Chart</h2>
          <div class="chart-metric" id="chartMetric">-</div>
        </div>
        <canvas id="priceChart" aria-label="Pool price chart"></canvas>
      </article>

      <article class="panel trade-panel">
        <section class="trade-section action-panel">
          <div class="panel-heading">
            <h2>Swap Order</h2>
            <div class="swap-tools">
              <label class="slippage-select">
                <span>Slippage</span>
                <select id="slippage" aria-label="Slippage percentage">
                  <option value="0.1">0.1%</option>
                  <option value="0.5">0.5%</option>
                  <option value="1" selected>1%</option>
                  <option value="2">2%</option>
                  <option value="5">5%</option>
                </select>
              </label>
              <button
                id="loadPool"
                class="secondary-button icon-button"
                type="button"
                aria-label="Refresh pool"
                title="Refresh pool"
              >
                <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                  <path d="M21 12a9 9 0 0 1-15 6.7" />
                  <path d="M3 12a9 9 0 0 1 15-6.7" />
                  <path d="M18 2v4h-4" />
                  <path d="M6 22v-4h4" />
                </svg>
              </button>
              <button
                id="reverseSwap"
                class="secondary-button icon-button"
                type="button"
                aria-label="Reverse swap direction"
                title="Reverse swap direction"
              >
                <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                  <path d="M7 7h13" />
                  <path d="m16 3 4 4-4 4" />
                  <path d="M17 17H4" />
                  <path d="m8 13-4 4 4 4" />
                </svg>
              </button>
            </div>
          </div>
          <select id="poolSelect" class="pair-select" aria-label="Pool and swap direction"></select>
          <div class="stacked-inputs">
            <div class="amount-field">
              <label>
                <span id="offeredLabel">ADA offered</span>
                <input id="swapAmount" value="5" inputmode="decimal" />
              </label>
              <div class="amount-tools">
                <span id="offeredBalance">Available -</span>
                <div class="amount-presets" aria-label="Set offered amount from wallet balance">
                  <button type="button" class="amount-chip" data-amount-preset="25" disabled>25%</button>
                  <button type="button" class="amount-chip" data-amount-preset="50" disabled>50%</button>
                  <button type="button" class="amount-chip" data-amount-preset="100" disabled>Max</button>
                </div>
              </div>
            </div>
          </div>
          <button id="swapTx" class="swap-button" type="button">Swap</button>
          <dl class="facts compact-facts">
            <dt>Receive</dt><dd id="estimateReceive">-</dd>
            <dt>Impact</dt><dd id="priceImpact">-</dd>
            <dt>LP fee</dt><dd id="poolFee">-</dd>
            <dt>Tx fee</dt><dd id="txFee">-</dd>
            <dt>Deposit</dt><dd id="orderDeposit">-</dd>
            <dt>Scooper</dt><dd id="scooperFee">-</dd>
            <dt>Tx hash</dt><dd id="txHash">-</dd>
          </dl>
        </section>
      </article>
    </section>

    <section class="log-panel">
      <h2>Log</h2>
      <pre id="log"></pre>
    </section>
  </section>
`;

const els = {
  dingoStatus: byId("dingoStatus"),
  walletDetails: byId<HTMLDetailsElement>("walletDetails"),
  walletSummary: byId<HTMLElement>("walletSummary"),
  walletSelect: byId<HTMLSelectElement>("walletSelect"),
  connectWallet: byId<HTMLButtonElement>("connectWallet"),
  walletNetwork: byId("walletNetwork"),
  changeAddress: byId("changeAddress"),
  walletBalance: byId("walletBalance"),
  poolSelect: byId<HTMLSelectElement>("poolSelect"),
  loadPool: byId<HTMLButtonElement>("loadPool"),
  poolFee: byId("poolFee"),
  reverseSwap: byId<HTMLButtonElement>("reverseSwap"),
  offeredLabel: byId("offeredLabel"),
  offeredBalance: byId("offeredBalance"),
  swapAmount: byId<HTMLInputElement>("swapAmount"),
  amountPresets: Array.from(document.querySelectorAll<HTMLButtonElement>("[data-amount-preset]")),
  slippage: byId<HTMLSelectElement>("slippage"),
  swapTx: byId<HTMLButtonElement>("swapTx"),
  txFee: byId("txFee"),
  estimateReceive: byId("estimateReceive"),
  priceImpact: byId("priceImpact"),
  orderDeposit: byId("orderDeposit"),
  scooperFee: byId("scooperFee"),
  txHash: byId("txHash"),
  chartMetric: byId("chartMetric"),
  priceChart: byId<HTMLCanvasElement>("priceChart"),
  log: byId<HTMLPreElement>("log"),
};

seedKnownAssetHints();
renderWallets();
renderPools();
renderSwapControls();
renderPriceChart();
void initializeDingo();

els.connectWallet.addEventListener("click", () => void connectWallet());
document.addEventListener("click", (event) => {
  if (els.walletDetails.open && !els.walletDetails.contains(event.target as Node)) {
    els.walletDetails.open = false;
  }
});
els.poolSelect.addEventListener("change", () => {
  state.pool = undefined;
  resetTransactionFields();
  renderSwapControls();
  renderPriceChart();
  void loadPool();
});
els.loadPool.addEventListener("click", () => void loadPool());
els.reverseSwap.addEventListener("click", () => {
  state.swapDirection = state.swapDirection === "adaToToken" ? "tokenToAda" : "adaToToken";
  resetTransactionFields();
  renderPools();
  renderSwapControls();
  renderPriceChart();
});
els.swapAmount.addEventListener("input", () => {
  resetTransactionFields();
  renderSwapControls();
  renderPriceChart();
});
els.slippage.addEventListener("change", () => resetTransactionFields());
for (const button of els.amountPresets) {
  button.addEventListener("click", () => {
    setSwapAmountFromBalance(Number(button.dataset.amountPreset ?? "0"));
  });
}
els.swapTx.addEventListener("click", () => void executeSwap());
window.addEventListener("resize", () => renderPriceChart());

function byId<T extends HTMLElement = HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) {
    throw new Error(`Missing element #${id}`);
  }
  return element as T;
}

function log(message: string): void {
  const stamp = new Date().toLocaleTimeString();
  els.log.textContent = `[${stamp}] ${message}\n${els.log.textContent ?? ""}`;
}

function renderWallets(): void {
  const wallets = getWallets();
  els.walletSelect.innerHTML = "";
  for (const [key, wallet] of wallets) {
    const option = document.createElement("option");
    option.value = key;
    option.textContent = wallet.name ?? key;
    els.walletSelect.append(option);
  }
  if (wallets.length === 0) {
    const option = document.createElement("option");
    option.textContent = "No CIP-30 wallet found";
    els.walletSelect.append(option);
    els.connectWallet.disabled = true;
    els.walletSummary.textContent = "Connect";
  }
}

function renderPools(): void {
  const selected = els.poolSelect.value || DEFAULT_POOL.ident;
  els.poolSelect.innerHTML = "";
  for (const preset of state.poolOptions) {
    const option = document.createElement("option");
    option.value = preset.ident;
    option.textContent = pairLabelForOption(preset);
    option.title = poolOptionTitle(preset);
    els.poolSelect.append(option);
  }
  els.poolSelect.value = state.poolOptions.some((option) => option.ident === selected)
    ? selected
    : (state.poolOptions[0]?.ident ?? DEFAULT_POOL.ident);
}

function selectedPoolOption(): PoolOption {
  return (
    state.poolOptions.find((preset) => preset.ident === els.poolSelect.value) ??
    state.poolOptions[0] ?? { ...DEFAULT_POOL, source: "preset" }
  );
}

function pairLabelForOption(option: PoolOption): string {
  const [adaLabel, tokenLabel] = pairLabelsForOption(option);
  return state.swapDirection === "adaToToken"
    ? `${adaLabel} / ${tokenLabel}`
    : `${tokenLabel} / ${adaLabel}`;
}

function poolOptionTitle(option: PoolOption): string {
  if (state.swapDirection === "tokenToAda") {
    return option.inversePriceLabel ?? option.priceLabel ?? option.label;
  }
  return option.priceLabel ?? option.inversePriceLabel ?? option.label;
}

function pairLabelsForOption(option: PoolOption): [string, string] {
  if (!option.pool) {
    return ["ADA", option.assetBLabel];
  }

  if (option.pool.assetA.assetId === ADA_METADATA.assetId) {
    return ["ADA", labelFor(option.pool.assetB)];
  }
  if (option.pool.assetB.assetId === ADA_METADATA.assetId) {
    return ["ADA", labelFor(option.pool.assetA)];
  }
  return [labelFor(option.pool.assetA), labelFor(option.pool.assetB)];
}

function getWallets(): Array<[string, Cip30Wallet]> {
  return Object.entries(window.cardano ?? {}).filter(
    (entry): entry is [string, Cip30Wallet] => {
      return typeof (entry[1] as Cip30Wallet).enable === "function";
    },
  );
}

async function initializeDingo(): Promise<void> {
  try {
    const status = await assertDingoReady(state.provider);
    const references = await state.queryProvider.validateProtocolReferences();
    els.dingoStatus.textContent = `Dingo: ready`;
    els.dingoStatus.classList.add("ready");
    log(`${status}; ${references}`);
    await hydratePoolsFromDingo();
    await loadPool();
  } catch (error) {
    els.dingoStatus.textContent = "Dingo: unavailable";
    els.dingoStatus.classList.add("error");
    log(errorMessage(error));
  }
}

async function connectWallet(): Promise<void> {
  try {
    const wallet = getWallets().find(([key]) => key === els.walletSelect.value)?.[1];
    if (!wallet) {
      throw new Error("Select a CIP-30 wallet.");
    }

    state.walletApi = await wallet.enable();
    const webWallet = new WebWallet(
      state.walletApi as unknown as ConstructorParameters<typeof WebWallet>[0],
    );
    const network = await webWallet.getNetworkId();
    if (network !== Core.NetworkId.Testnet) {
      throw new Error("Connect a Preview testnet wallet. CIP-30 reported mainnet.");
    }

    state.blaze = await Blaze.from(state.provider, webWallet);
    state.webWallet = webWallet;
    const changeAddress = await webWallet.getChangeAddress();
    const balance = await webWallet.getBalance();
    state.walletBalance = balance;
    els.walletNetwork.textContent = "Preview-compatible testnet";
    els.changeAddress.textContent = changeAddress.toBech32();
    els.walletBalance.textContent = `${formatAssetAmount(balance.coin(), 6)} ADA`;
    els.walletSummary.textContent = wallet.name ?? "Connected";
    els.walletDetails.open = false;
    renderSwapControls();
    log(`Wallet connected: ${wallet.name ?? els.walletSelect.value}`);
  } catch (error) {
    log(errorMessage(error));
  }
}

async function loadPool(): Promise<void> {
  const requestId = ++poolLoadRequestId;
  try {
    const option = selectedPoolOption();
    const ident = option.ident;
    if (!/^[0-9a-f]{56}$/i.test(ident)) {
      throw new Error("Pool ident must be a 56-character V3 hex identifier.");
    }

    applyPoolOptionHint(option);
    const pool = await state.queryProvider.findPoolDataByIdent({ ident });
    if (requestId !== poolLoadRequestId) {
      return;
    }

    state.pool = pool;
    resetTransactionFields();
    upsertPoolOption(poolOptionFromPool(state.pool, option));
    renderPools();
    els.poolSelect.value = ident;
    renderPool();
    log(`Loaded ${selectedPoolOption().label} from Dingo with ${spotPriceLabel(state.pool)}.`);
  } catch (error) {
    if (requestId === poolLoadRequestId) {
      log(errorMessage(error));
    }
  }
}

function renderPool(): void {
  if (!state.pool) {
    return;
  }

  els.poolSelect.title = `Pool ident ${state.pool.ident}`;
  els.poolFee.textContent = `${(state.pool.currentFee * 100).toFixed(3)}%`;
  renderSwapControls();
  renderPriceChart();
}

async function executeSwap(): Promise<void> {
  try {
    if (!state.blaze) {
      throw new Error("Connect a wallet first.");
    }
    if (!state.pool) {
      throw new Error("Load a pool first.");
    }

    els.swapTx.disabled = true;
    els.swapTx.textContent = "Building";
    const built = await buildSwapOrder({
      blaze: state.blaze,
      queryProvider: state.queryProvider,
      pool: state.pool,
      amount: els.swapAmount.value,
      direction: state.swapDirection,
      slippagePercent: els.slippage.value,
    });
    els.txFee.textContent = `${formatAssetAmount(built.txFee, 6)} ADA`;
    els.orderDeposit.textContent = `${formatAssetAmount(built.deposit, 6)} ADA`;
    els.scooperFee.textContent = `${formatAssetAmount(built.scooperFee, 6)} ADA`;
    log(`Built and evaluated order tx through Dingo. Unsigned CBOR: ${built.unsignedCbor.length} hex chars.`);

    els.swapTx.textContent = "Sign in wallet";
    const txId = await built.signAndSubmit();
    els.swapTx.textContent = "Submitted";
    els.txHash.textContent = txId;
    log(`Submitted through Dingo UTxO RPC: ${txId}`);

    const confirmed = await state.provider.awaitTransactionConfirmation(Core.TransactionId(txId), 120_000);
    log(confirmed ? `Confirmed: ${txId}` : `Submitted but not confirmed within 120 seconds: ${txId}`);
    await refreshWalletBalance();
  } catch (error) {
    log(errorMessage(error));
  } finally {
    els.swapTx.disabled = false;
    els.swapTx.textContent = "Swap";
  }
}

async function refreshWalletBalance(): Promise<void> {
  if (!state.webWallet) {
    return;
  }

  const balance = await state.webWallet.getBalance();
  state.walletBalance = balance;
  els.walletBalance.textContent = `${formatAssetAmount(balance.coin(), 6)} ADA`;
  renderSwapControls();
}

function labelFor(asset: { assetId: string; ticker?: string; decimals?: number }): string {
  if (asset.assetId === "ada.lovelace") {
    return "ADA";
  }
  return asset.ticker ?? `${asset.assetId.slice(0, 8)}...${asset.assetId.slice(-6)}`;
}

async function hydratePoolsFromDingo(): Promise<void> {
  try {
    const pools = await state.queryProvider.discoverPools();
    if (pools.length === 0) {
      log("Dingo returned no Sundae V3 pool UTxOs; keeping the preset pool list.");
      return;
    }

    const adaPools = pools.filter(poolHasAda);
    if (adaPools.length === 0) {
      log("Dingo returned no ADA-paired Sundae V3 pools; keeping the preset pool list.");
      return;
    }

    state.poolOptions = adaPools.map((pool) => poolOptionFromPool(pool));
    renderPools();
    log(`Loaded ${adaPools.length} ADA-paired Sundae V3 pools and spot prices from Dingo.`);
  } catch (error) {
    log(`Pool discovery failed; keeping preset pool list: ${errorMessage(error)}`);
  }
}

function seedKnownAssetHints(): void {
  for (const preset of POOL_PRESETS) {
    state.queryProvider.setAssetHint(preset.assetBAssetId, {
      label: preset.assetBLabel,
      decimals: preset.assetBDecimals,
    });
  }
}

function applyPoolOptionHint(option: PoolOption): void {
  state.queryProvider.setAssetHint(option.assetBAssetId, {
    label: option.assetBLabel,
    decimals: option.assetBDecimals,
  });
}

function poolOptionFromPool(pool: IPoolData, previous?: PoolOption): PoolOption {
  const label = `${labelFor(pool.assetA)} / ${labelFor(pool.assetB)}`;
  const token = tokenAssetForPool(pool) ?? pool.assetB;
  return {
    ...previous,
    ident: pool.ident,
    label: previous?.label ?? label,
    assetBLabel: labelFor(token),
    assetBAssetId: token.assetId,
    assetBDecimals: token.decimals ?? 0,
    source: "dingo",
    pool,
    priceLabel: spotPriceLabel(pool),
    inversePriceLabel: inverseSpotPriceLabel(pool),
  };
}

function upsertPoolOption(next: PoolOption): void {
  const index = state.poolOptions.findIndex((option) => option.ident === next.ident);
  if (index === -1) {
    state.poolOptions = [next, ...state.poolOptions];
    return;
  }

  state.poolOptions = state.poolOptions.map((option, optionIndex) =>
    optionIndex === index ? next : option,
  );
}

function spotPriceLabel(pool: IPoolData): string {
  return `1 ${labelFor(pool.assetA)} = ${formatRatio(
    pool.liquidity.bReserve,
    pool.assetB.decimals ?? 0,
    pool.liquidity.aReserve,
    pool.assetA.decimals ?? 0,
  )} ${labelFor(pool.assetB)}`;
}

function inverseSpotPriceLabel(pool: IPoolData): string {
  return `1 ${labelFor(pool.assetB)} = ${formatRatio(
    pool.liquidity.aReserve,
    pool.assetA.decimals ?? 0,
    pool.liquidity.bReserve,
    pool.assetB.decimals ?? 0,
  )} ${labelFor(pool.assetA)}`;
}

function renderSwapControls(): void {
  if (!state.pool || !poolHasAda(state.pool)) {
    const offered = offeredAssetForSelection();
    els.offeredLabel.textContent = `${labelFor(offered)} offered`;
    renderOfferedBalance(offered);
    els.estimateReceive.textContent = "-";
    els.priceImpact.textContent = "-";
    els.poolFee.textContent = "-";
    els.chartMetric.textContent = "-";
    return;
  }

  const offered = offeredAssetForDirection(state.pool);
  const received = receivedAssetForDirection(state.pool);
  els.offeredLabel.textContent = `${labelFor(offered)} offered`;
  renderOfferedBalance(offered);
  els.estimateReceive.textContent = estimatedReceiveLabel(state.pool, offered, received);
  els.priceImpact.textContent = priceImpactLabel(state.pool, offered, received);
  els.chartMetric.textContent = directionSpotPriceLabel(state.pool, offered, received);
}

function offeredAssetForSelection(): IPoolDataAsset {
  if (state.pool && poolHasAda(state.pool)) {
    return offeredAssetForDirection(state.pool);
  }

  if (state.swapDirection === "adaToToken") {
    return ADA_METADATA;
  }

  const option = selectedPoolOption();
  return metadataFor(option.assetBAssetId, {
    label: option.assetBLabel,
    decimals: option.assetBDecimals,
  });
}

function renderOfferedBalance(asset: IPoolDataAsset): void {
  const amount = walletAssetAmount(asset);
  const hasBalance = amount !== undefined;
  const spendableAmount = amount === undefined ? undefined : spendableBalanceFor(asset, amount);
  const label = labelFor(asset);
  els.offeredBalance.textContent = hasBalance
    ? `Available ${formatAssetAmount(spendableAmount ?? 0n, asset.decimals ?? 0)} ${label}`
    : `Available -`;
  for (const button of els.amountPresets) {
    button.disabled = !hasBalance || spendableAmount === undefined || spendableAmount <= 0n;
  }
}

function setSwapAmountFromBalance(percent: number): void {
  const offered = offeredAssetForSelection();
  const amount = walletAssetAmount(offered);
  if (amount === undefined || amount <= 0n || percent <= 0) {
    return;
  }

  const spendableAmount = spendableBalanceFor(offered, amount);
  if (spendableAmount <= 0n) {
    return;
  }

  const nextAmount = percent >= 100
    ? spendableAmount
    : (spendableAmount * BigInt(Math.floor(percent))) / 100n;
  els.swapAmount.value = formatAssetAmount(nextAmount, offered.decimals ?? 0);
  resetTransactionFields();
  renderSwapControls();
  renderPriceChart();
}

function spendableBalanceFor(asset: IPoolDataAsset, amount: bigint): bigint {
  if (asset.assetId !== ADA_METADATA.assetId) {
    return amount;
  }
  return maxBigint(0n, amount - ADA_SWAP_RESERVE_LOVELACE);
}

function walletAssetAmount(asset: IPoolDataAsset): bigint | undefined {
  if (!state.walletBalance) {
    return undefined;
  }

  if (asset.assetId === ADA_METADATA.assetId) {
    return state.walletBalance.coin();
  }

  return state.walletBalance.multiasset()?.get(Core.AssetId(unitFromAssetId(asset.assetId))) ?? 0n;
}

function renderPriceChart(): void {
  const canvas = els.priceChart;
  const cssWidth = Math.max(320, Math.floor(canvas.clientWidth));
  const cssHeight = Math.max(220, Math.floor(canvas.clientHeight));
  const pixelRatio = window.devicePixelRatio || 1;
  canvas.width = Math.floor(cssWidth * pixelRatio);
  canvas.height = Math.floor(cssHeight * pixelRatio);

  const context = canvas.getContext("2d");
  if (!context) {
    return;
  }

  context.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
  context.clearRect(0, 0, cssWidth, cssHeight);

  if (!state.pool || !poolHasAda(state.pool)) {
    drawEmptyChart(context, cssWidth, cssHeight);
    return;
  }

  const offered = offeredAssetForDirection(state.pool);
  const received = receivedAssetForDirection(state.pool);
  const points = chartPoints(state.pool, offered, received);
  if (points.length < 2) {
    drawEmptyChart(context, cssWidth, cssHeight);
    return;
  }

  drawChart(context, cssWidth, cssHeight, points, labelFor(offered), labelFor(received));
}

function resetTransactionFields(): void {
  els.txFee.textContent = "-";
  els.orderDeposit.textContent = "-";
  els.scooperFee.textContent = "-";
  els.txHash.textContent = "-";
}

function poolHasAda(pool: IPoolData): boolean {
  return pool.assetA.assetId === ADA_METADATA.assetId || pool.assetB.assetId === ADA_METADATA.assetId;
}

function tokenAssetForPool(pool: IPoolData): IPoolDataAsset | undefined {
  if (pool.assetA.assetId === ADA_METADATA.assetId) {
    return pool.assetB;
  }
  if (pool.assetB.assetId === ADA_METADATA.assetId) {
    return pool.assetA;
  }
  return undefined;
}

function offeredAssetForDirection(pool: IPoolData): IPoolDataAsset {
  if (state.swapDirection === "adaToToken") {
    return ADA_METADATA;
  }

  const token = tokenAssetForPool(pool);
  if (!token) {
    throw new Error("The selected pool is not an ADA pair.");
  }
  return token;
}

function receivedAssetForDirection(pool: IPoolData): IPoolDataAsset {
  if (state.swapDirection === "adaToToken") {
    const token = tokenAssetForPool(pool);
    if (!token) {
      throw new Error("The selected pool is not an ADA pair.");
    }
    return token;
  }

  return ADA_METADATA;
}

function reserveForAsset(pool: IPoolData, asset: IPoolDataAsset): bigint {
  if (pool.assetA.assetId === asset.assetId) {
    return pool.liquidity.aReserve;
  }
  if (pool.assetB.assetId === asset.assetId) {
    return pool.liquidity.bReserve;
  }
  throw new Error(`Asset ${asset.assetId} is not in pool ${pool.ident}.`);
}

function estimatedReceiveLabel(
  pool: IPoolData,
  offered: IPoolDataAsset,
  received: IPoolDataAsset,
): string {
  try {
    const inputAmount = parseAssetAmount(
      els.swapAmount.value,
      offered.decimals ?? 0,
      labelFor(offered),
    );
    if (inputAmount <= 0n) {
      return "-";
    }

    const outputAmount = quoteOutput(pool, offered, received, inputAmount);
    return `${formatAssetAmount(outputAmount, received.decimals ?? 0)} ${labelFor(received)}`;
  } catch {
    return "-";
  }
}

function priceImpactLabel(
  pool: IPoolData,
  offered: IPoolDataAsset,
  received: IPoolDataAsset,
): string {
  try {
    const inputAmount = parseAssetAmount(
      els.swapAmount.value,
      offered.decimals ?? 0,
      labelFor(offered),
    );
    const reserveIn = reserveForAsset(pool, offered);
    const reserveOut = reserveForAsset(pool, received);
    if (inputAmount <= 0n || reserveIn <= 0n || reserveOut <= 0n) {
      return "-";
    }

    const feeBps = poolFeeBasisPoints(pool);
    const inputAfterFee = (inputAmount * (10_000n - feeBps)) / 10_000n;
    const spotOutputAfterFee = (inputAfterFee * reserveOut) / reserveIn;
    const quotedOutput = quoteOutput(pool, offered, received, inputAmount);
    if (spotOutputAfterFee <= 0n || quotedOutput >= spotOutputAfterFee) {
      return "0.00%";
    }

    const loss = spotOutputAfterFee - quotedOutput;
    const impactBps = (loss * 10_000n + spotOutputAfterFee / 2n) / spotOutputAfterFee;
    if (impactBps === 0n) {
      return "<0.01%";
    }
    return formatBasisPointsPercent(impactBps);
  } catch {
    return "-";
  }
}

function directionSpotPriceLabel(
  pool: IPoolData,
  offered: IPoolDataAsset,
  received: IPoolDataAsset,
): string {
  return `1 ${labelFor(offered)} = ${formatRatio(
    reserveForAsset(pool, received),
    received.decimals ?? 0,
    reserveForAsset(pool, offered),
    offered.decimals ?? 0,
  )} ${labelFor(received)}`;
}

function quoteOutput(
  pool: IPoolData,
  offered: IPoolDataAsset,
  received: IPoolDataAsset,
  inputAmount: bigint,
): bigint {
  const reserveIn = reserveForAsset(pool, offered);
  const reserveOut = reserveForAsset(pool, received);
  if (inputAmount <= 0n || reserveIn <= 0n || reserveOut <= 0n) {
    return 0n;
  }

  const feeBps = poolFeeBasisPoints(pool);
  const inputAfterFee = (inputAmount * (10_000n - feeBps)) / 10_000n;
  return (inputAfterFee * reserveOut) / (reserveIn + inputAfterFee);
}

function poolFeeBasisPoints(pool: IPoolData): bigint {
  return BigInt(Math.max(0, Math.min(10_000, Math.round(pool.currentFee * 10_000))));
}

type ChartPoint = {
  inputAmount: bigint;
  outputAmount: bigint;
  x: number;
  y: number;
  current?: boolean;
};

function chartPoints(
  pool: IPoolData,
  offered: IPoolDataAsset,
  received: IPoolDataAsset,
): ChartPoint[] {
  const reserveIn = reserveForAsset(pool, offered);
  const reserveOut = reserveForAsset(pool, received);
  if (reserveIn <= 0n || reserveOut <= 0n) {
    return [];
  }

  const sampleBps = [0n, 10n, 25n, 50n, 100n, 200n, 500n, 1000n];
  const points: ChartPoint[] = sampleBps.map((bps) => {
    const inputAmount = bps === 0n ? 0n : maxBigint(1n, (reserveIn * bps) / 10_000n);
    const outputAmount =
      inputAmount === 0n ? 0n : quoteOutput(pool, offered, received, inputAmount);
    return {
      inputAmount,
      outputAmount,
      x: normalizedAmount(inputAmount, offered.decimals ?? 0),
      y:
        inputAmount === 0n
          ? ratioAsNumber(reserveOut, received.decimals ?? 0, reserveIn, offered.decimals ?? 0)
          : ratioAsNumber(outputAmount, received.decimals ?? 0, inputAmount, offered.decimals ?? 0),
    };
  });

  try {
    const currentInput = parseAssetAmount(els.swapAmount.value, offered.decimals ?? 0, labelFor(offered));
    if (currentInput > 0n) {
      const currentOutput = quoteOutput(pool, offered, received, currentInput);
      points.push({
        inputAmount: currentInput,
        outputAmount: currentOutput,
        x: normalizedAmount(currentInput, offered.decimals ?? 0),
        y: ratioAsNumber(currentOutput, received.decimals ?? 0, currentInput, offered.decimals ?? 0),
        current: true,
      });
    }
  } catch {
    // Ignore invalid in-progress input while drawing the chart.
  }

  return points
    .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y))
    .sort((a, b) => a.x - b.x);
}

function drawEmptyChart(context: CanvasRenderingContext2D, width: number, height: number): void {
  context.fillStyle = "#000a1b";
  context.fillRect(0, 0, width, height);
  drawGrid(context, width, height, 52, 18, 20, 38);
}

function drawChart(
  context: CanvasRenderingContext2D,
  width: number,
  height: number,
  points: ChartPoint[],
  offeredLabel: string,
  receivedLabel: string,
): void {
  const left = 58;
  const right = 18;
  const top = 22;
  const bottom = 42;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const minX = 0;
  const maxX = Math.max(...points.map((point) => point.x), 1);
  const minY = Math.min(...points.map((point) => point.y));
  const maxY = Math.max(...points.map((point) => point.y));
  const yPad = Math.max((maxY - minY) * 0.12, maxY * 0.01, 0.000001);
  const yMin = Math.max(0, minY - yPad);
  const yMax = maxY + yPad;

  const xFor = (value: number) => left + ((value - minX) / (maxX - minX)) * plotWidth;
  const yFor = (value: number) => top + (1 - (value - yMin) / (yMax - yMin)) * plotHeight;

  context.fillStyle = "#000a1b";
  context.fillRect(0, 0, width, height);
  drawGrid(context, width, height, left, right, top, bottom);

  context.font = "12px Inter, ui-sans-serif, system-ui, sans-serif";
  context.fillStyle = "#e6e7f4";
  context.textAlign = "right";
  context.textBaseline = "middle";
  for (let index = 0; index <= 4; index += 1) {
    const value = yMin + ((yMax - yMin) * index) / 4;
    context.fillText(formatChartNumber(value), left - 8, yFor(value));
  }

  context.textAlign = "center";
  context.textBaseline = "top";
  for (let index = 0; index <= 4; index += 1) {
    const value = minX + ((maxX - minX) * index) / 4;
    context.fillText(formatChartNumber(value), xFor(value), height - bottom + 14);
  }

  context.save();
  context.translate(14, top + plotHeight / 2);
  context.rotate(-Math.PI / 2);
  context.textAlign = "center";
  context.fillStyle = "#9b9b9c";
  context.fillText(`${receivedLabel} per ${offeredLabel}`, 0, 0);
  context.restore();

  context.fillStyle = "#9b9b9c";
  context.textAlign = "center";
  context.fillText(`${offeredLabel} offered`, left + plotWidth / 2, height - 14);

  const line = new Path2D();
  points.forEach((point, index) => {
    const x = xFor(point.x);
    const y = yFor(point.y);
    if (index === 0) {
      line.moveTo(x, y);
    } else {
      line.lineTo(x, y);
    }
  });

  const area = new Path2D(line);
  const last = points[points.length - 1];
  area.lineTo(xFor(last.x), height - bottom);
  area.lineTo(left, height - bottom);
  area.closePath();

  context.fillStyle = "rgba(66, 133, 244, 0.18)";
  context.fill(area);
  context.strokeStyle = "#4285f4";
  context.lineWidth = 2.5;
  context.stroke(line);

  const current = points.find((point) => point.current);
  if (current) {
    context.beginPath();
    context.arc(xFor(current.x), yFor(current.y), 5, 0, Math.PI * 2);
    context.fillStyle = "#d97706";
    context.fill();
    context.strokeStyle = "#ffffff";
    context.lineWidth = 1.5;
    context.stroke();
  }
}

function drawGrid(
  context: CanvasRenderingContext2D,
  width: number,
  height: number,
  left: number,
  right: number,
  top: number,
  bottom: number,
): void {
  context.strokeStyle = "rgba(255, 255, 255, 0.1)";
  context.lineWidth = 1;
  for (let index = 0; index <= 4; index += 1) {
    const x = left + ((width - left - right) * index) / 4;
    context.beginPath();
    context.moveTo(x, top);
    context.lineTo(x, height - bottom);
    context.stroke();

    const y = top + ((height - top - bottom) * index) / 4;
    context.beginPath();
    context.moveTo(left, y);
    context.lineTo(width - right, y);
    context.stroke();
  }
}

function normalizedAmount(amount: bigint, decimals: number): number {
  return Number(amount) / 10 ** decimals;
}

function ratioAsNumber(
  numerator: bigint,
  numeratorDecimals: number,
  denominator: bigint,
  denominatorDecimals: number,
): number {
  if (denominator === 0n) {
    return 0;
  }

  const scale = 1_000_000_000_000n;
  const scaled =
    (numerator * 10n ** BigInt(denominatorDecimals) * scale) /
    (denominator * 10n ** BigInt(numeratorDecimals));
  return Number(scaled) / Number(scale);
}

function maxBigint(a: bigint, b: bigint): bigint {
  return a > b ? a : b;
}

function parseAdaSwapReserve(): bigint {
  const configured = import.meta.env.VITE_ADA_SWAP_RESERVE_LOVELACE;
  if (!configured) {
    return DEFAULT_ADA_SWAP_RESERVE_LOVELACE;
  }
  if (!/^\d+$/.test(configured)) {
    throw new Error("VITE_ADA_SWAP_RESERVE_LOVELACE must be a whole lovelace amount.");
  }
  return BigInt(configured);
}

function formatChartNumber(value: number): string {
  if (value === 0) {
    return "0";
  }
  if (Math.abs(value) >= 1_000) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }
  if (Math.abs(value) >= 1) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  return value.toPrecision(3);
}

function formatBasisPointsPercent(bps: bigint): string {
  const clamped = bps > 10_000n ? 10_000n : bps;
  const whole = clamped / 100n;
  const fraction = (clamped % 100n).toString().padStart(2, "0");
  return `${whole}.${fraction}%`;
}

function formatRatio(
  numerator: bigint,
  numeratorDecimals: number,
  denominator: bigint,
  denominatorDecimals: number,
  precision = 6,
): string {
  if (denominator === 0n) {
    return "-";
  }

  const scaled =
    (numerator * 10n ** BigInt(denominatorDecimals) * 10n ** BigInt(precision)) /
    (denominator * 10n ** BigInt(numeratorDecimals));
  const divisor = 10n ** BigInt(precision);
  const whole = scaled / divisor;
  const fraction = (scaled % divisor).toString().padStart(precision, "0").replace(/0+$/, "");
  return fraction ? `${whole}.${fraction}` : whole.toString();
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
