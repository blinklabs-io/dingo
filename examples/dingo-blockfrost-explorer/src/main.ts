import "./styles.css";

const originalFetch = window.fetch.bind(window);
window.fetch = (input: RequestInfo | URL, init?: RequestInit) => {
  const target = new URL(
    typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
    window.location.href,
  );
  if (target.origin !== window.location.origin) {
    throw new Error(`Blocked non-Dingo external request: ${target.href}`);
  }
  return originalFetch(input, init);
};

type Amount = {
  unit: string;
  quantity: string;
};

type BlockResponse = {
  time: number;
  height: number;
  hash: string;
  slot: number;
  epoch: number;
  epoch_slot: number;
  slot_leader: string;
  size: number;
  tx_count: number;
  output: string | null;
  fees: string | null;
  previous_block: string;
  next_block: string | null;
  confirmations: number;
};

type EpochResponse = {
  epoch: number;
  start_time: number;
  end_time: number;
  first_block_time: number;
  last_block_time: number;
  block_count: number;
  tx_count: number;
  output: string;
  fees: string;
  active_stake: string | null;
};

type NetworkResponse = {
  supply: {
    max: string;
    total: string;
    circulating: string;
    locked: string;
    treasury: string;
    reserves: string;
  };
  stake: {
    live: string;
    active: string;
  };
};

type GenesisResponse = {
  active_slots_coefficient: number;
  max_lovelace_supply: string;
  network_magic: number;
  epoch_length: number;
  system_start: number;
  slots_per_kes_period: number;
  slot_length: number;
  max_kes_evolutions: number;
  security_param: number;
};

type TransactionResponse = {
  invalid_before: string | null;
  invalid_hereafter: string | null;
  output_amount: Amount[];
  hash: string;
  block: string;
  deposit: string;
  fees: string;
  slot: number;
  block_height: number;
  block_time: number;
  size: number;
  index: number;
  utxo_count: number;
  withdrawal_count: number;
  mir_cert_count: number;
  delegation_count: number;
  stake_cert_count: number;
  pool_update_count: number;
  pool_retire_count: number;
  asset_mint_or_burn_count: number;
  redeemer_count: number;
  valid_contract: boolean;
};

type TransactionUTXOsResponse = {
  hash: string;
  inputs: TransactionInputResponse[];
  outputs: TransactionOutputResponse[];
};

type TransactionInputResponse = {
  address: string;
  tx_hash: string;
  amount: Amount[];
  output_index: number;
  collateral: boolean;
  data_hash: string | null;
  inline_datum: string | null;
  reference_script_hash: string | null;
};

type TransactionOutputResponse = {
  address: string;
  amount: Amount[];
  output_index: number;
  collateral: boolean;
  consumed_by_tx: string | null;
  data_hash: string | null;
  inline_datum: string | null;
  reference_script_hash: string | null;
};

type TransactionMetadataResponse = {
  label: string;
  json_metadata: unknown;
};

type AddressUTXOResponse = {
  address: string;
  tx_hash: string;
  output_index: number;
  amount: Amount[];
  block: string;
  data_hash: string | null;
  inline_datum: string | null;
  reference_script_hash: string | null;
};

type AddressTransactionResponse = {
  tx_hash: string;
  tx_index: number;
  block_height: number;
  block_time: number;
};

type AssetResponse = {
  asset: string;
  policy_id: string;
  asset_name: string;
  asset_name_ascii: string;
  fingerprint: string;
  quantity: string;
  initial_mint_tx_hash: string;
  mint_or_burn_count: number;
  onchain_metadata: unknown | null;
  metadata: unknown | null;
};

type AccountResponse = {
  stake_address: string;
  active: boolean;
  active_epoch: number | null;
  controlled_amount: string;
  rewards_sum: string;
  withdrawals_sum: string;
  withdrawable_amount: string;
  pool_id: string | null;
};

type AccountAssociatedAddressResponse = {
  address: string;
};

type AccountDelegationHistoryResponse = {
  active_epoch: number;
  tx_hash: string;
  amount: string;
  pool_id: string;
};

type AccountRegistrationHistoryResponse = {
  tx_hash: string;
  action: string;
};

type AccountRewardHistoryResponse = {
  epoch: number;
  amount: string;
  pool_id: string;
};

type DRepResponse = {
  drep_id: string;
  hex: string;
  has_script: boolean;
  registered: boolean;
  epoch: number;
  amount: string;
  active: boolean;
  active_epoch: number;
  live_stake: string;
};

type PoolExtendedResponse = {
  pool_id: string;
  hex: string;
  vrf_key: string;
  active_stake: string;
  live_stake: string;
  declared_pledge: string;
  fixed_cost: string;
  margin_cost: number;
  relays: Array<{
    ipv4: string | null;
    ipv6: string | null;
    dns: string | null;
    port: number | null;
  }>;
};

type MetadataTransactionJSONResponse = {
  tx_hash: string;
  json_metadata: unknown;
};

type ErrorResponse = {
  status_code: number;
  error: string;
  message: string;
};

type HTMLContent = {
  readonly html: string;
};

type RenderContent = string | number | HTMLContent;

type Tab = "block" | "tx" | "address" | "account" | "asset" | "drep" | "metadata" | "pools";

type AppState = {
  health: "checking" | "ready" | "error";
  latestBlock?: BlockResponse;
  selectedBlockHash?: string;
  recentBlocks: BlockResponse[];
  recentBlocksLoading: boolean;
  blockTxHashes: string[];
  blockTxs: TransactionResponse[];
  blockTxPanelTitle: string;
  epoch?: EpochResponse;
  network?: NetworkResponse;
  genesis?: GenesisResponse;
  pools: PoolExtendedResponse[];
  poolTotal?: number;
  networkLoading: boolean;
  poolsLoading: boolean;
  networkUpdatedAt?: number;
  poolsUpdatedAt?: number;
  activeTab: Tab;
  loading: boolean;
  hasUserResult: boolean;
};

const tabConfig: Record<Tab, { label: string; placeholder: string; emptySearch: boolean }> = {
  block: {
    label: "Block",
    placeholder: "4342637 or 64-char block hash",
    emptySearch: false,
  },
  tx: {
    label: "Transaction",
    placeholder: "64-char transaction hash",
    emptySearch: false,
  },
  address: {
    label: "Address",
    placeholder: "addr1...",
    emptySearch: false,
  },
  account: {
    label: "Stake",
    placeholder: "stake1...",
    emptySearch: false,
  },
  asset: {
    label: "Asset",
    placeholder: "Policy ID plus asset name hex",
    emptySearch: false,
  },
  drep: {
    label: "DRep",
    placeholder: "DRep bech32 ID or credential hex",
    emptySearch: false,
  },
  metadata: {
    label: "Metadata",
    placeholder: "Metadata label",
    emptySearch: false,
  },
  pools: {
    label: "Pools",
    placeholder: "Pool ID or empty for list",
    emptySearch: true,
  },
};

const state: AppState = {
  health: "checking",
  recentBlocks: [],
  recentBlocksLoading: false,
  blockTxHashes: [],
  blockTxs: [],
  blockTxPanelTitle: "Block Transactions",
  pools: [],
  networkLoading: false,
  poolsLoading: false,
  activeTab: "block",
  loading: false,
  hasUserResult: false,
};

let refreshSequence = 0;
let blockTransactionSequence = 0;

function html(value: string): HTMLContent {
  return { html: value };
}

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("Missing app root.");
}

document.title = "Dingo Explorer";

app.innerHTML = `
  <section class="shell">
    <header class="topbar">
      <div>
        <h1>Dingo Explorer</h1>
        <p id="networkLabel">Blockfrost-compatible API</p>
      </div>
      <div class="top-actions">
        <div class="status" id="dingoStatus">Dingo: checking</div>
        <button id="refreshButton" type="button">Refresh</button>
      </div>
    </header>

    <main class="workspace">
      <section class="lookup-panel">
        <form id="lookupForm" class="lookup-form">
          <div class="tabs" role="tablist" aria-label="Explorer lookup type">
            ${Object.entries(tabConfig)
              .map(
                ([key, config]) => `
                  <button
                    type="button"
                    class="tab-button"
                    data-tab="${key}"
                    role="tab"
                    aria-selected="${key === state.activeTab ? "true" : "false"}"
                  >
                    ${config.label}
                  </button>
                `,
              )
              .join("")}
          </div>
          <div class="search-row">
            <input id="queryInput" autocomplete="off" spellcheck="false" />
            <button id="searchButton" type="submit">Search</button>
          </div>
        </form>
      </section>

      <section id="overviewPanel" class="overview-panel">
        <div class="recent-blocks-wrap">
          <div class="section-heading">
            <h2>Recent Blocks</h2>
            <span id="recentBlocksMeta">-</span>
          </div>
          <div id="recentBlocksList" class="recent-blocks-list"></div>
        </div>
        <div class="metric-grid" id="metricGrid"></div>
      </section>

      <section class="result-panel">
        <div class="section-heading">
          <h2 id="resultTitle">Explorer</h2>
          <span id="resultMeta">Waiting for Dingo</span>
        </div>
        <div id="resultContent" class="result-content"></div>
      </section>

      <section id="latestPanel" class="latest-panel">
        <div class="section-heading">
          <h2 id="blockTxTitle">Block Transactions</h2>
          <span id="blockTxCount">-</span>
        </div>
        <div id="blockTxList" class="tx-list"></div>
      </section>
    </main>
  </section>
`;

const els = {
  networkLabel: byId("networkLabel"),
  dingoStatus: byId("dingoStatus"),
  refreshButton: byId<HTMLButtonElement>("refreshButton"),
  overviewPanel: byId<HTMLElement>("overviewPanel"),
  recentBlocksMeta: byId("recentBlocksMeta"),
  recentBlocksList: byId("recentBlocksList"),
  metricGrid: byId("metricGrid"),
  latestPanel: byId<HTMLElement>("latestPanel"),
  blockTxTitle: byId("blockTxTitle"),
  blockTxCount: byId("blockTxCount"),
  blockTxList: byId("blockTxList"),
  lookupForm: byId<HTMLFormElement>("lookupForm"),
  queryInput: byId<HTMLInputElement>("queryInput"),
  searchButton: byId<HTMLButtonElement>("searchButton"),
  resultTitle: byId("resultTitle"),
  resultMeta: byId("resultMeta"),
  resultContent: byId("resultContent"),
  tabButtons: Array.from(document.querySelectorAll<HTMLButtonElement>("[data-tab]")),
};

app.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const copyButton = target.closest<HTMLButtonElement>("[data-copy]");
  if (copyButton) {
    event.preventDefault();
    event.stopPropagation();
    void copyToClipboard(copyButton.dataset.copy ?? "", copyButton);
    return;
  }

  if (handleEntityClick(target)) {
    event.preventDefault();
    event.stopPropagation();
  }
});
els.refreshButton.addEventListener("click", () => void refreshOverview());
els.lookupForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void runLookup();
});
els.queryInput.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") {
    return;
  }
  els.queryInput.value = "";
  clearResultState(tabConfig[state.activeTab].label);
  writeRouteState(state.activeTab, "", true);
});
els.queryInput.addEventListener("input", () => updateSearchButton());
for (const button of els.tabButtons) {
  button.addEventListener("click", () => {
    const tab = button.dataset.tab as Tab | undefined;
    if (tab) {
      activateTab(tab);
    }
  });
}
window.addEventListener("popstate", () => {
  const route = readRouteState();
  activateTab(route.tab, route.query, false);
  if (shouldRunRoute(route.tab, route.query)) {
    void runLookup(false);
  } else {
    clearResultState(tabConfig[route.tab].label);
  }
});

const initialRoute = readRouteState();
activateTab(initialRoute.tab, initialRoute.query, false);
renderOverview();
void refreshOverview();
if (shouldRunRoute(initialRoute.tab, initialRoute.query)) {
  void runLookup(false);
}

function byId<T extends HTMLElement = HTMLElement>(id: string): T {
  const el = document.getElementById(id);
  if (!el) {
    throw new Error(`Missing #${id}.`);
  }
  return el as T;
}

function activateTab(tab: Tab, query?: string, syncRoute = true): void {
  const tabChanged = state.activeTab !== tab;
  state.activeTab = tab;
  const config = tabConfig[tab];
  els.queryInput.placeholder = config.placeholder;
  els.queryInput.disabled = false;
  if (query !== undefined) {
    els.queryInput.value = query;
  } else if (tabChanged || config.emptySearch) {
    els.queryInput.value = "";
  }
  updateSearchButton();
  if (tabChanged && query === undefined) {
    clearResultState(config.label);
  }
  for (const button of els.tabButtons) {
    const selected = button.dataset.tab === tab;
    button.classList.toggle("active", selected);
    button.setAttribute("aria-selected", String(selected));
  }
  renderLayout();
  if (syncRoute) {
    writeRouteState(tab, els.queryInput.value.trim(), true);
  }
}

function renderLayout(): void {
  const showBlockPanels = state.activeTab === "block";
  els.overviewPanel.hidden = !showBlockPanels;
  els.latestPanel.hidden = !showBlockPanels;
}

function updateSearchButton(): void {
  els.searchButton.textContent =
    state.activeTab === "pools" && els.queryInput.value.trim() === "" ? "Load Pools" : "Search";
}

function handleEntityClick(target: HTMLElement): boolean {
  const txHash = target.closest<HTMLElement>("[data-tx-hash]")?.dataset.txHash;
  const blockID = target.closest<HTMLElement>("[data-block-id]")?.dataset.blockId;
  const address = target.closest<HTMLElement>("[data-address]")?.dataset.address;
  const account = target.closest<HTMLElement>("[data-account]")?.dataset.account;
  const asset = target.closest<HTMLElement>("[data-asset-id]")?.dataset.assetId;
  const poolID = target.closest<HTMLElement>("[data-pool-id]")?.dataset.poolId;
  const rowBlockID = target.closest<HTMLElement>("[data-row-block-id]")?.dataset.rowBlockId;
  if (txHash) {
    activateTab("tx", txHash);
  } else if (blockID) {
    activateTab("block", blockID);
  } else if (address) {
    activateTab("address", address);
  } else if (account) {
    activateTab("account", account);
  } else if (asset) {
    activateTab("asset", asset);
  } else if (poolID) {
    activateTab("pools", poolID);
  } else if (rowBlockID) {
    activateTab("block", rowBlockID);
  } else {
    return false;
  }
  void runLookup().then(() => {
    els.resultTitle.scrollIntoView({ block: "start", behavior: "smooth" });
  });
  return true;
}

function clearResultState(label: string): void {
  state.hasUserResult = false;
  state.selectedBlockHash = undefined;
  blockTransactionSequence += 1;
  els.resultTitle.textContent = label;
  setResultMeta("Ready");
  els.resultContent.innerHTML = "";
  renderRecentBlocks();
}

function readRouteState(): { tab: Tab; query: string } {
  const params = new URLSearchParams(window.location.search);
  const tabParam = params.get("tab");
  const tab = isTab(tabParam) ? tabParam : "block";
  return {
    tab,
    query: normalizeQuery(tab, params.get("q") ?? ""),
  };
}

function writeRouteState(tab: Tab, query: string, replace = false): void {
  const url = new URL(window.location.href);
  url.searchParams.set("tab", tab);
  const normalized = normalizeQuery(tab, query);
  if (normalized) {
    url.searchParams.set("q", normalized);
  } else {
    url.searchParams.delete("q");
  }
  const method = replace ? "replaceState" : "pushState";
  window.history[method]({}, "", url);
}

function shouldRunRoute(tab: Tab, query: string): boolean {
  return tabConfig[tab].emptySearch || query.length > 0;
}

function isTab(value: string | null): value is Tab {
  return value !== null && Object.hasOwn(tabConfig, value);
}

async function refreshOverview(): Promise<void> {
  const sequence = refreshSequence + 1;
  refreshSequence = sequence;
  state.loading = true;
  state.health = "checking";
  state.recentBlocksLoading = true;
  state.networkLoading = true;
  state.poolsLoading = true;
  renderOverview();
  if (!state.hasUserResult) {
    setResultMeta("Refreshing");
  }

  const healthPromise = blockfrostFetch<{ is_healthy: boolean }>("/health")
    .then((health) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.health = health.is_healthy ? "ready" : "error";
      renderOverview();
    })
    .catch(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.health = "error";
      renderOverview();
    });

  const blockPromise = blockfrostFetch<BlockResponse>("/api/v0/blocks/latest")
    .then((block) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.latestBlock = block;
      state.recentBlocks = [block];
      els.networkLabel.textContent = `Epoch ${block.epoch} at slot ${formatInteger(block.slot)}`;
      renderOverview();
      void loadRecentBlocks(block, sequence);
      if (!state.hasUserResult && state.activeTab === "block") {
        renderBlockResult(block);
        void loadLatestBlockTransactions(block);
      }
    })
    .catch(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.latestBlock = undefined;
      state.recentBlocks = [];
      state.recentBlocksLoading = false;
      renderOverview();
    });

  const epochPromise = blockfrostFetch<EpochResponse>("/api/v0/epochs/latest")
    .then((epoch) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.epoch = epoch;
      renderOverview();
    })
    .catch(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.epoch = undefined;
      renderOverview();
    });

  const genesisPromise = blockfrostFetch<GenesisResponse>("/api/v0/genesis")
    .then((genesis) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.genesis = genesis;
      renderOverview();
    })
    .catch(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.genesis = undefined;
      renderOverview();
    });

  void blockfrostFetch<NetworkResponse>("/api/v0/network")
    .then((network) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.network = network;
      state.networkUpdatedAt = Date.now();
    })
    .catch(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.network = undefined;
    })
    .finally(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.networkLoading = false;
      renderOverview();
    });

  void blockfrostFetchPage<PoolExtendedResponse>("/api/v0/pools/extended?count=8&page=1&order=desc")
    .then((pools) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.pools = pools.items;
      state.poolTotal = pools.total;
      state.poolsUpdatedAt = Date.now();
    })
    .catch(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.pools = [];
      state.poolTotal = undefined;
    })
    .finally(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.poolsLoading = false;
      renderOverview();
    });

  await Promise.allSettled([healthPromise, blockPromise, epochPromise, genesisPromise]);
  if (!isCurrentRefresh(sequence)) {
    return;
  }
  state.loading = false;
  if (!state.latestBlock) {
    state.recentBlocksLoading = false;
  }
  renderOverview();
}

function isCurrentRefresh(sequence: number): boolean {
  return sequence === refreshSequence;
}

async function loadRecentBlocks(latest: BlockResponse, sequence: number): Promise<void> {
  const count = 12;
  const previousHeights = Array.from(
    { length: Math.min(count - 1, Math.max(0, latest.height)) },
    (_, index) => latest.height - index - 1,
  ).filter((height) => height >= 0);

  try {
    const results = await Promise.allSettled(
      previousHeights.map((height) => blockfrostFetch<BlockResponse>(`/api/v0/blocks/${height}`)),
    );
    if (!isCurrentRefresh(sequence)) {
      return;
    }
    const blocks = results
      .filter((item): item is PromiseFulfilledResult<BlockResponse> => item.status === "fulfilled")
      .map((item) => item.value);
    state.recentBlocks = [latest, ...blocks]
      .filter((block, index, all) => all.findIndex((item) => item.hash === block.hash) === index)
      .sort((left, right) => right.height - left.height);
  } finally {
    if (!isCurrentRefresh(sequence)) {
      return;
    }
    state.recentBlocksLoading = false;
    renderOverview();
  }
}

async function loadLatestBlockTransactions(block: BlockResponse): Promise<void> {
  const sequence = blockTransactionSequence + 1;
  blockTransactionSequence = sequence;
  state.blockTxHashes = [];
  state.blockTxs = [];
  state.blockTxPanelTitle = blockTransactionsTitle(block);
  renderBlockTransactionsLoading();
  try {
    const hashes = await blockfrostFetch<string[]>("/api/v0/blocks/latest/txs");
    if (!isCurrentBlockTransactionRequest(sequence, block)) {
      return;
    }
    state.blockTxHashes = hashes;
    const details = await Promise.allSettled(
      hashes.slice(0, 8).map((hash) => blockfrostFetch<TransactionResponse>(`/api/v0/txs/${hash}`)),
    );
    if (!isCurrentBlockTransactionRequest(sequence, block)) {
      return;
    }
    state.blockTxs = details
      .filter((item): item is PromiseFulfilledResult<TransactionResponse> => item.status === "fulfilled")
      .map((item) => item.value);
  } catch (error) {
    if (!isCurrentBlockTransactionRequest(sequence, block)) {
      return;
    }
    state.blockTxHashes = [];
    state.blockTxs = [];
    renderBlockTransactionsError(errorMessage(error));
    return;
  }
  renderBlockTransactions();
}

async function showSelectedBlockTransactions(block: BlockResponse): Promise<void> {
  if (state.latestBlock?.hash === block.hash) {
    await loadLatestBlockTransactions(block);
    return;
  }
  blockTransactionSequence += 1;
  state.blockTxHashes = [];
  state.blockTxs = [];
  state.blockTxPanelTitle = blockTransactionsTitle(block);
  if (block.tx_count === 0) {
    renderBlockTransactions();
    return;
  }
  renderBlockTransactionsUnavailable(block);
}

function isCurrentBlockTransactionRequest(sequence: number, block: BlockResponse): boolean {
  return sequence === blockTransactionSequence && state.selectedBlockHash === block.hash;
}

async function runLookup(syncRoute = true): Promise<void> {
  const config = tabConfig[state.activeTab];
  const query = normalizeQuery(state.activeTab, els.queryInput.value);
  els.queryInput.value = query;
  if (!query && !config.emptySearch) {
    els.queryInput.focus();
    return;
  }
  const validationError = validateQuery(state.activeTab, query);
  if (validationError) {
    renderErrorResult(config.label, validationError);
    els.queryInput.focus();
    return;
  }

  state.hasUserResult = true;
  if (syncRoute) {
    writeRouteState(state.activeTab, query);
  }
  if (state.activeTab !== "block") {
    state.selectedBlockHash = undefined;
    blockTransactionSequence += 1;
    renderRecentBlocks();
  }
  setResultLoading(config.label);
  try {
    switch (state.activeTab) {
      case "block":
        {
          const block = await blockfrostFetch<BlockResponse>(`/api/v0/blocks/${encodeURIComponent(query)}`);
          renderBlockResult(block);
          await showSelectedBlockTransactions(block);
        }
        break;
      case "tx":
        await renderTransactionLookup(query);
        break;
      case "address":
        await renderAddressLookup(query);
        break;
      case "account":
        await renderAccountLookup(query);
        break;
      case "asset":
        renderAssetResult(await blockfrostFetch<AssetResponse>(`/api/v0/assets/${encodeURIComponent(query)}`));
        break;
      case "drep":
        renderDRepResult(
          await blockfrostFetch<DRepResponse>(`/api/v0/governance/dreps/${encodeURIComponent(query)}`),
        );
        break;
      case "metadata":
        await renderMetadataLookup(query);
        break;
      case "pools":
        if (query) {
          await renderPoolLookup(query);
        } else {
          await renderPoolsLookup();
        }
        break;
    }
  } catch (error) {
    renderErrorResult(config.label, errorMessage(error));
  }
}

function normalizeQuery(tab: Tab, value: string): string {
  const trimmed = value.trim();
  if (tab === "address" || tab === "account") {
    return trimmed.replace(/\s+/g, "");
  }
  return trimmed.replace(/\s+/g, "");
}

function validateQuery(tab: Tab, query: string): string | undefined {
  if (tabConfig[tab].emptySearch && query === "") {
    return undefined;
  }
  switch (tab) {
    case "block":
      if (/^\d+$/.test(query) || /^[0-9a-fA-F]{64}$/.test(query)) {
        return undefined;
      }
      return "Enter a block height or a 64-character block hash.";
    case "tx":
      return /^[0-9a-fA-F]{64}$/.test(query) ? undefined : "Enter a 64-character transaction hash.";
    case "address":
      return query.startsWith("addr") ? undefined : "Enter a payment address beginning with addr.";
    case "account":
      return query.startsWith("stake") ? undefined : "Enter a stake address beginning with stake.";
    case "asset":
      return /^[0-9a-fA-F]+$/.test(query) ? undefined : "Enter the asset ID as policy ID plus asset name hex.";
    case "drep":
      return query.startsWith("drep") || /^[0-9a-fA-F]{56}$/.test(query)
        ? undefined
        : "Enter a DRep bech32 ID or 56-character credential hex.";
    case "metadata":
      return /^\d+$/.test(query) ? undefined : "Enter a numeric metadata label.";
    case "pools":
      return query === "" || query.startsWith("pool") || /^[0-9a-fA-F]{56}$/.test(query)
        ? undefined
        : "Enter a pool ID beginning with pool, a 56-character pool key hash, or leave empty for the pool list.";
  }
}

async function renderTransactionLookup(hash: string): Promise<void> {
  const [tx, utxos, metadata, delegations, withdrawals, redeemers, requiredSigners] = await Promise.all([
    blockfrostFetch<TransactionResponse>(`/api/v0/txs/${encodeURIComponent(hash)}`),
    optionalFetch<TransactionUTXOsResponse>(`/api/v0/txs/${encodeURIComponent(hash)}/utxos`),
    optionalFetch<TransactionMetadataResponse[]>(`/api/v0/txs/${encodeURIComponent(hash)}/metadata`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/delegations`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/withdrawals`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/redeemers`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/required_signers`),
  ]);

  els.resultTitle.textContent = "Transaction";
  setResultMeta(`${formatDate(tx.block_time)} | block ${formatInteger(tx.block_height)}`);
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Hash", html(hashButton(tx.hash))],
      ["Block", html(blockButton(tx.block))],
      ["Slot", formatInteger(tx.slot)],
      ["Index", formatInteger(tx.index)],
      ["Fee", formatAda(tx.fees)],
      ["Deposit", formatAda(tx.deposit)],
      ["Size", `${formatInteger(tx.size)} bytes`],
      ["Valid contract", tx.valid_contract ? "yes" : "no"],
      ["Invalid before", tx.invalid_before ?? "-"],
      ["Invalid hereafter", tx.invalid_hereafter ?? "-"],
    ])}
    <div class="split-grid">
      ${smallStat("UTxOs", tx.utxo_count)}
      ${smallStat("Delegations", tx.delegation_count)}
      ${smallStat("Stake certs", tx.stake_cert_count)}
      ${smallStat("Withdrawals", tx.withdrawal_count)}
      ${smallStat("Pool updates", tx.pool_update_count)}
      ${smallStat("Redeemers", tx.redeemer_count)}
    </div>
    <section class="subsection">
      <h3>Output Amount</h3>
      ${amountList(tx.output_amount)}
    </section>
    ${
      utxos
        ? `
          <div class="two-column">
            <section class="subsection">
              <h3>Inputs</h3>
              ${renderInputList(utxos.inputs)}
            </section>
            <section class="subsection">
              <h3>Outputs</h3>
              ${renderOutputList(utxos.outputs)}
            </section>
          </div>
        `
        : ""
    }
    ${metadata && metadata.length > 0 ? renderJSONSection("Metadata", metadata) : ""}
    ${renderJSONCountSection("Delegations", delegations)}
    ${renderJSONCountSection("Withdrawals", withdrawals)}
    ${renderJSONCountSection("Redeemers", redeemers)}
    ${renderJSONCountSection("Required Signers", requiredSigners)}
  `;
}

async function renderAddressLookup(address: string): Promise<void> {
  const [utxos, txs] = await Promise.all([
    blockfrostFetchPage<AddressUTXOResponse>(
      `/api/v0/addresses/${encodeURIComponent(address)}/utxos?count=25&page=1&order=desc`,
    ),
    blockfrostFetchPage<AddressTransactionResponse>(
      `/api/v0/addresses/${encodeURIComponent(address)}/transactions?count=25&page=1&order=desc`,
    ),
  ]);

  const totalAda = utxos.items.reduce((sum, item) => sum + lovelaceFromAmounts(item.amount), 0n);

  els.resultTitle.textContent = "Address";
  setResultMeta(`${formatInteger(utxos.items.length)} UTxOs | ${formatInteger(txs.items.length)} tx rows`);
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Address", html(copyField(address))],
      ["Visible ADA", formatAda(totalAda.toString())],
      ["UTxO page total", formatMaybeTotal(utxos.total)],
      ["Tx page total", formatMaybeTotal(txs.total)],
    ])}
    <div class="two-column">
      <section class="subsection">
        <h3>UTxOs</h3>
        ${renderAddressUTXOList(utxos.items)}
      </section>
      <section class="subsection">
        <h3>Transactions</h3>
        ${dataTable(
          ["Transaction", "Block", "Time"],
          txs.items.map((tx) => [
            html(txCell(tx.tx_hash)),
            html(blockButton(tx.block_height)),
            formatDate(tx.block_time),
          ]),
          "No transactions returned.",
        )}
      </section>
    </div>
  `;
}

async function renderAccountLookup(stakeAddress: string): Promise<void> {
  const [account, addresses, delegations, registrations, rewards] = await Promise.all([
    blockfrostFetch<AccountResponse>(`/api/v0/accounts/${encodeURIComponent(stakeAddress)}`),
    blockfrostFetchPage<AccountAssociatedAddressResponse>(
      `/api/v0/accounts/${encodeURIComponent(stakeAddress)}/addresses?count=20&page=1&order=desc`,
    ),
    blockfrostFetchPage<AccountDelegationHistoryResponse>(
      `/api/v0/accounts/${encodeURIComponent(stakeAddress)}/delegations?count=20&page=1&order=desc`,
    ),
    blockfrostFetchPage<AccountRegistrationHistoryResponse>(
      `/api/v0/accounts/${encodeURIComponent(stakeAddress)}/registrations?count=20&page=1&order=desc`,
    ),
    blockfrostFetchPage<AccountRewardHistoryResponse>(
      `/api/v0/accounts/${encodeURIComponent(stakeAddress)}/rewards?count=20&page=1&order=desc`,
    ),
  ]);

  els.resultTitle.textContent = "Stake Account";
  setResultMeta(account.active ? `Active since epoch ${account.active_epoch ?? "-"}` : "Inactive");
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Stake address", html(copyField(account.stake_address))],
      ["Controlled", formatAda(account.controlled_amount)],
      ["Withdrawable", formatAda(account.withdrawable_amount)],
      ["Rewards", formatAda(account.rewards_sum)],
      ["Withdrawals", formatAda(account.withdrawals_sum)],
      ["Pool", account.pool_id ? html(poolID(account.pool_id)) : "-"],
    ])}
    <div class="two-column">
      <section class="subsection">
        <h3>Payment Addresses</h3>
        ${dataTable(
          ["Address"],
          addresses.items.map((item) => [html(addressCell(item.address))]),
          "No associated addresses returned.",
        )}
      </section>
      <section class="subsection">
        <h3>Delegations</h3>
        ${renderSimpleRows(
          delegations.items.map((item) => [
            `Epoch ${formatInteger(item.active_epoch)}`,
            html(poolID(item.pool_id)),
            formatAda(item.amount),
          ]),
        )}
      </section>
    </div>
    ${renderSimpleSection(
      "Registrations",
      registrations.items.map((item) => [item.action, html(txCell(item.tx_hash))]),
    )}
    ${renderSimpleSection(
      "Rewards",
      rewards.items.map((item) => [`Epoch ${formatInteger(item.epoch)}`, formatAda(item.amount), html(poolID(item.pool_id))]),
    )}
  `;
}

function renderAssetResult(asset: AssetResponse): void {
  els.resultTitle.textContent = "Asset";
  setResultMeta(asset.fingerprint);
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Asset", html(copyField(asset.asset))],
      ["Policy", html(copyField(asset.policy_id))],
      ["Name hex", asset.asset_name ? html(copyField(asset.asset_name)) : "-"],
      ["Name", asset.asset_name_ascii || "-"],
      ["Quantity", formatIntegerString(asset.quantity)],
      ["Mint/burn", formatInteger(asset.mint_or_burn_count)],
      ["Initial mint", html(txCell(asset.initial_mint_tx_hash))],
    ])}
    ${asset.onchain_metadata ? renderJSONSection("On-chain Metadata", asset.onchain_metadata) : ""}
    ${asset.metadata ? renderJSONSection("Metadata", asset.metadata) : ""}
  `;
}

function renderDRepResult(drep: DRepResponse): void {
  els.resultTitle.textContent = "DRep";
  setResultMeta(drep.active ? `Active epoch ${formatInteger(drep.active_epoch)}` : "Inactive");
  els.resultContent.innerHTML = detailGrid([
    ["DRep ID", html(copyField(drep.drep_id))],
    ["Credential", html(copyField(drep.hex))],
    ["Registered", drep.registered ? "yes" : "no"],
    ["Script", drep.has_script ? "yes" : "no"],
    ["Epoch", formatInteger(drep.epoch)],
    ["Amount", formatAda(drep.amount)],
    ["Live stake", formatAda(drep.live_stake)],
  ]);
}

async function renderMetadataLookup(label: string): Promise<void> {
  const rows = await blockfrostFetchPage<MetadataTransactionJSONResponse>(
    `/api/v0/metadata/txs/labels/${encodeURIComponent(label)}?count=20&page=1&order=desc`,
  );

  els.resultTitle.textContent = `Metadata Label ${label}`;
  setResultMeta(`${formatInteger(rows.items.length)} rows`);
  els.resultContent.innerHTML =
    rows.items.length > 0
      ? rows.items
          .map(
            (row) => `
              <article class="metadata-card">
                <div class="row-title">
                  ${txCell(row.tx_hash)}
                </div>
                <pre>${escapeHtml(JSON.stringify(row.json_metadata, null, 2))}</pre>
              </article>
            `,
          )
          .join("")
      : `<p class="empty">No metadata rows returned.</p>`;
}

async function renderPoolsLookup(): Promise<void> {
  const pools = await blockfrostFetchPage<PoolExtendedResponse>(
    "/api/v0/pools/extended?count=25&page=1&order=desc",
  );
  state.pools = pools.items;
  state.poolTotal = pools.total;
  els.resultTitle.textContent = "Stake Pools";
  setResultMeta(`${formatInteger(pools.items.length)} visible | ${formatMaybeTotal(pools.total)} total`);
  els.resultContent.innerHTML = renderPoolList(pools.items);
  renderOverview();
}

async function renderPoolLookup(poolID: string): Promise<void> {
  els.resultTitle.textContent = "Stake Pool";
  setResultMeta("Loading");
  const pool = await findPool(poolID);
  if (!pool) {
    renderErrorResult("Stake Pool", "Pool not found in active extended pools.");
    return;
  }
  renderPoolResult(pool);
}

async function findPool(poolID: string): Promise<PoolExtendedResponse | undefined> {
  const cached = state.pools.find((pool) => pool.pool_id === poolID || pool.hex === poolID);
  if (cached) {
    return cached;
  }
  const count = 100;
  let page = 1;
  let total: number | undefined;
  do {
    const result = await blockfrostFetchPage<PoolExtendedResponse>(
      `/api/v0/pools/extended?count=${count}&page=${page}&order=desc`,
    );
    if (page === 1) {
      total = result.total;
    }
    mergePools(result.items, total);
    const match = result.items.find((pool) => pool.pool_id === poolID || pool.hex === poolID);
    if (match) {
      return match;
    }
    if (result.items.length === 0) {
      return undefined;
    }
    page += 1;
  } while (total === undefined || (page - 1) * count < total);
  return undefined;
}

function mergePools(pools: PoolExtendedResponse[], total?: number): void {
  const byID = new Map(state.pools.map((pool) => [pool.pool_id, pool]));
  for (const pool of pools) {
    byID.set(pool.pool_id, pool);
  }
  state.pools = Array.from(byID.values());
  if (total !== undefined) {
    state.poolTotal = total;
  }
  state.poolsUpdatedAt = Date.now();
}

function renderPoolResult(pool: PoolExtendedResponse): void {
  els.resultTitle.textContent = "Stake Pool";
  setResultMeta(`${formatAdaShort(pool.live_stake)} ADA live stake | ${formatInteger(pool.relays.length)} relays`);
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Pool ID", html(poolID(pool.pool_id))],
      ["Hex", html(copyField(pool.hex))],
      ["VRF key", html(copyField(pool.vrf_key))],
      ["Live stake", formatAda(pool.live_stake)],
      ["Active stake", formatAda(pool.active_stake)],
      ["Declared pledge", formatAda(pool.declared_pledge)],
      ["Fixed cost", formatAda(pool.fixed_cost)],
      ["Margin", formatPercent(pool.margin_cost)],
    ])}
    <section class="subsection">
      <h3>Relays</h3>
      ${renderRelayList(pool.relays)}
    </section>
  `;
}

function renderRelayList(poolRelays: PoolExtendedResponse["relays"]): string {
  return dataTable(
    ["IPv4", "IPv6", "DNS", "Port"],
    poolRelays.map((relay) => [
      relay.ipv4 ?? "-",
      relay.ipv6 ?? "-",
      relay.dns ?? "-",
      relay.port === null ? "-" : formatInteger(relay.port),
    ]),
    "No relays returned.",
  );
}

function renderBlockResult(block: BlockResponse): void {
  state.selectedBlockHash = block.hash;
  renderRecentBlocks();
  els.resultTitle.textContent = `Block #${formatInteger(block.height)}`;
  setResultMeta(`${formatDate(block.time)} | ${formatInteger(block.confirmations)} confirmations`);
  els.resultContent.innerHTML = `
    <article class="block-detail-page">
      <div class="block-hero">
        <div class="block-hero-main">
          <span>Selected block</span>
          <strong>#${formatInteger(block.height)}</strong>
          ${copyField(block.hash)}
        </div>
        <div class="block-hero-actions">
          ${block.previous_block ? `<span>Previous ${blockButton(block.previous_block)}</span>` : ""}
          ${block.next_block ? `<span>Next ${blockButton(block.next_block)}</span>` : ""}
        </div>
      </div>
      <div class="split-grid">
        ${smallStat("Transactions", block.tx_count)}
        ${smallStat("Confirmations", block.confirmations)}
        ${smallStat("Size bytes", block.size)}
        ${smallStat("Slot", block.slot)}
        ${smallStat("Epoch", block.epoch)}
        ${smallStat("Epoch slot", block.epoch_slot)}
      </div>
      ${detailGrid([
        ["Hash", html(copyField(block.hash))],
        ["Slot leader", html(poolID(block.slot_leader))],
        ["Output", block.output ? formatAda(block.output) : "-"],
        ["Fees", block.fees ? formatAda(block.fees) : "-"],
        ["Time", formatDate(block.time)],
        ["Previous", block.previous_block ? html(blockCell(block.previous_block)) : "-"],
        ["Next", block.next_block ? html(blockCell(block.next_block)) : "-"],
      ])}
    </article>
  `;
}

function renderOverview(): void {
  els.dingoStatus.className = `status ${state.health}`;
  if (state.loading || state.health === "checking") {
    els.dingoStatus.textContent = "Dingo: checking";
  } else if (state.health === "ready") {
    els.dingoStatus.textContent = "Dingo: ready";
  } else {
    els.dingoStatus.textContent = "Dingo: offline";
  }
  els.refreshButton.disabled = state.loading;

  const block = state.latestBlock;
  const epoch = state.epoch;
  const network = state.network;
  const genesis = state.genesis;

  els.metricGrid.innerHTML = [
    metricCard("Tip", block ? `#${formatInteger(block.height)}` : "-", block ? `Slot ${formatInteger(block.slot)}` : "-"),
    metricCard("Epoch", epoch ? formatInteger(epoch.epoch) : block ? formatInteger(block.epoch) : "-", epoch ? `${formatInteger(epoch.block_count)} blocks` : "-"),
    metricCard("Latest Tx", block ? formatInteger(block.tx_count) : "-", block ? `${formatInteger(block.size)} bytes` : "-"),
    metricCard(
      "Supply",
      state.networkLoading && !network ? "Loading" : network ? `${formatAdaShort(network.supply.circulating)} ADA` : "-",
      state.networkLoading
        ? "Updating"
        : network
          ? `Live ${formatAdaShort(network.stake.live)} ADA | ${formatUpdatedAt(state.networkUpdatedAt)}`
          : "-",
    ),
    metricCard(
      "Pools",
      state.poolsLoading && state.poolTotal === undefined
        ? "Loading"
        : state.poolTotal !== undefined
          ? formatInteger(state.poolTotal)
          : "-",
      state.poolsLoading
        ? "Updating"
        : state.pools.length > 0
          ? `${state.pools.length} sampled | ${formatUpdatedAt(state.poolsUpdatedAt)}`
          : "-",
    ),
    metricCard("Network Magic", genesis ? formatInteger(genesis.network_magic) : "-", genesis ? `${formatInteger(genesis.slot_length)}s slots` : "-"),
  ].join("");

  renderRecentBlocks();
}

function renderRecentBlocks(): void {
  els.recentBlocksMeta.textContent = state.recentBlocksLoading
    ? `${formatInteger(state.recentBlocks.length)} loaded`
    : state.recentBlocks.length > 0
      ? `${formatInteger(state.recentBlocks.length)} rows`
      : "-";
  if (state.recentBlocksLoading && state.recentBlocks.length === 0) {
    els.recentBlocksList.innerHTML = `<div class="loading-bar"></div>`;
    return;
  }
  els.recentBlocksList.innerHTML = dataTable(
    ["Height", "Hash", "Slot", "Txs", "Leader"],
    state.recentBlocks.map((block) => [
      html(blockCell(block.height, `#${formatInteger(block.height)}`)),
      html(blockCell(block.hash)),
      formatInteger(block.slot),
      formatInteger(block.tx_count),
      html(poolID(block.slot_leader)),
    ]),
    "No recent blocks loaded.",
    {
      rowAttributes: state.recentBlocks.map(
        (block) => `data-row-block-id="${escapeHtml(block.hash)}" title="Open block details"`,
      ),
      rowClassName: "clickable-row-cell",
      rowClassNames: state.recentBlocks.map((block) =>
        block.hash === state.selectedBlockHash ? "selected-row-cell" : "",
      ),
    },
  );
}

function renderBlockTransactionsLoading(): void {
  els.blockTxTitle.textContent = state.blockTxPanelTitle;
  els.blockTxCount.textContent = "Loading";
  els.blockTxList.innerHTML = `<div class="loading-bar"></div>`;
}

function renderBlockTransactions(): void {
  els.blockTxTitle.textContent = state.blockTxPanelTitle;
  els.blockTxCount.textContent =
    state.blockTxHashes.length > 0 ? `${formatInteger(state.blockTxHashes.length)} hashes` : "-";
  if (state.blockTxHashes.length === 0) {
    els.blockTxList.innerHTML = `<p class="empty">No transactions returned for this block.</p>`;
    return;
  }
  const detailByHash = new Map(state.blockTxs.map((tx) => [tx.hash, tx]));
  els.blockTxList.innerHTML = dataTable(
    ["Transaction", "Fee", "UTxOs"],
    state.blockTxHashes.slice(0, 12).map((hash) => {
      const tx = detailByHash.get(hash);
      return [
        html(txCell(hash)),
        tx ? formatAda(tx.fees) : "-",
        tx ? formatInteger(tx.utxo_count) : "Pending",
      ];
    }),
    "No transactions returned for this block.",
  );
}

function renderBlockTransactionsError(message: string): void {
  els.blockTxTitle.textContent = state.blockTxPanelTitle;
  els.blockTxCount.textContent = "Error";
  els.blockTxList.innerHTML = `<p class="error-text">${escapeHtml(message)}</p>`;
}

function renderBlockTransactionsUnavailable(block: BlockResponse): void {
  els.blockTxTitle.textContent = state.blockTxPanelTitle;
  els.blockTxCount.textContent = `${formatInteger(block.tx_count)} txs`;
  els.blockTxList.innerHTML = `<p class="empty">Transaction hashes are only available for the latest block from this API.</p>`;
}

function blockTransactionsTitle(block: BlockResponse): string {
  return `Block #${formatInteger(block.height)} Transactions`;
}

async function blockfrostFetch<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(await responseErrorMessage(response));
  }
  return (await response.json()) as T;
}

async function blockfrostFetchPage<T>(path: string): Promise<{ items: T[]; total?: number }> {
  const response = await fetch(path, {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(await responseErrorMessage(response));
  }
  const totalHeader = response.headers.get("X-Pagination-Count-Total");
  const total = totalHeader ? Number(totalHeader) : undefined;
  return {
    items: (await response.json()) as T[],
    total: Number.isFinite(total) ? total : undefined,
  };
}

async function optionalFetch<T>(path: string): Promise<T | undefined> {
  try {
    return await blockfrostFetch<T>(path);
  } catch {
    return undefined;
  }
}

async function responseErrorMessage(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as Partial<ErrorResponse>;
    if (body.message || body.error) {
      return `${response.status} ${body.error ?? "Error"}: ${body.message ?? ""}`.trim();
    }
  } catch {
    return `${response.status} ${response.statusText}`;
  }
  return `${response.status} ${response.statusText}`;
}

function setResultLoading(label: string): void {
  els.resultTitle.textContent = label;
  setResultMeta("Loading");
  els.resultContent.innerHTML = `<div class="loading-bar"></div>`;
}

function renderErrorResult(title: string, message: string): void {
  els.resultTitle.textContent = title;
  setResultMeta("Request failed");
  els.resultContent.innerHTML = `<p class="error-text">${escapeHtml(message)}</p>`;
}

async function copyToClipboard(value: string, button: HTMLButtonElement): Promise<void> {
  if (!value) {
    return;
  }
  const originalTitle = button.title || "Copy";
  const originalLabel = button.getAttribute("aria-label") || originalTitle;
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(value);
    } else {
      fallbackCopy(value);
    }
    button.title = "Copied";
    button.setAttribute("aria-label", "Copied");
    button.classList.add("copied");
    button.disabled = true;
    window.setTimeout(() => {
      button.title = originalTitle;
      button.setAttribute("aria-label", originalLabel);
      button.classList.remove("copied");
      button.disabled = false;
    }, 900);
  } catch {
    button.title = "Copy failed";
    button.setAttribute("aria-label", "Copy failed");
    button.classList.add("failed");
    window.setTimeout(() => {
      button.title = originalTitle;
      button.setAttribute("aria-label", originalLabel);
      button.classList.remove("failed");
    }, 900);
  }
}

function fallbackCopy(value: string): void {
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.append(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  textarea.remove();
  if (!copied) {
    throw new Error("copy command failed");
  }
}

function setResultMeta(value: string): void {
  els.resultMeta.textContent = value;
}

function metricCard(label: string, value: string, subvalue: string): string {
  return `
    <article class="metric-card">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
      <small>${escapeHtml(subvalue)}</small>
    </article>
  `;
}

function smallStat(label: string, value: number): string {
  return `
    <article class="small-stat">
      <span>${escapeHtml(label)}</span>
      <strong>${formatInteger(value)}</strong>
    </article>
  `;
}

function detailGrid(rows: Array<[string, RenderContent]>): string {
  return `
    <dl class="detail-grid">
      ${rows
        .map(
          ([label, value]) => `
            <dt>${escapeHtml(label)}</dt>
            <dd>${renderContent(value)}</dd>
          `,
        )
        .join("")}
    </dl>
  `;
}

function copyButton(value: string, label = "Copy"): string {
  return `
    <button
      class="copy-button"
      type="button"
      data-copy="${escapeHtml(value)}"
      title="${escapeHtml(label)}"
      aria-label="${escapeHtml(label)}"
    >
      <span class="copy-icon" aria-hidden="true"></span>
    </button>
  `;
}

function copyField(value: string, display = value, className = "mono break"): string {
  return `
    <span class="copy-field">
      <span class="${escapeHtml(className)}">${escapeHtml(display)}</span>
      ${copyButton(value)}
    </span>
  `;
}

function amountList(amounts: Amount[]): string {
  if (amounts.length === 0) {
    return `<p class="empty">No amounts returned.</p>`;
  }
  return `
    <div class="amount-list">
      ${amounts
        .map(
          (amount) => `
            <div>
              <span>${amount.unit === "lovelace" ? "ADA" : assetCell(amount.unit)}</span>
              <strong>${escapeHtml(formatAmount(amount))}</strong>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

function dataTable(
  headers: string[],
  rows: RenderContent[][],
  emptyMessage: string,
  options?: { rowAttributes?: string[]; rowClassName?: string; rowClassNames?: string[] },
): string {
  if (rows.length === 0) {
    return `<p class="empty">${escapeHtml(emptyMessage)}</p>`;
  }
  return `
    <div class="data-table" style="--table-cols: ${headers.length}">
      ${headers.map((header) => `<div class="table-cell table-head">${escapeHtml(header)}</div>`).join("")}
      ${rows
        .map((row, rowIndex) => {
          const rowAttributes = options?.rowAttributes?.[rowIndex] ?? "";
          const rowClass = [options?.rowClassName, options?.rowClassNames?.[rowIndex]]
            .filter((value) => value && value.length > 0)
            .join(" ");
          const classAttribute = rowClass ? ` ${escapeHtml(rowClass)}` : "";
          return row
            .map(
              (cell) =>
                `<div class="table-cell${classAttribute}"${rowAttributes ? ` ${rowAttributes}` : ""}>${renderContent(cell)}</div>`,
            )
            .join("");
        })
        .join("")}
    </div>
  `;
}

function actionCell(content: string, copyValue: string): string {
  return `
    <span class="cell-actions">
      ${content}
      ${copyButton(copyValue)}
    </span>
  `;
}

function txCell(hash: string): string {
  return actionCell(txButton(hash), hash);
}

function assetButton(assetID: string): string {
  return `
    <button class="link-button mono" type="button" data-asset-id="${escapeHtml(assetID)}">
      ${escapeHtml(shortHash(assetID, 10))}
    </button>
  `;
}

function assetCell(assetID: string): string {
  return actionCell(assetButton(assetID), assetID);
}

function addressButton(address: string): string {
  return `
    <button class="link-button mono" type="button" data-address="${escapeHtml(address)}">
      ${escapeHtml(shortAddress(address))}
    </button>
  `;
}

function addressCell(address: string): string {
  return actionCell(addressButton(address), address);
}

function renderInputList(inputs: TransactionInputResponse[]): string {
  return dataTable(
    ["Address", "Input", "Amount"],
    inputs.map((input) => [
      html(addressCell(input.address)),
      html(`${txCell(input.tx_hash)} #${formatInteger(input.output_index)}`),
      html(formatAmountSummary(input.amount)),
    ]),
    "No inputs returned.",
  );
}

function renderOutputList(outputs: TransactionOutputResponse[]): string {
  return dataTable(
    ["Address", "Index", "Amount", "Status"],
    outputs.map((output) => [
      html(addressCell(output.address)),
      `#${formatInteger(output.output_index)}`,
      html(formatAmountSummary(output.amount)),
      output.consumed_by_tx ? html(`Spent by ${txCell(output.consumed_by_tx)}`) : "Unspent",
    ]),
    "No outputs returned.",
  );
}

function renderAddressUTXOList(utxos: AddressUTXOResponse[]): string {
  return dataTable(
    ["Output", "Amount", "Block"],
    utxos.map((utxo) => [
      html(`${txCell(utxo.tx_hash)} #${formatInteger(utxo.output_index)}`),
      html(formatAmountSummary(utxo.amount)),
      html(blockCell(utxo.block)),
    ]),
    "No UTxOs returned.",
  );
}

function renderPoolList(pools: PoolExtendedResponse[]): string {
  return dataTable(
    ["Pool", "Live stake", "Active stake", "Pledge", "Margin", "Relays"],
    pools.map((pool) => [
      html(poolID(pool.pool_id)),
      formatAda(pool.live_stake),
      formatAda(pool.active_stake),
      formatAda(pool.declared_pledge),
      formatPercent(pool.margin_cost),
      formatInteger(pool.relays.length),
    ]),
    "No pools returned.",
  );
}

function renderSimpleSection(title: string, rows: RenderContent[][]): string {
  return `
    <section class="subsection">
      <h3>${escapeHtml(title)}</h3>
      ${renderSimpleRows(rows)}
    </section>
  `;
}

function renderSimpleRows(rows: RenderContent[][]): string {
  if (rows.length === 0) {
    return `<p class="empty">No rows returned.</p>`;
  }
  return rows
    .map(
      (row) => `
        <article class="row-card">
          ${row.map((item) => `<span>${renderContent(item)}</span>`).join("")}
        </article>
      `,
    )
    .join("");
}

function renderJSONSection(title: string, value: unknown): string {
  return `
    <section class="subsection">
      <h3>${escapeHtml(title)}</h3>
      <pre>${escapeHtml(JSON.stringify(value, null, 2))}</pre>
    </section>
  `;
}

function renderJSONCountSection(title: string, rows: unknown[] | undefined): string {
  if (!rows || rows.length === 0) {
    return "";
  }
  return renderJSONSection(title, rows);
}

function txButton(hash: string): string {
  return `
    <button class="link-button mono" type="button" data-tx-hash="${escapeHtml(hash)}">
      ${escapeHtml(shortHash(hash))}
    </button>
  `;
}

function hashButton(hash: string): string {
  return copyField(hash);
}

function blockButton(blockID: string | number): string {
  const value = String(blockID);
  return `
    <button class="link-button mono" type="button" data-block-id="${escapeHtml(value)}">
      ${escapeHtml(shortHash(value))}
    </button>
  `;
}

function blockCell(blockID: string | number, display?: string): string {
  const value = String(blockID);
  return actionCell(
    `
      <button class="link-button mono" type="button" data-block-id="${escapeHtml(value)}">
        ${escapeHtml(display ?? shortHash(value))}
      </button>
    `,
    value,
  );
}

function poolID(pool: string): string {
  return actionCell(
    `<button class="link-button mono" type="button" data-pool-id="${escapeHtml(pool)}">${escapeHtml(shortHash(pool))}</button>`,
    pool,
  );
}

function formatAmountSummary(amounts: Amount[]): string {
  const ada = amounts.find((amount) => amount.unit === "lovelace");
  const assets = amounts.filter((amount) => amount.unit !== "lovelace");
  const parts: string[] = [];
  if (ada) {
    parts.push(escapeHtml(formatAda(ada.quantity)));
  }
  if (assets.length > 0) {
    const visibleAssets = assets.slice(0, 2).map((amount) => assetCell(amount.unit));
    const remaining = assets.length - visibleAssets.length;
    parts.push(
      [
        visibleAssets.join(" + "),
        remaining > 0 ? escapeHtml(`+ ${formatInteger(remaining)} asset${remaining === 1 ? "" : "s"}`) : "",
      ]
        .filter(Boolean)
        .join(" "),
    );
  }
  return parts.length > 0 ? parts.join(" + ") : "-";
}

function formatAmount(amount: Amount): string {
  if (amount.unit === "lovelace") {
    return formatAda(amount.quantity);
  }
  return formatIntegerString(amount.quantity);
}

function lovelaceFromAmounts(amounts: Amount[]): bigint {
  const lovelace = amounts.find((amount) => amount.unit === "lovelace")?.quantity;
  if (!lovelace) {
    return 0n;
  }
  try {
    return BigInt(lovelace);
  } catch {
    return 0n;
  }
}

function formatAda(lovelace: string): string {
  try {
    const value = BigInt(lovelace);
    const whole = value / 1_000_000n;
    const fraction = value % 1_000_000n;
    if (fraction === 0n) {
      return `${formatIntegerString(whole.toString())} ADA`;
    }
    const fractionText = fraction.toString().padStart(6, "0").replace(/0+$/, "");
    return `${formatIntegerString(whole.toString())}.${fractionText} ADA`;
  } catch {
    return `${lovelace} lovelace`;
  }
}

function formatAdaShort(lovelace: string): string {
  try {
    const ada = Number(BigInt(lovelace) / 1_000_000n);
    if (ada >= 1_000_000_000) {
      return `${(ada / 1_000_000_000).toFixed(2)}B`;
    }
    if (ada >= 1_000_000) {
      return `${(ada / 1_000_000).toFixed(2)}M`;
    }
    return formatInteger(ada);
  } catch {
    return "-";
  }
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

function formatInteger(value: number): string {
  return new Intl.NumberFormat().format(value);
}

function formatIntegerString(value: string): string {
  try {
    return new Intl.NumberFormat().format(BigInt(value));
  } catch {
    return value;
  }
}

function formatMaybeTotal(value: number | undefined): string {
  return value === undefined ? "unknown" : formatInteger(value);
}

function formatDate(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "-";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(new Date(seconds * 1000));
}

function formatUpdatedAt(timestamp: number | undefined): string {
  if (!timestamp) {
    return "not updated";
  }
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(timestamp));
}

function shortHash(value: string, edge = 12): string {
  if (value.length <= edge * 2 + 3) {
    return value;
  }
  return `${value.slice(0, edge)}...${value.slice(-edge)}`;
}

function shortAddress(value: string): string {
  if (value.length <= 30) {
    return value;
  }
  return `${value.slice(0, 16)}...${value.slice(-12)}`;
}

function escapeHtml(value: unknown): string {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderContent(value: RenderContent): string {
  if (typeof value === "object" && value !== null && "html" in value) {
    return value.html;
  }
  return escapeHtml(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
