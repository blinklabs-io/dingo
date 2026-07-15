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

type ProtocolParamsResponse = {
  epoch: number;
  min_fee_a: number;
  min_fee_b: number;
  max_block_size: number;
  max_tx_size: number;
  max_block_header_size: number;
  key_deposit: string;
  pool_deposit: string;
  e_max: number;
  n_opt: number;
  a0: number;
  rho: number;
  tau: number;
  decentralisation_param: number;
  protocol_major_ver: number;
  protocol_minor_ver: number;
  min_utxo: string;
  min_pool_cost: string;
  nonce: string;
  coins_per_utxo_size: string | null;
  coins_per_utxo_word: string;
  price_mem: number | null;
  price_step: number | null;
  max_tx_ex_mem: string | null;
  max_tx_ex_steps: string | null;
  max_block_ex_mem: string | null;
  max_block_ex_steps: string | null;
  max_val_size: string | null;
  collateral_percent: number | null;
  max_collateral_inputs: number | null;
  gov_action_deposit?: string | null;
  drep_deposit?: string | null;
};

type NetworkEraResponse = {
  era?: string;
  start: {
    time: number;
    slot: number;
    epoch: number;
  };
  end: {
    time: number;
    slot: number;
    epoch: number;
  } | null;
  parameters: {
    epoch_length: number;
    slot_length: number;
    safe_zone: number;
  };
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

type TransactionCBORResponse = {
  cbor: string;
};

type TransactionMetadataCBORResponse = {
  label: string;
  cbor_metadata: string | null;
  metadata: string;
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
  onchain_metadata_standard: string | null;
  onchain_metadata_extra: string | null;
  metadata: unknown | null;
};

type AssetAddressResponse = {
  address: string;
  quantity: string;
};

type AccountResponse = {
  stake_address: string;
  active: boolean;
  active_epoch: number | null;
  controlled_amount: string;
  rewards_sum: string;
  withdrawals_sum: string;
  reserves_sum: string;
  treasury_sum: string;
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

type MetadataTransactionCBORResponse = {
  tx_hash: string;
  cbor_metadata: string | null;
  metadata: string;
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

type Tab =
  | "dashboard"
  | "results"
  | "block"
  | "tx"
  | "address"
  | "account"
  | "asset"
  | "epoch"
  | "network"
  | "drep"
  | "metadata"
  | "pools";

type SearchHit = {
  title: string;
  category: string;
  meta: string;
  tab: DetailTab;
  query: string;
  detail?: string;
};

type DetailTab = Exclude<Tab, "dashboard" | "results">;

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
  protocolParams?: ProtocolParamsResponse;
  eras: NetworkEraResponse[];
  pools: PoolExtendedResponse[];
  poolTotal?: number;
  poolsLoading: boolean;
  networkUpdatedAt?: number;
  poolsUpdatedAt?: number;
  activeTab: Tab;
  loading: boolean;
  hasUserResult: boolean;
};

const tabConfig: Record<Tab, { label: string; placeholder: string; emptySearch: boolean; visible: boolean }> = {
  dashboard: {
    label: "Dashboard",
    placeholder: "Paste tx/block hash, height, address, stake, pool, asset ID, drep, epoch:NN, metadata:NN",
    emptySearch: false,
    visible: true,
  },
  results: {
    label: "Search Results",
    placeholder: "Paste tx/block hash, height, address, stake, pool, asset ID, drep, epoch:NN, metadata:NN",
    emptySearch: false,
    visible: false,
  },
  block: {
    label: "Block",
    placeholder: "4342637 or 64-char block hash",
    emptySearch: false,
    visible: true,
  },
  tx: {
    label: "Transaction",
    placeholder: "64-char transaction hash",
    emptySearch: false,
    visible: true,
  },
  address: {
    label: "Address",
    placeholder: "addr1...",
    emptySearch: false,
    visible: true,
  },
  account: {
    label: "Stake",
    placeholder: "stake1...",
    emptySearch: false,
    visible: true,
  },
  asset: {
    label: "Asset",
    placeholder: "Policy ID plus asset name hex",
    emptySearch: false,
    visible: true,
  },
  epoch: {
    label: "Epoch",
    placeholder: "latest or epoch number",
    emptySearch: true,
    visible: true,
  },
  network: {
    label: "Network",
    placeholder: "Leave empty for network stats",
    emptySearch: true,
    visible: true,
  },
  drep: {
    label: "DRep",
    placeholder: "DRep bech32 ID or credential hex",
    emptySearch: false,
    visible: true,
  },
  metadata: {
    label: "Metadata",
    placeholder: "Metadata label",
    emptySearch: false,
    visible: true,
  },
  pools: {
    label: "Pools",
    placeholder: "Pool ID or empty for list",
    emptySearch: true,
    visible: true,
  },
};

const state: AppState = {
  health: "checking",
  recentBlocks: [],
  recentBlocksLoading: false,
  blockTxHashes: [],
  blockTxs: [],
  blockTxPanelTitle: "Block Transactions",
  eras: [],
  pools: [],
  poolsLoading: false,
  activeTab: "dashboard",
  loading: false,
  hasUserResult: false,
};

let refreshSequence = 0;
let blockTransactionSequence = 0;
let poolLoadCount = 0;

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
      <div class="brand">
        <div class="brand-mark" aria-hidden="true">D</div>
        <div class="brand-copy">
          <h1>Dingo Explorer</h1>
          <p id="networkLabel">Blockfrost-compatible API</p>
        </div>
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
              .filter(([, config]) => config.visible)
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
        <div class="metric-grid" id="metricGrid"></div>
        <div class="dashboard-activity" id="activityPanel"></div>
        <div class="dashboard-grid" id="dashboardDetails"></div>
        <div class="recent-blocks-wrap">
          <div class="section-heading">
            <h2>Recent Blocks</h2>
            <span id="recentBlocksMeta">-</span>
          </div>
          <div id="recentBlocksList" class="recent-blocks-list"></div>
        </div>
      </section>

      <section id="resultPanel" class="result-panel">
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
  resultPanel: byId<HTMLElement>("resultPanel"),
  recentBlocksMeta: byId("recentBlocksMeta"),
  recentBlocksList: byId("recentBlocksList"),
  metricGrid: byId("metricGrid"),
  activityPanel: byId("activityPanel"),
  dashboardDetails: byId("dashboardDetails"),
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
  void handleLookupSubmit();
});
els.queryInput.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") {
    return;
  }
  els.queryInput.value = "";
  const tab = state.activeTab === "results" ? "dashboard" : state.activeTab;
  activateTab(tab, "", false);
  void runActiveView();
});
els.queryInput.addEventListener("input", () => updateSearchButton());
for (const button of els.tabButtons) {
  button.addEventListener("click", () => {
    const tab = button.dataset.tab as Tab | undefined;
    if (tab) {
      activateTab(tab, undefined, false);
      void runActiveView();
    }
  });
}
window.addEventListener("popstate", () => {
  const route = readRouteState();
  activateTab(route.tab, route.query, false);
  void runActiveView(false);
});

const initialRoute = readRouteState();
activateTab(initialRoute.tab, initialRoute.query, false);
renderOverview();
void refreshOverview();
void runActiveView(false);

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
  const showOverviewPanel = state.activeTab === "dashboard";
  els.overviewPanel.hidden = !showOverviewPanel;
  els.resultPanel.hidden = state.activeTab === "dashboard";
  els.latestPanel.hidden = state.activeTab !== "block";
}

function updateSearchButton(): void {
  if (state.activeTab === "pools" && els.queryInput.value.trim() === "") {
    els.searchButton.textContent = "Load Pools";
    return;
  }
  if (state.activeTab === "network" && els.queryInput.value.trim() === "") {
    els.searchButton.textContent = "Load Network";
    return;
  }
  if (state.activeTab === "epoch" && els.queryInput.value.trim() === "") {
    els.searchButton.textContent = "Latest Epoch";
    return;
  }
  els.searchButton.textContent = "Search";
}

function handleEntityClick(target: HTMLElement): boolean {
  let detailPromise: Promise<void> | undefined;
  const jump = target.closest<HTMLElement>("[data-jump-tab]");
  if (jump) {
    const tab = jump.dataset.jumpTab ?? null;
    if (!isDetailTab(tab)) {
      return false;
    }
    detailPromise = openDetail(tab, jump.dataset.jumpQuery ?? "");
  } else {
    const txHash = target.closest<HTMLElement>("[data-tx-hash]")?.dataset.txHash;
    const blockID = target.closest<HTMLElement>("[data-block-id]")?.dataset.blockId;
    const address = target.closest<HTMLElement>("[data-address]")?.dataset.address;
    const account = target.closest<HTMLElement>("[data-account]")?.dataset.account;
    const asset = target.closest<HTMLElement>("[data-asset-id]")?.dataset.assetId;
    const poolID = target.closest<HTMLElement>("[data-pool-id]")?.dataset.poolId;
    const epoch = target.closest<HTMLElement>("[data-epoch-id]")?.dataset.epochId;
    const rowBlockID = target.closest<HTMLElement>("[data-row-block-id]")?.dataset.rowBlockId;
    if (txHash) {
      detailPromise = openDetail("tx", txHash);
    } else if (blockID) {
      detailPromise = openDetail("block", blockID);
    } else if (address) {
      detailPromise = openDetail("address", address);
    } else if (account) {
      detailPromise = openDetail("account", account);
    } else if (asset) {
      detailPromise = openDetail("asset", asset);
    } else if (poolID) {
      detailPromise = openDetail("pools", poolID);
    } else if (epoch) {
      detailPromise = openDetail("epoch", epoch);
    } else if (rowBlockID) {
      detailPromise = openDetail("block", rowBlockID);
    } else {
      return false;
    }
  }
  void detailPromise?.then(() => {
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
  let tab: Tab = isTab(tabParam) ? tabParam : "dashboard";
  if (tabParam === "search") {
    tab = params.get("q") ? "results" : "dashboard";
  }
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
  if (tab === "dashboard") {
    return false;
  }
  if (tab === "results") {
    return query.length > 0;
  }
  return tabConfig[tab].emptySearch || query.length > 0;
}

function isTab(value: string | null): value is Tab {
  return value !== null && Object.hasOwn(tabConfig, value);
}

function isDetailTab(value: string | null): value is DetailTab {
  return isTab(value) && value !== "dashboard" && value !== "results";
}

async function refreshOverview(): Promise<void> {
  const sequence = refreshSequence + 1;
  refreshSequence = sequence;
  state.loading = true;
  state.health = "checking";
  state.recentBlocksLoading = true;
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

  const blockPromise = fetchLatestBlock()
    .then((block) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.latestBlock = block;
      state.recentBlocks = [block];
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

  void blockfrostFetch<ProtocolParamsResponse>("/api/v0/epochs/latest/parameters")
    .then((params) => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.protocolParams = params;
    })
    .catch(() => {
      if (!isCurrentRefresh(sequence)) {
        return;
      }
      state.protocolParams = undefined;
    })
    .finally(() => {
      if (isCurrentRefresh(sequence)) {
        renderOverview();
      }
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

function beginPoolLoad(): void {
  poolLoadCount += 1;
  state.poolsLoading = true;
}

function endPoolLoad(): void {
  poolLoadCount = Math.max(0, poolLoadCount - 1);
  state.poolsLoading = poolLoadCount > 0;
}

async function fetchLatestBlock(): Promise<BlockResponse> {
  try {
    return await blockfrostFetch<BlockResponse>("/api/v0/blocks/latest");
  } catch (latestError) {
    const metricsHeight = await fetchMetricNumber("cardano_node_metrics_blockNum_int");
    if (metricsHeight === undefined) {
      throw latestError;
    }
    const block = await findLatestIndexedBlock(Math.trunc(metricsHeight));
    if (!block) {
      throw latestError;
    }
    return block;
  }
}

async function fetchMetricNumber(metricName: string): Promise<number | undefined> {
  try {
    const response = await fetch("/metrics", {
      headers: {
        Accept: "text/plain",
      },
    });
    if (!response.ok) {
      return undefined;
    }
    const metrics = await response.text();
    const pattern = new RegExp(`^${escapeRegExp(metricName)}(?:\\{[^}]*\\})?\\s+([^\\s]+)`, "m");
    const value = metrics.match(pattern)?.[1];
    if (!value) {
      return undefined;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

async function findLatestIndexedBlock(anchorHeight: number): Promise<BlockResponse | undefined> {
  if (!Number.isFinite(anchorHeight) || anchorHeight < 0) {
    return undefined;
  }
  let latest: BlockResponse | undefined;
  const scanAhead = 30;
  for (let height = anchorHeight; height <= anchorHeight + scanAhead; height += 1) {
    const block = await optionalFetch<BlockResponse>(`/api/v0/blocks/${height}`);
    if (!block) {
      if (latest) {
        return latest;
      }
      break;
    }
    latest = block;
  }
  if (latest) {
    return latest;
  }
  const scanBack = 120;
  for (let height = anchorHeight - 1; height >= Math.max(0, anchorHeight - scanBack); height -= 1) {
    const block = await optionalFetch<BlockResponse>(`/api/v0/blocks/${height}`);
    if (block) {
      return block;
    }
  }
  return undefined;
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
    const hashes = await fetchBlockTransactionHashes(block);
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
  await loadLatestBlockTransactions(block);
}

function isCurrentBlockTransactionRequest(sequence: number, block: BlockResponse): boolean {
  return sequence === blockTransactionSequence && state.selectedBlockHash === block.hash;
}

async function fetchBlockTransactionHashes(block: BlockResponse): Promise<string[]> {
  const candidates =
    state.latestBlock?.hash === block.hash
      ? ["latest", block.hash, String(block.height)]
      : [block.hash, String(block.height)];
  let lastError: unknown;
  for (const candidate of candidates) {
    try {
      return await blockfrostFetchStringArray(`/api/v0/blocks/${encodeURIComponent(candidate)}/txs`);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Block transactions are not available.");
}

async function blockfrostFetchStringArray(path: string): Promise<string[]> {
  const value = await blockfrostFetch<unknown>(path);
  if (Array.isArray(value) && value.every((item) => typeof item === "string")) {
    return value;
  }
  throw new Error("Dingo returned an unexpected transaction hash payload.");
}

async function handleLookupSubmit(): Promise<void> {
  const query = normalizeQuery("results", els.queryInput.value);
  els.queryInput.value = query;
  if (query) {
    await runSearch(query);
    return;
  }
  if (isDetailTab(state.activeTab) && tabConfig[state.activeTab].emptySearch) {
    await openDetail(state.activeTab, "");
    return;
  }
  els.queryInput.focus();
}

async function runActiveView(syncRoute = true): Promise<void> {
  const query = normalizeQuery(state.activeTab, els.queryInput.value);
  els.queryInput.value = query;
  if (state.activeTab === "dashboard") {
    if (syncRoute) {
      writeRouteState("dashboard", "", true);
    }
    clearResultState(tabConfig.dashboard.label);
    renderLayout();
    return;
  }
  if (state.activeTab === "results") {
    if (query) {
      await runSearch(query, syncRoute);
      return;
    }
    clearResultState(tabConfig.results.label);
    renderLayout();
    return;
  }
  if (shouldRunRoute(state.activeTab, query)) {
    await runDetailLookup(state.activeTab, query, syncRoute);
    return;
  }
  if (isDetailTab(state.activeTab)) {
    await renderDefaultDetailView(state.activeTab, syncRoute);
  }
}

async function renderDefaultDetailView(tab: DetailTab, syncRoute = true): Promise<void> {
  if (syncRoute) {
    writeRouteState(tab, "", true);
  }
  switch (tab) {
    case "block":
      state.hasUserResult = false;
      if (state.latestBlock) {
        renderBlockResult(state.latestBlock);
        await showSelectedBlockTransactions(state.latestBlock);
        return;
      }
      renderDetailHome("block", "", true);
      return;
    case "tx":
      await renderTransactionHome();
      return;
    case "address":
    case "account":
    case "asset":
    case "drep":
    case "metadata":
      renderDetailHome(tab);
      return;
    case "epoch":
    case "network":
    case "pools":
      await runDetailLookup(tab, "", false);
      return;
  }
}

async function renderTransactionHome(): Promise<void> {
  const sequence = blockTransactionSequence + 1;
  blockTransactionSequence = sequence;
  state.hasUserResult = true;
  state.selectedBlockHash = undefined;
  state.blockTxHashes = [];
  state.blockTxs = [];
  renderRecentBlocks();
  els.resultTitle.textContent = "Transactions";
  setResultMeta("Loading latest block sample");
  els.resultContent.innerHTML = `<div class="loading-bar"></div>`;

  try {
    const block = state.latestBlock ?? (await fetchLatestBlock());
    if (!state.latestBlock) {
      state.latestBlock = block;
      state.recentBlocks = [block];
      renderOverview();
    }
    const hashes = block.tx_count > 0 ? await fetchBlockTransactionHashes(block) : [];
    if (sequence !== blockTransactionSequence || state.activeTab !== "tx") {
      return;
    }
    state.blockTxHashes = hashes;
    const details = await Promise.allSettled(
      hashes.slice(0, 10).map((hash) => blockfrostFetch<TransactionResponse>(`/api/v0/txs/${hash}`)),
    );
    if (sequence !== blockTransactionSequence || state.activeTab !== "tx") {
      return;
    }
    state.blockTxs = details
      .filter((item): item is PromiseFulfilledResult<TransactionResponse> => item.status === "fulfilled")
      .map((item) => item.value);
    els.resultTitle.textContent = "Transactions";
    setResultMeta(`${formatInteger(hashes.length)} txs in latest block #${formatInteger(block.height)}`);
    els.resultContent.innerHTML = renderTransactionHomeResult(block, hashes, state.blockTxs);
  } catch (error) {
    if (sequence !== blockTransactionSequence || state.activeTab !== "tx") {
      return;
    }
    renderDetailHome(
      "tx",
      capabilityNote(`Latest transaction sample unavailable from Dingo: ${errorMessage(error)}`),
    );
  }
}

async function runSearch(query: string, syncRoute = true): Promise<void> {
  const normalized = normalizeQuery("results", query);
  if (!normalized) {
    els.queryInput.focus();
    return;
  }
  activateTab("results", normalized, false);
  state.hasUserResult = true;
  state.selectedBlockHash = undefined;
  blockTransactionSequence += 1;
  renderRecentBlocks();
  setResultLoading(tabConfig.results.label);
  try {
    const { hits, notes } = await performSmartSearch(normalized);
    if (hits.length === 1) {
      await openDetail(hits[0].tab, hits[0].query, syncRoute);
      return;
    }
    if (syncRoute) {
      writeRouteState("results", normalized);
    }
    els.resultTitle.textContent = "Search Results";
    setResultMeta(`${formatInteger(hits.length)} result${hits.length === 1 ? "" : "s"}`);
    els.resultContent.innerHTML = renderSearchResults(hits, notes);
  } catch (error) {
    if (syncRoute) {
      writeRouteState("results", normalized);
    }
    renderErrorResult("Search Results", errorMessage(error));
  }
}

async function openDetail(tab: DetailTab, query: string, syncRoute = true): Promise<void> {
  activateTab(tab, query, false);
  await runDetailLookup(tab, normalizeQuery(tab, query), syncRoute);
}

async function runDetailLookup(tab: DetailTab, query: string, syncRoute = true): Promise<void> {
  const config = tabConfig[tab];
  if (!query && !config.emptySearch) {
    clearResultState(config.label);
    els.queryInput.focus();
    return;
  }
  const validationError = validateQuery(tab, query);
  if (validationError) {
    if (syncRoute) {
      writeRouteState(tab, query);
    }
    renderErrorResult(config.label, validationError);
    els.queryInput.focus();
    return;
  }

  state.hasUserResult = true;
  if (syncRoute) {
    writeRouteState(tab, query);
  }
  if (tab !== "block") {
    state.selectedBlockHash = undefined;
    blockTransactionSequence += 1;
    renderRecentBlocks();
  }
  setResultLoading(config.label);
  try {
    switch (tab) {
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
        await renderAssetLookup(query);
        break;
      case "epoch":
        await renderEpochLookup(query);
        break;
      case "network":
        await renderNetworkLookup();
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
    case "epoch":
      return query === "" || query.toLowerCase() === "latest" || /^\d+$/.test(query)
        ? undefined
        : "Enter an epoch number or latest.";
    case "network":
      return query === "" ? undefined : "Leave the network query empty.";
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
    case "dashboard":
    case "results":
      return query.length > 0 ? undefined : "Paste an ID or use epoch:NN / metadata:NN.";
  }
}

async function performSmartSearch(query: string): Promise<{ hits: SearchHit[]; notes: string[] }> {
  const normalized = query.trim();
  const lower = normalized.toLowerCase();
  const hits: SearchHit[] = [];
  const notes: string[] = [];
  const seen = new Set<string>();
  const addHit = (hit: SearchHit): void => {
    const key = `${hit.tab}:${hit.query}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    hits.push(hit);
  };

  const tasks: Promise<void>[] = [];
  const metadataMatch = lower.match(/^(metadata|label):(\d+)$/);
  const epochMatch = lower.match(/^epoch:(latest|\d+)$/);

  if (metadataMatch) {
    const label = metadataMatch[2];
    tasks.push(
      optionalFetchPage<MetadataTransactionJSONResponse>(
        `/api/v0/metadata/txs/labels/${label}?count=1&page=1&order=desc`,
      ).then((rows) => {
        if (rows) {
          addHit({
            title: `Metadata label ${label}`,
            category: "metadata",
            meta: `${formatMaybeTotal(rows.total)} matching txs`,
            tab: "metadata",
            query: label,
          });
        }
      }),
    );
  }

  if (epochMatch) {
    const epochQuery = epochMatch[1];
    tasks.push(
      optionalEpochParams(epochQuery).then((params) => {
        if (params) {
          addHit({
            title: `Epoch ${formatInteger(params.epoch)}`,
            category: "epoch",
            meta: `protocol ${params.protocol_major_ver}.${params.protocol_minor_ver}`,
            tab: "epoch",
            query: String(params.epoch),
          });
        }
      }),
    );
  }

  if (/^[0-9a-fA-F]{64}$/.test(normalized)) {
    tasks.push(
      optionalFetch<TransactionResponse>(`/api/v0/txs/${normalized}`).then((tx) => {
        if (tx) {
          addHit({
            title: `Transaction ${shortHash(tx.hash)}`,
            category: "transaction",
            meta: `${formatDate(tx.block_time)} | block ${formatInteger(tx.block_height)}`,
            tab: "tx",
            query: tx.hash,
          });
        }
      }),
      optionalFetch<BlockResponse>(`/api/v0/blocks/${normalized}`).then((block) => {
        if (block) {
          addHit({
            title: `Block #${formatInteger(block.height)}`,
            category: "block",
            meta: `${formatDate(block.time)} | ${formatInteger(block.tx_count)} txs`,
            tab: "block",
            query: block.hash,
          });
        }
      }),
    );
  }

  if (/^\d+$/.test(normalized)) {
    tasks.push(
      optionalFetch<BlockResponse>(`/api/v0/blocks/${normalized}`).then((block) => {
        if (block) {
          addHit({
            title: `Block #${formatInteger(block.height)}`,
            category: "block",
            meta: `${formatDate(block.time)} | epoch ${formatInteger(block.epoch)}`,
            tab: "block",
            query: String(block.height),
          });
        }
      }),
      optionalEpochParams(normalized).then((params) => {
        if (params) {
          addHit({
            title: `Epoch ${formatInteger(params.epoch)}`,
            category: "epoch",
            meta: `protocol ${params.protocol_major_ver}.${params.protocol_minor_ver}`,
            tab: "epoch",
            query: String(params.epoch),
          });
        }
      }),
    );
  }

  if (normalized.startsWith("addr")) {
    tasks.push(
      optionalFetchPage<AddressUTXOResponse>(
        `/api/v0/addresses/${encodeURIComponent(normalized)}/utxos?count=1&page=1&order=desc`,
      ).then((rows) => {
        if (rows) {
          addHit({
            title: `Address ${shortAddress(normalized)}`,
            category: "address",
            meta: `${formatMaybeTotal(rows.total)} UTxOs`,
            tab: "address",
            query: normalized,
          });
        }
      }),
    );
  }

  if (normalized.startsWith("stake")) {
    tasks.push(
      optionalFetch<AccountResponse>(`/api/v0/accounts/${encodeURIComponent(normalized)}`).then((account) => {
        if (account) {
          addHit({
            title: `Stake ${shortAddress(account.stake_address)}`,
            category: "stake",
            meta: `${formatAda(account.controlled_amount)} controlled`,
            tab: "account",
            query: account.stake_address,
          });
        }
      }),
    );
  }

  const canBePool = normalized.startsWith("pool");
  if (canBePool) {
    tasks.push(
      findPool(normalized).then((pool) => {
        if (pool) {
          addHit({
            title: `Pool ${shortHash(pool.pool_id)}`,
            category: "pool",
            meta: `${formatAdaShort(pool.live_stake)} ADA live stake`,
            tab: "pools",
            query: pool.pool_id,
          });
        }
      }),
    );
  } else if (/^[0-9a-fA-F]{56}$/.test(normalized)) {
    notes.push("Pool hex lookup scans active pools and is available from the Pools tab.");
  }

  const canBeDRep = normalized.startsWith("drep") || /^[0-9a-fA-F]{56}$/.test(normalized);
  if (canBeDRep) {
    tasks.push(
      optionalFetch<DRepResponse>(`/api/v0/governance/dreps/${encodeURIComponent(normalized)}`).then((drep) => {
        if (drep) {
          addHit({
            title: `DRep ${shortHash(drep.drep_id)}`,
            category: "drep",
            meta: drep.active ? `${formatAda(drep.live_stake)} live stake` : "inactive",
            tab: "drep",
            query: drep.drep_id,
          });
        }
      }),
    );
  }

  if (/^[0-9a-fA-F]{56,}$/.test(normalized)) {
    tasks.push(
      optionalFetch<AssetResponse>(`/api/v0/assets/${normalized}`).then((asset) => {
        if (asset) {
          addHit({
            title: asset.asset_name_ascii || `Asset ${shortHash(asset.asset)}`,
            category: "asset",
            meta: `${formatIntegerString(asset.quantity)} supply`,
            tab: "asset",
            query: asset.asset,
            detail: asset.fingerprint,
          });
        }
      }),
    );
  } else if (normalized.startsWith("asset")) {
    notes.push("Asset fingerprint lookup is not exposed by Dingo's current Blockfrost endpoints; paste the asset ID instead.");
  }

  await Promise.allSettled(tasks);
  if (hits.length === 0 && notes.length === 0) {
    notes.push("No matching Dingo Blockfrost resource was found.");
  }
  return { hits, notes };
}

function renderDetailHome(tab: DetailTab, extraMarkup = "", allowRefreshFill = false): void {
  const spec = detailHomeSpec(tab);
  state.hasUserResult = !allowRefreshFill;
  state.selectedBlockHash = undefined;
  blockTransactionSequence += 1;
  if (tab === "block") {
    state.blockTxHashes = [];
    state.blockTxs = [];
    state.blockTxPanelTitle = "Block Transactions";
    renderBlockTransactions();
  }
  renderRecentBlocks();
  els.resultTitle.textContent = spec.title;
  setResultMeta(spec.meta);
  els.resultContent.innerHTML = `
    <section class="detail-home">
      <div class="detail-home-grid">
        <article class="detail-home-card detail-home-card-primary">
          <span>${escapeHtml(spec.eyebrow)}</span>
          <strong>${escapeHtml(spec.headline)}</strong>
          <small>${escapeHtml(spec.summary)}</small>
        </article>
        <article class="detail-home-card">
          <span>View Surface</span>
          ${compactGrid(spec.fields)}
        </article>
      </div>
      ${renderDetailHomeActions(tab)}
      ${extraMarkup}
    </section>
  `;
}

function detailHomeSpec(tab: DetailTab): {
  title: string;
  eyebrow: string;
  headline: string;
  summary: string;
  meta: string;
  fields: Array<[string, RenderContent]>;
} {
  switch (tab) {
    case "block":
      return {
        title: "Block",
        eyebrow: "Live block",
        headline: state.latestBlock ? `#${formatInteger(state.latestBlock.height)}` : "No block loaded",
        summary: state.latestBlock
          ? `${formatInteger(state.latestBlock.tx_count)} txs | slot ${formatInteger(state.latestBlock.slot)}`
          : "Waiting for indexed tip data.",
        meta: state.latestBlock ? "Latest block ready" : "Waiting for Dingo",
        fields: [
          ["Lookup", "Block height or 64-character hash"],
          ["Shows", "Leader, epoch, size, output, fees, adjacent blocks"],
          ["Transactions", "Loads from the block transaction endpoint when available"],
        ],
      };
    case "tx":
      return {
        title: "Transactions",
        eyebrow: "Transaction detail",
        headline: "No transaction selected",
        summary: state.latestBlock
          ? `Latest block #${formatInteger(state.latestBlock.height)} has ${formatInteger(state.latestBlock.tx_count)} txs.`
          : "Latest block sample not loaded.",
        meta: "No selection",
        fields: [
          ["Lookup", "64-character transaction hash"],
          ["Shows", "Fees, deposits, UTxOs, metadata, certificates, redeemers"],
          ["Default", "Latest block transaction sample"],
        ],
      };
    case "address":
      return {
        title: "Address",
        eyebrow: "Payment address",
        headline: "No address selected",
        summary: "Address detail is loaded after an addr1 payment address is selected.",
        meta: "No selection",
        fields: [
          ["Lookup", "addr1 payment address"],
          ["Shows", "Visible UTxOs, recent transactions, visible balance"],
          ["Endpoint", "/addresses/{address}/utxos and /transactions"],
        ],
      };
    case "account":
      return {
        title: "Stake Account",
        eyebrow: "Stake credential",
        headline: "No stake account selected",
        summary: "Stake detail is loaded after a stake1 account is selected.",
        meta: "No selection",
        fields: [
          ["Lookup", "stake1 reward account"],
          ["Shows", "Controlled amount, rewards, delegation, registrations"],
          ["Endpoint", "/accounts/{stake_address}"],
        ],
      };
    case "asset":
      return {
        title: "Asset",
        eyebrow: "Native asset",
        headline: "No asset selected",
        summary: "Asset detail is loaded by asset ID: policy ID plus asset name hex.",
        meta: "No selection",
        fields: [
          ["Lookup", "Policy ID plus asset name hex"],
          ["Shows", "Policy, name, quantity, initial mint, metadata"],
          ["Fingerprint", "Requires additional Dingo lookup surface"],
        ],
      };
    case "epoch":
      return {
        title: "Epoch",
        eyebrow: "Epoch detail",
        headline: state.epoch ? `Epoch ${formatInteger(state.epoch.epoch)}` : "Latest epoch",
        summary: "Epoch summary and protocol parameters load without a query.",
        meta: "Latest available",
        fields: [
          ["Lookup", "latest or epoch number"],
          ["Shows", "Epoch summary and protocol parameters"],
          ["Progress", epochProgressText()],
        ],
      };
    case "network":
      return {
        title: "Network",
        eyebrow: "Network detail",
        headline: networkName(state.genesis),
        summary: "Network supply, stake, genesis, era, and protocol state load without a query.",
        meta: "Latest available",
        fields: [
          ["Network", networkName(state.genesis)],
          ["Magic", state.genesis ? formatInteger(state.genesis.network_magic) : "-"],
          ["Epoch", state.epoch ? html(epochButton(state.epoch.epoch)) : "-"],
        ],
      };
    case "drep":
      return {
        title: "DRep",
        eyebrow: "Governance representative",
        headline: "No DRep selected",
        summary: "DRep detail is loaded by bech32 DRep ID or 56-character credential hex.",
        meta: "No selection",
        fields: [
          ["Lookup", "drep1 ID or credential hex"],
          ["Shows", "Registration, script flag, deposit amount, live stake"],
          ["Endpoint", "/governance/dreps/{drep_id}"],
        ],
      };
    case "metadata":
      return {
        title: "Metadata",
        eyebrow: "Transaction metadata",
        headline: "No label selected",
        summary: "Metadata rows are loaded by numeric metadata label.",
        meta: "No selection",
        fields: [
          ["Lookup", "Numeric metadata label"],
          ["Shows", "JSON rows and CBOR rows when available"],
          ["Endpoint", "/metadata/txs/labels/{label}"],
        ],
      };
    case "pools":
      return {
        title: "Stake Pools",
        eyebrow: "Pool detail",
        headline: state.poolTotal !== undefined ? `${formatMaybeTotal(state.poolTotal)} pools` : "Pool list",
        summary: "Active extended pool state loads without a query.",
        meta: state.pools.length > 0 ? `${formatInteger(state.pools.length)} cached` : "Latest available",
        fields: [
          ["Lookup", "pool1 ID, pool key hash, or empty"],
          ["Shows", "Live stake, active stake, pledge, margin, relays"],
          ["Cached", formatInteger(state.pools.length)],
        ],
      };
  }
}

function renderDetailHomeActions(tab: DetailTab): string {
  const actions: string[] = [];
  if (tab === "block") {
    if (state.latestBlock) {
      actions.push(quickJumpButton("block", String(state.latestBlock.height), `Latest #${formatInteger(state.latestBlock.height)}`));
    }
    actions.push(
      ...state.recentBlocks
        .filter((block) => block.hash !== state.latestBlock?.hash)
        .slice(0, 5)
        .map((block) => quickJumpButton("block", block.hash, `#${formatInteger(block.height)}`)),
    );
  } else if (tab === "tx") {
    actions.push(...state.blockTxHashes.slice(0, 6).map((hash) => quickJumpButton("tx", hash, shortHash(hash))));
    if (actions.length === 0 && state.latestBlock) {
      actions.push(quickJumpButton("block", String(state.latestBlock.height), `Block #${formatInteger(state.latestBlock.height)}`));
    }
  } else if (tab === "epoch" && state.epoch) {
    actions.push(quickJumpButton("epoch", String(state.epoch.epoch), `Epoch ${formatInteger(state.epoch.epoch)}`));
  } else if (tab === "pools") {
    actions.push(...state.pools.slice(0, 6).map((pool) => quickJumpButton("pools", pool.pool_id, shortHash(pool.pool_id))));
  }
  if (actions.length === 0) {
    return "";
  }
  return `
    <article class="detail-home-card">
      <span>Available Now</span>
      <div class="quick-actions">${actions.join("")}</div>
    </article>
  `;
}

function quickJumpButton(tab: DetailTab, query: string, label: string): string {
  return `
    <button
      class="quick-action"
      type="button"
      data-jump-tab="${escapeHtml(tab)}"
      data-jump-query="${escapeHtml(query)}"
    >
      ${escapeHtml(label)}
    </button>
  `;
}

function renderTransactionHomeResult(
  block: BlockResponse,
  hashes: string[],
  details: TransactionResponse[],
): string {
  const detailByHash = new Map(details.map((tx) => [tx.hash, tx]));
  return `
    <section class="detail-home">
      <div class="detail-home-grid">
        <article class="detail-home-card detail-home-card-primary">
          <span>Latest block transactions</span>
          <strong>${formatInteger(hashes.length)}</strong>
          <small>Block #${formatInteger(block.height)} | slot ${formatInteger(block.slot)}</small>
        </article>
        <article class="detail-home-card">
          <span>Block Context</span>
          ${compactGrid([
            ["Block", html(blockCell(block.height, `#${formatInteger(block.height)}`))],
            ["Hash", html(copyField(block.hash, shortHash(block.hash)))],
            ["Epoch", html(epochButton(block.epoch))],
            ["Reported txs", formatInteger(block.tx_count)],
          ])}
        </article>
      </div>
    </section>
    <section class="subsection">
      <h3>Latest Transactions</h3>
      ${dataTable(
        ["Transaction", "Block", "Fee", "UTxOs"],
        hashes.slice(0, 12).map((hash) => {
          const tx = detailByHash.get(hash);
          return [
            html(txCell(hash)),
            html(blockCell(tx?.block_height ?? block.height, `#${formatInteger(tx?.block_height ?? block.height)}`)),
            tx ? formatAda(tx.fees) : "-",
            tx ? formatInteger(tx.utxo_count) : "Pending",
          ];
        }),
        "No transactions returned for the latest block.",
      )}
    </section>
  `;
}

async function renderEpochLookup(query: string): Promise<void> {
  const epochQuery = query === "" ? "latest" : query.toLowerCase();
  const [latestEpoch, params] = await Promise.all([
    optionalFetch<EpochResponse>("/api/v0/epochs/latest"),
    optionalEpochParams(epochQuery),
  ]);
  if (!params) {
    throw new Error("Epoch parameters were not found.");
  }
  if (latestEpoch) {
    state.epoch = latestEpoch;
  }
  state.protocolParams = params;

  const latestMatches = latestEpoch?.epoch === params.epoch;
  els.resultTitle.textContent = `Epoch ${formatInteger(params.epoch)}`;
  setResultMeta(latestMatches ? "latest epoch and protocol parameters" : "protocol parameters");
  els.resultContent.innerHTML = `
    ${
      latestMatches && latestEpoch
        ? `
          <section class="subsection">
            <h3>Epoch Summary</h3>
            ${detailGrid([
              ["Start", formatDate(latestEpoch.start_time)],
              ["End", formatDate(latestEpoch.end_time)],
              ["Blocks", formatInteger(latestEpoch.block_count)],
              ["Transactions", formatInteger(latestEpoch.tx_count)],
              ["Output", formatAda(latestEpoch.output)],
              ["Fees", formatAda(latestEpoch.fees)],
              ["Active stake", latestEpoch.active_stake ? formatAda(latestEpoch.active_stake) : "-"],
            ])}
          </section>
        `
        : capabilityNote("Dingo currently exposes historical epoch protocol parameters; aggregate epoch stats are available for the latest epoch.")
    }
    ${renderProtocolParams(params)}
  `;
}

async function optionalEpochParams(epochQuery: string): Promise<ProtocolParamsResponse | undefined> {
  if (epochQuery === "" || epochQuery === "latest") {
    return optionalFetch<ProtocolParamsResponse>("/api/v0/epochs/latest/parameters");
  }
  return optionalFetch<ProtocolParamsResponse>(`/api/v0/epochs/${encodeURIComponent(epochQuery)}/parameters`);
}

async function renderNetworkLookup(): Promise<void> {
  const [network, genesis, epoch, params, eras] = await Promise.all([
    blockfrostFetch<NetworkResponse>("/api/v0/network"),
    blockfrostFetch<GenesisResponse>("/api/v0/genesis"),
    blockfrostFetch<EpochResponse>("/api/v0/epochs/latest"),
    blockfrostFetch<ProtocolParamsResponse>("/api/v0/epochs/latest/parameters"),
    optionalFetch<NetworkEraResponse[]>("/api/v0/network/eras"),
  ]);
  state.network = network;
  state.genesis = genesis;
  state.epoch = epoch;
  state.protocolParams = params;
  state.eras = eras ?? [];
  state.networkUpdatedAt = Date.now();
  renderOverview();

  els.resultTitle.textContent = "Network";
  setResultMeta(`epoch ${formatInteger(epoch.epoch)} | ${formatUpdatedAt(state.networkUpdatedAt)}`);
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Current epoch", html(epochButton(epoch.epoch))],
      ["Epoch progress", epochProgressText()],
      ["Slot length", `${formatInteger(genesis.slot_length)}s`],
      ["Epoch length", `${formatInteger(genesis.epoch_length)} slots`],
      ["Network magic", formatInteger(genesis.network_magic)],
      ["Security param", formatInteger(genesis.security_param)],
      ["Circulating supply", formatAda(network.supply.circulating)],
      ["Treasury", formatAda(network.supply.treasury)],
      ["Reserves", formatAda(network.supply.reserves)],
      ["Live stake", formatAda(network.stake.live)],
      ["Active stake", formatAda(network.stake.active)],
      ["Protocol", `${params.protocol_major_ver}.${params.protocol_minor_ver}`],
    ])}
    ${renderEraTable(state.eras)}
    ${renderProtocolParams(params)}
  `;
}

async function renderTransactionLookup(hash: string): Promise<void> {
  const [
    tx,
    txCbor,
    utxos,
    metadata,
    metadataCbor,
    delegations,
    stakes,
    withdrawals,
    mirs,
    poolUpdates,
    poolRetires,
    redeemers,
    requiredSigners,
  ] = await Promise.all([
    blockfrostFetch<TransactionResponse>(`/api/v0/txs/${encodeURIComponent(hash)}`),
    optionalFetch<TransactionCBORResponse>(`/api/v0/txs/${encodeURIComponent(hash)}/cbor`),
    optionalFetch<TransactionUTXOsResponse>(`/api/v0/txs/${encodeURIComponent(hash)}/utxos`),
    optionalFetch<TransactionMetadataResponse[]>(`/api/v0/txs/${encodeURIComponent(hash)}/metadata`),
    optionalFetch<TransactionMetadataCBORResponse[]>(`/api/v0/txs/${encodeURIComponent(hash)}/metadata/cbor`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/delegations`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/stakes`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/withdrawals`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/mirs`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/pool_updates`),
    optionalFetch<unknown[]>(`/api/v0/txs/${encodeURIComponent(hash)}/pool_retires`),
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
      ${smallStat("MIR", tx.mir_cert_count)}
      ${smallStat("Pool updates", tx.pool_update_count)}
      ${smallStat("Pool retires", tx.pool_retire_count)}
      ${smallStat("Mint/burn", tx.asset_mint_or_burn_count)}
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
    ${metadataCbor && metadataCbor.length > 0 ? renderJSONSection("Metadata CBOR", metadataCbor) : ""}
    ${renderJSONCountSection("Delegations", delegations)}
    ${renderJSONCountSection("Stake Certificates", stakes)}
    ${renderJSONCountSection("Withdrawals", withdrawals)}
    ${renderJSONCountSection("MIR Certificates", mirs)}
    ${renderJSONCountSection("Pool Updates", poolUpdates)}
    ${renderJSONCountSection("Pool Retires", poolRetires)}
    ${renderJSONCountSection("Redeemers", redeemers)}
    ${renderJSONCountSection("Required Signers", requiredSigners)}
    ${txCbor ? renderCborSection("Transaction CBOR", txCbor.cbor) : ""}
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

  const visibleAmounts = aggregateVisibleAmounts(utxos.items);
  const totalAda = lovelaceFromAmounts(visibleAmounts);
  const visibleAssetCount = visibleAmounts.filter((amount) => amount.unit !== "lovelace").length;

  els.resultTitle.textContent = "Address";
  setResultMeta(`${formatInteger(utxos.items.length)} UTxOs | ${formatInteger(txs.items.length)} tx rows`);
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Address", html(copyField(address))],
      ["Visible ADA", formatAda(totalAda.toString())],
      ["Visible assets", formatInteger(visibleAssetCount)],
      ["UTxO page total", formatMaybeTotal(utxos.total)],
      ["Tx page total", formatMaybeTotal(txs.total)],
    ])}
    <section class="subsection">
      <h3>Visible Balance</h3>
      ${amountList(visibleAmounts)}
    </section>
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
      ["Reserves", formatAda(account.reserves_sum)],
      ["Treasury", formatAda(account.treasury_sum)],
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

async function renderAssetLookup(assetID: string): Promise<void> {
  const encodedAssetID = encodeURIComponent(assetID);
  const [asset, holders] = await Promise.all([
    blockfrostFetch<AssetResponse>(`/api/v0/assets/${encodedAssetID}`),
    optionalFetchPage<AssetAddressResponse>(
      `/api/v0/assets/${encodedAssetID}/addresses?count=25&page=1&order=desc`,
    ),
  ]);
  renderAssetResult(asset, holders);
}

function renderAssetResult(
  asset: AssetResponse,
  holders?: { items: AssetAddressResponse[]; total?: number },
): void {
  els.resultTitle.textContent = "Asset";
  const holderSummary = holders
    ? `${formatMaybeTotal(holders.total)} holder${holders.total === 1 ? "" : "s"}`
    : "no live holders";
  setResultMeta(`${asset.fingerprint} | ${holderSummary}`);
  els.resultContent.innerHTML = `
    ${detailGrid([
      ["Asset", html(copyField(asset.asset))],
      ["Policy", html(copyField(asset.policy_id))],
      ["Name hex", asset.asset_name ? html(copyField(asset.asset_name)) : "-"],
      ["Name", asset.asset_name_ascii || "-"],
      ["Quantity", formatIntegerString(asset.quantity)],
      ["Mint/burn", formatInteger(asset.mint_or_burn_count)],
      ["Initial mint", html(txCell(asset.initial_mint_tx_hash))],
      ["Metadata standard", asset.onchain_metadata_standard ?? "-"],
    ])}
    <section class="subsection">
      <h3>Current Holders</h3>
      ${dataTable(
        ["Address", "Quantity"],
        (holders?.items ?? []).map((holder) => [
          html(addressCell(holder.address)),
          formatIntegerString(holder.quantity),
        ]),
        "No live holders returned.",
      )}
      ${holders?.total !== undefined && holders.total > holders.items.length
        ? capabilityNote(`Showing the top ${formatInteger(holders.items.length)} of ${formatInteger(holders.total)} holder addresses by quantity.`)
        : ""}
    </section>
    ${asset.onchain_metadata ? renderJSONSection("On-chain Metadata", asset.onchain_metadata) : ""}
    ${asset.metadata ? renderJSONSection("Metadata", asset.metadata) : ""}
    ${capabilityNote("Dingo exposes current asset holders and aggregate mint/burn information by asset ID. Fingerprint lookup and individual mint/burn transaction history are not exposed yet.")}
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
  const [rows, cborRows] = await Promise.all([
    blockfrostFetchPage<MetadataTransactionJSONResponse>(
      `/api/v0/metadata/txs/labels/${encodeURIComponent(label)}?count=20&page=1&order=desc`,
    ),
    optionalFetchPage<MetadataTransactionCBORResponse>(
      `/api/v0/metadata/txs/labels/${encodeURIComponent(label)}/cbor?count=20&page=1&order=desc`,
    ),
  ]);
  const cborByTx = new Map((cborRows?.items ?? []).map((row) => [row.tx_hash, row]));

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
                ${
                  cborByTx.has(row.tx_hash)
                    ? `<pre>${escapeHtml(JSON.stringify(cborByTx.get(row.tx_hash), null, 2))}</pre>`
                    : ""
                }
              </article>
            `,
          )
          .join("")
      : `<p class="empty">No metadata rows returned.</p>`;
}

async function renderPoolsLookup(): Promise<void> {
  beginPoolLoad();
  renderOverview();
  try {
    const pools = await blockfrostFetchPage<PoolExtendedResponse>(
      "/api/v0/pools/extended?count=25&page=1&order=desc",
    );
    state.pools = pools.items;
    state.poolTotal = pools.total;
    state.poolsUpdatedAt = Date.now();
    els.resultTitle.textContent = "Stake Pools";
    setResultMeta(`${formatInteger(pools.items.length)} visible | ${formatMaybeTotal(pools.total)} total`);
    els.resultContent.innerHTML = renderPoolList(pools.items);
  } finally {
    endPoolLoad();
    renderOverview();
  }
}

async function renderPoolLookup(poolID: string): Promise<void> {
  els.resultTitle.textContent = "Stake Pool";
  setResultMeta("Loading");
  beginPoolLoad();
  renderOverview();
  try {
    const pool = await findPool(poolID);
    if (!pool) {
      renderErrorResult("Stake Pool", "Pool not found in active extended pools.");
      return;
    }
    renderPoolResult(pool);
  } finally {
    endPoolLoad();
    renderOverview();
  }
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
    ${capabilityNote("Dingo's current Blockfrost endpoints expose active extended pool state. Delegator lists, pool blocks, and historical pool stats need additional Dingo explorer endpoints.")}
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
        ["Epoch", html(epochButton(block.epoch))],
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
  const genesis = state.genesis;
  const params = state.protocolParams;
  els.networkLabel.textContent = headerNetworkLabel(block, genesis);

  els.metricGrid.innerHTML = [
    metricCard("Tip", block ? `#${formatInteger(block.height)}` : "-", block ? `Slot ${formatInteger(block.slot)}` : "-"),
    metricCard(
      "Epoch",
      epoch ? formatInteger(epoch.epoch) : block ? formatInteger(block.epoch) : "-",
      block && genesis ? epochProgressText() : epoch ? `${formatInteger(epoch.block_count)} blocks` : "-",
    ),
    metricCard("Latest Tx", block ? formatInteger(block.tx_count) : "-", block ? `${formatInteger(block.size)} bytes` : "-"),
    metricCard(
      "Slot in Epoch",
      block ? formatInteger(block.epoch_slot) : "-",
      genesis ? `${formatInteger(genesis.epoch_length)} slots` : "Epoch length loading",
    ),
  ].join("");

  els.activityPanel.innerHTML = renderDashboardActivity(block, epoch, genesis);

  els.dashboardDetails.innerHTML = `
    <section class="dashboard-card dashboard-card-tight">
      <h3>Epoch Progress</h3>
      <div class="progress-summary">
        <strong>${escapeHtml(epochProgressPercent())}%</strong>
        <span>${escapeHtml(epochProgressText())}</span>
      </div>
      <div class="progress-track">
        <span style="width: ${escapeHtml(epochProgressPercent())}%"></span>
      </div>
      ${compactGrid([
        ["Epoch", epoch ? html(epochButton(epoch.epoch)) : block ? html(epochButton(block.epoch)) : "-"],
        ["Slot", block ? `${formatInteger(block.epoch_slot)} / ${genesis ? formatInteger(genesis.epoch_length) : "-"}` : "-"],
        ["End", epoch ? formatDate(epoch.end_time) : "-"],
      ])}
    </section>
    <section class="dashboard-card dashboard-card-tight">
      <h3>Protocol Snapshot</h3>
      ${
        params
          ? `
            ${renderProtocolLimitBars(params)}
            ${compactGrid([
              ["Version", `${params.protocol_major_ver}.${params.protocol_minor_ver}`],
              ["Max tx", `${formatInteger(params.max_tx_size)} bytes`],
              ["Max block", `${formatInteger(params.max_block_size)} bytes`],
              ["Min fee", `${formatInteger(params.min_fee_a)} * size + ${formatInteger(params.min_fee_b)}`],
              ["Key deposit", formatAda(params.key_deposit)],
              ["Pool deposit", formatAda(params.pool_deposit)],
              ["Gov action deposit", formatMaybeAda(params.gov_action_deposit)],
              ["DRep deposit", formatMaybeAda(params.drep_deposit)],
            ])}
          `
          : `<p class="empty">Protocol parameters not loaded.</p>`
      }
    </section>
  `;

  renderRecentBlocks();
}

function epochProgressPercent(): string {
  const block = state.latestBlock;
  const genesis = state.genesis;
  if (!block || !genesis || genesis.epoch_length <= 0) {
    return "0";
  }
  return Math.min(100, Math.max(0, (block.epoch_slot / genesis.epoch_length) * 100)).toFixed(2);
}

function renderDashboardActivity(
  block: BlockResponse | undefined,
  epoch: EpochResponse | undefined,
  genesis: GenesisResponse | undefined,
): string {
  const blocks = state.recentBlocks.slice().sort((left, right) => left.height - right.height);
  return `
    <section class="dashboard-card activity-card activity-stack-card">
      <div class="activity-section">
        <div class="card-heading">
          <h3>Epoch Activity</h3>
          <span>${epoch ? `Epoch ${formatInteger(epoch.epoch)}` : "Waiting"}</span>
        </div>
        ${renderEpochActivity(epoch, genesis)}
      </div>
      <div class="activity-section">
        <div class="card-heading">
          <h3>Recent Block Activity</h3>
          <span>${blocks.length > 0 ? `Blocks as time | ${formatInteger(blocks.length)} samples` : "Waiting"}</span>
        </div>
        ${renderRecentBlockChart(blocks)}
      </div>
    </section>
    <section class="dashboard-card activity-card activity-tempo">
      <div class="card-heading">
        <h3>Block Tempo</h3>
        <span>${block ? `#${formatInteger(block.height)}` : "Waiting"}</span>
      </div>
      ${renderBlockTempo(blocks, block)}
    </section>
  `;
}

function renderRecentBlockChart(blocks: BlockResponse[]): string {
  if (state.recentBlocksLoading && blocks.length === 0) {
    return `<div class="loading-bar"></div>`;
  }
  if (blocks.length === 0) {
    return `<p class="empty">No recent block data loaded.</p>`;
  }
  const txValues = blocks.map((block) => block.tx_count);
  const sizeValues = blocks.map((block) => block.size);
  const maxTx = Math.max(1, ...txValues);
  const maxSize = Math.max(1, ...sizeValues);
  const first = blocks[0];
  const last = blocks[blocks.length - 1];
  return `
    <div class="block-time-series" role="img" aria-label="Recent block transactions and size by block height">
      <svg viewBox="0 0 100 64" preserveAspectRatio="none" aria-hidden="true">
        <defs>
          <linearGradient id="txSeriesFill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stop-color="#ffaa4e" stop-opacity="0.34" />
            <stop offset="100%" stop-color="#ffaa4e" stop-opacity="0" />
          </linearGradient>
        </defs>
        <polygon class="series-fill" points="${escapeHtml(timeSeriesAreaPoints(txValues, maxTx))}" />
        <polyline class="series-line series-size" points="${escapeHtml(timeSeriesPoints(sizeValues, maxSize))}" />
        <polyline class="series-line series-tx" points="${escapeHtml(timeSeriesPoints(txValues, maxTx))}" />
      </svg>
      <div class="series-hit-map">
        ${blocks
          .map(
            (item, index) => `
              <button
                class="series-point"
                type="button"
                data-block-id="${escapeHtml(item.hash)}"
                style="--point-x: ${timeSeriesXPercent(index, blocks.length)}%; --point-y: ${timeSeriesYPercent(item.tx_count, maxTx)}%"
                title="Block #${formatInteger(item.height)} | ${formatInteger(item.tx_count)} txs | ${formatInteger(item.size)} bytes"
                aria-label="Open block #${formatInteger(item.height)}"
              >
                <span></span>
              </button>
            `,
          )
          .join("")}
      </div>
      <div class="time-axis">
        <span>#${formatInteger(first.height)}</span>
        <span>#${formatInteger(last.height)}</span>
      </div>
    </div>
    <div class="chart-legend">
      <span><i class="legend-tx"></i>Transactions</span>
      <span><i class="legend-size"></i>Size</span>
    </div>
  `;
}

function renderBlockTempo(blocks: BlockResponse[], block: BlockResponse | undefined): string {
  const pairs = blocks.slice(1).map((item, index) => ({
    block: item,
    seconds: Math.max(0, item.time - blocks[index].time),
  }));
  const tipAgeSeconds = block ? Math.max(0, Math.floor(Date.now() / 1000) - block.time) : undefined;
  const maxGap = Math.max(1, ...pairs.map((item) => item.seconds));
  return `
    <div class="tip-age ${tipAgeSeconds !== undefined && tipAgeSeconds > 900 ? "stale" : ""}">
      <span>Tip age</span>
      <strong>${tipAgeSeconds === undefined ? "-" : formatDurationCompact(tipAgeSeconds)}</strong>
      <small>${block ? formatDate(block.time) : "No tip loaded"}</small>
    </div>
    ${
      pairs.length > 0
        ? `
          <div class="interval-chart">
            ${pairs
              .map(
                (item) => `
                  <button class="interval-row" type="button" data-block-id="${escapeHtml(item.block.hash)}">
                    <span>#${formatInteger(item.block.height)}</span>
                    <i><b style="width: ${chartPercent(item.seconds, maxGap)}%"></b></i>
                    <strong>${formatDurationCompact(item.seconds)}</strong>
                  </button>
                `,
              )
              .join("")}
          </div>
        `
        : `<p class="empty">No interval data loaded.</p>`
    }
  `;
}

function renderEpochActivity(epoch: EpochResponse | undefined, genesis: GenesisResponse | undefined): string {
  const progress = Number(epochProgressPercent());
  const expectedBlocks = genesis ? Math.max(1, Math.round(genesis.epoch_length * genesis.active_slots_coefficient)) : 1;
  const blockCount = epoch?.block_count ?? 0;
  const txCount = epoch?.tx_count ?? 0;
  const averageTx = blockCount > 0 ? txCount / blockCount : 0;
  return `
    <div class="epoch-activity">
      <div class="epoch-ring" style="--progress: ${Number.isFinite(progress) ? progress : 0}%">
        <strong>${Number.isFinite(progress) ? progress.toFixed(1) : "0.0"}%</strong>
        <span>complete</span>
      </div>
      <div class="meter-stack">
        ${meterBar("Blocks", blockCount, expectedBlocks, `${formatInteger(blockCount)} / ${formatInteger(expectedBlocks)} expected`)}
        ${meterBar("Txs", txCount, Math.max(1, txCount, blockCount), formatInteger(txCount))}
        ${meterBar("Avg tx/block", averageTx, Math.max(1, averageTx, 2), formatDecimal(averageTx))}
      </div>
    </div>
  `;
}

function renderProtocolLimitBars(params: ProtocolParamsResponse): string {
  return `
    <div class="protocol-bars">
      ${meterBar(
        "Tx size cap",
        params.max_tx_size,
        Math.max(params.max_block_size, params.max_tx_size),
        `${formatInteger(params.max_tx_size)} bytes`,
      )}
      ${meterBar(
        "Header cap",
        params.max_block_header_size,
        Math.max(params.max_block_size, params.max_block_header_size),
        `${formatInteger(params.max_block_header_size)} bytes`,
      )}
      ${meterBar("Collateral", params.collateral_percent ?? 0, 100, `${formatInteger(params.collateral_percent ?? 0)}%`)}
    </div>
  `;
}

function meterBar(label: string, value: number, max: number, display: string): string {
  return `
    <div class="meter-row" style="--meter-width: ${chartPercent(value, max)}%">
      <div>
        <span>${escapeHtml(label)}</span>
        <strong>${escapeHtml(display)}</strong>
      </div>
      <i><b></b></i>
    </div>
  `;
}

function chartPercent(value: number, max: number): string {
  if (!Number.isFinite(value) || !Number.isFinite(max) || max <= 0 || value <= 0) {
    return "0";
  }
  return Math.min(100, Math.max(2, (value / max) * 100)).toFixed(2);
}

function timeSeriesPoints(values: number[], max: number): string {
  if (values.length === 0) {
    return "";
  }
  return values.map((value, index) => `${timeSeriesX(index, values.length)} ${timeSeriesY(value, max)}`).join(" ");
}

function timeSeriesAreaPoints(values: number[], max: number): string {
  if (values.length === 0) {
    return "";
  }
  return `0 60 ${timeSeriesPoints(values, max)} 100 60`;
}

function timeSeriesX(index: number, count: number): string {
  if (count <= 1) {
    return "50";
  }
  return ((index / (count - 1)) * 100).toFixed(2);
}

function timeSeriesXPercent(index: number, count: number): string {
  return timeSeriesX(index, count);
}

function timeSeriesY(value: number, max: number): string {
  if (!Number.isFinite(value) || !Number.isFinite(max) || max <= 0) {
    return "60";
  }
  const normalized = Math.min(1, Math.max(0, value / max));
  return (58 - normalized * 48).toFixed(2);
}

function timeSeriesYPercent(value: number, max: number): string {
  const y = Number(timeSeriesY(value, max));
  return Number.isFinite(y) ? ((y / 64) * 100).toFixed(2) : "100";
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

async function optionalFetchPage<T>(path: string): Promise<{ items: T[]; total?: number } | undefined> {
  try {
    return await blockfrostFetchPage<T>(path);
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

function renderSearchResults(hits: SearchHit[], notes: string[]): string {
  const hitMarkup =
    hits.length > 0
      ? `
        <div class="search-results">
          ${hits
            .map(
              (hit) => `
                <article class="search-hit">
                  <span>${escapeHtml(hit.category)}</span>
                  <strong>${escapeHtml(hit.title)}</strong>
                  <small>${escapeHtml(hit.meta)}</small>
                  ${hit.detail ? `<code>${escapeHtml(hit.detail)}</code>` : ""}
                  <button
                    type="button"
                    data-jump-tab="${escapeHtml(hit.tab)}"
                    data-jump-query="${escapeHtml(hit.query)}"
                  >
                    Open
                  </button>
                </article>
              `,
            )
            .join("")}
        </div>
      `
      : `<p class="empty">No matching Dingo result found.</p>`;
  const noteMarkup = notes.map((note) => capabilityNote(note)).join("");
  return `${hitMarkup}${noteMarkup}`;
}

function renderProtocolParams(params: ProtocolParamsResponse): string {
  return `
    <section class="subsection">
      <h3>Protocol Parameters</h3>
      ${detailGrid([
        ["Epoch", html(epochButton(params.epoch))],
        ["Protocol", `${params.protocol_major_ver}.${params.protocol_minor_ver}`],
        ["Min fee", `${formatInteger(params.min_fee_a)} * size + ${formatInteger(params.min_fee_b)}`],
        ["Max block size", `${formatInteger(params.max_block_size)} bytes`],
        ["Max tx size", `${formatInteger(params.max_tx_size)} bytes`],
        ["Max header size", `${formatInteger(params.max_block_header_size)} bytes`],
        ["Key deposit", formatAda(params.key_deposit)],
        ["Pool deposit", formatAda(params.pool_deposit)],
        ["Min pool cost", formatAda(params.min_pool_cost)],
        ["Pool count target", formatInteger(params.n_opt)],
        ["Pool influence", formatDecimal(params.a0)],
        ["Monetary expansion", formatDecimal(params.rho)],
        ["Treasury growth", formatDecimal(params.tau)],
        ["Coins per UTxO size", params.coins_per_utxo_size ?? params.coins_per_utxo_word],
        ["Price mem", params.price_mem === null ? "-" : formatDecimal(params.price_mem)],
        ["Price step", params.price_step === null ? "-" : formatDecimal(params.price_step)],
        ["Max tx ex mem", params.max_tx_ex_mem ?? "-"],
        ["Max tx ex steps", params.max_tx_ex_steps ?? "-"],
        ["Max block ex mem", params.max_block_ex_mem ?? "-"],
        ["Max block ex steps", params.max_block_ex_steps ?? "-"],
        ["Collateral percent", params.collateral_percent === null ? "-" : `${formatInteger(params.collateral_percent)}%`],
        ["Max collateral inputs", params.max_collateral_inputs === null ? "-" : formatInteger(params.max_collateral_inputs)],
      ])}
    </section>
  `;
}

function renderEraTable(eras: NetworkEraResponse[]): string {
  return `
    <section class="subsection">
      <h3>Era Summary</h3>
      ${dataTable(
        ["Era", "Start", "End", "Slots", "Slot length"],
        eras.map((era, index) => [
          era.era ?? `Era ${formatInteger(index + 1)}`,
          `Epoch ${formatInteger(era.start.epoch)} / slot ${formatInteger(era.start.slot)}`,
          era.end ? `Epoch ${formatInteger(era.end.epoch)} / slot ${formatInteger(era.end.slot)}` : "current",
          formatInteger(era.parameters.epoch_length),
          `${formatInteger(era.parameters.slot_length)}s`,
        ]),
        "No era summaries returned.",
      )}
    </section>
  `;
}

function capabilityNote(message: string): string {
  return `<p class="capability-note">${escapeHtml(message)}</p>`;
}

function renderCborSection(title: string, cbor: string): string {
  return `
    <section class="subsection">
      <h3>${escapeHtml(title)}</h3>
      <pre>${escapeHtml(cbor)}</pre>
    </section>
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

function compactGrid(rows: Array<[string, RenderContent]>): string {
  return `
    <dl class="compact-grid">
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
            .map((cell, cellIndex) => {
              const label = headers[cellIndex] ?? "";
              return `<div class="table-cell${classAttribute}" data-label="${escapeHtml(label)}"${rowAttributes ? ` ${rowAttributes}` : ""}>${renderContent(cell)}</div>`;
            })
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
    ["Address", "Input", "Amount", "Datum / script"],
    inputs.map((input) => [
      html(addressCell(input.address)),
      html(`${txCell(input.tx_hash)} #${formatInteger(input.output_index)}`),
      html(formatAmountSummary(input.amount)),
      html(renderUtxoFeatures(input)),
    ]),
    "No inputs returned.",
  );
}

function renderOutputList(outputs: TransactionOutputResponse[]): string {
  return dataTable(
    ["Address", "Index", "Amount", "Status", "Datum / script"],
    outputs.map((output) => [
      html(addressCell(output.address)),
      `#${formatInteger(output.output_index)}`,
      html(formatAmountSummary(output.amount)),
      output.consumed_by_tx ? html(`Spent by ${txCell(output.consumed_by_tx)}`) : "Unspent",
      html(renderUtxoFeatures(output)),
    ]),
    "No outputs returned.",
  );
}

function renderAddressUTXOList(utxos: AddressUTXOResponse[]): string {
  return dataTable(
    ["Output", "Amount", "Block", "Datum / script"],
    utxos.map((utxo) => [
      html(`${txCell(utxo.tx_hash)} #${formatInteger(utxo.output_index)}`),
      html(formatAmountSummary(utxo.amount)),
      html(blockCell(utxo.block)),
      html(renderUtxoFeatures(utxo)),
    ]),
    "No UTxOs returned.",
  );
}

function renderUtxoFeatures(utxo: {
  data_hash: string | null;
  inline_datum: string | null;
  reference_script_hash: string | null;
}): string {
  const rows: Array<[string, string]> = [];
  if (utxo.data_hash) {
    rows.push(["Datum hash", copyField(utxo.data_hash, shortHash(utxo.data_hash, 8))]);
  }
  if (utxo.inline_datum) {
    rows.push(["Inline datum", copyField(utxo.inline_datum, shortHash(utxo.inline_datum, 8))]);
  }
  if (utxo.reference_script_hash) {
    rows.push([
      "Reference script",
      copyField(utxo.reference_script_hash, shortHash(utxo.reference_script_hash, 8)),
    ]);
  }
  if (rows.length === 0) {
    return "-";
  }
  return rows
    .map(([label, value]) => `<span class="utxo-feature"><small>${escapeHtml(label)}</small>${value}</span>`)
    .join("");
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

function epochButton(epoch: number): string {
  const value = String(epoch);
  return actionCell(
    `<button class="link-button mono" type="button" data-epoch-id="${escapeHtml(value)}">Epoch ${escapeHtml(value)}</button>`,
    value,
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

function aggregateVisibleAmounts(utxos: AddressUTXOResponse[]): Amount[] {
  const totals = new Map<string, bigint>();
  for (const utxo of utxos) {
    for (const amount of utxo.amount) {
      try {
        totals.set(amount.unit, (totals.get(amount.unit) ?? 0n) + BigInt(amount.quantity));
      } catch {
        totals.set(amount.unit, totals.get(amount.unit) ?? 0n);
      }
    }
  }
  const units = Array.from(totals.keys()).sort((left, right) => {
    if (left === "lovelace") {
      return -1;
    }
    if (right === "lovelace") {
      return 1;
    }
    return left.localeCompare(right);
  });
  return units.map((unit) => ({
    unit,
    quantity: (totals.get(unit) ?? 0n).toString(),
  }));
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

function formatMaybeAda(lovelace: string | null | undefined): string {
  return lovelace ? formatAda(lovelace) : "-";
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

function formatDecimal(value: number): string {
  if (!Number.isFinite(value)) {
    return "-";
  }
  return value.toLocaleString(undefined, {
    maximumFractionDigits: 8,
  });
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

function headerNetworkLabel(block: BlockResponse | undefined, genesis: GenesisResponse | undefined): string {
  const name = networkName(genesis);
  const parts = [name];
  if (block) {
    parts.push(`Epoch ${formatInteger(block.epoch)}`);
    parts.push(`Slot ${formatInteger(block.slot)}`);
  } else if (genesis) {
    parts.push(`${formatInteger(genesis.slot_length)}s slots`);
  } else {
    parts.push("Blockfrost-compatible API");
  }
  return parts.join(" | ");
}

function networkName(genesis: GenesisResponse | undefined): string {
  if (!genesis) {
    return "Dingo";
  }
  switch (genesis.network_magic) {
    case 764824073:
      return "Mainnet";
    case 1:
      return "Preprod";
    case 2:
      return "Preview";
    case 4:
      return "Sanchonet";
    default:
      return `Network ${formatInteger(genesis.network_magic)}`;
  }
}

function epochProgressText(): string {
  const block = state.latestBlock;
  const genesis = state.genesis;
  if (!block || !genesis || genesis.epoch_length <= 0) {
    return "-";
  }
  const percent = Math.min(100, Math.max(0, (block.epoch_slot / genesis.epoch_length) * 100));
  const remainingSlots = Math.max(0, genesis.epoch_length - block.epoch_slot);
  const remainingSeconds = remainingSlots * genesis.slot_length;
  return `${percent.toFixed(1)}% | ${formatDuration(remainingSeconds)} remaining`;
}

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "0m";
  }
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

function formatDurationCompact(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "0s";
  }
  if (seconds < 60) {
    return `${Math.round(seconds)}s`;
  }
  if (seconds < 3_600) {
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.round(seconds % 60);
    return remainingSeconds > 0 ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`;
  }
  return formatDuration(seconds);
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

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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
