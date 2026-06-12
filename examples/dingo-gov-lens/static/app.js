const state = {
  view: "proposals",
  status: null,
};

const $ = (selector) => document.querySelector(selector);
const statusEl = $("#status");
const metadataNotice = $("#metadata-notice");
const proposalRows = $("#proposal-rows");
const drepRows = $("#drep-rows");
const dialog = $("#detail-dialog");
const detailContent = $("#detail-content");

document.querySelectorAll(".tab").forEach((button) => {
  button.addEventListener("click", asyncHandler(() => {
    state.view = button.dataset.view;
    document.querySelectorAll(".tab").forEach((tab) => {
      const selected = tab === button;
      tab.classList.toggle("active", selected);
      tab.setAttribute("aria-selected", String(selected));
    });
    document.querySelectorAll(".view").forEach((view) => {
      const selected = view.id === button.getAttribute("aria-controls");
      view.classList.toggle("active", selected);
      view.setAttribute("aria-hidden", String(!selected));
    });
    return refresh();
  }));
});

$("#refresh").addEventListener("click", asyncHandler(refresh));
$("#proposal-lifecycle").addEventListener("change", asyncHandler(loadProposals));
$("#proposal-type").addEventListener("change", asyncHandler(loadProposals));
$("#drep-active").addEventListener("change", asyncHandler(loadDreps));
$("#wallet-connect").addEventListener("click", asyncHandler(connectWallet, showStakeError));
$("#stake-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const credential = $("#stake-credential").value.trim().toLowerCase();
  const credentialTag = $("#stake-credential-tag").value;
  const result = $("#stake-result");
  if (!credential) {
    result.innerHTML = `<div class="empty">Enter a stake credential hex value.</div>`;
    return;
  }
  try {
    const data = await api(
      `/api/stake/${encodeURIComponent(credential)}?credential_tag=${encodeURIComponent(credentialTag)}`,
    );
    result.innerHTML = `
      <div class="detail-grid">
        ${metric("Stake", shortHex(data.stakingKey))}
        ${metric("Type", credentialTypeName(data.credentialTag))}
        ${metric("Active", data.active ? "yes" : "no")}
        ${metric("Reward", lovelace(data.reward))}
        ${metric("DRep type", data.drepType)}
      </div>
      <p class="mono">Pool: ${data.pool || "none"}</p>
      <p class="mono">DRep: ${data.drep || "none"}</p>
    `;
  } catch (error) {
    result.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
  }
});

loadWallets();

async function refresh() {
  await loadStatus();
  if (state.view === "proposals") {
    await loadProposals();
  } else if (state.view === "dreps") {
    await loadDreps();
  }
}

async function loadStatus() {
  const data = await api("/api/status");
  state.status = data;
  statusEl.innerHTML = [
    metric("Network", data.network || "unknown"),
    metric("Storage", data.storageMode || "unknown"),
    metric("Tip slot", number(data.tip?.slot)),
    metric("Epoch", number(data.latestEpoch?.epochId)),
    metric("Actions", number(data.proposalCount)),
    metric("Votes", number(data.governanceVoteCount)),
    metric("Active DReps", number(data.activeDrepCount)),
    metric("Backfill", data.backfill?.completed ? "complete" : number(data.backfill?.lastSlot)),
  ].join("");
  renderMetadataNotice(data);
}

async function loadProposals() {
  proposalRows.innerHTML = loadingRow(6);
  const params = new URLSearchParams();
  const lifecycle = $("#proposal-lifecycle").value;
  const type = $("#proposal-type").value;
  if (lifecycle) params.set("lifecycle", lifecycle);
  if (type) params.set("action_type", type);
  const rows = await api(`/api/proposals?${params.toString()}`);
  if (!rows.length) {
    proposalRows.innerHTML = emptyRow(6, "No matching governance actions.");
    return;
  }
  proposalRows.innerHTML = rows
    .map((row) => `
      <tr>
        <td>
          <div class="mono">${shortHex(row.txHash)}#${row.actionIndex}</div>
          ${renderExternalLink(row.anchorUrl, "anchor")}
        </td>
        <td>${escapeHtml(row.actionTypeName)}</td>
        <td><span class="pill ${row.lifecycle}">${escapeHtml(row.lifecycle)}</span></td>
        <td>proposed ${number(row.proposedEpoch)}<br />expires ${number(row.expiresEpoch)}</td>
        <td>${voteGrid(row.votes.total)}</td>
        <td>
          <button class="row-button" data-proposal="${row.txHash}" data-index="${row.actionIndex}" type="button">Details</button>
        </td>
      </tr>
    `)
    .join("");
  proposalRows.querySelectorAll("[data-proposal]").forEach((button) => {
    button.addEventListener(
      "click",
      asyncHandler(() => showProposal(button.dataset.proposal, button.dataset.index)),
    );
  });
}

async function showProposal(txHash, index) {
  const data = await api(`/api/proposals/${txHash}/${index}`);
  const p = data.proposal;
  detailContent.innerHTML = `
    <h2>${escapeHtml(p.actionTypeName)}</h2>
    <p class="mono">${p.txHash}#${p.actionIndex}</p>
    <div class="detail-grid">
      ${metric("State", p.lifecycle)}
      ${metric("Proposed", number(p.proposedEpoch))}
      ${metric("Expires", number(p.expiresEpoch))}
      ${metric("Deposit", lovelace(p.deposit))}
    </div>
    ${renderExternalLink(p.govtoolUrl, "Open in GovTool", "", "p")}
    ${p.anchorUrl ? `<p>Anchor: ${renderLinkedUrl(p.anchorUrl)}</p>` : ""}
    <div class="detail-section">
      <h3>Vote Summary</h3>
      <div class="detail-grid">
        ${metric("Committee", plainVotes(data.summary.committee))}
        ${metric("DRep", plainVotes(data.summary.drep))}
        ${metric("SPO", plainVotes(data.summary.spo))}
      </div>
    </div>
    <div class="detail-section">
      <h3>Votes</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Voter</th><th>Type</th><th>Vote</th><th>Slot</th></tr></thead>
          <tbody>
            ${data.votes.length ? data.votes.map((vote) => `
              <tr>
                <td class="mono">${shortHex(vote.voterCredential)}</td>
                <td>${escapeHtml(vote.voterTypeName)}</td>
                <td>${escapeHtml(vote.voteName)}</td>
                <td>${number(vote.updatedSlot || vote.addedSlot)}</td>
              </tr>
            `).join("") : emptyRow(4, voteEmptyText())}
          </tbody>
        </table>
      </div>
    </div>
  `;
  dialog.showModal();
}

async function loadDreps() {
  drepRows.innerHTML = loadingRow(5);
  const params = new URLSearchParams();
  const active = $("#drep-active").value;
  if (active) params.set("active", active);
  const rows = await api(`/api/dreps?${params.toString()}`);
  if (!rows.length) {
    drepRows.innerHTML = emptyRow(5, "No matching DReps.");
    return;
  }
  drepRows.innerHTML = rows
    .map((row) => `
      <tr>
        <td>
          <button class="link-button" data-drep="${row.credential}" data-credential-tag="${row.credentialTag}" type="button">
            <span class="mono">${shortHex(row.credential)}</span>
          </button>
          <br /><span class="muted">${credentialTypeName(row.credentialTag)}</span>
          ${renderAnchorBreak(row.anchorUrl)}
        </td>
        <td><span class="pill ${row.active ? "active" : "inactive"}">${row.active ? "active" : "inactive"}</span></td>
        <td>activity ${number(row.lastActivityEpoch)}<br />expiry ${number(row.expiryEpoch)}</td>
        <td>${number(row.delegatorCount)}</td>
        <td>${number(row.voteCount)}</td>
      </tr>
    `)
    .join("");
  drepRows.querySelectorAll("[data-drep]").forEach((button) => {
    button.addEventListener(
      "click",
      asyncHandler(() => showDrep(button.dataset.drep, button.dataset.credentialTag)),
    );
  });
}

async function showDrep(credential, credentialTag) {
  const data = await api(
    `/api/dreps/${credential}?credential_tag=${encodeURIComponent(credentialTag)}`,
  );
  const d = data.drep;
  detailContent.innerHTML = `
    <h2>DRep</h2>
    <p class="mono">${d.credential}</p>
    <div class="detail-grid">
      ${metric("Type", credentialTypeName(d.credentialTag))}
      ${metric("Status", d.active ? "active" : "inactive")}
      ${metric("Delegators", number(d.delegatorCount))}
      ${metric("Votes", number(d.voteCount))}
      ${metric("Expiry", number(d.expiryEpoch))}
    </div>
    ${d.anchorUrl ? `<p>Anchor: ${renderLinkedUrl(d.anchorUrl)}</p>` : ""}
    <div class="detail-section">
      <h3>Recent votes</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Action</th><th>Type</th><th>Vote</th><th>Slot</th></tr></thead>
          <tbody>
            ${data.recentVotes.length ? data.recentVotes.map((vote) => `
              <tr>
                <td>${renderProposalActionLink(vote)}</td>
                <td>${escapeHtml(vote.actionTypeName)}</td>
                <td>${escapeHtml(vote.voteName)}</td>
                <td>${number(vote.updatedSlot || vote.addedSlot)}</td>
              </tr>
            `).join("") : emptyRow(4, voteEmptyText())}
          </tbody>
        </table>
      </div>
    </div>
    <div class="detail-section">
      <h3>Recent delegators</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Stake credential</th><th>Reward</th><th>Active</th></tr></thead>
          <tbody>
            ${data.delegations.length ? data.delegations.map((account) => `
              <tr>
                <td><span class="mono">${shortHex(account.stakingKey)}</span><br /><span class="muted">${credentialTypeName(account.credentialTag)}</span></td>
                <td>${lovelace(account.reward)}</td>
                <td>${account.active ? "yes" : "no"}</td>
              </tr>
            `).join("") : emptyRow(3, "No active delegators returned.")}
          </tbody>
        </table>
      </div>
    </div>
  `;
  dialog.showModal();
}

async function api(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }
  return response.json();
}

function asyncHandler(fn, onError = showStatusError) {
  return (...args) => {
    Promise.resolve(fn(...args)).catch(onError);
  };
}

function showStatusError(error) {
  statusEl.innerHTML = `
    <div class="metric">
      <div class="metric-label">Error</div>
      <div class="metric-value">${escapeHtml(errorMessage(error))}</div>
    </div>
  `;
}

function showStakeError(error) {
  $("#stake-result").innerHTML = `<div class="empty">${escapeHtml(errorMessage(error))}</div>`;
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function renderExternalLink(url, label, className = "", wrapper = "") {
  const href = safeExternalHref(url);
  if (!href) return "";
  const classAttr = className ? ` class="${escapeAttr(className)}"` : "";
  const link = `<a${classAttr} href="${escapeAttr(href)}" target="_blank" rel="noreferrer">` +
    `${escapeHtml(String(label))}</a>`;
  return wrapper ? `<${wrapper}>${link}</${wrapper}>` : link;
}

function renderLinkedUrl(url) {
  return renderExternalLink(url, String(url)) || escapeHtml(String(url));
}

function renderAnchorBreak(url) {
  const link = renderExternalLink(url, "anchor");
  return link ? `<br />${link}` : "";
}

function renderProposalActionLink(vote) {
  const label = `${shortHex(vote.proposalTxHash)}#${vote.actionIndex}`;
  return renderExternalLink(vote.proposalGovtoolUrl, label, "mono") ||
    `<span class="mono">${escapeHtml(label)}</span>`;
}

function safeExternalHref(value) {
  if (!value) return "";
  try {
    const url = new URL(String(value).trim());
    if (url.protocol !== "https:" && url.protocol !== "http:") return "";
    return url.href;
  } catch {
    return "";
  }
}

function metric(label, value) {
  return `
    <div class="metric">
      <div class="metric-label">${escapeHtml(String(label))}</div>
      <div class="metric-value">${escapeHtml(String(value ?? "n/a"))}</div>
    </div>
  `;
}

function credentialTypeName(value) {
  return Number(value) === 1 ? "Script" : "Key";
}

function voteGrid(votes) {
  if (state.status?.voteBackfillPending && totalVotes(votes) === 0) {
    return `<span class="pill pending">backfilling</span>`;
  }
  return `
    <div class="vote-grid">
      <div class="vote-cell"><span>Yes</span>${number(votes?.yes)}</div>
      <div class="vote-cell"><span>No</span>${number(votes?.no)}</div>
      <div class="vote-cell"><span>Abs</span>${number(votes?.abstain)}</div>
    </div>
  `;
}

function plainVotes(votes) {
  if (state.status?.voteBackfillPending && totalVotes(votes) === 0) {
    return "backfilling";
  }
  return `Y ${number(votes?.yes)} / N ${number(votes?.no)} / A ${number(votes?.abstain)}`;
}

function renderMetadataNotice(data) {
  if (!metadataNotice) return;
  if (!data.voteBackfillPending) {
    metadataNotice.hidden = true;
    metadataNotice.innerHTML = "";
    return;
  }
  metadataNotice.hidden = false;
  metadataNotice.innerHTML = `
    <strong>Vote metadata is still backfilling.</strong>
    Indexed through slot ${number(data.backfill?.lastSlot)}; active proposal votes start around slot ${number(data.minLiveProposalSlot)}.
  `;
}

function voteEmptyText() {
  return state.status?.voteBackfillPending ? "Vote metadata is still backfilling." : "No votes recorded.";
}

function totalVotes(votes) {
  return Number(votes?.yes || 0) + Number(votes?.no || 0) + Number(votes?.abstain || 0);
}

function loadingRow(cols) {
  return `<tr><td colspan="${cols}" class="empty">Loading...</td></tr>`;
}

function emptyRow(cols, text) {
  return `<tr><td colspan="${cols}" class="empty">${escapeHtml(text)}</td></tr>`;
}

function number(value) {
  if (value === undefined || value === null || value === "") return "n/a";
  return Number(value).toLocaleString();
}

function lovelace(value) {
  if (value === undefined || value === null || value === "") return "n/a";
  const raw = String(value);
  if (!/^\d+$/.test(raw)) return "n/a";
  const amount = BigInt(raw);
  const whole = amount / 1_000_000n;
  const fraction = (amount % 1_000_000n).toString().padStart(6, "0").replace(/0+$/, "");
  return `${groupDigits(whole.toString())}${fraction ? `.${fraction}` : ""} ADA`;
}

function groupDigits(value) {
  return value.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function shortHex(value) {
  if (!value) return "";
  if (value.length <= 18) return value;
  return `${value.slice(0, 12)}...${value.slice(-8)}`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  })[char]);
}

function escapeAttr(value) {
  return escapeHtml(String(value));
}

function loadWallets() {
  const select = $("#wallet-select");
  const wallets = Object.entries(window.cardano || {})
    .filter(([, wallet]) => wallet && typeof wallet.enable === "function")
    .map(([id, wallet]) => ({ id, name: wallet.name || id }));
  if (!wallets.length) {
    select.innerHTML = `<option value="">No CIP-30 wallet</option>`;
    $("#wallet-connect").disabled = true;
    return;
  }
  select.innerHTML = wallets
    .map((wallet) => `<option value="${escapeAttr(wallet.id)}">${escapeHtml(wallet.name)}</option>`)
    .join("");
}

async function connectWallet() {
  const walletId = $("#wallet-select").value;
  const wallet = window.cardano?.[walletId];
  if (!wallet) return;
  const api = await wallet.enable();
  const rewards = await api.getRewardAddresses();
  const rewardAddress = rewards?.[0]?.toLowerCase();
  if (!rewardAddress || rewardAddress.length < 58) {
    $("#stake-result").innerHTML = `<div class="empty">Wallet did not return a usable reward address.</div>`;
    return;
  }
  $("#stake-credential").value = rewardAddress.slice(2, 58);
  $("#stake-credential-tag").value = rewardAddress.slice(0, 1) === "f" ? "1" : "0";
  $("#stake-form").requestSubmit();
}

refresh().catch((error) => {
  showStatusError(error);
});
