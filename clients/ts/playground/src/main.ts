// Playground entry — wires tabs, connection, and all panels.

import {
  WaveHouseClient,
  type TableSchema,
  type SSESubscription,
  type StreamEvent,
  type StructuredQuery,
} from "@wavehouse/sdk";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let client: WaveHouseClient | null = null;
let schemas: TableSchema[] = [];
let sseSub: SSESubscription | null = null;
let streamCount = 0;

const $ = <T extends HTMLElement>(sel: string) => document.querySelector<T>(sel)!;
const $$ = <T extends HTMLElement>(sel: string) => document.querySelectorAll<T>(sel);

// ---------------------------------------------------------------------------
// Tabs (main + sub-tabs)
// ---------------------------------------------------------------------------

function initTabs(selector: string, panelPrefix: string) {
  for (const btn of $$(selector)) {
    btn.addEventListener("click", () => {
      for (const b of $$(selector)) b.classList.remove("active");
      btn.classList.add("active");
      const key = btn.dataset.panel ?? btn.dataset.qtab ?? btn.dataset.atab ?? "";
      for (const p of $$<HTMLElement>(`[id^="${panelPrefix}"]`)) {
        p.classList.toggle("active", p.id === `${panelPrefix}${key}`);
      }
    });
  }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

async function connect() {
  const url = $<HTMLInputElement>("#base-url").value.replace(/\/+$/, "");
  const token = $<HTMLInputElement>("#jwt-token").value || undefined;
  client = new WaveHouseClient({ baseUrl: url, token });
  const badge = $("#status-badge");
  try {
    const ok = await client.health();
    if (!ok) throw new Error("unhealthy");
    badge.textContent = "connected";
    badge.className = "badge connected";
    await loadSchemas();
  } catch (err) {
    badge.textContent = "error";
    badge.className = "badge disconnected";
    client = null;
    console.error(err);
  }
}

// ---------------------------------------------------------------------------
// Schema panel
// ---------------------------------------------------------------------------

async function loadSchemas() {
  if (!client) return;
  schemas = await client.schemas();
  renderTableList();
  populateTableSelects();
}

function renderTableList() {
  const el = $("#table-list");
  if (!schemas.length) {
    el.innerHTML = '<p class="muted">No tables found</p>';
    return;
  }
  el.innerHTML = schemas
    .map((s) => `<div class="table-item" data-table="${s.name}">${s.name}</div>`)
    .join("");
  for (const item of el.querySelectorAll<HTMLElement>(".table-item")) {
    item.addEventListener("click", () => {
      for (const i of el.querySelectorAll(".table-item")) i.classList.remove("selected");
      item.classList.add("selected");
      renderTableDetail(item.dataset.table!);
    });
  }
}

function renderTableDetail(tableName: string) {
  const schema = schemas.find((s) => s.name === tableName);
  if (!schema) return;
  const rows = schema.columns
    .map(
      (c: { name: string; type: string; is_nullable: boolean }) =>
        `<tr><td>${c.name}</td><td class="type-badge">${c.type}</td><td class="${c.is_nullable ? "nullable-yes" : "nullable-no"}">${c.is_nullable ? "yes" : "no"}</td></tr>`
    )
    .join("");
  $("#table-detail").innerHTML = `
    <h3 style="margin-bottom:8px">${tableName}</h3>
    <table class="schema-table">
      <thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function populateTableSelects() {
  const opts = schemas.map((s) => `<option value="${s.name}">${s.name}</option>`).join("");
  const base = `<option value="">Select table…</option>${opts}`;
  $<HTMLSelectElement>("#ingest-table").innerHTML = base;
  $<HTMLSelectElement>("#sq-table").innerHTML = base;
}

// ---------------------------------------------------------------------------
// Ingest panel
// ---------------------------------------------------------------------------

function onIngestTableChange() {
  const table = $<HTMLSelectElement>("#ingest-table").value;
  const container = $("#ingest-fields");
  const btn = $<HTMLButtonElement>("#btn-ingest");
  if (!table) {
    container.innerHTML = "";
    btn.disabled = true;
    return;
  }
  btn.disabled = false;
  const schema = schemas.find((s) => s.name === table);
  if (!schema) return;
  container.innerHTML = schema.columns
    .map(
      (c: { name: string; type: string; is_nullable: boolean }) => `
    <div class="ingest-field">
      <label>${c.name}</label>
      <span class="field-type">${c.type}</span>
      <input data-col="${c.name}" data-type="${c.type}" placeholder="${c.is_nullable ? "(nullable)" : ""}" />
    </div>`
    )
    .join("");
}

async function doIngest() {
  if (!client) return;
  const table = $<HTMLSelectElement>("#ingest-table").value;
  if (!table) return;
  const data: Record<string, unknown> = {};
  for (const inp of $$("#ingest-fields input") as NodeListOf<HTMLInputElement>) {
    const val = inp.value.trim();
    if (!val) continue;
    const col = inp.dataset.col!;
    const chType = inp.dataset.type ?? "";
    data[col] = coerceValue(val, chType);
  }
  const result = $("#ingest-result");
  try {
    await client.ingest(table, data);
    result.innerHTML = `<span class="success">✓ Ingested into ${table}</span>\n${JSON.stringify(data, null, 2)}`;
  } catch (err: any) {
    result.innerHTML = `<span class="error">✗ ${err.message}</span>`;
  }
}

function coerceValue(val: string, chType: string): unknown {
  if (/^(UInt|Int|Float|Decimal)/i.test(chType)) {
    const n = Number(val);
    return isNaN(n) ? val : n;
  }
  if (/^Bool/i.test(chType)) return val === "true" || val === "1";
  return val;
}

// ---------------------------------------------------------------------------
// Query panel
// ---------------------------------------------------------------------------

async function doRawQuery() {
  if (!client) return;
  const sql = $<HTMLTextAreaElement>("#sql-input").value.trim();
  if (!sql) return;
  const out = $("#query-result");
  try {
    const res = await client.rawQuery(sql);
    out.innerHTML = `<span class="${res.meta.cached ? "success" : "muted"}">cached: ${res.meta.cached} | rows: ${res.data.length}</span>\n${JSON.stringify(res.data, null, 2)}`;
  } catch (err: any) {
    out.innerHTML = `<span class="error">${err.message}</span>`;
  }
}

async function doStructuredQuery() {
  if (!client) return;
  const table = $<HTMLSelectElement>("#sq-table").value;
  if (!table) return;
  const sq: StructuredQuery = {};

  const cols = $<HTMLInputElement>("#sq-columns").value.trim();
  if (cols) sq.columns = cols.split(",").map((c) => c.trim());

  const aggFn = $<HTMLSelectElement>("#sq-agg-fn").value;
  const aggCol = $<HTMLInputElement>("#sq-agg-col").value.trim();
  if (aggFn && aggCol) {
    sq.aggregations = [{ fn: aggFn as any, column: aggCol, alias: $<HTMLInputElement>("#sq-agg-alias").value.trim() || undefined }];
  }

  const groupBy = $<HTMLInputElement>("#sq-groupby").value.trim();
  if (groupBy) sq.group_by = groupBy.split(",").map((c) => c.trim());

  const filterCol = $<HTMLInputElement>("#sq-filter-col").value.trim();
  const filterVal = $<HTMLInputElement>("#sq-filter-val").value.trim();
  if (filterCol && filterVal) {
    const op = $<HTMLSelectElement>("#sq-filter-op").value as any;
    let v: unknown = filterVal;
    if (op === "in") {
      try { v = JSON.parse(filterVal); } catch { v = filterVal.split(",").map((s) => s.trim()); }
    } else {
      const n = Number(filterVal);
      if (!isNaN(n)) v = n;
    }
    sq.filters = [{ column: filterCol, op, value: v }];
  }

  const orderCol = $<HTMLInputElement>("#sq-order-col").value.trim();
  if (orderCol) {
    sq.order_by = [{ column: orderCol, dir: $<HTMLSelectElement>("#sq-order-dir").value as any }];
  }

  const limit = $<HTMLInputElement>("#sq-limit").value.trim();
  if (limit) sq.limit = parseInt(limit, 10);

  const trCol = $<HTMLInputElement>("#sq-tr-column").value.trim();
  const trSince = $<HTMLInputElement>("#sq-tr-since").value.trim();
  if (trCol && trSince) {
    sq.time_range = { column: trCol, since: trSince };
  }

  const out = $("#query-result");
  try {
    const res = await client.query(table, sq);
    out.innerHTML = `<span class="${res.meta.cached ? "success" : "muted"}">cached: ${res.meta.cached} | rows: ${res.data.length}</span>\n${JSON.stringify(res.data, null, 2)}`;
  } catch (err: any) {
    out.innerHTML = `<span class="error">${err.message}</span>`;
  }
}

async function doPipeQuery() {
  if (!client) return;
  const name = $<HTMLInputElement>("#pipe-name").value.trim();
  if (!name) return;
  let params: Record<string, unknown> | undefined;
  const raw = $<HTMLInputElement>("#pipe-params").value.trim();
  if (raw) {
    try { params = JSON.parse(raw); } catch { params = undefined; }
  }
  const out = $("#query-result");
  try {
    const res = await client.pipe(name, params);
    out.innerHTML = `<span class="muted">rows: ${res.data.length}</span>\n${JSON.stringify(res.data, null, 2)}`;
  } catch (err: any) {
    out.innerHTML = `<span class="error">${err.message}</span>`;
  }
}

// ---------------------------------------------------------------------------
// Stream panel
// ---------------------------------------------------------------------------

function startStream() {
  if (!client) return;
  const topic = $<HTMLInputElement>("#stream-topic").value.trim() || undefined;
  streamCount = 0;
  updateStreamCount();

  sseSub = client.subscribe({
    topic,
    onEvent(event: StreamEvent) {
      streamCount++;
      updateStreamCount();
      const log = $("#stream-log");
      const div = document.createElement("div");
      div.className = "event";
      div.innerHTML = `<span class="ts">${event.received_timestamp}</span> <span class="table">${event.table_name}</span> ${JSON.stringify(event.data)}`;
      log.prepend(div);
      // Cap at 500 displayed events.
      while (log.children.length > 500) log.removeChild(log.lastChild!);
    },
    onOpen() {
      $<HTMLButtonElement>("#btn-stream-start").disabled = true;
      $<HTMLButtonElement>("#btn-stream-stop").disabled = false;
    },
    onError() {
      // Allow reconnect.
    },
  });
}

function stopStream() {
  sseSub?.close();
  sseSub = null;
  $<HTMLButtonElement>("#btn-stream-start").disabled = false;
  $<HTMLButtonElement>("#btn-stream-stop").disabled = true;
}

function updateStreamCount() {
  $("#stream-count").textContent = `${streamCount} events`;
}

// ---------------------------------------------------------------------------
// Admin panel
// ---------------------------------------------------------------------------

async function loadPolicy() {
  if (!client) return;
  try {
    const p = await client.getPolicy();
    $<HTMLTextAreaElement>("#policy-editor").value = JSON.stringify(p, null, 2);
    $("#policy-result").innerHTML = '<span class="success">Loaded</span>';
  } catch (err: any) {
    $("#policy-result").innerHTML = `<span class="error">${err.message}</span>`;
  }
}

async function savePolicy() {
  if (!client) return;
  try {
    const p = JSON.parse($<HTMLTextAreaElement>("#policy-editor").value);
    await client.setPolicy(p);
    $("#policy-result").innerHTML = '<span class="success">Saved ✓</span>';
  } catch (err: any) {
    $("#policy-result").innerHTML = `<span class="error">${err.message}</span>`;
  }
}

async function validatePolicy() {
  if (!client) return;
  const out = $("#policy-result");
  try {
    const body = JSON.parse($<HTMLTextAreaElement>("#policy-editor").value);
    const { data } = await (await fetch(`${$<HTMLInputElement>("#base-url").value}/v1/admin/policy/validate`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...(client ? {} : {}) },
      body: JSON.stringify(body),
    })).json() as any;
    out.innerHTML = `<span class="success">Valid ✓</span>`;
  } catch (err: any) {
    out.innerHTML = `<span class="error">${err.message}</span>`;
  }
}

async function loadPipes() {
  if (!client) return;
  try {
    const pipes = await client.listPipes();
    const el = $("#pipes-list");
    if (!pipes.length) {
      el.innerHTML = '<p class="muted">No pipes registered</p>';
      return;
    }
    el.innerHTML = pipes
      .map((p: { name: string; description?: string }) => `<div class="table-item">${p.name} — ${p.description ?? ""}</div>`)
      .join("");
    $("#pipes-result").innerHTML = JSON.stringify(pipes, null, 2);
  } catch (err: any) {
    $("#pipes-result").innerHTML = `<span class="error">${err.message}</span>`;
  }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {
  initTabs(".tab", "panel-");
  initTabs(".qtab", "qtab-");
  initTabs(".atab", "atab-");

  $("#btn-connect").addEventListener("click", connect);
  $("#btn-refresh-schema").addEventListener("click", async () => {
    if (!client) return;
    await client.refreshSchema();
    await loadSchemas();
  });

  // Ingest
  $<HTMLSelectElement>("#ingest-table").addEventListener("change", onIngestTableChange);
  $("#btn-ingest").addEventListener("click", doIngest);

  // Query
  $("#btn-query-raw").addEventListener("click", doRawQuery);
  $("#btn-query-structured").addEventListener("click", doStructuredQuery);
  $("#btn-query-pipe").addEventListener("click", doPipeQuery);

  // Stream
  $("#btn-stream-start").addEventListener("click", startStream);
  $("#btn-stream-stop").addEventListener("click", stopStream);
  $("#btn-stream-clear").addEventListener("click", () => {
    $("#stream-log").innerHTML = "";
    streamCount = 0;
    updateStreamCount();
  });

  // Admin
  $("#btn-policy-load").addEventListener("click", loadPolicy);
  $("#btn-policy-save").addEventListener("click", savePolicy);
  $("#btn-policy-validate").addEventListener("click", validatePolicy);
  $("#btn-pipes-load").addEventListener("click", loadPipes);

  // Auto-connect if URL is pre-filled.
  if ($<HTMLInputElement>("#base-url").value) {
    connect();
  }
});
