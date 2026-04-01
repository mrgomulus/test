/* ── OPC UA Browser – Frontend ──────────────────────────────────────── */
'use strict';

// ── Globals ──────────────────────────────────────────────────────────────
let activeConnId = null;    // currently selected connection id
let selectedNode = null;    // currently selected node info
let currentTab = 'attributes';
let ws = null;              // active WebSocket
let subscribedNodes = new Set();
let favorites = [];         // loaded per connection
let connections = [];       // all saved connections

// ── Utility ───────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

function logMsg(text, level = 'info') {
  const el = document.createElement('div');
  el.className = `log-entry log-${level}`;
  const ts = new Date().toLocaleTimeString();
  el.innerHTML = `<span class="log-ts">[${ts}]</span><span>${escHtml(text)}</span>`;
  const list = $('log-list');
  list.prepend(el);
  if (list.children.length > 200) list.lastChild.remove();
}

function toast(msg, type = 'info') {
  const el = document.createElement('div');
  el.className = `toast toast-${type}`;
  el.textContent = msg;
  $('toast-container').appendChild(el);
  setTimeout(() => el.remove(), 3200);
}

function escHtml(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

async function apiFetch(path, opts = {}) {
  const resp = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || resp.statusText);
  }
  if (resp.status === 204) return null;
  return resp.json();
}

// ── Node class helpers ────────────────────────────────────────────────────
function ncBadge(nc) {
  const map = {
    Variable: 'badge-var', Object: 'badge-obj', Method: 'badge-method',
    ObjectType: 'badge-type', VariableType: 'badge-type', DataType: 'badge-type',
    ReferenceType: 'badge-type', View: 'badge-other',
  };
  return `<span class="badge ${map[nc] || 'badge-other'}">${nc}</span>`;
}

// ── Connections ───────────────────────────────────────────────────────────
async function loadConnections() {
  connections = await apiFetch('/api/connections');
  renderConnections();
}

function renderConnections() {
  const list = $('connection-list');
  list.innerHTML = '';
  if (!connections.length) {
    list.innerHTML = '<p class="hint">Keine Verbindungen</p>';
    return;
  }
  connections.forEach(c => {
    const div = document.createElement('div');
    div.className = `conn-item${c.connected ? ' connected' : ''}${c.id === activeConnId ? ' active' : ''}`;
    div.dataset.id = c.id;
    // Use textContent / DOM properties – no innerHTML for user data
    const dot = document.createElement('span');
    dot.className = 'conn-dot';
    const info = document.createElement('div');
    info.style.cssText = 'flex:1;overflow:hidden';
    const nameEl = document.createElement('div');
    nameEl.className = 'conn-name';
    nameEl.textContent = c.name;
    const urlEl = document.createElement('div');
    urlEl.className = 'conn-url';
    urlEl.textContent = c.url;
    info.appendChild(nameEl);
    info.appendChild(urlEl);
    const actions = document.createElement('div');
    actions.className = 'conn-actions';
    const makeBtn = (title, text, style, handler) => {
      const btn = document.createElement('button');
      btn.title = title;
      btn.textContent = text;
      if (style) btn.style.cssText = style;
      btn.addEventListener('click', (e) => { e.stopPropagation(); handler(); });
      return btn;
    };
    actions.appendChild(makeBtn('Verbinden / Trennen', '⚡', '', () => toggleConnect(c.id)));
    actions.appendChild(makeBtn('Bearbeiten', '✎', '', () => editConnection(c.id)));
    actions.appendChild(makeBtn('Info', 'ℹ', '', () => showServerInfo(c.id)));
    actions.appendChild(makeBtn('Löschen', '✕', 'color:#e05c5c', () => deleteConnection(c.id)));
    div.appendChild(dot);
    div.appendChild(info);
    div.appendChild(actions);
    div.addEventListener('click', () => selectConnection(c.id));
    list.appendChild(div);
  });
}

async function selectConnection(id) {
  activeConnId = id;
  renderConnections();
  const c = connections.find(x => x.id === id);
  if (!c) return;
  $('status-text').textContent = `Verbindung: ${c.name} (${c.url})`;
  $('status-text').className = c.connected ? 'status-connected' : 'status-idle';
  if (c.connected) {
    await loadFavorites();
    await refreshTree();
  } else {
    $('favorites-list').innerHTML = '<p class="hint">Nicht verbunden</p>';
    $('tree-container').innerHTML = '<p class="hint">Nicht verbunden – Verbindung herstellen</p>';
  }
}

async function toggleConnect(id, e) {
  e && e.stopPropagation();
  const c = connections.find(x => x.id === id);
  if (!c) return;
  if (c.connected) {
    try {
      await apiFetch(`/api/connections/${id}/disconnect`, { method: 'POST' });
      logMsg(`Verbindung getrennt: ${c.name}`, 'warn');
      toast(`Getrennt: ${c.name}`, 'info');
    } catch (ex) { logMsg(`Trennen fehlgeschlagen: ${ex.message}`, 'error'); }
  } else {
    logMsg(`Verbinde mit ${c.url}…`, 'info');
    try {
      const info = await apiFetch(`/api/connections/${id}/connect`, { method: 'POST' });
      logMsg(`Verbunden: ${c.name} – ${info.server_name || ''}`, 'success');
      toast(`Verbunden: ${c.name}`, 'info');
      if (activeConnId === id) {
        await loadFavorites();
        await refreshTree();
      }
    } catch (ex) {
      logMsg(`Verbindungsfehler: ${ex.message}`, 'error');
      toast(`Fehler: ${ex.message}`, 'error');
    }
  }
  await loadConnections();
  if (activeConnId === id) await selectConnection(id);
}

function openConnectionModal(existing = null) {
  $('modal-conn-title').textContent = existing ? 'Verbindung bearbeiten' : 'Neue Verbindung';
  $('cf-name').value = existing?.name || '';
  $('cf-url').value = existing?.url || 'opc.tcp://localhost:4840';
  $('cf-username').value = existing?.username || '';
  $('cf-password').value = '';
  $('cf-security-mode').value = existing?.security_mode || '1';
  $('cf-security-policy').value = existing?.security_policy || 'None';
  $('cf-timeout').value = existing?.timeout || 10;
  $('cf-description').value = existing?.description || '';
  $('modal-conn').classList.remove('hidden');
  $('modal-conn').dataset.editId = existing?.id || '';
  $('cf-name').focus();
}

async function saveConnection() {
  const payload = {
    name: $('cf-name').value.trim() || 'New Connection',
    url: $('cf-url').value.trim(),
    username: $('cf-username').value.trim(),
    password: $('cf-password').value,
    security_mode: parseInt($('cf-security-mode').value),
    security_policy: $('cf-security-policy').value,
    timeout: parseInt($('cf-timeout').value) || 10,
    description: $('cf-description').value.trim(),
  };
  const editId = $('modal-conn').dataset.editId;
  try {
    if (editId) {
      await apiFetch(`/api/connections/${editId}`, { method: 'PUT', body: payload });
      logMsg(`Verbindung aktualisiert: ${payload.name}`, 'success');
    } else {
      await apiFetch('/api/connections', { method: 'POST', body: payload });
      logMsg(`Verbindung erstellt: ${payload.name}`, 'success');
    }
    $('modal-conn').classList.add('hidden');
    await loadConnections();
  } catch (ex) {
    logMsg(`Speichern fehlgeschlagen: ${ex.message}`, 'error');
    toast(`Fehler: ${ex.message}`, 'error');
  }
}

async function editConnection(id, e) {
  e && e.stopPropagation();
  const c = connections.find(x => x.id === id);
  if (c) openConnectionModal(c);
}

async function deleteConnection(id, e) {
  e && e.stopPropagation();
  const c = connections.find(x => x.id === id);
  if (!confirm(`Verbindung "${c?.name}" löschen?`)) return;
  try {
    await apiFetch(`/api/connections/${id}`, { method: 'DELETE' });
    if (activeConnId === id) {
      activeConnId = null;
      $('tree-container').innerHTML = '<p class="hint">Verbindung herstellen um den Adressraum zu browsen</p>';
      $('favorites-list').innerHTML = '<p class="hint">Keine Verbindung aktiv</p>';
    }
    await loadConnections();
    logMsg(`Verbindung gelöscht: ${c?.name}`, 'warn');
  } catch (ex) {
    logMsg(`Löschen fehlgeschlagen: ${ex.message}`, 'error');
  }
}

async function showServerInfo(id, e) {
  e && e.stopPropagation();
  try {
    const info = await apiFetch(`/api/connections/${id}/status`);
    const c = connections.find(x => x.id === id);
    renderServerInfo(info, c);
    $('modal-server-info').classList.remove('hidden');
  } catch (ex) {
    logMsg(`Info fehlgeschlagen: ${ex.message}`, 'error');
  }
}

function renderServerInfo(info, conn) {
  const rows = [
    ['Verbindung', conn?.name || ''],
    ['Endpoint URL', info.endpoint_url || conn?.url || ''],
    ['Status', info.connected ? '<span class="status-connected">Verbunden</span>' : '<span class="status-error">Nicht verbunden</span>'],
    ['Server Name', info.server_name || ''],
    ['Server URI', info.server_uri || ''],
    ['Product URI', info.product_uri || ''],
    ['Sicherheitsmodus', info.security_mode || ''],
    ['Sicherheitsrichtlinie', info.security_policy || ''],
    ['Serverstatus', info.server_state || ''],
    ['Startzeit', info.start_time || ''],
    ['Aktuelle Zeit', info.current_time || ''],
  ];
  if (info.build_info) {
    const b = info.build_info;
    rows.push(
      ['Produktname', b.product_name || ''],
      ['Hersteller', b.manufacturer_name || ''],
      ['Softwareversion', b.software_version || ''],
      ['Build-Nummer', b.build_number || ''],
      ['Build-Datum', b.build_date || ''],
    );
  }
  let html = '<div class="server-info-grid">';
  rows.forEach(([k, v]) => {
    if (v) html += `<div class="server-info-row"><div class="server-info-key">${escHtml(k)}</div><div class="server-info-val">${v}</div></div>`;
  });
  if (info.namespaces?.length) {
    html += '<div class="server-info-row"><div class="server-info-key">Namespaces</div><div class="server-info-val">';
    info.namespaces.forEach((ns, i) => {
      html += `<span class="ns-badge">[${i}] ${escHtml(ns)}</span>`;
    });
    html += '</div></div>';
  }
  html += '</div>';
  $('server-info-content').innerHTML = html;
}

// ── Address Space Tree ────────────────────────────────────────────────────
async function refreshTree() {
  if (!activeConnId) return;
  const container = $('tree-container');
  container.innerHTML = '<p class="hint">Lade Adressraum…</p>';
  try {
    const nodes = await apiFetch(`/api/browse?conn_id=${activeConnId}`);
    container.innerHTML = '';
    nodes.forEach(n => container.appendChild(buildTreeNode(n, 0)));
    if (!nodes.length) container.innerHTML = '<p class="hint">Keine Knoten gefunden</p>';
  } catch (ex) {
    container.innerHTML = `<p class="hint" style="color:var(--red)">${escHtml(ex.message)}</p>`;
    logMsg(`Browse fehlgeschlagen: ${ex.message}`, 'error');
  }
}

function buildTreeNode(nodeInfo, depth) {
  const wrapper = document.createElement('div');
  wrapper.className = 'tree-node';

  const row = document.createElement('div');
  row.className = 'tree-row';
  row.style.paddingLeft = `${8 + depth * 16}px`;

  // Toggle
  const toggle = document.createElement('span');
  toggle.className = `tree-toggle${nodeInfo.has_children ? '' : ' leaf'}`;
  toggle.textContent = '▶';

  // Icon
  const icon = document.createElement('span');
  icon.className = `node-icon icon-${nodeInfo.icon || 'node'}`;

  // Label
  const label = document.createElement('span');
  label.className = 'tree-label';
  label.innerHTML = `${escHtml(nodeInfo.display_name || nodeInfo.browse_name)}` +
    `<span class="node-class-badge">${nodeInfo.node_class}</span>`;

  row.appendChild(toggle);
  row.appendChild(icon);
  row.appendChild(label);
  wrapper.appendChild(row);

  // Children container
  const children = document.createElement('div');
  children.className = 'tree-children';
  wrapper.appendChild(children);

  let expanded = false;
  let loaded = false;

  // Select on click
  row.addEventListener('click', (e) => {
    e.stopPropagation();
    document.querySelectorAll('.tree-row.selected').forEach(r => r.classList.remove('selected'));
    row.classList.add('selected');
    selectNode(nodeInfo);
  });

  // Expand/collapse on toggle click
  if (nodeInfo.has_children) {
    toggle.addEventListener('click', async (e) => {
      e.stopPropagation();
      if (!expanded) {
        toggle.classList.add('expanded');
        children.classList.add('open');
        if (!loaded) {
          loaded = true;
          const loading = document.createElement('div');
          loading.className = 'tree-loading';
          loading.textContent = 'Lade…';
          children.appendChild(loading);
          try {
            const sub = await apiFetch(`/api/browse?conn_id=${activeConnId}&node_id=${encodeURIComponent(nodeInfo.node_id)}`);
            loading.remove();
            if (sub.length) {
              sub.forEach(n => children.appendChild(buildTreeNode(n, depth + 1)));
            } else {
              loading.textContent = '(leer)';
              children.appendChild(loading);
              toggle.classList.add('leaf');
              toggle.classList.remove('expanded');
            }
          } catch (ex) {
            loading.textContent = `Fehler: ${ex.message}`;
            logMsg(`Browse Fehler: ${ex.message}`, 'error');
          }
        }
        expanded = true;
      } else {
        toggle.classList.remove('expanded');
        children.classList.remove('open');
        expanded = false;
      }
    });
  }

  return wrapper;
}

// ── Node selection & details ──────────────────────────────────────────────
async function selectNode(nodeInfo) {
  selectedNode = nodeInfo;
  // show favorite button
  const favBtn = $('btn-fav-toggle');
  favBtn.style.display = '';
  const isFav = favorites.some(f => f.node_id === nodeInfo.node_id);
  favBtn.classList.toggle('active', isFav);
  favBtn.title = isFav ? 'Favorit entfernen' : 'Als Favorit speichern';

  await loadNodeDetail(nodeInfo.node_id);
}

async function loadNodeDetail(nodeId) {
  if (!activeConnId || !nodeId) return;
  $('detail-content').innerHTML = '<p class="hint">Lade…</p>';
  try {
    const data = await apiFetch(`/api/attributes?conn_id=${activeConnId}&node_id=${encodeURIComponent(nodeId)}`);
    renderDetail(data);
  } catch (ex) {
    $('detail-content').innerHTML = `<p class="hint" style="color:var(--red)">${escHtml(ex.message)}</p>`;
    logMsg(`Attribute Fehler: ${ex.message}`, 'error');
  }
}

function renderDetail(data) {
  if (currentTab === 'attributes') renderAttributes(data);
  else if (currentTab === 'value') renderValue(data);
  else if (currentTab === 'references') renderReferences(data);
}

function renderAttributes(data) {
  const a = data.attributes || {};
  let html = '<table class="attr-table">';
  html += '<tr class="section-header"><td colspan="2">Allgemeine Attribute</td></tr>';
  const order = ['NodeId','NodeClass','BrowseName','DisplayName','Description','WriteMask','UserWriteMask'];
  order.forEach(k => {
    if (k in a) html += attrRow(k, a[k]);
  });
  const varKeys = ['DataType','ValueRank','ArrayDimensions','AccessLevel','UserAccessLevel','MinimumSamplingInterval','Historizing'];
  const hasVar = varKeys.some(k => k in a);
  if (hasVar) {
    html += '<tr class="section-header"><td colspan="2">Variablen-Attribute</td></tr>';
    varKeys.forEach(k => {
      if (k in a) html += attrRow(k, a[k]);
    });
  }
  const methodKeys = ['Executable','UserExecutable'];
  if (methodKeys.some(k => k in a)) {
    html += '<tr class="section-header"><td colspan="2">Methoden-Attribute</td></tr>';
    methodKeys.forEach(k => { if (k in a) html += attrRow(k, a[k]); });
  }
  const objKeys = ['EventNotifier'];
  if (objKeys.some(k => k in a)) {
    html += '<tr class="section-header"><td colspan="2">Objekt-Attribute</td></tr>';
    objKeys.forEach(k => { if (k in a) html += attrRow(k, a[k]); });
  }
  const typeKeys = ['IsAbstract','Symmetric','InverseName'];
  if (typeKeys.some(k => k in a)) {
    html += '<tr class="section-header"><td colspan="2">Typ-Attribute</td></tr>';
    typeKeys.forEach(k => { if (k in a) html += attrRow(k, a[k]); });
  }
  html += '</table>';
  $('detail-content').innerHTML = html;
}

function attrRow(key, val) {
  let disp;
  if (val === null || val === undefined) disp = '<em>null</em>';
  else if (typeof val === 'object') {
    if (key === 'DataType') disp = `${escHtml(val.name)} <span style="color:var(--text-dim);font-size:10px">(${escHtml(val.node_id)})</span>`;
    else if (key === 'AccessLevel' || key === 'UserAccessLevel') disp = `${val.value} – ${escHtml(val.readable)}`;
    else if (key === 'ValueRank') disp = `${val.value} (${escHtml(val.meaning)})`;
    else if (key === 'EventNotifier') {
      const flags = [];
      if (val.subscribes_to_events) flags.push('SubscribesToEvents');
      if (val.history_readable) flags.push('HistoryReadable');
      if (val.history_writable) flags.push('HistoryWritable');
      disp = `${val.value}${flags.length ? ' – ' + flags.join(', ') : ''}`;
    } else disp = escHtml(JSON.stringify(val));
  } else {
    disp = escHtml(String(val));
  }
  return `<tr><td>${escHtml(key)}</td><td>${disp}</td></tr>`;
}

function renderValue(data) {
  const nc = data.attributes?.NodeClass;
  const dv = data.data_value;
  let html = '';

  if (nc === 'Variable' && dv) {
    if (dv.error) {
      html = `<div class="value-display"><p style="color:var(--red)">${escHtml(dv.error)}</p></div>`;
    } else {
      const sc = dv.status_code || 'Good';
      const scClass = sc.startsWith('Good') ? 'status-good' : 'status-bad';
      const valStr = dv.value === null ? '<em>null</em>' : escHtml(JSON.stringify(dv.value));
      html = `
        <div class="value-display">
          <div class="value-main">${valStr}</div>
          <div class="value-meta">
            <span><strong>Status:</strong> <span class="${scClass}">${escHtml(sc)}</span> (0x${(dv.status_code_value||0).toString(16).toUpperCase()})</span>
            ${dv.source_timestamp ? `<span><strong>Quellzeitstempel:</strong> ${escHtml(dv.source_timestamp)}</span>` : ''}
            ${dv.server_timestamp ? `<span><strong>Serverzeitstempel:</strong> ${escHtml(dv.server_timestamp)}</span>` : ''}
            ${data.attributes?.DataType ? `<span><strong>Datentyp:</strong> ${escHtml(data.attributes.DataType.name)}</span>` : ''}
          </div>
        </div>`;
      const al = data.attributes?.UserAccessLevel;
      const canWrite = al ? (al.value & 2) : false;
      const subbed = subscribedNodes.has(data.node_id);
      html += `<div class="value-actions">
        ${canWrite ? `<button class="btn-primary" id="btn-do-write">Schreiben</button>` : ''}
        <button class="btn-secondary ${subbed ? 'sub-active' : ''}" id="btn-do-sub">${subbed ? '⏸ Abo aktiv' : '▶ Abo starten'}</button>
        <button class="btn-secondary" id="btn-do-refresh">↺ Aktualisieren</button>
      </div>`;
    }
  } else if (nc === 'Method') {
    html = buildMethodUI(data);
  } else {
    html = '<p class="hint">Kein Wert (kein Variable-Knoten)</p>';
  }
  $('detail-content').innerHTML = html;
  // Attach value-tab button listeners after DOM insertion to avoid inline onclick XSS
  const nodeId = data.node_id;
  const btnWrite = document.getElementById('btn-do-write');
  const btnSub = document.getElementById('btn-do-sub');
  const btnRefresh = document.getElementById('btn-do-refresh');
  const btnCallMethod = document.getElementById('btn-do-call-method');
  if (btnWrite) btnWrite.addEventListener('click', openWriteModal);
  if (btnSub) btnSub.addEventListener('click', () => toggleSubscription(nodeId));
  if (btnRefresh) btnRefresh.addEventListener('click', refreshCurrentNode);
  if (btnCallMethod) btnCallMethod.addEventListener('click', () => callMethod(nodeId));
}

function buildMethodUI(data) {
  return `
    <div class="value-display">
      <div class="value-meta"><span><strong>Ausführbar:</strong> ${data.attributes?.Executable ?? 'N/A'}</span></div>
    </div>
    <p class="hint" style="text-align:left;padding:8px 0">Methode aufrufen – Elternknoten-ID und Argumente eingeben:</p>
    <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--text-dim);margin-bottom:8px">
      Elternknoten-ID
      <input id="method-parent-id" type="text" style="background:var(--bg3);border:1px solid var(--border);color:var(--text-bright);padding:6px 8px;border-radius:4px;font-size:12px" placeholder="z.B. i=85" />
    </label>
    <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--text-dim);margin-bottom:8px">
      Argumente (JSON-Array)
      <input id="method-args" type="text" style="background:var(--bg3);border:1px solid var(--border);color:var(--text-bright);padding:6px 8px;border-radius:4px;font-size:12px" placeholder='z.B. [1, "hello"]' />
    </label>
    <button id="btn-do-call-method" class="btn-primary">Methode aufrufen</button>
    <div id="method-result" style="margin-top:10px"></div>`;
}

async function callMethod(methodId) {
  if (!activeConnId) return;
  const parentId = $('method-parent-id')?.value?.trim();
  const argsRaw = $('method-args')?.value?.trim() || '[]';
  let args;
  try { args = JSON.parse(argsRaw); } catch { args = []; }
  if (!parentId) { toast('Elternknoten-ID eingeben', 'error'); return; }
  try {
    const res = await apiFetch('/api/call-method', { method: 'POST', body: { conn_id: activeConnId, parent_id: parentId, method_id: methodId, args } });
    const el = $('method-result');
    if (el) el.innerHTML = `<div style="background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:8px;font-family:var(--mono);font-size:12px;color:var(--green)">Ergebnis: ${escHtml(JSON.stringify(res.result))}</div>`;
    logMsg(`Methode aufgerufen: ${methodId} → ${JSON.stringify(res.result)}`, 'success');
  } catch (ex) {
    const el = $('method-result');
    if (el) el.innerHTML = `<div style="color:var(--red);font-size:12px">${escHtml(ex.message)}</div>`;
    logMsg(`Methodenfehler: ${ex.message}`, 'error');
  }
}

function renderReferences(data) {
  const refs = data.references || [];
  if (!refs.length) { $('detail-content').innerHTML = '<p class="hint">Keine Referenzen</p>'; return; }
  const container = $('detail-content');
  const table = document.createElement('table');
  table.className = 'ref-table';
  table.innerHTML = '<thead><tr><th>Richtung</th><th>Referenztyp</th><th>Zielknoten</th><th>Anzeigename</th><th>Knotenklasse</th></tr></thead>';
  const tbody = document.createElement('tbody');
  refs.forEach(r => {
    const tr = document.createElement('tr');
    const dir = r.is_forward
      ? '<span class="ref-forward">→ Forward</span>'
      : '<span class="ref-backward">← Inverse</span>';
    tr.innerHTML = `
      <td>${dir}</td>
      <td><span class="ref-nodeid">${escHtml(r.reference_type_id)}</span></td>
      <td></td>
      <td>${escHtml(r.display_name)}</td>
      <td>${r.node_class ? ncBadge(r.node_class) : ''}</td>`;
    // Build node_id cell safely without inline onclick
    const tdNode = tr.querySelectorAll('td')[2];
    const span = document.createElement('span');
    span.className = 'ref-nodeid ref-link';
    span.textContent = r.node_id;
    span.addEventListener('click', () => navigateTo(r.node_id));
    tdNode.appendChild(span);
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.innerHTML = '';
  container.appendChild(table);
}

async function navigateTo(nodeId) {
  if (!activeConnId) return;
  try {
    const data = await apiFetch(`/api/attributes?conn_id=${activeConnId}&node_id=${encodeURIComponent(nodeId)}`);
    const fakeNode = { node_id: nodeId, display_name: data.attributes?.DisplayName || nodeId, node_class: data.attributes?.NodeClass || '', icon: 'node', has_children: false };
    selectedNode = fakeNode;
    renderDetail(data);
    logMsg(`Navigiert zu: ${nodeId}`, 'info');
  } catch (ex) {
    logMsg(`Navigation fehlgeschlagen: ${ex.message}`, 'error');
  }
}

async function refreshCurrentNode() {
  if (selectedNode) await loadNodeDetail(selectedNode.node_id);
}

// ── Tabs ──────────────────────────────────────────────────────────────────
function switchTab(tab) {
  currentTab = tab;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  if (selectedNode) loadNodeDetail(selectedNode.node_id);
  else $('detail-content').innerHTML = '<p class="hint">Knoten auswählen</p>';
}

// ── Write ─────────────────────────────────────────────────────────────────
function openWriteModal() {
  if (!selectedNode) return;
  $('write-node-label').textContent = `Knoten: ${selectedNode.node_id}`;
  $('write-value-input').value = '';
  $('write-type-hint').textContent = '';
  $('modal-write').classList.remove('hidden');
  $('write-value-input').focus();
}

async function confirmWrite() {
  if (!selectedNode || !activeConnId) return;
  const val = $('write-value-input').value;
  try {
    await apiFetch('/api/write', { method: 'POST', body: { conn_id: activeConnId, node_id: selectedNode.node_id, value: val } });
    toast('Wert geschrieben', 'success');
    logMsg(`Wert geschrieben: ${selectedNode.node_id} = ${val}`, 'success');
    $('modal-write').classList.add('hidden');
    if (currentTab === 'value') await loadNodeDetail(selectedNode.node_id);
  } catch (ex) {
    toast(`Schreiben fehlgeschlagen: ${ex.message}`, 'error');
    logMsg(`Schreiben fehlgeschlagen: ${ex.message}`, 'error');
  }
}

// ── Favorites ─────────────────────────────────────────────────────────────
async function loadFavorites() {
  if (!activeConnId) return;
  favorites = await apiFetch(`/api/favorites?conn_id=${activeConnId}`);
  renderFavorites();
}

function renderFavorites() {
  const list = $('favorites-list');
  list.innerHTML = '';
  if (!favorites.length) { list.innerHTML = '<p class="hint">Keine Favoriten</p>'; return; }
  favorites.forEach(f => {
    const div = document.createElement('div');
    div.className = `fav-item${selectedNode?.node_id === f.node_id ? ' active' : ''}`;
    const iconSpan = document.createElement('span');
    iconSpan.className = 'fav-icon';
    iconSpan.textContent = '★';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'fav-name';
    nameSpan.title = f.node_id;          // safe: .title property, not innerHTML
    nameSpan.textContent = f.display_name || f.node_id;
    const removeBtn = document.createElement('button');
    removeBtn.className = 'fav-remove';
    removeBtn.title = 'Favorit entfernen';
    removeBtn.textContent = '✕';
    removeBtn.addEventListener('click', (e) => { e.stopPropagation(); removeFavorite(f.id); });
    div.appendChild(iconSpan);
    div.appendChild(nameSpan);
    div.appendChild(removeBtn);
    div.addEventListener('click', () => {
      selectedNode = { node_id: f.node_id, display_name: f.display_name, node_class: f.node_class, icon: 'node', has_children: false };
      loadNodeDetail(f.node_id);
      renderFavorites();
    });
    list.appendChild(div);
  });
}

async function toggleFavorite() {
  if (!selectedNode || !activeConnId) return;
  const existing = favorites.find(f => f.node_id === selectedNode.node_id);
  if (existing) {
    await removeFavorite(existing.id);
  } else {
    try {
      await apiFetch('/api/favorites', {
        method: 'POST',
        body: {
          conn_id: activeConnId,
          node_id: selectedNode.node_id,
          display_name: selectedNode.display_name || selectedNode.node_id,
          browse_name: selectedNode.browse_name || '',
          node_class: selectedNode.node_class || '',
          data_type: '',
        }
      });
      logMsg(`Favorit gespeichert: ${selectedNode.display_name || selectedNode.node_id}`, 'success');
      toast('Als Favorit gespeichert', 'success');
      await loadFavorites();
      $('btn-fav-toggle').classList.add('active');
      $('btn-fav-toggle').title = 'Favorit entfernen';
    } catch (ex) {
      logMsg(`Favorit-Fehler: ${ex.message}`, 'error');
    }
  }
}

async function removeFavorite(id, e) {
  e && e.stopPropagation();
  try {
    await apiFetch(`/api/favorites/${id}`, { method: 'DELETE' });
    await loadFavorites();
    if (selectedNode) {
      const isFav = favorites.some(f => f.node_id === selectedNode.node_id);
      $('btn-fav-toggle').classList.toggle('active', isFav);
      $('btn-fav-toggle').title = isFav ? 'Favorit entfernen' : 'Als Favorit speichern';
    }
  } catch (ex) {
    logMsg(`Favorit löschen fehlgeschlagen: ${ex.message}`, 'error');
  }
}

// ── WebSocket subscriptions ───────────────────────────────────────────────
function openWS(connId) {
  if (ws) { ws.close(); ws = null; }
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws/${connId}`);
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'datachange') {
      handleDataChange(msg);
    } else if (msg.type === 'error') {
      logMsg(`WS Fehler (${msg.node_id}): ${msg.message}`, 'error');
    }
  };
  ws.onclose = () => {
    logMsg('WebSocket getrennt', 'warn');
  };
  ws.onerror = () => {
    logMsg('WebSocket Fehler', 'error');
  };
}

function waitForWS(socket, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    if (socket.readyState === WebSocket.OPEN) { resolve(); return; }
    const timer = setTimeout(() => {
      socket.removeEventListener('open', onOpen);
      socket.removeEventListener('error', onErr);
      reject(new Error('WebSocket-Verbindungs-Timeout'));
    }, timeoutMs);
    function onOpen() { clearTimeout(timer); socket.removeEventListener('error', onErr); resolve(); }
    function onErr() { clearTimeout(timer); socket.removeEventListener('open', onOpen); reject(new Error('WebSocket Fehler')); }
    socket.addEventListener('open', onOpen);
    socket.addEventListener('error', onErr);
  });
}

function handleDataChange(msg) {
  const ts = msg.source_timestamp || msg.server_timestamp || '';
  logMsg(`Datenänderung: ${msg.node_id} = ${JSON.stringify(msg.value)} [${msg.status_code}] ${ts ? '@' + ts : ''}`, 'info');
  // Refresh detail if this is the selected node
  if (selectedNode?.node_id === msg.node_id && currentTab === 'value') {
    loadNodeDetail(msg.node_id);
  }
}

async function toggleSubscription(nodeId) {
  if (!activeConnId) return;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    openWS(activeConnId);
    try {
      await waitForWS(ws);
    } catch (ex) {
      logMsg(`WebSocket konnte nicht geöffnet werden: ${ex.message}`, 'error');
      toast(`WebSocket Fehler: ${ex.message}`, 'error');
      return;
    }
  }
  if (subscribedNodes.has(nodeId)) {
    ws.send(JSON.stringify({ action: 'unsubscribe', node_id: nodeId }));
    subscribedNodes.delete(nodeId);
    logMsg(`Abo gestoppt: ${nodeId}`, 'info');
    toast('Abo gestoppt', 'info');
  } else {
    ws.send(JSON.stringify({ action: 'subscribe', node_id: nodeId }));
    subscribedNodes.add(nodeId);
    logMsg(`Abo gestartet: ${nodeId}`, 'success');
    toast('Abo gestartet', 'success');
  }
  // Re-render value tab to update button state
  if (selectedNode?.node_id === nodeId && currentTab === 'value') {
    loadNodeDetail(nodeId);
  }
}

// ── Modal helpers ─────────────────────────────────────────────────────────
function closeModal(id) {
  $(id).classList.add('hidden');
}

// ── Event listeners ───────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadConnections();

  $('btn-add-conn').addEventListener('click', () => openConnectionModal());
  $('btn-conn-save').addEventListener('click', saveConnection);
  $('btn-conn-cancel').addEventListener('click', () => closeModal('modal-conn'));
  $('btn-write-cancel').addEventListener('click', () => closeModal('modal-write'));
  $('btn-write-confirm').addEventListener('click', confirmWrite);
  $('btn-fav-toggle').addEventListener('click', toggleFavorite);
  $('btn-refresh-tree').addEventListener('click', refreshTree);
  $('btn-clear-log').addEventListener('click', () => { $('log-list').innerHTML = ''; });

  // Tab switching
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  // Close modals via X buttons
  document.querySelectorAll('.modal-close').forEach(btn => {
    btn.addEventListener('click', () => closeModal(btn.dataset.modal));
  });

  // Close modal on backdrop click
  document.querySelectorAll('.modal').forEach(m => {
    m.addEventListener('click', (e) => {
      if (e.target === m) m.classList.add('hidden');
    });
  });

  // Enter key in write dialog
  $('write-value-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') confirmWrite();
  });

  // Enter key in connection name field
  $('cf-url').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') saveConnection();
  });

  logMsg('OPC UA Browser gestartet', 'info');
});

// No global window exports needed – all event handlers are attached via addEventListener
