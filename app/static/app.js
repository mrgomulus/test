let debounceTimer;
let wizardState = { step: 1 };

function showTab(id) {
  document.querySelectorAll('.tab').forEach(x => x.classList.add('hidden'));
  document.getElementById(id).classList.remove('hidden');
  if (id === 'analytics') loadAnalytics();
  if (id === 'entry') loadWizard();
}

async function fetchDisturbances() {
  const params = new URLSearchParams({
    q: document.getElementById('searchInput').value,
    category: document.getElementById('filterCategory').value,
    responsible: document.getElementById('filterResponsible').value,
    min_duration: document.getElementById('filterMinDuration').value,
    max_duration: document.getElementById('filterMaxDuration').value,
    start: document.getElementById('filterStart').value,
    end: document.getElementById('filterEnd').value,
  });
  const res = await fetch(`/api/disturbances?${params.toString()}`);
  const data = await res.json();
  const tbody = document.querySelector('#results tbody');
  tbody.innerHTML = '';
  data.forEach(row => {
    tbody.innerHTML += `<tr><td>${row.event_time || ''}</td><td>${row.stoerung_text || ''}</td><td>${row.kategorie || ''}</td><td>${row.dauer_minuten || ''}</td><td>${row.verantwortlicher || ''}</td></tr>`;
  });
}

['searchInput','filterCategory','filterResponsible','filterMinDuration','filterMaxDuration','filterStart','filterEnd']
  .forEach(id => document.getElementById(id)?.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(fetchDisturbances, 250);
  }));

async function loadWizard() {
  const md = await (await fetch('/api/master-data')).json();
  renderWizard(md);
}

function renderWizard(md) {
  const root = document.getElementById('wizard');
  const out = document.getElementById('wizardData');
  out.innerText = JSON.stringify(wizardState, null, 2);

  if (wizardState.step === 1) {
    root.innerHTML = '<h4>1) Linie wählen</h4>' + md.lines.map(l => `<button onclick="selectLine(${l.id})">${l.name}</button>`).join('');
    return;
  }
  if (wizardState.step === 2) {
    const allowed = md.mapping.filter(m => m.line_id === wizardState.line_id).map(m => m.subplant_id);
    const sps = md.subplants.filter(s => allowed.includes(s.id));
    root.innerHTML = '<h4>2) Teilanlage wählen</h4>' + sps.map(s => `<button onclick="selectSubplant(${s.id}, '${s.name}')">${s.name}</button>`).join('') + '<button onclick="otherSubplant()">Andere Teilanlage</button>';
    return;
  }
  if (wizardState.step === 3) {
    root.innerHTML = '<h4>3) Zeit wählen</h4>' + ['Heute','Gestern','Letzte Woche','Benutzerdefiniert'].map(x => `<button onclick="selectTime('${x}')">${x}</button>`).join('');
    return;
  }
  if (wizardState.step === 4) {
    root.innerHTML = '<h4>4) Kategorie</h4>' + ['Elektrik','Mechanik','Software','Sonstiges'].map(x => `<button onclick="selectCategory('${x}')">${x}</button>`).join('');
    return;
  }
  root.innerHTML = `<h4>5) Details</h4>
    <input id="fStoerung" placeholder="Störung"/>
    <input id="fUrsache" placeholder="Ursache"/>
    <input id="fBehebung" placeholder="Behebung"/>
    <input id="fDauer" type="number" placeholder="Dauer"/>
    <input id="fVerantwortlicher" placeholder="Verantwortlicher"/>
    <button onclick="submitWizard()">Speichern</button>
    <button onclick="goBack()">Zurück</button>`;
}

function selectLine(id){ wizardState.line_id = id; wizardState.step = 2; loadWizard(); }
function selectSubplant(id, name){ wizardState.teilanlage_id = id; wizardState.teilanlage_name = name; wizardState.step = 3; loadWizard(); }
function otherSubplant(){ const x = prompt('Teilanlage Name'); wizardState.teilanlage_name = x; wizardState.step = 3; loadWizard(); }
function selectTime(value){ wizardState.zeit = value; wizardState.step = 4; loadWizard(); }
function selectCategory(value){ wizardState.kategorie = value; wizardState.step = 5; loadWizard(); }
function goBack(){ wizardState.step = Math.max(1, wizardState.step - 1); loadWizard(); }

async function submitWizard() {
  const payload = {
    stoerung_text: document.getElementById('fStoerung').value,
    ursache: document.getElementById('fUrsache').value,
    behebung: document.getElementById('fBehebung').value,
    dauer_minuten: Number(document.getElementById('fDauer').value || 0),
    verantwortlicher: document.getElementById('fVerantwortlicher').value,
    kategorie: wizardState.kategorie,
    teilanlage_id: wizardState.teilanlage_id,
    event_time: new Date().toISOString()
  };
  await fetch('/api/disturbances', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  alert('Gespeichert');
  wizardState = {step:1};
  loadWizard();
}

let topChart, categoryChart, trendChart;
async function loadAnalytics() {
  const data = await (await fetch('/api/analytics')).json();
  const topLabels = data.top.map(x => x.subplant);
  const topVals = data.top.map(x => x.count);
  topChart?.destroy();
  topChart = new Chart(document.getElementById('topChart'), {type:'bar', data:{labels: topLabels, datasets:[{label:'Anzahl', data: topVals}]}});

  categoryChart?.destroy();
  categoryChart = new Chart(document.getElementById('categoryChart'), {type:'pie', data:{labels: data.categories.map(x=>x.category), datasets:[{data: data.categories.map(x=>x.count)}]}});

  trendChart?.destroy();
  trendChart = new Chart(document.getElementById('trendChart'), {type:'line', data:{labels: data.trend.map(x=>x.day), datasets:[{label:'Trend', data: data.trend.map(x=>x.count)}]}});

  document.getElementById('prediction').innerText = JSON.stringify(data.prediction?.prediction_json || {}, null, 2);
}

async function testAd() {
  const payload = {
    server: adServer.value,
    domain: adDomain.value,
    username: adUser.value,
    password: adPassword.value
  };
  const res = await fetch('/api/settings/ad-test', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  const data = await res.json();
  adResult.innerText = data.message;
}

async function saveSettings() {
  await fetch('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({'preview.enabled': previewEnabled.checked})});
  alert('Gespeichert');
}

async function createDummy() {
  const res = await fetch('/api/preview/dummy', {method:'POST'});
  const data = await res.json();
  alert(JSON.stringify(data));
}

async function changePassword(){
  const fd = new FormData();
  fd.append('new_password', newPassword.value);
  await fetch('/auth/change-password', {method:'POST', body: fd});
  alert('Passwort geändert');
}

fetchDisturbances();
