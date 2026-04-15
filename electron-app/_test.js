
// ────────────────────────────────────────────────────────
// TRADUCTIONS
// ────────────────────────────────────────────────────────
const T = {
  fr: {
    active: 'Actifs', done: 'Terminés', failed: 'Échoués',
    clearBtn: '🗑 Effacer tout',
    launchTitle: 'Lancer un agent', roleLabel: 'Rôle',
    instrLabel: 'Instructions', instrPh: 'Décris la tâche à accomplir...',
    modelLabel: 'Modèle', launchBtn: "Lancer l'agent", artyBtn: '3 agents Arty',
    appTitle: "Lancer l'application", appUrlLabel: 'URL de l\'application',
    appBtn: "Ouvrir l'application", dashBtn: 'Ouvrir dans le navigateur',
    tabAgents: 'Agents', tabChat: 'Chat', sendBtn: 'Envoyer',
    chatPh: 'Dis-moi ce que tu veux faire...',
    emptyAgents: 'Aucun agent. Lancez-en un !',
    docsTitle: 'Documentation',
    confirmClear: 'Supprimer tous les agents ? Action irréversible.',
    noPrompt: 'Veuillez entrer des instructions.',
    noUrl: 'Entrez une URL (ex: http://localhost:3000)',
    errMsg: 'Erreur : ', thinking: 'Réflexion en cours...', toolUsed: 'Outil : '
  },
  en: {
    active: 'Active', done: 'Done', failed: 'Failed',
    clearBtn: '🗑 Clear all',
    launchTitle: 'Launch an agent', roleLabel: 'Role',
    instrLabel: 'Instructions', instrPh: 'Describe the task...',
    modelLabel: 'Model', launchBtn: 'Launch agent', artyBtn: '3 Arty agents',
    appTitle: 'Launch application', appUrlLabel: 'Application URL',
    appBtn: 'Open application', dashBtn: 'Open in browser',
    tabAgents: 'Agents', tabChat: 'Chat', sendBtn: 'Send',
    chatPh: 'Tell me what you want to do...',
    emptyAgents: 'No agents yet. Launch one!',
    docsTitle: 'Documentation',
    confirmClear: 'Delete all agents? This cannot be undone.',
    noPrompt: 'Please enter instructions.',
    noUrl: 'Enter a URL (e.g. http://localhost:3000)',
    errMsg: 'Error: ', thinking: 'Thinking...', toolUsed: 'Tool: '
  }
};

const DOCS = {
  fr: `
  <div class="doc-sec"><h4>🤖 Qu'est-ce que l'Orchestrateur ?</h4>
    <p>Un système multi-agents alimenté par Claude (Anthropic). Chaque agent est une instance IA spécialisée.
    L'orchestrateur gère leur cycle de vie et coordonne les résultats.</p></div>
  <div class="doc-sec"><h4>🚀 Lancer un agent</h4><ul>
    <li>Choisissez un <strong>rôle</strong> (Codeur, Testeur, Bug Hunter, Reviewer, Custom)</li>
    <li>Rédigez des <strong>instructions</strong> précises</li>
    <li>Sélectionnez un <strong>modèle</strong> : Sonnet = puissant, Haiku = rapide/économique</li>
    <li>Cliquez sur <em>Lancer l'agent</em></li></ul></div>
  <div class="doc-sec"><h4>🔄 Pipeline automatique (Supervisor)</h4>
    <p>Le supervisor (Claude Opus) analyse les agents terminés et décide de lancer des suivis :</p><ul>
    <li><span class="dk dk-o">Codeur</span> → peut déclencher <span class="dk dk-b">Testeur</span></li>
    <li><span class="dk dk-b">Testeur</span> → peut déclencher <span class="dk dk-g">Bug Hunter</span></li>
    <li>Décision toutes les 60 secondes</li></ul></div>
  <div class="doc-sec"><h4>💬 Chat</h4>
    <p>L'onglet <strong>Chat</strong> permet de dialoguer directement avec un assistant IA pour poser des questions,
    analyser du code ou orchestrer des tâches complexes.</p></div>
  <div class="doc-sec"><h4>🚀 Lancer l'application</h4>
    <p>Le panneau <strong>Lancer l'application</strong> permet d'ouvrir votre application cible directement
    depuis l'orchestrateur. Entrez son URL et cliquez.</p></div>
  <div class="doc-sec"><h4>📊 Statuts</h4><ul>
    <li><span class="dk dk-b">Running</span> En cours</li>
    <li><span class="dk dk-g">Done</span> Terminé avec succès</li>
    <li><span class="dk" style="background:#fdedec;color:#e74c3c">Failed</span> Échec — relançable</li>
    <li><span class="dk" style="background:#f2f3f4;color:#95a5a6">Interrupted</span> Interrompu — relançable</li></ul></div>`,
  en: `
  <div class="doc-sec"><h4>🤖 What is the Orchestrator?</h4>
    <p>A multi-agent system powered by Claude (Anthropic). Each agent is a specialized AI instance.
    The orchestrator manages their lifecycle and coordinates results.</p></div>
  <div class="doc-sec"><h4>🚀 Launching an agent</h4><ul>
    <li>Choose a <strong>role</strong> (Coder, Tester, Bug Hunter, Reviewer, Custom)</li>
    <li>Write clear <strong>instructions</strong></li>
    <li>Select a <strong>model</strong>: Sonnet = powerful, Haiku = fast/cheap</li>
    <li>Click <em>Launch agent</em></li></ul></div>
  <div class="doc-sec"><h4>🔄 Automatic pipeline (Supervisor)</h4>
    <p>The supervisor (Claude Opus) analyzes completed agents and may spawn follow-ups:</p><ul>
    <li><span class="dk dk-o">Coder</span> → may trigger <span class="dk dk-b">Tester</span></li>
    <li><span class="dk dk-b">Tester</span> → may trigger <span class="dk dk-g">Bug Hunter</span></li>
    <li>Decision every 60 seconds</li></ul></div>
  <div class="doc-sec"><h4>💬 Chat</h4>
    <p>The <strong>Chat</strong> tab lets you talk to an AI assistant directly — ask questions,
    analyze code or orchestrate complex tasks.</p></div>
  <div class="doc-sec"><h4>🚀 Launch application</h4>
    <p>The <strong>Launch application</strong> panel lets you open your target app directly
    from the orchestrator. Enter its URL and click.</p></div>
  <div class="doc-sec"><h4>📊 Statuses</h4><ul>
    <li><span class="dk dk-b">Running</span> Currently executing</li>
    <li><span class="dk dk-g">Done</span> Completed successfully</li>
    <li><span class="dk" style="background:#fdedec;color:#e74c3c">Failed</span> Failed — retryable</li>
    <li><span class="dk" style="background:#f2f3f4;color:#95a5a6">Interrupted</span> Interrupted — retryable</li></ul></div>`
};

// ────────────────────────────────────────────────────────
// i18n
// ────────────────────────────────────────────────────────
let lang = localStorage.getItem('lang') || 'fr';
function t(k) { return T[lang][k] || k; }

function applyLang() {
  document.getElementById('btn-fr').classList.toggle('on', lang === 'fr');
  document.getElementById('btn-en').classList.toggle('on', lang === 'en');
  document.querySelectorAll('[data-i]').forEach(function(el) {
    el.textContent = t(el.getAttribute('data-i'));
  });
  document.querySelectorAll('[data-iph]').forEach(function(el) {
    el.placeholder = t(el.getAttribute('data-iph'));
  });
}

function setLang(l) {
  lang = l;
  localStorage.setItem('lang', l);
  applyLang();
  if (document.getElementById('docs-content').innerHTML) {
    renderDocs();
  }
}

// ────────────────────────────────────────────────────────
// TABS
// ────────────────────────────────────────────────────────
let chatLoaded = false;

function switchTab(name) {
  document.getElementById('tab-agents').classList.toggle('active', name === 'agents');
  document.getElementById('tab-chat').classList.toggle('active', name === 'chat');
  document.getElementById('pane-agents').classList.toggle('active', name === 'agents');
  document.getElementById('pane-chat').classList.toggle('active', name === 'chat');
  if (name === 'chat' && !chatLoaded) loadChatHistory();
}

// ────────────────────────────────────────────────────────
// STATS & AGENTS
// ────────────────────────────────────────────────────────
async function fetchStats() {
  try {
    const d = await (await fetch('/api/stats')).json();
    document.getElementById('s-active').textContent = d.active;
    document.getElementById('s-done').textContent = d.done;
    document.getElementById('s-failed').textContent = d.failed;
    document.getElementById('s-tok').textContent = d.tokens_used.toLocaleString();
  } catch(e) {}
}

async function fetchAgents() {
  try {
    const agents = await (await fetch('/api/agents')).json();
    renderAgents(agents);
  } catch(e) {}
}

function badge(s) {
  const map = {
    running: '<span class="badge b-running"><span class="spinner"></span> Running</span>',
    done:    '<span class="badge b-done">✓ Done</span>',
    failed:  '<span class="badge b-failed">✗ Failed</span>',
    interrupted: '<span class="badge b-interrupted">■ Interrupted</span>',
    pending: '<span class="badge b-pending">⌛ Pending</span>'
  };
  return map[s] || ('<span class="badge">' + s + '</span>');
}

function fmtDate(iso) {
  if (!iso) return '';
  var d = new Date(iso + 'Z');
  return d.toLocaleString(lang === 'fr' ? 'fr-FR' : 'en-GB', { day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit' });
}

function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function escAttr(s) {
  if (!s) return '';
  return String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/\n/g,'\\n');
}

function escTpl(s) {
  if (!s) return '';
  return String(s).replace(/\\/g,'\\\\').replace(/`/g,'\\`').replace(/\$/g,'\\$');
}

function renderAgents(agents) {
  var el = document.getElementById('agent-list');
  if (!agents.length) {
    el.innerHTML = '<div class="empty">' + t('emptyAgents') + '</div>';
    return;
  }
  var html = '';
  for (var i = 0; i < agents.length; i++) {
    var a = agents[i];
    var raw = a.result || '';
    var preview = raw.length > 0 ? raw.substring(0, 180) + (raw.length > 180 ? '…' : '') : '';
    var canRetry = a.status === 'interrupted' || a.status === 'failed';
    var parentInfo = a.parent_id ? '↳ #' + a.parent_id + ' &bull; ' : '';
    html += '<div class="card">';
    html += '<div class="card-hdr"><span class="card-role">#' + a.id + ' — ' + esc(a.role) + '</span>' + badge(a.status) + '</div>';
    html += '<div class="card-meta">' + parentInfo + esc(a.model) + ' &bull; ' + fmtDate(a.created_at);
    if (a.tokens_used) html += ' &bull; ' + a.tokens_used.toLocaleString() + ' tok';
    html += '</div>';
    if (preview) html += '<div class="card-preview">' + esc(preview) + '</div>';
    html += '<div class="card-actions">';
    if (raw) html += '<button class="btn btn-sm btn-view" onclick="showResult(' + a.id + ',\'' + escAttr(a.role) + '\',`' + escTpl(raw) + '`)">Voir</button>';
    if (canRetry) html += '<button class="btn btn-sm btn-retry" onclick="retryAgent(' + a.id + ')">Relancer</button>';
    html += '</div></div>';
  }
  el.innerHTML = html;
}

async function launchAgent() {
  var role = document.getElementById('role').value;
  var prompt = document.getElementById('prompt').value.trim();
  var model = document.getElementById('model').value;
  if (!prompt) { alert(t('noPrompt')); return; }
  try {
    await fetch('/api/agents', { method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ role: role, prompt: prompt, model: model }) });
    document.getElementById('prompt').value = '';
    await refresh();
  } catch(e) { alert(t('errMsg') + e.message); }
}

async function launch3Agents() {
  var agents = [
    { role: 'Codeur', model: 'claude-sonnet-4-6', prompt: 'Analyse la structure du projet Arty, identifie les composants principaux et propose des améliorations concrètes.' },
    { role: 'Testeur', model: 'claude-sonnet-4-6', prompt: 'Génère une suite de tests unitaires et d\'intégration pour le projet Arty.' },
    { role: 'Bug Hunter', model: 'claude-sonnet-4-6', prompt: 'Analyse la sécurité et la qualité du projet Arty, identifie les bugs et vulnérabilités.' }
  ];
  try {
    await Promise.all(agents.map(function(a) {
      return fetch('/api/agents', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(a) });
    }));
    await refresh();
  } catch(e) { alert(t('errMsg') + e.message); }
}

async function retryAgent(id) {
  try { await fetch('/api/agents/' + id + '/retry', { method: 'POST' }); await refresh(); } catch(e) {}
}

async function clearAll() {
  if (!confirm(t('confirmClear'))) return;
  try { await fetch('/api/agents', { method: 'DELETE' }); await refresh(); } catch(e) { alert(t('errMsg') + e.message); }
}

// ────────────────────────────────────────────────────────
// LAUNCH APPLICATION
// ────────────────────────────────────────────────────────
function openApp() {
  var url = document.getElementById('app-url').value.trim();
  if (!url) { alert(t('noUrl')); return; }
  window.open(url, '_blank');
}

function openDashboard() {
  window.open('http://127.0.0.1:8000', '_blank');
}

// ────────────────────────────────────────────────────────
// CHAT
// ────────────────────────────────────────────────────────
async function loadChatHistory() {
  chatLoaded = true;
  try {
    var d = await (await fetch('/api/chat/history')).json();
    d.messages.forEach(function(m) { addMsg(m.role, m.content); });
    scrollChat();
  } catch(e) {}
}

function addMsg(role, content, cls) {
  var el = document.getElementById('chat-msgs');
  var d = document.createElement('div');
  d.className = 'msg ' + (cls || role);
  d.innerHTML = content.replace(/\n/g, '<br>').replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
  el.appendChild(d);
  scrollChat();
  return d;
}

function scrollChat() { var el = document.getElementById('chat-msgs'); el.scrollTop = el.scrollHeight; }

async function sendChat() {
  var inp = document.getElementById('chat-in');
  var text = inp.value.trim();
  if (!text) return;
  inp.value = '';
  document.getElementById('chat-send').disabled = true;
  addMsg('user', text);

  var typing = document.createElement('div');
  typing.className = 'msg-typing'; typing.id = 'typing'; typing.textContent = t('thinking');
  document.getElementById('chat-msgs').appendChild(typing); scrollChat();

  var aDiv = null; var aText = '';
  try {
    var resp = await fetch('/api/chat', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({message: text}) });
    var reader = resp.body.getReader(); var dec = new TextDecoder();
    while (true) {
      var res = await reader.read();
      if (res.done) break;
      var lines = dec.decode(res.value).split('\n');
      for (var i = 0; i < lines.length; i++) {
        var line = lines[i];
        if (!line.startsWith('data: ')) continue;
        try {
          var ev = JSON.parse(line.slice(6));
          if (ev.type === 'text') {
            var ty = document.getElementById('typing'); if (ty) ty.remove();
            if (!aDiv) aDiv = addMsg('assistant', '');
            aText += ev.content;
            aDiv.innerHTML = aText.replace(/\n/g, '<br>').replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
            scrollChat();
          } else if (ev.type === 'tool_start') {
            var ty2 = document.getElementById('typing'); if (ty2) ty2.textContent = t('toolUsed') + ev.tool;
          } else if (ev.type === 'tool_result') {
            addMsg('assistant', '🔧 ' + ev.tool + ': ' + ev.result.substring(0, 100) + '…', 'tool');
          } else if (ev.type === 'done') {
            var ty3 = document.getElementById('typing'); if (ty3) ty3.remove();
          }
        } catch(e) {}
      }
    }
  } catch(e) { addMsg('assistant', 'Erreur de connexion.'); }

  var ty4 = document.getElementById('typing'); if (ty4) ty4.remove();
  document.getElementById('chat-send').disabled = false;
  inp.focus();
}

document.getElementById('chat-in').addEventListener('keydown', function(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
});
document.getElementById('chat-in').addEventListener('input', function() {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 96) + 'px';
});

// ────────────────────────────────────────────────────────
// MODALS
// ────────────────────────────────────────────────────────
function showResult(id, role, result) {
  document.getElementById('res-title').textContent = 'Agent #' + id + ' — ' + role;
  document.getElementById('res-content').textContent = result;
  document.getElementById('ov-result').classList.add('show');
}

function renderDocs() { document.getElementById('docs-content').innerHTML = DOCS[lang]; }
function showDocs() { renderDocs(); document.getElementById('ov-docs').classList.add('show'); }

function closeOverlay(id, e) {
  if (!e || e.target === document.getElementById(id)) {
    document.getElementById(id).classList.remove('show');
  }
}

function copyText() {
  var text = document.getElementById('res-content').textContent;
  navigator.clipboard.writeText(text).then(function() {
    var btn = document.getElementById('copy-btn');
    btn.textContent = '✓ Copié !'; btn.classList.add('ok');
    setTimeout(function() { btn.textContent = '📋 Copier'; btn.classList.remove('ok'); }, 2000);
  });
}

document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    document.getElementById('ov-result').classList.remove('show');
    document.getElementById('ov-docs').classList.remove('show');
  }
});

// ────────────────────────────────────────────────────────
// POLLING
// ────────────────────────────────────────────────────────
async function refresh() { await Promise.all([fetchStats(), fetchAgents()]); }
applyLang();
refresh();
setInterval(refresh, 5000);
