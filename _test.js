
var currentUrl = null;
var creationRunning = false;

// ── Exemples ──────────────────────────────────────────────────────────────────
var EXAMPLES = {
  'Liste de tâches': 'Une application de liste de tâches avec la possibilité d\'ajouter des tâches, les cocher comme faites, et les supprimer. Interface moderne et claire.',
  'Calculatrice': 'Une calculatrice avec les opérations de base (addition, soustraction, multiplication, division) et un historique des calculs.',
  'Convertisseur de devises': 'Un convertisseur pour les devises courantes (EUR, USD, GBP, JPY) avec des taux approximatifs. Interface simple avec deux champs.',
  'Prise de notes': 'Une application de notes avec titre et contenu, possibilité d\'ajouter et supprimer des notes. Sauvegarde en session.',
  'Minuteur / Chronomètre': 'Un minuteur avec décompte configurable et un chronomètre avec démarrer/arrêter/réinitialiser. Design épuré.',
  'Générateur de mots de passe': 'Un générateur de mots de passe sécurisés avec options de longueur, majuscules, chiffres, symboles. Bouton copier.'
};
function fillExample(btn) {
  var text = btn.textContent.trim();
  document.getElementById('description').value = EXAMPLES[text] || text;
}

// ── Stats ─────────────────────────────────────────────────────────────────────
async function loadStats() {
  try {
    var d = await (await fetch('/api/stats')).json();
    document.getElementById('hdr-total').textContent = d.total;
    document.getElementById('hdr-running').textContent = d.running;
  } catch(e) {}
}

// ── Liste des apps ────────────────────────────────────────────────────────────
async function loadApps() {
  try {
    var apps = await (await fetch('/api/apps')).json();
    renderApps(apps);
    await loadStats();
  } catch(e) {}
}

function statusInfo(s) {
  var map = {
    running:  { text: '● En ligne',      cls: 'st-running',  dot: 'dot-running',  icon: '●' },
    stopped:  { text: '◯ Arrêtée',       cls: 'st-stopped',  dot: 'dot-stopped',  icon: '◯' },
    failed:   { text: '✗ Erreur',         cls: 'st-failed',   dot: 'dot-failed',   icon: '✗' },
    creating: { text: '⏳ En création...', cls: 'st-creating', dot: 'dot-creating', icon: '⏳' },
  };
  return map[s] || map['stopped'];
}

function renderApps(apps) {
  var el = document.getElementById('apps-grid');
  if (!apps.length) {
    el.innerHTML = '<div class="empty-state"><span style="font-size:2.5rem">🚀</span><p>Votre première application apparaîtra ici.</p></div>';
    return;
  }
  el.innerHTML = apps.map(function(a) {
    var si = statusInfo(a.status);
    var url = 'http://localhost:' + a.port;
    var isRunning = a.status === 'running';
    var isStopped = a.status === 'stopped' || a.status === 'failed';
    return '<div class="app-card" id="app-' + a.id + '">'
      + '<div class="app-card-header">'
      + '<span class="app-name">' + esc(a.name || ('Application #' + a.id)) + '</span>'
      + '<span class="status-dot ' + si.dot + '"></span>'
      + '</div>'
      + '<div class="app-desc">' + esc(a.description.substring(0, 100) + (a.description.length > 100 ? '…' : '')) + '</div>'
      + '<span class="app-status-text ' + si.cls + '">' + si.text + '</span>'
      + (a.error && a.status === 'failed' ? '<div style="font-size:.76rem;color:#e74c3c;background:#fdedec;padding:6px 10px;border-radius:6px">' + esc(a.error.substring(0,120)) + '</div>' : '')
      + '<div class="app-actions">'
      + (isRunning ? '<button class="btn btn-open" onclick="openApp(\'' + url + '\')">🌐 Ouvrir</button>' : '')
      + (isRunning ? '<button class="btn btn-stop" onclick="stopApp(' + a.id + ')">⏹ Arrêter</button>' : '')
      + (isStopped ? '<button class="btn btn-start" onclick="startApp(' + a.id + ')">▶ Relancer</button>' : '')
      + '<button class="btn btn-delete" onclick="deleteApp(' + a.id + ')">🗑</button>'
      + '</div></div>';
  }).join('');
}

function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Création ──────────────────────────────────────────────────────────────────
var STEP_MAP = {
  'réfléchis': 0, 'réflexion': 0, 'réfléchit': 0,
  'écris': 1, 'écriture': 1, 'code': 1, 'j\'écris': 1,
  'corrige': 2, 'vérif': 2, 'correction': 2, 'syntaxe': 2,
  'installe': 3, 'bibliothèque': 3, 'composant': 3, 'install': 3,
  'démarre': 4, 'démarr': 4, 'lance': 4, 'start': 4,
  'attends': 5, 'attend': 5, 'prête': 5, 'prêt': 5,
};

function detectStep(msg) {
  var lower = msg.toLowerCase();
  for (var k in STEP_MAP) {
    if (lower.includes(k)) return STEP_MAP[k];
  }
  return -1;
}

function setStep(stepIdx) {
  for (var i = 0; i < 6; i++) {
    var el = document.getElementById('step-' + i);
    if (!el) continue;
    el.className = 'step ' + (i < stepIdx ? 'done' : i === stepIdx ? 'active' : 'pending');
  }
  document.getElementById('progress').style.width = Math.round((stepIdx / 5) * 85) + '%';
}

async function startCreation() {
  var desc = document.getElementById('description').value.trim();
  if (!desc) { document.getElementById('description').focus(); return; }

  creationRunning = true;
  currentUrl = null;
  document.getElementById('btn-create').disabled = true;
  document.getElementById('success-box').className = 'success-box';
  document.getElementById('error-box').className = 'error-box';
  document.getElementById('error-box').textContent = '';
  document.getElementById('btn-cancel').style.display = '';
  for (var i = 0; i < 6; i++) {
    var s = document.getElementById('step-' + i);
    if (s) s.className = 'step pending';
  }
  document.getElementById('progress').style.width = '0%';
  document.getElementById('overlay').classList.add('show');

  try {
    var resp = await fetch('/api/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: desc })
    });

    var reader = resp.body.getReader();
    var dec = new TextDecoder();

    while (true) {
      var res = await reader.read();
      if (res.done) break;
      var lines = dec.decode(res.value).split('\n');
      for (var i = 0; i < lines.length; i++) {
        var line = lines[i];
        if (!line.startsWith('data: ')) continue;
        try {
          var ev = JSON.parse(line.slice(6));
          if (ev.type === 'step') {
            var s = detectStep(ev.msg);
            if (s >= 0) setStep(s);
          }
          if (ev.type === 'done') {
            currentUrl = ev.url;
            document.getElementById('progress').style.width = '100%';
            for (var j = 0; j < 6; j++) {
              var el = document.getElementById('step-' + j);
              if (el) el.className = 'step done';
            }
            document.getElementById('success-box').className = 'success-box show';
            document.getElementById('btn-cancel').style.display = 'none';
            await loadApps();
            // Ouvrir automatiquement après 1.5s
            setTimeout(function() { if (currentUrl) openApp(currentUrl); }, 1500);
          }
          if (ev.type === 'error') {
            document.getElementById('error-box').textContent = '❌ ' + ev.msg;
            document.getElementById('error-box').className = 'error-box show';
            document.getElementById('btn-cancel').textContent = 'Fermer';
            await loadApps();
          }
        } catch(e) {}
      }
    }
  } catch(e) {
    document.getElementById('error-box').textContent = '❌ Erreur de connexion : ' + e.message;
    document.getElementById('error-box').className = 'error-box show';
  }

  creationRunning = false;
  document.getElementById('btn-create').disabled = false;
}

function cancelCreation() {
  document.getElementById('overlay').classList.remove('show');
  document.getElementById('btn-cancel').textContent = 'Annuler';
  loadApps();
}

function openResult() {
  if (currentUrl) openApp(currentUrl);
  cancelCreation();
}

function openApp(url) {
  if (window.electronAPI && window.electronAPI.openExternal) {
    window.electronAPI.openExternal(url);
  } else {
    window.open(url, '_blank');
  }
}

// ── Actions sur les apps ──────────────────────────────────────────────────────
async function stopApp(id) {
  await fetch('/api/apps/' + id + '/stop', { method: 'POST' });
  loadApps();
}

async function startApp(id) {
  var card = document.getElementById('app-' + id);
  if (card) {
    var btn = card.querySelector('.btn-start');
    if (btn) { btn.textContent = '⏳ Démarrage...'; btn.disabled = true; }
  }
  var r = await fetch('/api/apps/' + id + '/start', { method: 'POST' });
  var d = await r.json();
  if (d.status === 'running') openApp(d.url);
  loadApps();
}

async function deleteApp(id) {
  if (!confirm('Supprimer cette application ? Elle sera définitivement effacée.')) return;
  await fetch('/api/apps/' + id, { method: 'DELETE' });
  loadApps();
}

// ── Init ──────────────────────────────────────────────────────────────────────
loadApps();
setInterval(loadApps, 8000);
