import re

# ── 1. Backend : endpoint /api/launch-demo ────────────────────────────────────
path_orch = r'C:\Users\Tellop\claude-managed-agents\orchestrator.py'
with open(path_orch, 'r', encoding='utf-8') as f:
    c = f.read()

DEMO_APP = '''
@app.get("/api/launch-demo")
async def launch_demo(folder: str, request: Request):
    """Cree une app demo Flask et la lance immediatement."""
    from pathlib import Path as _PD
    import json as _jd

    target = _PD(folder)
    if len(str(target)) <= 3:
        raise HTTPException(400, "Choisissez un sous-dossier")
    target.mkdir(parents=True, exist_ok=True)

    # App Flask simple
    flask_app = """from flask import Flask, render_template_string
import datetime

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Mon Application</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; align-items: center; justify-content: center; }
.card { background: white; border-radius: 16px; padding: 48px; text-align: center; box-shadow: 0 20px 60px rgba(0,0,0,0.3); max-width: 500px; }
h1 { font-size: 2rem; color: #2b2b2b; margin-bottom: 12px; }
p { color: #666; line-height: 1.6; margin-bottom: 24px; }
.badge { background: #E8825A; color: white; padding: 8px 20px; border-radius: 20px; font-weight: 600; font-size: 0.9rem; }
.time { color: #999; font-size: 0.85rem; margin-top: 20px; }
</style>
</head>
<body>
<div class="card">
  <h1>Application lancee !</h1>
  <p>Cette application a ete creee et lancee automatiquement par l\'orchestrateur d\'agents IA.</p>
  <span class="badge">Multi-Agent Orchestrator</span>
  <p class="time">Demarre a {{ time }}</p>
</div>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML, time=datetime.datetime.now().strftime("%H:%M:%S"))

@app.route("/api/hello")
def hello():
    return {"message": "Hello depuis votre app!", "status": "running"}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)
"""

    # requirements.txt
    reqs = "flask\\n"

    (target / "app.py").write_text(flask_app, encoding="utf-8")
    (target / "requirements.txt").write_text(reqs, encoding="utf-8")

    async def stream():
        yield f"data: {_jd.dumps({'type':'info','msg':f'Application creee dans {folder}'})}\\n\\n"
        yield f"data: {_jd.dumps({'type':'step','msg':'Installation Flask...','cmd':'pip install flask -q'})}\\n\\n"

        py_exe = _PD(__file__).parent / ".venv" / "Scripts" / "python.exe"
        exe = str(py_exe) if py_exe.exists() else _sys.executable

        # Installer flask
        inst = await asyncio.create_subprocess_exec(
            exe, "-m", "pip", "install", "flask", "-q",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        async for raw in inst.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip()
            if line and "already" not in line.lower():
                yield f"data: {_jd.dumps({'type':'out','msg':line})}\\n\\n"
        await inst.wait()

        yield f"data: {_jd.dumps({'type':'ok','msg':'Flask installe'})}\\n\\n"
        yield f"data: {_jd.dumps({'type':'step','msg':'Demarrage du serveur...','cmd':exe+' app.py'})}\\n\\n"

        global app_process
        app_process = await asyncio.create_subprocess_exec(
            exe, "app.py", cwd=str(target),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        yield f"data: {_jd.dumps({'type':'started','url':'http://localhost:3000','port':3000,'pid':app_process.pid})}\\n\\n"

        import time as _t
        deadline = _t.time() + 30
        async for raw in app_process.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip()
            if line:
                yield f"data: {_jd.dumps({'type':'out','msg':line})}\\n\\n"
            if _t.time() > deadline:
                yield f"data: {_jd.dumps({'type':'running','msg':'Serveur actif en arriere-plan'})}\\n\\n"
                break

    return StreamingResponse(stream(), media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

'''

if '/api/launch-demo' not in c:
    end = c.rfind('\nif __name__')
    if end == -1:
        end = len(c)
    c = c[:end] + DEMO_APP + c[end:]
    with open(path_orch, 'w', encoding='utf-8') as f:
        f.write(c)
    print("launch-demo OK")
else:
    print("deja present")

# ── 2. Frontend : bouton demo dans le panneau ─────────────────────────────────
path_html = r'C:\Users\Tellop\claude-managed-agents\static\index.html'
with open(path_html, 'r', encoding='utf-8') as f:
    h = f.read()

OLD_BTN = '      <button class="btn btn-orange" onclick="launchProject()" style="margin-bottom:8px">▶ Relancer (dossier existant)</button>'
NEW_BTN = '''      <button class="btn btn-orange" onclick="launchProject()" style="margin-bottom:8px">▶ Relancer (dossier existant)</button>
      <button class="btn" style="background:#9b59b6;color:#fff;margin-bottom:8px" onclick="launchDemo()">⚡ App démo (test rapide)</button>'''

if OLD_BTN in h:
    h = h.replace(OLD_BTN, NEW_BTN)
    print("Bouton demo HTML OK")
else:
    print("ERREUR bouton non trouve")

# Ajouter la fonction JS launchDemo
OLD_JS = '// ────────────────────────────────────────────────────────\n// POLLING'
NEW_JS = '''// ────────────────────────────────────────────────────────
// DEMO LAUNCH
// ────────────────────────────────────────────────────────
async function launchDemo() {
  var folder = document.getElementById('proj-folder').value.trim();
  if (!folder || folder.length <= 3) {
    alert('Cliquez sur 📁 pour choisir un dossier destination (ex: C:\\\\Users\\\\Tellop\\\\DemoApp)');
    return;
  }
  openTerminal('⚡ Création app démo...');
  setStatus('⏳ Génération...');
  try {
    var resp = await fetch('/api/launch-demo?folder=' + encodeURIComponent(folder));
    var reader = resp.body.getReader();
    var dec = new TextDecoder();
    while (true) {
      var res = await reader.read(); if (res.done) break;
      var lines = dec.decode(res.value).split('\\n');
      for (var i=0; i<lines.length; i++) {
        var line=lines[i]; if(!line.startsWith('data: ')) continue;
        try {
          var ev=JSON.parse(line.slice(6));
          if(ev.type==='info')    addTermLine('📁 '+ev.msg,'step');
          if(ev.type==='step')    addTermLine('⚡ '+ev.msg+(ev.cmd?' — '+ev.cmd:''),'step');
          if(ev.type==='out')     addTermLine(ev.msg);
          if(ev.type==='ok')      addTermLine(ev.msg,'ok');
          if(ev.type==='running') addTermLine(ev.msg,'ok');
          if(ev.type==='error')   { addTermLine('❌ '+ev.msg,'err'); setStatus('❌ Erreur'); }
          if(ev.type==='started') {
            projUrl=ev.url;
            addTermLine('\\n✅ App démo active sur '+ev.url,'ok');
            setStatus('✅ En ligne → '+ev.url);
            setTimeout(function(){ window.open(projUrl,'_blank'); }, 2500);
          }
        }catch(e){}
      }
    }
  } catch(e) { addTermLine('❌ '+e.message,'err'); }
}

// ────────────────────────────────────────────────────────
// POLLING'''

if OLD_JS in h:
    h = h.replace(OLD_JS, NEW_JS)
    print("JS demo OK")
else:
    print("ERREUR JS non trouve")

with open(path_html, 'w', encoding='utf-8') as f:
    f.write(h)
print("Tout mis a jour")
