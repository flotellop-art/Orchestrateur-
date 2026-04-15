import re

# ── 1. orchestrator.py : prompts systeme par rôle + port dynamique ────────────
path_orch = r'C:\Users\Tellop\claude-managed-agents\orchestrator.py'
with open(path_orch, 'r', encoding='utf-8') as f:
    c = f.read()

# Ajouter les prompts systeme specialises
ROLE_PROMPTS = '''
# ─── Prompts système par rôle ─────────────────────────────────────────────────
ROLE_SYSTEM_PROMPTS = {
    "Développeur Web": """Tu es un développeur web expert. Tu dois créer des applications web COMPLÈTES et EXÉCUTABLES.

RÈGLES ABSOLUES :
1. Génère TOUJOURS du code complet dans des blocs de code markdown (```python ou ```javascript)
2. Le serveur doit écouter sur le port 3000
3. Inclus TOUJOURS le fichier principal ET requirements.txt (pour Python) ou package.json (pour Node.js)
4. Le code doit être exécutable SANS modification
5. Crée une interface HTML intégrée, moderne et fonctionnelle
6. Utilise Flask pour Python ou Express pour Node.js
7. Commence le bloc Python par "# filename: app.py" ou le bloc JS par "// filename: server.js"
8. N'explique PAS le code, génère DIRECTEMENT les fichiers complets""",

    "Codeur": """Tu es un expert en développement logiciel. Génère du code précis et fonctionnel.
Si tu crées un serveur ou une application, mets TOUJOURS le code dans des blocs ```python ou ```javascript.
Indique le nom du fichier avec un commentaire : # filename: app.py""",

    "Testeur": """Tu es un expert en tests logiciels. Génère des suites de tests complètes et exécutables.
Mets toujours le code dans des blocs de code markdown avec le langage approprié.""",

    "Bug Hunter": """Tu es un expert en détection de bugs et sécurité. Analyse le code avec précision.
Si tu génères du code correctif, mets-le dans des blocs de code avec le nom du fichier.""",

    "Reviewer": """Tu es un expert en revue de code. Fournis une analyse structurée et des suggestions concrètes.""",
}

def get_system_prompt(role: str) -> str:
    base = ROLE_SYSTEM_PROMPTS.get(role)
    if base:
        return base
    return f"Tu es un agent spécialisé dans le rôle: {role}. Réponds de manière précise et utile."

'''

# Insérer avant run_agent
marker = 'async def run_agent'
if 'ROLE_SYSTEM_PROMPTS' not in c and marker in c:
    c = c.replace(marker, ROLE_PROMPTS + '\n' + marker)
    print("Prompts systeme ajoutes OK")
elif 'ROLE_SYSTEM_PROMPTS' in c:
    print("Deja present")
else:
    print("ERREUR marker non trouve")

# Remplacer l'ancien system prompt dans run_agent
old_sys = '''        message = await client.messages.create(
            model=agent["model"],
            max_tokens=4096,
            messages=[{"role": "user", "content": agent["prompt"]}],
            system=f"Tu es un agent spécialisé dans le rôle: {agent['role']}. Réponds de manière précise et utile.",
        )'''

new_sys = '''        message = await client.messages.create(
            model=agent["model"],
            max_tokens=4096,
            messages=[{"role": "user", "content": agent["prompt"]}],
            system=get_system_prompt(agent["role"]),
        )'''

if old_sys in c:
    c = c.replace(old_sys, new_sys)
    print("System prompt run_agent mis a jour OK")
else:
    print("ATTENTION : ancien system prompt non trouve (deja modifie?)")

with open(path_orch, 'w', encoding='utf-8') as f:
    f.write(c)

# ── 2. index.html : ajouter le rôle + prompt automatique ─────────────────────
path_html = r'C:\Users\Tellop\claude-managed-agents\static\index.html'
with open(path_html, 'r', encoding='utf-8') as f:
    h = f.read()

# Ajouter le rôle Développeur Web dans le select
old_role = '''        <select id="role">
          <option value="Codeur">Codeur / Coder</option>
          <option value="Testeur">Testeur / Tester</option>
          <option value="Bug Hunter">Bug Hunter</option>
          <option value="Reviewer">Reviewer</option>
          <option value="Custom">Custom</option>
        </select>'''

new_role = '''        <select id="role" onchange="onRoleChange(this.value)">
          <option value="Développeur Web">🌐 Développeur Web (serveur)</option>
          <option value="Codeur">Codeur / Coder</option>
          <option value="Testeur">Testeur / Tester</option>
          <option value="Bug Hunter">Bug Hunter</option>
          <option value="Reviewer">Reviewer</option>
          <option value="Custom">Custom</option>
        </select>'''

if old_role in h:
    h = h.replace(old_role, new_role)
    print("Role select mis a jour OK")
else:
    print("ERREUR role select non trouve")

# Ajouter la fonction onRoleChange et les prompts dans le JS
OLD_JS_MARKER = '// ────────────────────────────────────────────────────────\n// TABS'
NEW_JS = '''// ────────────────────────────────────────────────────────
// ROLE PROMPTS
// ────────────────────────────────────────────────────────
var ROLE_PROMPTS = {
  "Développeur Web": "Crée une application web complète avec Flask (Python). L\\'application doit :\\n- Avoir une belle interface HTML intégrée\\n- Écouter sur le port 3000\\n- Avoir plusieurs pages ou fonctionnalités\\n- Être directement exécutable\\n\\nGénère le code complet dans des blocs ```python avec le commentaire # filename: app.py en première ligne. Génère aussi un bloc ```text pour requirements.txt.",
  "Codeur": "",
  "Testeur": "",
  "Bug Hunter": "",
};

function onRoleChange(role) {
  var prompt = ROLE_PROMPTS[role];
  if (prompt) {
    document.getElementById('prompt').value = prompt;
  }
}

// ────────────────────────────────────────────────────────
// TABS'''

if OLD_JS_MARKER in h:
    h = h.replace(OLD_JS_MARKER, NEW_JS)
    print("JS onRoleChange OK")
else:
    print("ERREUR JS marker non trouve")

with open(path_html, 'w', encoding='utf-8') as f:
    f.write(h)
print("Tout mis a jour")
