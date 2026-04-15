@echo off
cd /d C:\Users\Tellop\claude-managed-agents
call .venv\Scripts\activate
pip install -r requirements_orchestrator.txt -q
echo Orchestrateur demarre sur http://localhost:8000
start http://localhost:8000
python orchestrator.py
