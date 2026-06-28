"""Place la racine du depot sur sys.path pour que les tests importent les
modules de premier niveau (orchestrator, team, ...)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
