"""Version unique utilisee par l'API, l'application desktop et les builds."""

from pathlib import Path


_VERSION_FILE = Path(__file__).resolve().with_name("VERSION")


def get_version() -> str:
    try:
        value = _VERSION_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        value = "0.0.0+unknown"
    return value or "0.0.0+unknown"


__version__ = get_version()
