"""Entrada del despliegue gratuito con MLflow SQLite integrado."""
import atexit
import gzip
import hashlib
import os
from pathlib import Path
import runpy
import sys
import urllib.request

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.update({"PETSTOP_EMBEDDED_TRACKING": "1", "MLFLOW_DISABLE_AGENT_HINT": "1",
                   "PREFECT_SERVER_ANALYTICS_ENABLED": "false"})


@st.cache_resource(show_spinner=False)
def initialize():
    binary = Path(os.getenv("PETSTOP_LLAMA_BINARY", str(ROOT / "data/llama-server")))
    if not binary.is_file():
        binary.parent.mkdir(parents=True, exist_ok=True)
        url = "https://github.com/mateobsapia-lang/petstop-automation/releases/download/runtime-2026-11-26/llama-server-linux-x86_64.gz"
        manifest_url = url.rsplit("/", 1)[0] + "/runtime-manifest.json"
        import json
        with urllib.request.urlopen(manifest_url) as stream:
            manifest = json.load(stream)
        compressed = binary.with_suffix(".gz")
        urllib.request.urlretrieve(url, compressed)
        if hashlib.sha256(compressed.read_bytes()).hexdigest() != manifest["compressed_sha256"]:
            raise ValueError("El binario descargado no coincide con el manifiesto")
        binary.write_bytes(gzip.decompress(compressed.read_bytes()))
        binary.chmod(0o755)
    os.environ["PETSTOP_LLAMA_BINARY"] = str(binary)
    from space_bootstrap import main, stop
    processes = main(start_ui=False)
    atexit.register(stop)
    return processes


with st.spinner("Preparando PetStop y sus consultas de ejemplo. El primer inicio puede tardar unos minutos."):
    initialize()

runpy.run_path(str(ROOT / "dashboard.py"), run_name="__main__")
