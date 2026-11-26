"""Inicio del laboratorio en un único contenedor. Publica solo Streamlit."""
import os
import asyncio
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

import httpx

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
EMBEDDED = os.getenv("PETSTOP_EMBEDDED_TRACKING") == "1"
os.environ.update({"MLFLOW_TRACKING_URI": f"sqlite:///{DATA / 'mlflow.db'}" if EMBEDDED else "http://127.0.0.1:5000",
                   "PREFECT_API_URL": "http://127.0.0.1:4200/api",
                   "PREFECT_HOME": str(DATA / "prefect"),
                   "PREFECT_UI_STATIC_DIRECTORY": str(DATA / "prefect/ui"),
                   "LOCAL_LLM_URL": "http://127.0.0.1:8088/v1",
                   "PETSTOP_API_URL": "http://127.0.0.1:8765"})
children = []


def start(*command):
    child = subprocess.Popen(command, cwd=ROOT)
    children.append(child)
    return child


def wait(url, timeout=300):
    limit = time.monotonic() + timeout
    while time.monotonic() < limit:
        if any(child.poll() is not None for child in children):
            raise RuntimeError("Un servicio terminó antes de completar el inicio")
        try:
            if httpx.get(url, timeout=3).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    raise RuntimeError("El servicio no respondió: " + url)


def stop(*_):
    for child in children:
        child.terminate()


def main(start_ui=True):
    if start_ui:
        signal.signal(signal.SIGTERM, stop)
    model = DATA / "qwen.gguf"
    if not model.is_file():
        temporary = model.with_suffix(".part")
        urllib.request.urlretrieve(
            "https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF/resolve/main/qwen2.5-1.5b-instruct-q4_k_m.gguf",
            temporary)
        temporary.replace(model)
    start(os.getenv("PETSTOP_LLAMA_BINARY", "llama-server"), "-m", str(model), "--host", "127.0.0.1", "--port", "8088",
          "--ctx-size", "8192", "--threads", "2", "--parallel", "1",
          "--alias", "qwen2.5-1.5b-instruct")
    if not EMBEDDED:
        start("mlflow", "server", "--host", "127.0.0.1", "--port", "5000", "--workers", "1",
              "--backend-store-uri", f"sqlite:///{DATA / 'mlflow.db'}",
              "--artifacts-destination", str(DATA / "artifacts"))
    start("prefect", "server", "start", "--host", "127.0.0.1")
    start(sys.executable, "demo_api.py")
    endpoints = ["http://127.0.0.1:8088/health", "http://127.0.0.1:4200/api/health",
                 "http://127.0.0.1:8765/ventas"]
    if not EMBEDDED:
        endpoints.append("http://127.0.0.1:5000/health")
    for endpoint in endpoints:
        wait(endpoint)
    if EMBEDDED:
        import mlflow
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        if mlflow.get_experiment_by_name("AI_Automation_Pipeline") is None:
            mlflow.create_experiment("AI_Automation_Pipeline", artifact_location=str(DATA / "artifacts"))
    from pipeline_trazable import pipeline_trazable
    asyncio.run(pipeline_trazable())
    from audit import records
    from inferencia import infer
    examples = ["¿Cuánto dura una reserva confirmada?", "¿Dónde se paga el pedido?",
                "¿Se puede comprar automáticamente al proveedor?",
                "¿Qué hay que revisar antes de confirmar stock?", "¿Cuál es el precio del proveedor?"]
    if len(records()) < 5:
        for question in examples[len(records()):]:
            infer(question)
    if not start_ui:
        return children
    app = start("streamlit", "run", "dashboard.py", "--server.address=0.0.0.0",
                "--server.port=7860", "--browser.gatherUsageStats=false")
    while app.poll() is None:
        if any(child.poll() is not None for child in children):
            raise RuntimeError("Un servicio se detuvo durante la ejecución")
        time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    finally:
        stop()
