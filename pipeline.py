"""Orquesta la ingesta de PetStop y conserva una captura por ejecución."""

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run
from prefect.tasks import exponential_backoff

from main import ROOT, SOURCES
from src.ingestion.client import DataCollector
from src.ingestion.storage import save_snapshot


@task(name="Ingesta", retries=2,
      retry_delay_seconds=exponential_backoff(backoff_factor=1))
async def fetch_data(url: str) -> dict:
    logger = get_run_logger()
    logger.info("Inicio de ingesta de ventas e inventario")
    token = os.getenv("PETSTOP_API_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    parsed_url = httpx.URL(url)
    if token and parsed_url.scheme != "https" and parsed_url.host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("El token requiere HTTPS o la API local")
    async with httpx.AsyncClient(base_url=url, timeout=2, headers=headers) as session:
        # El cliente intenta una vez y Prefect administra los dos reintentos.
        collector = DataCollector(session, attempts=1)
        records = await asyncio.gather(
            *(collector.fetch(source) for source in SOURCES),
            return_exceptions=True,
        )
        # Se esperan ambas respuestas antes de cerrar la sesión compartida.
        for response in records:
            if isinstance(response, Exception):
                raise response
    result = dict(zip(SOURCES, records))
    logger.info("Fin de ingesta, %d fuentes recibidas", len(result))
    return result


@task(name="Validación y transformación")
def transform_data(raw: dict) -> dict:
    logger = get_run_logger()
    logger.info("Inicio de validación y transformación")
    expected = {"ventas": ("pedido", "sku", "unidades", "fecha"),
                "inventario": ("sku", "sucursal", "stock")}
    for source, required in expected.items():
        for row in raw[source]:
            if any(field not in row or row[field] is None for field in required):
                raise ValueError(f"Registro incompleto en {source}")
            amount = row["unidades" if source == "ventas" else "stock"]
            if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
                raise ValueError(f"Cantidad inválida en {source}")
    result = {source: [dict(row) for row in rows] for source, rows in raw.items()}
    logger.info("Fin de validación, %d registros", sum(map(len, result.values())))
    return result


@task(name="Persistencia JSON")
def save_results(records: dict, output_dir: str, run_id: str) -> str:
    logger = get_run_logger()
    logger.info("Inicio de persistencia")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = Path(output_dir) / f"petstop_{timestamp}_{run_id}.json"
    save_snapshot(path, records)
    logger.info("Fin de persistencia, archivo %s", path.name)
    return str(path)


@flow(name="PetStop - ingesta y persistencia", log_prints=True)
async def petstop_pipeline(url: str = "http://127.0.0.1:8765",
                           output_dir: str = "data/runs") -> str:
    raw = await fetch_data(url)
    clean = transform_data(raw)
    return save_results(clean, output_dir, str(flow_run.id))


if __name__ == "__main__":
    load_dotenv(ROOT / ".env")
    asyncio.run(petstop_pipeline(os.getenv("PETSTOP_API_URL") or "http://127.0.0.1:8765"))
