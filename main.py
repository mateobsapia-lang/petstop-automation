import argparse
import asyncio
import logging
import os
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

from src.ingestion.client import DataCollector, IngestionError
from src.ingestion.storage import save_snapshot


ROOT = Path(__file__).resolve().parent
SOURCES = ("ventas", "inventario")
LOGGER = logging.getLogger("petstop")


async def collect(args: argparse.Namespace) -> int:
    token = os.getenv("PETSTOP_API_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    start = time.perf_counter()
    async with httpx.AsyncClient(
        base_url=args.url, timeout=args.timeout, headers=headers
    ) as session:
        collector = DataCollector(session)

        async def fetch(resource: str) -> list[dict] | None:
            try:
                return await collector.fetch(resource)
            except IngestionError:
                return None

        if args.sequential:
            responses = [await fetch(resource) for resource in SOURCES]
        else:
            responses = await asyncio.gather(*(fetch(resource) for resource in SOURCES))

    elapsed = time.perf_counter() - start
    if any(records is None for records in responses):
        LOGGER.error("resultado=incompleto duracion_s=%.3f captura=sin_reemplazar", elapsed)
        return 1

    results = {resource: records for resource, records in zip(SOURCES, responses) if records is not None}
    try:
        save_snapshot(args.output, results)
    except OSError:
        LOGGER.error("resultado=error_de_escritura")
        return 1
    LOGGER.info(
        "resultado=ok modo=%s fuentes=%d duracion_s=%.3f",
        "secuencial" if args.sequential else "concurrente",
        len(results),
        elapsed,
    )
    return 0


def main() -> int:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Descarga ventas e inventario de PetStop.")
    parser.add_argument("--url", default=os.getenv("PETSTOP_API_URL") or "http://127.0.0.1:8765")
    parser.add_argument("--timeout", type=float, default=2.0, help="Segundos de espera por operación HTTP.")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "ingesta.json")
    parser.add_argument("--sequential", action="store_true", help="Modo de comparación.")
    args = parser.parse_args()
    if not 0 < args.timeout < float("inf"):
        parser.error("--timeout debe ser un número positivo y finito")
    try:
        base = httpx.URL(args.url)
    except httpx.InvalidURL:
        parser.error("--url debe ser una URL HTTP(S) válida")
    if base.scheme not in {"http", "https"} or not base.host or base.username or base.password:
        parser.error("--url debe ser una URL HTTP(S) sin credenciales")
    if os.getenv("PETSTOP_API_TOKEN") and base.scheme != "https" and base.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("El token solo puede usarse por HTTPS o en la API local")
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return asyncio.run(collect(args))


if __name__ == "__main__":
    raise SystemExit(main())
