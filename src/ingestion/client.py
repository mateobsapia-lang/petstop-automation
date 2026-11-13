import asyncio
import logging

import httpx

from .parsing import parse_records


TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}
LOGGER = logging.getLogger(__name__)


class IngestionError(Exception):
    """Error controlado al obtener o validar una fuente."""


class DataCollector:
    """Reutiliza una sesión HTTP para las solicitudes de una corrida."""

    def __init__(self, client: httpx.AsyncClient, attempts: int = 3) -> None:
        self.client = client
        self.attempts = attempts

    async def fetch(self, resource: str) -> list[dict]:
        for attempt in range(1, self.attempts + 1):
            try:
                response = await self.client.get(f"/{resource}")
                response.raise_for_status()
            except (httpx.TimeoutException, httpx.NetworkError) as error:
                reason = type(error).__name__
            except httpx.HTTPStatusError as error:
                status = error.response.status_code
                reason = f"HTTP {status}"
                if status not in TRANSIENT_STATUS_CODES:
                    LOGGER.error("fuente=%s tipo=permanente motivo=%s", resource, reason)
                    raise IngestionError(f"{resource}: {reason}") from None
            except (httpx.RequestError, httpx.InvalidURL) as error:
                reason = type(error).__name__
                LOGGER.error("fuente=%s tipo=permanente motivo=%s", resource, reason)
                raise IngestionError(f"{resource}: {reason}") from None
            else:
                try:
                    records = parse_records(response.content)
                except ValueError:
                    LOGGER.error("fuente=%s tipo=permanente motivo=respuesta_invalida", resource)
                    raise IngestionError(f"{resource}: respuesta inválida") from None

                LOGGER.info("fuente=%s filas=%d intento=%d", resource, len(records), attempt)
                return records

            LOGGER.error("fuente=%s tipo=transitorio intento=%d motivo=%s", resource, attempt, reason)
            if attempt < self.attempts:
                await asyncio.sleep(0.25 * 2 ** (attempt - 1))

        raise IngestionError(f"{resource}: reintentos agotados")
