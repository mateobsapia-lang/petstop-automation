import json


def parse_records(payload: bytes) -> list[dict]:
    records = json.loads(payload)
    if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
        raise ValueError("Se esperaba una lista JSON de objetos")
    return records
