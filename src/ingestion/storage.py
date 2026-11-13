import json
from pathlib import Path
from tempfile import NamedTemporaryFile


def save_snapshot(path: Path, results: dict[str, list[dict]]) -> None:
    """Reemplaza el archivo solo después de escribir la captura completa."""
    payload = json.dumps(results, ensure_ascii=False, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            output.write(payload.encode("utf-8"))
            output.close()
            temporary.replace(path)
        finally:
            output.close()
            temporary.unlink(missing_ok=True)
