"""Comprueba el entorno de desarrollo sin imprimir valores de credenciales."""

import os
import platform
import sys
from pathlib import Path


VARIABLES = ("OPENAI_API_KEY", "MLFLOW_TRACKING_URI", "DATABASE_URL")


def main() -> int:
    print(f"Python {platform.python_version()} | {platform.system()}")
    print(f"Carpeta de trabajo: {Path.cwd()}")
    dentro_de_docker = Path("/.dockerenv").is_file()
    print(f"Contenedor Docker: {'detectado' if dentro_de_docker else 'no detectado'}")

    faltantes = []
    for nombre in VARIABLES:
        if nombre in os.environ:
            estado = "definida (valor reservado)" if os.environ[nombre] else "definida y vacía"
        else:
            faltantes.append(nombre)
            estado = "no definida"
        print(f"{nombre}: {estado}")

    if sys.version_info < (3, 10) or not dentro_de_docker or faltantes:
        print("Verificación pendiente: se requiere Python 3.10+, Docker y las variables declaradas.")
        return 1

    print("Entorno validado correctamente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
