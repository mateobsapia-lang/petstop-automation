# PetStop Automation

Mateo Sapia · AI Automation para Data Scientist · Checkpoint 3

PetStop descarga ventas e inventario en paralelo y guarda una captura JSON para el análisis de reposición. La API de práctica devuelve pedidos, productos, unidades y stock ficticios de una sucursal. Se puede reproducir en la misma máquina sin cuenta ni credenciales.

| Carpeta | Uso |
| --- | --- |
| `src/ingestion/` | Cliente HTTP reutilizable, validación de JSON y almacenamiento de la captura. |
| `data/` | Datos locales. Git conserva `.gitkeep`. |
| `config/` | Configuración de los módulos siguientes. |
| `notebooks/` | Exploración y experimentos. |

## Ejecutar la ingesta

Desde la raíz, instalá las dependencias con Python 3.12. El Dev Container ya las instala al construir la imagen.

```sh
python -m pip install -r requirements.txt
```

En Windows podés usar `py -3.12` en lugar de `python`. Copiá `.env.example` a `.env` si querés modificar la URL o configurar un token para una API compatible. Usá HTTPS para servicios remotos y dejá el token vacío para la API local.

En una terminal ejecutá `python demo_api.py`. En otra terminal compará ambos modos contra el mismo servidor:

```sh
python main.py --sequential --output data/ingesta_secuencial.json
python main.py
```

Las dos capturas contienen los mismos datos. La API de práctica agrega 450 ms de demora a cada fuente. Compará `duracion_s` en los registros: la ingesta secuencial acumula las dos esperas y la concurrente permite que se superpongan con `asyncio.gather()`.

En una prueba local en Docker, con dos registros por fuente, ambos modos produjeron la misma captura. El modo secuencial tardó 1,043 s y el concurrente 0,628 s. Los tiempos varían según el equipo.

El cliente usa una sola sesión y limita la espera por operación HTTP a dos segundos de forma predeterminada. Reintenta hasta tres veces ante timeouts, errores de red y respuestas transitorias como 429 o 503. Un 401, un 404 o un JSON inválido se registran como fallos permanentes. Los logs incluyen fecha UTC, nivel, fuente y resultado. Si falta una fuente, el programa termina con código 1 y conserva la última captura completa.

Con la API local todavía en ejecución, `python main.py --timeout 0.1` permite ver los reintentos y comprobar que se conserva la captura. Si el puerto 8765 está ocupado, iniciá `python demo_api.py --port 8766` y usá `python main.py --url http://127.0.0.1:8766`.

Para probar dentro de Docker, desde la raíz en PowerShell:

```powershell
docker build -t petstop-automation:checkpoint2 .
docker run --rm -it --mount "type=bind,source=$PWD,target=/workspaces/petstop-automation" petstop-automation:checkpoint2 bash
```

Dentro del contenedor podés iniciar `python demo_api.py &` y luego ejecutar las dos comparaciones.

## Validar desde VS Code

## Orquestación con Prefect

Con las dependencias instaladas, iniciá el servidor local en una terminal.

```sh
prefect server start --host 127.0.0.1
```

Dejá la API de práctica en otra terminal con `python demo_api.py`.
En PowerShell, desde la raíz del proyecto, ejecutá el flujo.

```powershell
$env:PREFECT_API_URL = "http://127.0.0.1:4200/api"
python pipeline.py
```

El dashboard está en http://127.0.0.1:4200. El flujo separa ingesta,
validación y persistencia en tres tareas. La ingesta usa dos reintentos
de Prefect con esperas exponenciales y una sola solicitud por intento
del cliente. Cada ejecución completa escribe un JSON en `data/runs/`
con fecha UTC e identificador de ejecución. Ante un fallo previo al guardado,
se conserva el historial de capturas completas.

### Comprobación del entorno

1. Con Docker en ejecución y la extensión **Dev Containers**, abrí esta carpeta en VS Code.
2. Elegí **Dev Containers: Reopen in Container** desde la paleta de comandos.
3. En el terminal integrado ejecutá `python src/check_env.py`.

La prueba muestra la versión de Python, comprueba que el proceso se ejecuta en Docker y verifica que se puede acceder a `OPENAI_API_KEY`, `MLFLOW_TRACKING_URI` y `DATABASE_URL`. El entorno de desarrollo las define vacías y la prueba solo informa su estado.

Para reproducir la misma comprobación desde la raíz con Docker CLI:

```sh
docker build -t petstop-automation:checkpoint2 .
docker run --rm --env-file .env.example --mount "type=bind,source=$PWD,target=/workspaces/petstop-automation,readonly" petstop-automation:checkpoint2 python src/check_env.py
```

`.env.example` enumera los nombres para futuras integraciones. Git y el contexto de Docker excluyen `.env` y los archivos de datos locales.
