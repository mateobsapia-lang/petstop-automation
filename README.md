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

## Motor, agentes y monitoreo

El proyecto reúne la ingesta de PetStop, el registro de modelos, las consultas al manual y el control de las respuestas. Los datos de ventas son de prueba. El análisis de viviendas usa California Housing de scikit-learn.

### Abrir la aplicación

1. Copiá `.env.example` a `.env` y dejá las claves vacías para la prueba local.
2. Descargá `qwen2.5-1.5b-instruct-q4_k_m.gguf` desde el repositorio oficial Qwen/Qwen2.5-1.5B-Instruct-GGUF. Guardalo como `qwen.gguf` en una carpeta de tu equipo.
3. Indicá esa carpeta y levantá los servicios:

```powershell
$env:PETSTOP_MODEL_DIR = "C:/ruta/modelos"
docker compose up --build -d
```

Abrí http://127.0.0.1:8501. El primer inicio necesita descargar las imágenes y cargar el modelo. La interfaz consulta el manual; cada respuesta pasa por Prefect y queda registrada en MLflow. La interfaz también muestra drift, auditoría y el control humano. Los puertos de MLflow, Prefect y el modelo quedan dentro de Docker.

Para probar la consulta desde la consola:

```sh
docker compose exec ai_agent python inferencia.py
```

Para generar el modelo inicial dentro del contenedor:

```sh
docker compose exec -d ai_agent python demo_api.py
docker compose exec -e PETSTOP_API_URL=http://127.0.0.1:8765 ai_agent python pipeline_trazable.py
```

### Qué hace cada archivo

| Archivo | Función |
|---|---|
| `pipeline_trazable.py` | Ingesta, entrenamiento de prueba, métricas y registro de modelo. |
| `inferencia.py` | Recuperación FAISS, contexto del manual, salida Pydantic y alerta de revisión. |
| `agente_mlops_checkpoint4.py` | Agente LangGraph: consulta métricas, limpia una copia de datos y pide aprobación antes de la promoción simulada. |
| `multiagente.py` | Coordinador, ejecución y revisión en AG2. Guarda código generado, gráfico y conversación. |
| `checkpoint_n8n_Sapia_Mateo.json` | Webhook, condición de confianza y aviso al laboratorio. |
| `audit.py` | Registro acumulativo en SQLite, hashes encadenados y bloqueo de UPDATE/DELETE. |
| `monitoring.py` | Prueba de drift, últimas cinco inferencias y ficha de gobernanza. |
| `dashboard.py` | Consultas y seguimiento desde Streamlit. |

### Revisión humana

Cada pausa de LangGraph se guarda por `thread_id` en SQLite. La aprobación debe corresponder a una persona que haya revisado la solicitud.

```sh
python agente_mlops_checkpoint4.py --thread prueba-1 --question "Promové el modelo PetStopReposicionLaboratorio"
python agente_mlops_checkpoint4.py --thread prueba-1 --decision approve
# Para rechazar: --decision reject
```

La prueba escribe una promoción simulada; no cambia el registro productivo de MLflow ni ejecuta una compra. Las métricas y el dataset están limitados al laboratorio.

### Controles y límites

- Los reintentos de ingesta están en Prefect; los errores previos al guardado no generan una captura incompleta.
- La salida del motor se valida con Pydantic y sus fuentes se limitan a los fragmentos recuperados. Si no hay fuentes, se requiere revisión y se registra una alerta.
- La búsqueda usa una representación léxica local de 512 dimensiones. No se presenta como un modelo de embeddings semánticos.
- Cada inferencia guarda prompt, parámetros, costo de API y latencia en MLflow. El costo de API local es cero; no se cuantifica el consumo del equipo.
- SQLite bloquea UPDATE y DELETE y encadena los hashes. Un administrador del equipo todavía puede cambiar el archivo: no es almacenamiento externo inmutable.
- La promoción requiere una aprobación explícita. Un operador puede detener los flows o bajar los servicios con `docker compose down`.
- Los volúmenes conservan los datos después de `down` y `up`. `down -v` elimina los volúmenes y no se usa en las pruebas de persistencia.
- `requirements-lock.txt` conserva las versiones verificadas en Linux. El modelo GGUF se monta por separado y no contiene claves.
- El modelo local puede equivocarse; las respuestas no autorizan compras ni reemplazan una revisión del equipo.

### Alertas n8n

Importá el JSON en n8n y configurá una credencial SMTP propia. El archivo conserva la referencia `petstopSMTP`, que se usó con un servidor de correo local de laboratorio. La prueba no envió correos a personas reales.

```json
{"confianza": 0.45, "resumen": "Revisar resultado"}
```

El POST a `/webhook/petstop-review` genera un correo si la confianza es menor a 0,70. Una confianza mayor o igual se registra sin avisar. Ambos caminos se probaron con ejecuciones exitosas y un único correo recibido en el servidor local.

### Publicación

La aplicación funciona localmente con Docker. La publicación cloud y la comprobación de su URL pública siguen pendientes. No se presenta la dirección local como una aplicación publicada.

Fuentes: [Qwen GGUF](https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF), [Docker Spaces](https://huggingface.co/docs/hub/spaces-sdks-docker), [criterios de riesgo y transparencia](https://ai-act-service-desk.ec.europa.eu/en/ai-act/faq/what-are-obligations-high-risk-transparency-risk-and-minimal-risk-ai-systems).

Para preparar Docker Spaces se agregaron `Dockerfile.space` y `space_bootstrap.py`. Son la configuración de despliegue pendiente de construir y verificar, no una publicación completada. En el repositorio del Space se usa `Dockerfile.space` como `Dockerfile` y un encabezado README con `sdk: docker` y `app_port: 7860`. Se publica solo Streamlit; los servicios internos escuchan en 127.0.0.1.

El arranque descarga Qwen desde su repositorio oficial, inicia los servicios y genera el modelo del laboratorio. El almacenamiento gratuito del Space es efímero: una reconstrucción puede borrar el registro. Los volúmenes persistentes verificados corresponden a Docker Compose local; para conservar un historial cloud se necesita definir y comprobar almacenamiento externo antes de usarlo fuera de la demostración.

### Evidencia de la entrega 8

El JSON se exportó desde la instancia n8n del laboratorio. Se verificaron dos POST desde Python con `requests.post()`: confianza 0,45 genera el aviso y confianza 0,95 toma el registro silencioso. La confianza de estas pruebas es un valor controlado para comprobar la condición, no una probabilidad estimada por el modelo.

- [Ejecución con aviso](docs/n8n/n8n_rama_alerta.png)
- [Ejecución silenciosa](docs/n8n/n8n_rama_silenciosa.png)
- [Entradas y resultados de la prueba Python](docs/n8n/n8n_prueba_python.json)

Las capturas muestran las ejecuciones 1 y 2; la repetición desde Python creó las ejecuciones 3 y 4, también exitosas. El export conserva la referencia SMTP, sin su contraseña. Al importarlo, hay que seleccionar una credencial SMTP de la instancia de destino.

### Despliegue en Streamlit Community Cloud

La cuenta de Hugging Face consultada el 26/11/2026 exige un plan pago para crear Docker o Gradio Spaces. No se contrató un plan. Se preparó una entrada alternativa en `community/app.py` para Community Cloud.

Este perfil conserva Prefect, LangGraph y MLflow. MLflow usa SQLite dentro del mismo proceso, sin un servidor web adicional; así se reduce el consumo de memoria. Al iniciar se registra el modelo y se generan cinco consultas de ejemplo si todavía no existen. El binario Linux de llama.cpp se descarga de la release `runtime-2026-11-26` de este repositorio y se verifica con SHA256. No usa una API de inferencia paga ni necesita claves.

Configuración:

- Repositorio: `mateobsapia-lang/petstop-automation`
- Rama: `main`
- Entrada: `community/app.py`
- Python: `3.12`
- Dependencias: `community/requirements.txt` y `packages.txt`

La entrada se probó en Linux con límite de 2.500 MB y 2 CPU: entrenamiento, cinco inferencias FINISHED y consulta de métricas desde la interfaz. La publicación y la comprobación del enlace externo siguen pendientes. El primer inicio tarda mientras se descargan el binario y el modelo. Los archivos locales del proveedor pueden perderse en un reinicio; la persistencia entre reinicios verificada con volúmenes corresponde al perfil Docker Compose.

Referencia: https://docs.streamlit.io/deploy/streamlit-community-cloud
