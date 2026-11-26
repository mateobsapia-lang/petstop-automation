"""Integra Prefect y MLflow con un modelo de reposición de laboratorio."""
import asyncio
import os
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run, deployment
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.model_selection import train_test_split

from main import ROOT
from pipeline import fetch_data, transform_data, save_results


def training_data(seed: int = 42) -> tuple[pd.DataFrame, np.ndarray]:
    """Datos sintéticos, con demanda futura distinta de las entradas."""
    rng = np.random.default_rng(seed)
    demand = rng.uniform(0.2, 7, 600)
    stock = rng.integers(0, 40, 600)
    lead = rng.integers(1, 8, 600)
    observed = rng.poisson(demand * 7)
    future = rng.poisson(demand * lead)
    return pd.DataFrame({"stock": stock, "units_7d": observed, "lead_days": lead}), (future > stock).astype(int)


@task(name="Entrenar modelo de reposición")
def train_model(seed: int, trees: int) -> tuple:
    logger = get_run_logger()
    logger.info("Inicio de entrenamiento sobre datos sintéticos")
    x, y = training_data(seed)
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.25, stratify=y, random_state=seed
    )
    model = RandomForestClassifier(n_estimators=trees, max_depth=5, random_state=seed)
    model.fit(x_train, y_train)
    metrics = {"f1": float(f1_score(y_test, model.predict(x_test))),
               "roc_auc": float(roc_auc_score(y_test, model.predict_proba(x_test)[:, 1]))}
    logger.info("Fin de entrenamiento y evaluación en test reservado")
    return model, metrics, x_test.head()


@flow(name="PetStop - pipeline trazable")
async def pipeline_trazable(url: str = "http://127.0.0.1:8765",
                            seed: int = 42, trees: int = 50) -> dict:
    uri = os.getenv("MLFLOW_TRACKING_URI")
    if not uri:
        raise ValueError("Definir MLFLOW_TRACKING_URI antes de ejecutar")
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("AI_Automation_Pipeline")
    # Un run abarca todo el flow y enlaza ambas herramientas.
    with mlflow.start_run(run_name="PetStop-reposicion") as run:
        mlflow.log_params({"source_url": url, "seed": seed, "trees": trees,
                           "test_size": 0.25, "max_depth": 5, "data_kind": "synthetic"})
        mlflow.set_tag("prefect_flow_run_id", str(flow_run.id))
        mlflow.set_tag("prefect_deployment_id", str(deployment.id or "local"))
        raw = await fetch_data(url)
        clean = transform_data(raw)
        path = save_results(clean, "data/runs", str(flow_run.id))
        mlflow.log_artifact(path, artifact_path="ingesta")
        model, metrics, example = train_model(seed, trees)
        mlflow.log_metrics(metrics)
        # El registro identifica una versión de laboratorio reproducible.
        info = mlflow.sklearn.log_model(
            sk_model=model, name="modelo_reposicion",
            registered_model_name="PetStopReposicionLaboratorio", input_example=example,
            skops_trusted_types=["sklearn.tree._tree.Tree"],
        )
        result = {"run_id": run.info.run_id, "prefect_flow_run_id": str(flow_run.id),
                  "model_uri": info.model_uri, "metrics": metrics}
        mlflow.log_dict(result, "evaluacion.json")
        return result


if __name__ == "__main__":
    load_dotenv(ROOT / ".env")
    print(asyncio.run(pipeline_trazable(os.getenv("PETSTOP_API_URL") or "http://127.0.0.1:8765")))
