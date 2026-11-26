"""Agente con herramientas y pausa de aprobación persistida en SQLite."""
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import TypedDict

import mlflow
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt, Command
from prefect import flow
from prefect.runtime import flow_run
from pydantic import BaseModel

from inferencia import local_model
from main import ROOT


class State(TypedDict, total=False):
    messages: list[dict]
    action: str
    model_name: str
    dataset: str
    artifact_path: str
    result: dict


class Plan(BaseModel):
    action: str
    model_name: str = "PetStopReposicionLaboratorio"
    dataset: str = "ultima_ingesta"


def get_model_metrics(model_name: str) -> dict:
    if model_name != "PetStopReposicionLaboratorio":
        raise ValueError("Modelo fuera del laboratorio")
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        return {"error": "Modelo no encontrado"}
    version = max(versions, key=lambda v: int(v.version))
    return {"model_name": model_name, "version": version.version,
            **client.get_run(version.run_id).data.metrics}


def clean_dataset(dataset: str) -> dict:
    if dataset != "ultima_ingesta":
        raise ValueError("Dataset fuera del laboratorio")
    path = max((ROOT / "data/runs").glob("*.json"), key=lambda p: p.stat().st_mtime)
    data = json.loads(path.read_text(encoding="utf-8"))
    rows_before = sum(map(len, data.values()))
    result = {k: list({json.dumps(row, sort_keys=True): row for row in rows}.values())
              for k, rows in data.items()}
    target = ROOT / "data/agent/ingesta_limpia.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return {"before": rows_before, "after": sum(map(len, result.values())), "artifact_path": str(target)}


def plan(state: State) -> State:
    model = local_model().with_structured_output(Plan, method="json_schema")
    decision = model.invoke([
        SystemMessage(content="Elegí action entre metrics, clean, promote. "
                      "Extraé model_name o dataset de la consulta. promote solo propone una promoción."),
        HumanMessage(content=state["messages"][-1]["content"]),
    ])
    if decision.action not in {"metrics", "clean", "promote"}:
        raise ValueError("Herramienta no permitida")
    return {**decision.model_dump(), "messages": state["messages"] + [
        {"role": "assistant", "content": decision.model_dump_json()}]}


def execute_tool(state: State) -> State:
    if state["action"] == "metrics":
        result = get_model_metrics(state["model_name"])
    else:
        result = clean_dataset(state["dataset"])
    return {"result": result, "artifact_path": result.get("artifact_path", ""),
            "messages": state["messages"] + [{"role": "tool", "content": json.dumps(result)}]}


def approve_promotion(state: State) -> State:
    # El nodo se suspende antes de cambiar el registro. El solicitante reanuda con Command.
    approved = interrupt({"approval_required": True, "model": state["model_name"],
                          "action": "promoción simulada en el registro del laboratorio"})
    result = {"decision": "rechazada", "registry_changed": False}
    if approved is True:
        target = ROOT / "data/agent/promocion_simulada.json"
        target.write_text(json.dumps({"model": state["model_name"], "approved": True,
                                      "simulated": True}, indent=2), encoding="utf-8")
        result = {"decision": "aprobada", "registry_changed": False, "simulated": True,
                  "artifact_path": str(target)}
    return {"result": result, "artifact_path": result.get("artifact_path", ""), "messages": state["messages"] + [
        {"role": "tool", "content": json.dumps(result)}]}


def graph(checkpointer):
    workflow = StateGraph(State)
    workflow.add_node("plan", plan)
    workflow.add_node("tools", execute_tool)
    workflow.add_node("approval", approve_promotion)
    workflow.add_edge(START, "plan")
    workflow.add_conditional_edges("plan", lambda s: "approval" if s["action"] == "promote" else "tools")
    workflow.add_edge("tools", END)
    workflow.add_edge("approval", END)
    return workflow.compile(checkpointer=checkpointer)


@flow(name="PetStop - agente MLOps")
def run_agent(question: str, thread_id: str, resume: bool | None = None) -> dict:
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment("AI_Automation_Pipeline")
    folder = ROOT / "data/agent"
    folder.mkdir(parents=True, exist_ok=True)
    # SQLite conserva la pausa y el estado entre procesos.
    with sqlite3.connect(folder / "checkpoints.sqlite", check_same_thread=False) as connection:
        agent = graph(SqliteSaver(connection))
        with mlflow.start_run(run_name="agente-mlops"):
            started = time.perf_counter()
            mlflow.set_tag("prefect_flow_run_id", str(flow_run.id))
            mlflow.log_param("thread_id", thread_id)
            payload = Command(resume=resume) if resume is not None else {
                "messages": [{"role": "user", "content": question}]}
            state = agent.invoke(payload, config={"configurable": {"thread_id": thread_id}})
            paused = bool(state.get("__interrupt__"))
            decision = "espera_humana" if paused else json.dumps(state.get("result", {}))
            mlflow.set_tag("decision_final", decision)
            record = {"paused": paused, "messages": state.get("messages"),
                      "result": state.get("result"), "artifact_path": state.get("artifact_path")}
            mlflow.log_dict(record, "estado_agente.json")
            mlflow.log_dict({"question": question, "resume": resume}, "solicitud.json")
            mlflow.log_metrics({"latency_seconds": time.perf_counter() - started, "api_cost_usd": 0})
            return record


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--thread", required=True)
    parser.add_argument("--question", default="")
    parser.add_argument("--decision", choices=["approve", "reject"])
    args = parser.parse_args()
    approval = None if args.decision is None else args.decision == "approve"
    print(json.dumps(run_agent(args.question, args.thread, approval), ensure_ascii=False))
