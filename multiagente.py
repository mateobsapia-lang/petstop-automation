"""Tres roles AutoGen con código analítico y revisión."""
import json
import os
from pathlib import Path

from autogen import AssistantAgent, UserProxyAgent, GroupChat, GroupChatManager
from sklearn.datasets import fetch_california_housing

from main import ROOT


def analyze_housing() -> dict:
    directory = ROOT / "data/multiagente"
    directory.mkdir(parents=True, exist_ok=True)
    dataset = fetch_california_housing(as_frame=True)
    dataset.frame.to_csv(directory / "housing.csv", index=False)
    config = {"config_list": [{
        "model": os.getenv("LOCAL_LLM_MODEL", "qwen2.5-1.5b-instruct"),
        "base_url": os.getenv("LOCAL_LLM_URL", "http://127.0.0.1:8088/v1"),
        "api_key": "local", "price": [0, 0],
    }], "temperature": 0, "timeout": 180, "cache_seed": None}
    coordinator = AssistantAgent(
        "Coordinador_Analitico", llm_config=config,
        system_message="Escribí solo un bloque Python ejecutable. Hay una herramienta local "
        "analysis_tools.analyze_csv(path,x,y) que calcula Pearson, n, nulos, y guarda gráfico y JSON. "
        "Usá esa herramienta para analizar housing.csv, MedInc y MedHouseVal. "
        "No reimplementes el cálculo ni importes otras librerías. Imprimí el resultado. "
        "La importación correcta es: from analysis_tools import analyze_csv. "
        "Encabezá el bloque con # filename: analisis_viviendas.py.",
    )
    scientist = UserProxyAgent(
        "Cientifico_de_Datos", human_input_mode="NEVER", llm_config=False,
        code_execution_config={"work_dir": str(directory), "use_docker": False,
                               "timeout": 60, "last_n_messages": 1},
    )
    reviewer = AssistantAgent(
        "Revisor_de_Calidad", llm_config={**config, "max_tokens": 180},
        system_message="Respondé en tres frases breves y terminá con TERMINATE. "
        "Si la ejecución terminó con exitcode 0 y n=20640, aprobá el cálculo e informá "
        "correlación, cantidad de filas y nulos. Concluí solo que la relación es positiva. "
        "No evalúes significancia estadística: no hay un test de hipótesis. "
        "Advertí que no implica causalidad y que son datos históricos con precios topados. "
        "No escribas código ni repitas frases.",
    )
    team = GroupChat(agents=[coordinator, scientist, reviewer], messages=[],
                     max_round=3, speaker_selection_method="round_robin")
    manager = GroupChatManager(groupchat=team, llm_config=False,
                              is_termination_msg=lambda m: "TERMINATE" in (m.get("content") or ""))
    import shutil
    shutil.copy2(ROOT / "config/analysis_tools.py", directory / "analysis_tools.py")
    task = ("Importá analyze_csv desde analysis_tools y ejecutá "
            "analyze_csv('housing.csv', 'MedInc', 'MedHouseVal'). Imprimí su resultado.")
    proposal = coordinator.generate_reply(messages=[{"role": "user", "content": task}])
    from autogen.code_utils import extract_code
    code = "\n\n".join(value for language, value in extract_code(proposal) if language == "python")
    if not code:
        raise ValueError("El coordinador no generó código Python")
    (directory / "analisis_viviendas.py").write_text(code, encoding="utf-8")
    coordinator.initiate_chat(manager, message=proposal, clear_history=True)
    transcript = [{"name": m.get("name"), "content": str(m.get("content", ""))} for m in team.messages]
    (directory / "conversacion.json").write_text(json.dumps(transcript, ensure_ascii=False, indent=2), encoding="utf-8")
    output = json.loads((directory / "resultado.json").read_text(encoding="utf-8"))
    assert output["n"] == len(dataset.frame)
    expected = dataset.frame["MedInc"].corr(dataset.frame["MedHouseVal"])
    if abs(output["correlacion"] - expected) > 0.001:
        raise ValueError("La correlación del informe difiere del cálculo de control")
    if not (directory / "scatter_plot.png").is_file():
        raise ValueError("Falta el gráfico")
    return {"metrics": output, "transcript": transcript, "artifact_path": str(directory / "scatter_plot.png")}


if __name__ == "__main__":
    print(json.dumps(analyze_housing(), ensure_ascii=False))
