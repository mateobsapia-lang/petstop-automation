"""RAG local con FAISS, LangChain y salida validada."""
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import mlflow
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.vectorstores import FAISS
from langchain_openai import ChatOpenAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
from prefect import flow
from prefect.runtime import flow_run
from pydantic import BaseModel, Field
from sklearn.feature_extraction.text import HashingVectorizer

from main import ROOT


class LocalEmbeddings(Embeddings):
    """Representación léxica reproducible, sin servicio de embeddings."""
    def __init__(self):
        self.vectorizer = HashingVectorizer(n_features=512, alternate_sign=False, norm="l2")

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self.vectorizer.transform(texts).toarray().tolist()

    def embed_query(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]


class Answer(BaseModel):
    respuesta: str = Field(description="Respuesta breve basada únicamente en el contexto")
    fuentes: list[str] = Field(description="Identificadores de documentos usados")
    requiere_revision: bool = Field(description="Verdadero si falta información o hay que aprobar una acción")


def local_model() -> ChatOpenAI:
    return ChatOpenAI(
        base_url=os.getenv("LOCAL_LLM_URL", "http://127.0.0.1:8088/v1"),
        api_key="local", model=os.getenv("LOCAL_LLM_MODEL", "qwen2.5-1.5b-instruct"),
        temperature=0, timeout=180, max_retries=1, max_tokens=450,
    )


def build_index(chunk_size: int = 400) -> FAISS:
    text = (ROOT / "config/petstop_manual.md").read_text(encoding="utf-8")
    sections = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=50).split_text(text)
    docs = [Document(page_content=value, metadata={"source": f"manual-{i+1}"})
            for i, value in enumerate(sections)]
    index = FAISS.from_documents(docs, LocalEmbeddings())
    index.save_local(str(ROOT / "data/vector_index"))
    return index


@flow(name="PetStop - inferencia RAG")
def infer(question: str, chunk_size: int = 400, retrieval_k: int = 3) -> dict:
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment("AI_Automation_Pipeline")
    with mlflow.start_run(run_name="inferencia-RAG") as run:
        started = time.perf_counter()
        model = local_model()
        mlflow.log_params({"chunk_size": chunk_size, "model_name": model.model_name,
                           "retrieval_k": retrieval_k, "temperature": 0})
        mlflow.set_tag("prefect_flow_run_id", str(flow_run.id))
        index = build_index(chunk_size)
        docs = index.similarity_search(question, k=retrieval_k)
        context = "\n\n".join(f"[{d.metadata['source']}] {d.page_content}" for d in docs)
        parser = PydanticOutputParser(pydantic_object=Answer)
        schema = Answer.model_json_schema()
        schema["properties"]["fuentes"]["items"]["enum"] = [d.metadata["source"] for d in docs]
        structured_model = model.bind(response_format={
            "type": "json_schema",
            "json_schema": {"name": "PetStopAnswer", "strict": True, "schema": schema},
        })
        prompt = ChatPromptTemplate.from_messages([
            ("system", "Usá solo el contexto. Si falta información, decí que no está disponible. "
             "No inventes precios, números ni fuentes. Las compras necesitan revisión humana. "
             "Respondé SOLO un objeto JSON, sin bloques Markdown.\n{format}\nContexto\n{context}"),
            ("human", "{question}"),
        ])
        chain = prompt | structured_model | parser
        answer = chain.invoke({"format": parser.get_format_instructions(), "context": context, "question": question})
        allowed = {d.metadata["source"] for d in docs}
        if not set(answer.fuentes).issubset(allowed):
            raise ValueError("La respuesta cita una fuente fuera del contexto")
        if not answer.fuentes:
            answer.requiere_revision = True
        result = answer.model_dump()
        elapsed = time.perf_counter() - started
        record = {"Timestamp": datetime.now(timezone.utc).isoformat(),
                  "Input_Hash": hashlib.sha256(question.encode()).hexdigest(),
                  "Model_Version": model.model_name, "Agent_Action": result["respuesta"],
                  "Human_Override_Flag": False, "latency_seconds": elapsed,
                  "mlflow_run_id": run.info.run_id, "pregunta": question, **result}
        path = ROOT / "data/inferences" / f"{run.info.run_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        mlflow.log_metrics({"latency_seconds": elapsed, "api_cost_usd": 0})
        mlflow.log_dict({"system_context": context, "question": question}, "prompt.json")
        mlflow.log_artifact(str(path), artifact_path="inferencia")
        if result["requiere_revision"]:
            alert = {"event": "revision_requerida", "run_id": run.info.run_id,
                     "reason": "El motor solicita revisión o no aporta fuentes"}
            logging.getLogger("petstop.alertas").warning(json.dumps(alert))
            mlflow.log_dict(alert, "alerta.json")
        from audit import append
        append(record)
        return record


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    print(json.dumps(infer("¿Cuánto tiempo se guarda una reserva confirmada?"), ensure_ascii=False, indent=2))
