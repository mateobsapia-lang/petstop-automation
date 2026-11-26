import pandas as pd
import streamlit as st
import uuid

from inferencia import infer
from monitoring import drift_report, audit_table, governance
from agente_mlops_checkpoint4 import run_agent


st.set_page_config(page_title="PetStop · Análisis", layout="wide")
st.title("PetStop")
st.caption("Consultas operativas y monitoreo del laboratorio · Respuestas generadas por IA a partir del manual")
question = st.text_input("Consulta", "¿Cuánto tiempo se guarda una reserva confirmada?")
if st.button("Analizar consulta", type="primary"):
    with st.spinner("Consultando el manual"):
        try:
            record = infer(question)
            st.write(record["respuesta"])
            st.caption("Fuentes · " + ", ".join(record["fuentes"]))
            if record["requiere_revision"]:
                st.info("Esta consulta requiere revisión del equipo.")
        except Exception:
            st.error("La consulta no pudo completarse. Revisá los servicios del laboratorio.")

st.subheader("Cambio en los datos")
drift = drift_report()
st.caption("Comparación de referencia y producción sintéticas. Umbral de alerta KS de 0,20.")
st.dataframe(pd.DataFrame(drift["features"]), hide_index=True, width="stretch")
if any(row["alert"] for row in drift["features"]):
    st.warning("Se detectó un cambio de distribución que requiere revisión.")

st.subheader("Auditoría de inferencias")
st.dataframe(audit_table(), hide_index=True, width="stretch")
st.subheader("Control humano")
st.write(governance()["human_control"])
st.caption(governance()["scope"])

st.subheader("Revisión del modelo")
if st.button("Consultar métricas del modelo"):
    try:
        result = run_agent("Consultá las métricas de PetStopReposicionLaboratorio", str(uuid.uuid4()))
        st.json(result["result"])
    except Exception:
        st.error("No se pudieron obtener las métricas. Comprobá la carga inicial del modelo.")
