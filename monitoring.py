"""Drift de datos sintéticos y auditoría de inferencias."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

from main import ROOT
from pipeline_trazable import training_data


def drift_report() -> dict:
    reference, _ = training_data(42)
    production = reference.copy()
    # Escenario de prueba documentado de cambio de demanda.
    production["units_7d"] += 12
    comparisons = []
    for column in reference.columns:
        statistic = ks_2samp(reference[column], production[column])
        comparisons.append({"feature": column, "ks": float(statistic.statistic),
                            "p_value": float(statistic.pvalue),
                            "alert": bool(statistic.statistic > 0.2)})
    return {"data_kind": "synthetic", "n_reference": len(reference),
            "n_production": len(production), "threshold_ks": 0.2, "features": comparisons}


def audit_table() -> pd.DataFrame:
    from audit import records
    columns = ["Timestamp", "Input_Hash", "Model_Version", "Agent_Action", "Human_Override_Flag",
               "mlflow_run_id", "Previous_Hash", "Event_Hash"]
    return pd.DataFrame(records()[-5:]).reindex(columns=columns)


def governance() -> dict:
    return {
        "use_case": "Asistente de consultas operativas y propuestas de reposición de PetStop",
        "scope": "Laboratorio con registros sintéticos, sin personas identificables",
        "academic_risk_assessment": "Uso operativo sin finalidad de alto riesgo identificada; "
                                    "el asistente generativo requiere transparencia",
        "transparency": "La interfaz informa que las respuestas son generadas por IA a partir del manual",
        "source": "https://ai-act-service-desk.ec.europa.eu/en/ai-act/faq/what-are-obligations-high-risk-transparency-risk-and-minimal-risk-ai-systems",
        "risk_basis": "No decide acceso a empleo, crédito, salud ni servicios esenciales",
        "human_control": "Las promociones simuladas del registro requieren aprobación explícita; "
                         "un operador puede detener los flows de Prefect y bloquear el trigger",
        "limits": "La evaluación académica debe revisarse si cambia la finalidad o el entorno de uso",
    }


if __name__ == "__main__":
    target = ROOT / "data/monitoring"
    target.mkdir(parents=True, exist_ok=True)
    (target / "drift.json").write_text(json.dumps(drift_report(), indent=2), encoding="utf-8")
    audit_table().to_csv(target / "auditoria.csv", index=False)
    (target / "gobernanza.json").write_text(json.dumps(governance(), ensure_ascii=False, indent=2), encoding="utf-8")
