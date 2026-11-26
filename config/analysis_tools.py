"""Herramienta determinista de cálculo y exportación para el científico de datos."""
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def analyze_csv(path, x, y):
    df = pd.read_csv(path)
    result = {"correlacion": float(df[x].corr(df[y])), "n": int(len(df)),
              "nulos": int(df.isna().sum().sum())}
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.scatter(df[x], df[y], alpha=0.15, s=8)
    ax.set(xlabel="Ingreso mediano (decenas de miles USD)",
           ylabel="Valor mediano vivienda (100.000 USD)", title="California Housing: ingreso y vivienda")
    fig.tight_layout()
    fig.savefig("scatter_plot.png", dpi=150)
    plt.close(fig)
    with open("resultado.json", "w", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    return result
