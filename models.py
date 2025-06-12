# modelos.py
from fastapi import APIRouter, Query
import pandas as pd
from statsmodels.nonparametric.smoothers_lowess import lowess

router = APIRouter()

def preparar_dados(df):
    df = df.copy()
    df['NU_IDADE_N'] = df['NU_IDADE_N'] - 4000
    df['DT_NOTIFIC'] = pd.to_datetime(df['DT_NOTIFIC'], errors='coerce')
    df = df[df['ANT_MUNIC_'].astype(str).str.startswith('35')]
    df = df[(df['NU_IDADE_N'] >= 0) & (df['NU_IDADE_N'] <= 125)]
    df = df.dropna(subset=['NU_IDADE_N'])
    df['NU_ANO'] = df['DT_NOTIFIC'].dt.year
    return df

def dados_idade_casos(df, ano: int = None, tipo_animal: list[int] = None, municipio: str = None):
    df = preparar_dados(df)
    if ano:
        df = df[df['NU_ANO'] == ano]
    if tipo_animal:
        df = df[df['TP_ACIDENT'].isin(tipo_animal)]
    if municipio:
        df = df[df['Nome_Município'] == municipio]

    idade_counts = df['NU_IDADE_N'].value_counts().reset_index()
    idade_counts.columns = ['NU_IDADE_N', 'num_cases']
    idade_counts = idade_counts.sort_values(by='NU_IDADE_N')

    smoothed = lowess(idade_counts['num_cases'], idade_counts['NU_IDADE_N'], frac=0.2)
    smoothed_data = [{"x": int(x), "y": float(y)} for x, y in smoothed]

    return {
        "labels": idade_counts['NU_IDADE_N'].tolist(),
        "datasets": [
            {
                "label": "Número de Casos",
                "data": idade_counts['num_cases'].tolist(),
                "backgroundColor": "rgba(59, 130, 246, 0.5)"
            },
            {
                "label": "LOESS Suavizado",
                "data": [point["y"] for point in smoothed_data],
                "borderColor": "blue",
                "type": "line",
                "fill": False
            }
        ]
    }

def dados_idade_por_animal(df, ano: int = None, municipio: str = None):
    df = preparar_dados(df)
    if ano:
        df = df[df['NU_ANO'] == ano]
    if municipio:
        df = df[df['Nome_Município'] == municipio]

    categorias_acidente = {
        1: "Serpente",
        2: "Escorpião",
        3: "Aranha",
        4: "Lagarta",
        5: "Abelha",
        6: "Outros"
    }

    resultados = []
    for codigo, nome in categorias_acidente.items():
        df_subset = df[df['TP_ACIDENT'] == codigo]
        if df_subset.empty:
            continue

        idade_counts = df_subset['NU_IDADE_N'].value_counts().reset_index()
        idade_counts.columns = ['NU_IDADE_N', 'num_cases']
        idade_counts = idade_counts.sort_values(by='NU_IDADE_N')

        smoothed = lowess(idade_counts['num_cases'], idade_counts['NU_IDADE_N'], frac=0.2)
        smoothed_data = [{"x": int(x), "y": float(y)} for x, y in smoothed]

        resultados.append({
            "animal": nome,
            "labels": idade_counts['NU_IDADE_N'].tolist(),
            "datasets": [
                {
                    "label": f"Número de Casos ({nome})",
                    "data": idade_counts['num_cases'].tolist(),
                    "backgroundColor": "rgba(34,197,94,0.5)"
                },
                {
                    "label": "LOESS Suavizado",
                    "data": [point["y"] for point in smoothed_data],
                    "borderColor": "blue",
                    "type": "line",
                    "fill": False
                }
            ]
        })

    return resultados

from sklearn.preprocessing import StandardScaler
from statsmodels.nonparametric.smoothers_lowess import lowess

# Coeficientes do GWR por tipo de acidente (obtidos previamente)
gwr_coefs = {
    1: [0, -0.00015182],
    2: [0, 3.42264404e-05],
    3: [0, -0.00010949],
    4: [0, -8.63162609e-05],
    5: [0, 0.00013075],
    6: [0, 0.00022234]
}

categorias_acidente = {
    1: "Serpente",
    2: "Escorpião",
    3: "Aranha",
    4: "Lagarta",
    5: "Abelha",
    6: "Outros"
}


def prever_casos_por_idade(df, ano: int = None, municipio: str = None):
    df = preparar_dados(df)

    # Filtros por ano e município
    if ano:
        df = df[df['NU_ANO'] == ano]
    if municipio:
        df = df[df['Nome_Município'] == municipio]

    resultados = []

    for codigo, nome in categorias_acidente.items():
        # Pula se o animal não estiver nos coeficientes GWR
        if codigo not in gwr_coefs:
            continue

        df_cat = df[df['TP_ACIDENT'] == codigo].copy()
        if df_cat.empty:
            continue

        # Normalizar idade
        idade_mean = df_cat['NU_IDADE_N'].mean()
        idade_std = df_cat['NU_IDADE_N'].std()
        if idade_std == 0 or np.isnan(idade_std):
            df_cat['NU_IDADE_N_scaled'] = 0
        else:
            df_cat['NU_IDADE_N_scaled'] = (df_cat['NU_IDADE_N'] - idade_mean) / idade_std

        # Previsão com GWR
        intercept, coef = gwr_coefs[codigo]
        df_cat['predicted'] = np.exp(intercept + coef * df_cat['NU_IDADE_N_scaled'])

        # Agrupamento por idade
        previsao_idade = df_cat.groupby('NU_IDADE_N')['predicted'].sum().reset_index()

        # Suavização com LOESS
        smoothed = lowess(previsao_idade['predicted'], previsao_idade['NU_IDADE_N'], frac=0.2)
        smoothed_data = [{"x": int(x), "y": float(y)} for x, y in smoothed]

        resultados.append({
            "animal": nome,
            "labels": previsao_idade['NU_IDADE_N'].tolist(),
            "datasets": [
                {
                    "label": f"Casos Previstos ({nome})",
                    "data": previsao_idade['predicted'].tolist(),
                    "backgroundColor": "rgba(59,130,246,0.5)"
                },
                {
                    "label": "LOESS Suavizado",
                    "data": [point["y"] for point in smoothed_data],
                    "borderColor": "orange",
                    "type": "line",
                    "fill": False
                }
            ]
        })

    return resultados

