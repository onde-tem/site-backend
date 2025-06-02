# graphics.py

import pandas as pd


def listar_municipios(df):
    municipios = sorted(df['nome_municipio'].dropna().unique().tolist())
    return municipios


def dados_casos_por_ano(df, tipo_animal=None, municipio=None):
    casos_ano = df['nu_ano'].value_counts().sort_index()
    return {
        "labels": casos_ano.index.tolist(),
        "datasets": [
            {
                "label": "Casos por Ano",
                "data": casos_ano.values.tolist(),
                "borderColor": "#3b82f6",
                "fill": False,
                "tension": 0.1
            }
        ]
    }


def dados_casos_por_municipio(df, ano=None, tipo_animal=None, municipio=None):
    casos_municipio = df['nome_municipio'].value_counts().sort_values(ascending=False)
    return {
        "labels": casos_municipio.index.tolist(),
        "datasets": [
            {
                "label": f"Casos por Município ({ano if ano else 'Todos os Anos'})",
                "data": casos_municipio.values.tolist(),
                "backgroundColor": "#10b981"
            }
        ]
    }


def dados_distribuicao_tipo_animal(df, ano=None, municipio=None, tipo_animal=None):
    casos_tipo = df['tp_acident'].value_counts()
    cores = ["#6366f1", "#f59e0b", "#ef4444", "#10b981", "#3b82f6", "#a855f7", "#eab308"]

    return {
        "labels": casos_tipo.index.tolist(),
        "datasets": [
            {
                "label": "Distribuição por Tipo de Animal",
                "data": casos_tipo.values.tolist(),
                "backgroundColor": cores[:len(casos_tipo)]
            }
        ]
    }


def dados_classificacao_gravidade(df, ano=None, municipio=None, tipo_animal=None):
    casos = df['tra_classi'].value_counts()
    return {
        "labels": casos.index.tolist(),
        "datasets": [{
            "label": "Classificação de Gravidade",
            "data": casos.values.tolist(),
            "backgroundColor": ["#facc15", "#f87171", "#60a5fa"]
        }]
    }


def dados_relacao_trabalho(df, ano=None, municipio=None, tipo_animal=None):
    casos = df['doenca_tra'].value_counts()
    return {
        "labels": casos.index.tolist(),
        "datasets": [{
            "label": "Acidente Relacionado ao Trabalho",
            "data": casos.values.tolist(),
            "backgroundColor": ["#34d399", "#f87171"]
        }]
    }


def dados_resumo_estatisticas(df, ano=None, municipio=None, tipo_animal=None):
    df = df.copy()
    total = len(df)

    obitos = df['evolucao'].eq(2).sum()
    taxa_obitos = round((obitos / total) * 100, 2) if total > 0 else 0.0

    if 'ant_tempo' in df:
        tempos_validos = df[df['ant_tempo'] != 9]['ant_tempo']
        tempo_medio = round(tempos_validos.mean(), 2) if not tempos_validos.empty else None
    else:
        tempo_medio = None

    return {
        "total": total,
        "taxa_obitos": f"{taxa_obitos}%",
        "tempo_medio": f"{tempo_medio} horas" if tempo_medio is not None else "-"
    }
