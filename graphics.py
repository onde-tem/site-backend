# graphics.py

import pandas as pd

def mapear_categoria_acidente(df):
    if 'TP_ACIDENT' not in df.columns:
        raise KeyError("Coluna 'TP_ACIDENT' não encontrada no DataFrame.")
    
    categorias_acidente = {
        1: 'Serpente',
        2: 'Aranha',
        3: 'Escorpião',
        4: 'Lagarta',
        5: 'Abelha',
        6: 'Outros',
        9: 'Ignorado'
    }
    df = df.copy()
    df['Categoria_Acidente'] = df['TP_ACIDENT'].map(categorias_acidente)
    return df


def listar_municipios(df):
    municipios = sorted(df['Nome_Município'].dropna().unique().tolist())
    return municipios


def dados_casos_por_ano(df, tipo_animal=None, municipio=None):
    df = mapear_categoria_acidente(df)

    if tipo_animal:  # lista não vazia
        df = df[df['Categoria_Acidente'].isin(tipo_animal)]
    if municipio:  # string não vazia
        df = df[df['Nome_Município'] == municipio]

    casos_ano = df['NU_ANO'].value_counts().sort_index()
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
    df = mapear_categoria_acidente(df)

    if ano:
        df = df[df['NU_ANO'] == ano]
    if tipo_animal:
        df = df[df['Categoria_Acidente'].isin(tipo_animal)]
    if municipio:
        df = df[df['Nome_Município'] == municipio]

    casos_municipio = df['Nome_Município'].value_counts().sort_values(ascending=False)
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
    df = mapear_categoria_acidente(df)

    if ano:
        df = df[df['NU_ANO'] == ano]
    if municipio:
        df = df[df['Nome_Município'] == municipio]
    if tipo_animal:
        df = df[df['Categoria_Acidente'].isin(tipo_animal)]

    casos_tipo = df['Categoria_Acidente'].value_counts()
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
    df = mapear_categoria_acidente(df)
    if ano:
        df = df[df['NU_ANO'] == ano]
    if municipio:
        df = df[df['Nome_Município'] == municipio]
    if tipo_animal:
        df = df[df['Categoria_Acidente'].isin(tipo_animal)]

    casos = df['TRA_CLASSI'].value_counts()
    return {
        "labels": casos.index.tolist(),
        "datasets": [{
            "label": "Classificação de Gravidade",
            "data": casos.values.tolist(),
            "backgroundColor": ["#facc15", "#f87171", "#60a5fa"]
        }]
    }

def dados_relacao_trabalho(df, ano=None, municipio=None, tipo_animal=None):
    df = mapear_categoria_acidente(df)
    if ano:
        df = df[df['NU_ANO'] == ano]
    if municipio:
        df = df[df['Nome_Município'] == municipio]
    if tipo_animal:
        df = df[df['Categoria_Acidente'].isin(tipo_animal)]

    casos = df['DOENCA_TRA'].value_counts()
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
    df = mapear_categoria_acidente(df)

    if ano:
        df = df[df['NU_ANO'] == ano]
    if municipio:
        df = df[df['Nome_Município'] == municipio]
    if tipo_animal:
        df = df[df['Categoria_Acidente'].isin(tipo_animal)]

    total = len(df)

    # Taxa de óbitos (apenas código 2)
    obitos = df['EVOLUCAO'].eq(2).sum()
    taxa_obitos = round((obitos / total) * 100, 2) if total > 0 else 0.0

    # Tempo médio de atendimento (ignorando código 9)
    if 'ANT_TEMPO_' in df:
        tempos_validos = df[df['ANT_TEMPO_'] != 9]['ANT_TEMPO_']
        tempo_medio = round(tempos_validos.mean(), 2) if not tempos_validos.empty else None
    else:
        tempo_medio = None

    return {
        "total": total,
        "taxa_obitos": f"{taxa_obitos}%",
        "tempo_medio": f"{tempo_medio} horas" if tempo_medio is not None else "-"
    }



