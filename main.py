#main.py
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import pandas as pd
import os

from graphics import (
    dados_casos_por_ano,
    dados_distribuicao_tipo_animal,
    listar_municipios,
    dados_classificacao_gravidade,
    dados_relacao_trabalho,
    dados_resumo_estatisticas
)

from models import (
    dados_idade_casos,
    dados_idade_por_animal,
    prever_casos_por_idade
)

from busca import (
    processar_acidente,
    obter_todos_os_postos
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Funções auxiliares de carregamento otimizado ===

def processar_em_chunks(caminho, filtro_fn, usecols=None, chunksize=100_000):
    if not os.path.exists(caminho):
        return pd.DataFrame()

    resultado = []
    try:
        for chunk in pd.read_csv(caminho, chunksize=chunksize, usecols=usecols, low_memory=False):
            df_filtrado = filtro_fn(chunk)
            if not df_filtrado.empty:
                resultado.append(df_filtrado)
    except Exception as e:
        print(f"Erro ao ler {caminho}: {e}")

    return pd.concat(resultado, ignore_index=True) if resultado else pd.DataFrame()

def carregar_dados_otimizado(anos: Optional[List[int]] = None, filtro_fn=lambda df: df, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    if anos is None:
        anos = list(range(2007, 2024))

    dados = []
    for ano in anos:
        caminho = os.path.join("dados_por_ano", f"dados{ano}.csv")
        df = processar_em_chunks(caminho, filtro_fn, usecols)
        if not df.empty:
            dados.append(df)

    return pd.concat(dados, ignore_index=True) if dados else pd.DataFrame()

@app.get("/")
def read_root():
    return {"message": "API de Acidentes com Animais Peçonhentos em SP"}

@app.get("/municipios")
def get_municipios():
    data = carregar_dados_otimizado()
    return listar_municipios(data)

@app.get("/grafico-casos-por-ano")
def grafico_casos_por_ano(tipo_animal: list[str] = Query(default=[]), municipio: str = Query(default=None)):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        if tipo_animal:
            df = df[df["tipo_animal"].isin(tipo_animal)]
        return df

    data = carregar_dados_otimizado(filtro_fn=filtro_fn)
    return dados_casos_por_ano(data, tipo_animal, municipio)

@app.get("/grafico-distribuicao-tipo-animal")
def grafico_distribuicao_tipo_animal(ano: int = Query(None), municipio: str = Query(None), tipo_animal: list[str] = Query(default=[])):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        if tipo_animal:
            df = df[df["tipo_animal"].isin(tipo_animal)]
        return df

    data = carregar_dados_otimizado([ano] if ano else None, filtro_fn)
    return dados_distribuicao_tipo_animal(data, ano, municipio, tipo_animal)

@app.get("/grafico-gravidade")
def grafico_gravidade(ano: int = Query(None), tipo_animal: list[str] = Query(default=[]), municipio: str = Query(default=None)):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        if tipo_animal:
            df = df[df["tipo_animal"].isin(tipo_animal)]
        return df

    data = carregar_dados_otimizado([ano] if ano else None, filtro_fn)
    return dados_classificacao_gravidade(data, ano, municipio, tipo_animal)

@app.get("/grafico-trabalho")
def grafico_trabalho(ano: int = Query(None), tipo_animal: list[str] = Query(default=[]), municipio: str = Query(default=None)):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        if tipo_animal:
            df = df[df["tipo_animal"].isin(tipo_animal)]
        return df

    data = carregar_dados_otimizado([ano] if ano else None, filtro_fn)
    return dados_relacao_trabalho(data, ano, municipio, tipo_animal)

@app.get("/resumo-estatisticas")
def resumo_estatisticas(ano: Optional[int] = Query(None), municipio: Optional[str] = Query(default=None), tipo_animal: List[str] = Query(default=[])):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        if tipo_animal:
            df = df[df["tipo_animal"].isin(tipo_animal)]
        return df

    data = carregar_dados_otimizado([ano] if ano else None, filtro_fn)
    return dados_resumo_estatisticas(data, ano=ano, municipio=municipio, tipo_animal=tipo_animal)

@app.get("/modelo/idade-casos")
def idade_casos(ano: int = Query(None), tipo_animal: list[int] = Query(default=[]), municipio: str = Query(default=None)):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        if tipo_animal:
            df = df[df["tipo_animal"].isin(tipo_animal)]
        return df

    data = carregar_dados_otimizado([ano] if ano else None, filtro_fn)
    return dados_idade_casos(data, ano, tipo_animal, municipio)

@app.get("/modelo/idade-por-animal")
def idade_por_animal(ano: int = Query(None), municipio: str = Query(default=None)):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        return df

    data = carregar_dados_otimizado([ano] if ano else None, filtro_fn)
    return dados_idade_por_animal(data, ano, municipio)

@app.get("/modelo/gwr-por-idade")
def previsao_por_idade(ano: int = Query(None), municipio: str = Query(default=None)):
    def filtro_fn(df):
        if municipio:
            df = df[df["municipio"] == municipio]
        return df

    data = carregar_dados_otimizado([ano] if ano else None, filtro_fn)
    return prever_casos_por_idade(data, ano, municipio)

@app.get("/busca/postos-mais-proximo")
def buscar_postos_proximos(endereco: str = Query(...), animal: str = Query(...), transporte: str = Query(...)):
    try:
        resultado = processar_acidente(
            endereco_origem=endereco,
            animal=animal,
            modo_transporte=transporte,
            geojson_path="geojson_sp.json",
            caminho_csv="postos_geolocalizados.csv",
        )
        if "erro" in resultado:
            return JSONResponse(status_code=400, content=resultado)

        return {
            "cidade_origem": resultado["cidade_origem"],
            "origem_coords": resultado["origem_coords"],
            "posto_mais_proximo": resultado["posto_mais_proximo"],
            "postos_proximos": resultado["top_10_postos"]
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})

@app.get("/busca/todos-postos")
def buscar_todos_os_postos():
    try:
        postos = obter_todos_os_postos("postos_geolocalizados.csv")
        return {"postos": postos}
    except Exception as e:
        return JSONResponse(status_code=500, content={"erro": str(e)})