#main.py

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from typing import List, Optional
import os
from sqlalchemy import create_engine, text

from graphics import (
    dados_casos_por_ano,
    dados_distribuicao_tipo_animal,
    listar_municipios,
    dados_classificacao_gravidade,
    # dados_relacao_trabalho,
    dados_resumo_estatisticas
)

# from models import(
#     dados_idade_casos,
#     dados_idade_por_animal,
#     prever_casos_por_idade
# )

from busca import(
    processar_acidente,
    obter_todos_os_postos
)

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexão com banco PostgreSQL
DATABASE_URL = "postgresql://neondb_owner:npg_obD7ARHn9Kzw@ep-noisy-credit-acainov5-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"
engine = create_engine(DATABASE_URL)

@app.get("/")
def read_root():
    return {"message": "API de Acidentes com Animais Peçonhentos em SP"}

@app.get("/grafico-casos-por-ano")
def grafico_casos_por_ano(
    tipo_animal: List[int] = Query(default=[]),
    municipio: Optional[str] = Query(default=None)
):
    if not tipo_animal and not municipio:
        return []

    query = "SELECT nu_ano, nome_municipio, tp_acident FROM data WHERE 1=1"
    params = {}

    if tipo_animal:
        query += " AND tp_acident = ANY(:tipos)"
        params["tipos"] = tipo_animal

    if municipio:
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    return dados_casos_por_ano(df, tipo_animal, municipio)


@app.get("/grafico-distribuicao-tipo-animal")
def grafico_distribuicao_tipo_animal(
    ano: Optional[int] = Query(default=None),
    municipio: Optional[str] = Query(default=None),
    tipo_animal: Optional[List[str]] = Query(default=None)
):
    if not any([ano, municipio, tipo_animal]):
        return []

    query = "SELECT nu_ano, nome_municipio, tp_acident FROM data WHERE 1=1"
    params = {}

    if ano:
        query += " AND nu_ano = :ano"
        params["ano"] = ano

    if municipio:
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    if tipo_animal:
        tipo_animal = [float(t) for t in tipo_animal]
        query += " AND tp_acident = ANY(:tipos)"
        params["tipos"] = tipo_animal

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    return dados_distribuicao_tipo_animal(df, ano, municipio, tipo_animal)

@app.get("/grafico-gravidade")
def grafico_gravidade(
    ano: Optional[int] = Query(default=None),
    tipo_animal: List[str] = Query(default=[]),
    municipio: Optional[str] = Query(default=None)
):
    if not any([ano, tipo_animal, municipio]):
        return []

    query = "SELECT nu_ano, tp_acident, nome_municipio, tra_classi FROM data WHERE 1=1"
    params = {}

    if ano:
        query += " AND nu_ano = :ano"
        params["ano"] = ano

    if tipo_animal:
        query += " AND tp_acident = ANY(:tipos)"
        params["tipos"] = tipo_animal

    if municipio:
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    return dados_classificacao_gravidade(df, ano, municipio, tipo_animal)


@app.get("/resumo-estatisticas")
def resumo_estatisticas(
    ano: Optional[int] = Query(default=None),
    municipio: Optional[str] = Query(default=None),
    tipo_animal: Optional[List[str]] = Query(default=None)
):
    if not any([ano, municipio, tipo_animal]):
        return []

    query = """
        SELECT nu_ano, nome_municipio, tp_acident, evolucao, ant_tempo_
        FROM data
        WHERE 1=1
    """
    params = {}

    if ano is not None:
        query += " AND nu_ano = :ano"
        params["ano"] = ano

    if municipio is not None:
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    if tipo_animal:
        tipo_animal = [float(t) for t in tipo_animal]
        query += " AND tp_acident = ANY(:tipos)"
        params["tipos"] = tipo_animal

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    return dados_resumo_estatisticas(df, ano, municipio, tipo_animal)

# @app.get("/grafico-trabalho")
# def grafico_trabalho(ano: int = Query(None), tipo_animal: list[str] = Query(default=[]), municipio: str = Query(default=None)):
#     query = "SELECT nu_ano, tp_acident, nome_municipio, tp_acident FROM data WHERE 1=1"
#     params = {}
#     if ano:
#         query += " AND nu_ano = :ano"
#         params["ano"] = ano
#     if tipo_animal:
#         query += " AND tp_acident = ANY(:tipos)"
#         params["tipos"] = tipo_animal
#     if municipio:
#         query += " AND nome_municipio = :municipio"
#         params["municipio"] = municipio
#     with engine.connect() as conn:
#         df = pd.read_sql(text(query), conn, params=params)
#     return dados_relacao_trabalho(df, ano, municipio, tipo_animal)

# @app.get("/modelo/idade-casos")
# def idade_casos(ano: int = Query(None), tipo_animal: list[str] = Query(default=[]), municipio: str = Query(default=None)):
#     query = "SELECT nu_ano, nu_idade_n, tp_acident, nome_municipio FROM data WHERE 1=1"
#     params = {}
#     if ano:
#         query += " AND nu_ano = :ano"
#         params["ano"] = ano
#     if tipo_animal:
#         query += " AND tp_acident = ANY(:tipos)"
#         params["tipos"] = tipo_animal
#     if municipio:
#         query += " AND nome_municipio = :municipio"
#         params["municipio"] = municipio
#     with engine.connect() as conn:
#         df = pd.read_sql(text(query), conn, params=params)
#     return dados_idade_casos(df, ano, tipo_animal, municipio)

# @app.get("/modelo/idade-por-animal")
# def idade_por_animal(ano: int = Query(None), municipio: str = Query(default=None)):
#     query = "SELECT nu_ano, nu_idade_n, tp_acident, nome_municipio FROM data WHERE 1=1"
#     params = {}
#     if ano:
#         query += " AND nu_ano = :ano"
#         params["ano"] = ano
#     if municipio:
#         query += " AND nome_municipio = :municipio"
#         params["municipio"] = municipio
#     with engine.connect() as conn:
#         df = pd.read_sql(text(query), conn, params=params)
#     return dados_idade_por_animal(df, ano, municipio)

# @app.get("/modelo/gwr-por-idade")
# def previsao_por_idade(ano: int = Query(None), municipio: str = Query(default=None)):
#     query = "SELECT nu_ano, nu_idade_n, nome_municipio FROM data WHERE 1=1"
#     params = {}
#     if ano:
#         query += " AND nu_ano = :ano"
#         params["ano"] = ano
#     if municipio:
#         query += " AND nome_municipio = :municipio"
#         params["municipio"] = municipio
#     with engine.connect() as conn:
#         df = pd.read_sql(text(query), conn, params=params)
#     return prever_casos_por_idade(df, ano, municipio)

@app.get("/busca/postos-mais-proximo")
def buscar_postos_proximos(endereco: str = Query(..., description="Endereço de origem"), animal: str = Query(..., description="Animal causador do acidente"), transporte: str = Query(..., description="Modo de transporte (carro, bicicleta, caminhando)")):
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
