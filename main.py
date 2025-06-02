# main.py

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from typing import List, Optional
import os
from sqlalchemy import create_engine, text

# Assumo que 'graphics.py' está no mesmo nível ou acessível
from graphics import (
    dados_casos_por_ano,
    dados_distribuicao_tipo_animal,
    dados_classificacao_gravidade,
    # dados_relacao_trabalho, # Descomente se for usar
    dados_resumo_estatisticas
)

# Adicione aqui o mapeamento de nomes de animais para IDs numéricos do seu BD
# ESTE É UM PONTO CRÍTICO: VOCÊ PRECISA SABER QUAIS SÃO ESSES CÓDIGOS NO SEU BANCO DE DADOS
# Se "Serpente" é o código 1, "Aranha" é 2, etc., configure assim:
ANIMAL_MAP = {
    "Serpente": 1,
    "Aranha": 2,
    "Escorpião": 3,
    "Lagarta": 4,
    "Abelha": 5,
    "Outros": 99, # Exemplo para 'Outros' se tiver um código específico
    # Adicione outros mapeamentos conforme necessário no seu DB
    # Verifique também se 'Ignorado' tem um código, se for relevante para os gráficos
    # "Ignorado": X,
}

# Inverso para buscar o nome a partir do código, útil para logging ou depuração
ID_TO_ANIMAL_MAP = {v: k for k, v in ANIMAL_MAP.items()}


app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Para desenvolvimento, '*' é ok. Para produção, especifique seus domínios.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexão com banco PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://neondb_owner:npg_obD7ARHn9Kzw@ep-noisy-credit-acainov5-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require")
engine = create_engine(DATABASE_URL)

@app.get("/")
def read_root():
    return {"message": "API de Acidentes com Animais Peçonhentos em SP está rodando!"}

@app.get("/municipios")
def get_municipios():
    """
    Endpoint para listar todos os municípios disponíveis no banco de dados.
    Isso é mais robusto do que ter uma lista estática no frontend.
    """
    try:
        with engine.connect() as conn:
            # Assumindo que 'nome_municipio' é uma coluna na sua tabela 'data'
            # e que você quer uma lista única de municípios.
            result = conn.execute(text("SELECT DISTINCT nome_municipio FROM data ORDER BY nome_municipio"))
            municipios_db = [row[0] for row in result.fetchall()]
            return ["Todos"] + sorted(municipios_db) # Adiciona "Todos" e garante ordem alfabética
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar municípios do BD: {e}")


@app.get("/grafico-casos-por-ano")
def get_grafico_casos_por_ano(
    tipo_animal: List[str] = Query(default=[]), # Agora espera lista de strings
    municipio: Optional[str] = Query(default=None)
):
    query = "SELECT nu_ano, nome_municipio, tp_acident FROM data WHERE 1=1"
    params = {}

    # Filtrar 'Todos' do tipo_animal e converter para os IDs numéricos
    tipos_numericos = []
    if tipo_animal and "Todos" not in tipo_animal:
        for animal_str in tipo_animal:
            if animal_str in ANIMAL_MAP:
                tipos_numericos.append(ANIMAL_MAP[animal_str])
            # else: Logar erro ou levantar exceção se um animal_str for inválido

    if tipos_numericos: # Aplica o filtro apenas se houver tipos específicos
        query += " AND tp_acident = ANY(:tipos)"
        params["tipos"] = tipos_numericos

    if municipio and municipio != "Todos": # Aplica o filtro apenas se o município não for 'Todos'
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        # A função dados_casos_por_ano no graphics.py deve retornar:
        # {
        #   "labels": [lista de anos],
        #   "datasets": [{ "label": "Casos por Ano", "data": [lista de dados] }]
        # }
        return dados_casos_por_ano(df, tipos_numericos, municipio)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar gráfico de casos por ano: {e}")


@app.get("/grafico-distribuicao-tipo-animal")
def get_grafico_distribuicao_tipo_animal(
    ano: Optional[int] = Query(default=None),
    municipio: Optional[str] = Query(default=None),
    tipo_animal: Optional[List[str]] = Query(default=None) # Agora espera lista de strings
):
    query = "SELECT nu_ano, nome_municipio, tp_acident FROM data WHERE 1=1"
    params = {}

    if ano is not None and ano != "Todos": # Garante que 'Todos' não seja filtrado
        query += " AND nu_ano = :ano"
        params["ano"] = ano

    if municipio and municipio != "Todos":
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    # Converter tipo_animal de string para numérico, ignorando "Todos"
    tipos_numericos = []
    if tipo_animal and "Todos" not in tipo_animal:
        for animal_str in tipo_animal:
            if animal_str in ANIMAL_MAP:
                tipos_numericos.append(ANIMAL_MAP[animal_str])
            # else: Logar erro ou levantar exceção se um animal_str for inválido

    if tipos_numericos: # Aplica o filtro apenas se houver tipos específicos
        query += " AND tp_acident = ANY(:tipos)"
        params["tipos"] = tipos_numericos

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        # A função dados_distribuicao_tipo_animal no graphics.py deve retornar:
        # {
        #   "labels": [lista de tipos de animais],
        #   "datasets": [{ "label": "Distribuição...", "data": [lista de dados] }]
        # }
        return dados_distribuicao_tipo_animal(df, ano, municipio, tipos_numericos)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar gráfico de distribuição por tipo de animal: {e}")


@app.get("/grafico-gravidade")
def get_grafico_gravidade(
    ano: Optional[int] = Query(default=None),
    tipo_animal: List[str] = Query(default=[]), # Agora espera lista de strings
    municipio: Optional[str] = Query(default=None)
):
    query = "SELECT nu_ano, tp_acident, nome_municipio, tra_classi FROM data WHERE 1=1"
    params = {}

    if ano is not None and ano != "Todos":
        query += " AND nu_ano = :ano"
        params["ano"] = ano

    # Converter tipo_animal de string para numérico, ignorando "Todos"
    tipos_numericos = []
    if tipo_animal and "Todos" not in tipo_animal:
        for animal_str in tipo_animal:
            if animal_str in ANIMAL_MAP:
                tipos_numericos.append(ANIMAL_MAP[animal_str])
            # else: Logar erro ou levantar exceção se um animal_str for inválido

    if tipos_numericos: # Aplica o filtro apenas se houver tipos específicos
        query += " AND tp_acident = ANY(:tipos)" # Use ANY para listas no PostgreSQL
        params["tipos"] = tipos_numericos

    if municipio and municipio != "Todos":
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        # A função dados_classificacao_gravidade no graphics.py deve retornar:
        # {
        #   "labels": [lista de classes de gravidade],
        #   "datasets": [{ "label": "Gravidade...", "data": [lista de dados] }]
        # }
        return dados_classificacao_gravidade(df, ano, municipio, tipos_numericos)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar gráfico de gravidade: {e}")


@app.get("/resumo-estatisticas")
def get_resumo_estatisticas(
    ano: Optional[int] = Query(default=None),
    municipio: Optional[str] = Query(default=None),
    tipo_animal: Optional[List[str]] = Query(default=None) # Agora espera lista de strings
):
    query = """
        SELECT nu_ano, nome_municipio, tp_acident, evolucao, ant_tempo_
        FROM data
        WHERE 1=1
    """
    params = {}

    if ano is not None and ano != "Todos":
        query += " AND nu_ano = :ano"
        params["ano"] = ano

    if municipio is not None and municipio != "Todos":
        query += " AND nome_municipio = :municipio"
        params["municipio"] = municipio

    # Converter tipo_animal de string para numérico, ignorando "Todos"
    tipos_numericos = []
    if tipo_animal and "Todos" not in tipo_animal:
        for animal_str in tipo_animal:
            if animal_str in ANIMAL_MAP:
                tipos_numericos.append(ANIMAL_MAP[animal_str])
            # else: Logar erro ou levantar exceção se um animal_str for inválido

    if tipos_numericos: # Aplica o filtro apenas se houver tipos específicos
        query += " AND tp_acident = ANY(:tipos)"
        params["tipos"] = tipos_numericos

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        # A função dados_resumo_estatisticas no graphics.py deve retornar:
        # { "total": X, "taxa_obitos": "Y%", "tempo_medio": "Z" }
        return dados_resumo_estatisticas(df, ano, municipio, tipos_numericos)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar resumo estatísticas: {e}")


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
