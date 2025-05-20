#main.py

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import pandas as pd
import gdown

from graphics import (
    dados_casos_por_ano,
    dados_distribuicao_tipo_animal,
    listar_municipios,
    dados_classificacao_gravidade,
    dados_relacao_trabalho,
    dados_resumo_estatisticas
)
from models import(
    dados_idade_casos,
    dados_idade_por_animal,
    prever_casos_por_idade
)
from busca import(
    processar_acidente,
    obter_todos_os_postos
)
import os

app = FastAPI()

# Caminho temporário do CSV
CSV_PATH = "/tmp/arquivo.csv"

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Baixar CSV do Google Drive na inicialização
@app.on_event("startup")
def startup_event():
    file_id = "1GR_CibmsSuSmxL-Fhd1tdQZmVLGGLNCy"
    url = f"https://drive.google.com/uc?id={file_id}"
    gdown.download(url, CSV_PATH, quiet=False)
    print("Arquivo CSV baixado com sucesso.")

# Função para carregar CSV sob demanda com tipos otimizados
def carregar_csv_otimizado() -> pd.DataFrame:
    dtypes = {
        "ano": "int16",
        "nome_municipio": "category",
        "tipo_animal": "category",
        "gravidade": "category",
        "trabalho_relacionado": "category",
        "faixa_etaria": "category"
        # Adicione mais colunas aqui conforme necessário
    }
    return pd.read_csv(CSV_PATH, dtype=dtypes)

@app.get("/")
def read_root():
    return {"message": "API de Acidentes com Animais Peçonhentos em SP"}

@app.get("/municipios")
def get_municipios():
    df = carregar_csv_otimizado()
    return listar_municipios(df)

@app.get("/grafico-casos-por-ano")
def grafico_casos_por_ano(
    tipo_animal: list[str] = Query(default=[]),
    municipio: str = Query(default=None)
):
    df = carregar_csv_otimizado()
    return dados_casos_por_ano(df, tipo_animal, municipio)

@app.get("/grafico-distribuicao-tipo-animal")
def grafico_distribuicao_tipo_animal(
    ano: int = Query(None),
    municipio: str = Query(None),
    tipo_animal: list[str] = Query(default=[])
):
    df = carregar_csv_otimizado()
    return dados_distribuicao_tipo_animal(df, ano, municipio, tipo_animal)

@app.get("/grafico-gravidade")
def grafico_gravidade(
    ano: int = Query(None),
    tipo_animal: list[str] = Query(default=[]),
    municipio: str = Query(default=None)
):
    df = carregar_csv_otimizado()
    return dados_classificacao_gravidade(df, ano, municipio, tipo_animal)

@app.get("/grafico-trabalho")
def grafico_trabalho(
    ano: int = Query(None),
    tipo_animal: list[str] = Query(default=[]),
    municipio: str = Query(default=None)
):
    df = carregar_csv_otimizado()
    return dados_relacao_trabalho(df, ano, municipio, tipo_animal)

@app.get("/resumo-estatisticas")
def resumo_estatisticas(
    ano: Optional[int] = Query(None),
    municipio: Optional[str] = Query(None),
    tipo_animal: List[str] = Query(default=[])
):
    df = carregar_csv_otimizado()
    return dados_resumo_estatisticas(df, ano=ano, municipio=municipio, tipo_animal=tipo_animal)

@app.get("/modelo/idade-casos")
def idade_casos(
    ano: int = Query(None),
    tipo_animal: list[int] = Query(default=[]),
    municipio: str = Query(default=None)
):
    df = carregar_csv_otimizado()
    return dados_idade_casos(df, ano, tipo_animal, municipio)

@app.get("/modelo/idade-por-animal")
def idade_por_animal(
    ano: int = Query(None),
    municipio: str = Query(default=None)
):
    df = carregar_csv_otimizado()
    return dados_idade_por_animal(df, ano, municipio)

@app.get("/modelo/gwr-por-idade")
def previsao_por_idade(
    ano: int = Query(None),
    municipio: str = Query(default=None)
):
    df = carregar_csv_otimizado()
    return prever_casos_por_idade(df, ano, municipio)

@app.get("/busca/postos-mais-proximo")
def buscar_postos_proximos(
    endereco: str = Query(..., description="Endereço de origem"),
    animal: str = Query(..., description="Animal causador do acidente"),
    transporte: str = Query(..., description="Modo de transporte (carro, bicicleta, caminhando)")
):
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
