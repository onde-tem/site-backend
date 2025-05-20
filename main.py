from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from typing import List, Optional
import requests
import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

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


# === DOWNLOAD DE DATASET DO KAGGLE ===

def download_dataset_from_kaggle():
    dataset_zip_path = 'datasets/kaggle_dataset.zip'
    dataset_folder = 'datasets'

    # Cria pasta datasets, se não existir
    if not os.path.exists(dataset_folder):
        os.makedirs(dataset_folder)

    # Se o arquivo ZIP não existir, faz o download
    if not os.path.isfile(dataset_zip_path):
        print("Baixando dataset do Kaggle...")
        api = KaggleApi()
        api.authenticate()
        # Ajuste para seu dataset
        api.dataset_download_files('karinecerqueira/onde-tem', path=dataset_folder, unzip=False)

    # Confirma que o arquivo ZIP existe antes de abrir
    if not os.path.isfile(dataset_zip_path):
        raise FileNotFoundError(f"Arquivo ZIP não encontrado após download: {dataset_zip_path}")

    # Agora abre o ZIP
    with zipfile.ZipFile(dataset_zip_path, 'r') as zip_ref:
        zip_ref.extractall(dataset_folder)

    # Retorna o caminho do arquivo CSV esperado
    csv_path = os.path.join(dataset_folder, 'onde-tem.csv')  # ajuste para o nome real do CSV no ZIP
    return csv_path


# === CARREGAMENTO DO DATAFRAME ===

csv_path = "animais_peconhentos_SP_completo.csv"

if not os.path.exists(csv_path):
    print("Arquivo CSV local não encontrado, tentando baixar do Kaggle...")
    csv_path = download_dataset_from_kaggle()

data = pd.read_csv(csv_path)


# === CONFIGURAÇÃO DO FASTAPI ===

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"message": "API de Acidentes com Animais Peçonhentos em SP"}


@app.get("/municipios")
def get_municipios():
    return listar_municipios(data)


@app.get("/grafico-casos-por-ano")
def grafico_casos_por_ano(
    tipo_animal: list[str] = Query(default=[]),
    municipio: str = Query(default=None)
):
    return dados_casos_por_ano(data, tipo_animal, municipio)


@app.get("/grafico-distribuicao-tipo-animal")
def grafico_distribuicao_tipo_animal(
    ano: int = Query(None),
    municipio: str = Query(None),
    tipo_animal: list[str] = Query(default=[]),
):
    return dados_distribuicao_tipo_animal(data, ano, municipio, tipo_animal)


@app.get("/grafico-gravidade")
def grafico_gravidade(
    ano: int = Query(None),
    tipo_animal: list[str] = Query(default=[]),
    municipio: str = Query(default=None)
):
    return dados_classificacao_gravidade(data, ano, municipio, tipo_animal)


@app.get("/grafico-trabalho")
def grafico_trabalho(
    ano: int = Query(None),
    tipo_animal: list[str] = Query(default=[]),
    municipio: str = Query(default=None)
):
    return dados_relacao_trabalho(data, ano, municipio, tipo_animal)


@app.get("/resumo-estatisticas")
def resumo_estatisticas(
    ano: Optional[int] = Query(None),
    municipio: Optional[str] = Query(None),
    tipo_animal: List[str] = Query(default=[])
):
    return dados_resumo_estatisticas(data, ano=ano, municipio=municipio, tipo_animal=tipo_animal)


@app.get("/modelo/idade-casos")
def idade_casos(
    ano: int = Query(None),
    tipo_animal: list[int] = Query(default=[]),
    municipio: str = Query(default=None)
):
    return dados_idade_casos(data, ano, tipo_animal, municipio)


@app.get("/modelo/idade-por-animal")
def idade_por_animal(
    ano: int = Query(None),
    municipio: str = Query(default=None)
):
    return dados_idade_por_animal(data, ano, municipio)


@app.get("/modelo/gwr-por-idade")
def previsao_por_idade(
    ano: int = Query(None),
    municipio: str = Query(default=None)
):
    return prever_casos_por_idade(data, ano, municipio)


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
