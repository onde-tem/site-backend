import pandas as pd
import numpy as np
import requests
import json
import unicodedata
from shapely.geometry import shape
from scipy.sparse.csgraph import dijkstra
import time
import folium
from fastapi import APIRouter

router = APIRouter()

MAX_DESTINOS = 40
SLEEP_TIME = 0.5
api_key = '5b3ce3597851110001cf6248aacd4755cfdd453e8b72af7e913c7fad'

def normalizar(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8').lower().strip()

def geocodificar_origem(endereco):
    url = 'https://api.openrouteservice.org/geocode/search'
    params = {
        'api_key': api_key,
        'text': endereco,
        'boundary.country': 'BR'
    }
    response = requests.get(url, params=params)
    data = response.json()
    if 'features' not in data or len(data['features']) == 0:
        raise ValueError("Endereço não encontrado.")
    coords = data['features'][0]['geometry']['coordinates']
    return f"{coords[1]},{coords[0]}"  # lat, lon

def carregar_postos_soro(caminho_csv, soro_necessario):
    df = pd.read_csv(caminho_csv)
    if 'Tipos de Soro' not in df.columns:
        raise ValueError("Coluna 'Tipos de Soro' não encontrada no CSV")
    df = df[df['Tipos de Soro'].notna()]  # remove linhas nulas
    df = df[df['Tipos de Soro'].str.contains(soro_necessario, case=False, na=False)]
    return df


def calcular_distancias_com_blocos(df, origem_coords, modo):
    df = df.copy()
    df['Distância (km)'] = None
    df['Tempo estimado (min)'] = None
    lat_origem, lon_origem = map(float, origem_coords.split(','))
    modos_ors = {'driving': 'driving-car', 'walking': 'foot-walking', 'bicycling': 'cycling-regular'}
    modo_ors = modos_ors.get(modo, 'driving-car')

    for i in range(0, len(df), MAX_DESTINOS):
        bloco = df.iloc[i:i+MAX_DESTINOS].copy()
        locations = [[lon_origem, lat_origem]] + bloco[['longitude', 'latitude']].values.tolist()
        url = f"https://api.openrouteservice.org/v2/matrix/{modo_ors}"
        headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
        payload = {
            "locations": locations,
            "metrics": ["distance", "duration"],
            "units": "km",
            "sources": [0],
            "destinations": list(range(1, len(locations)))
        }

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        if 'distances' in data:
            df.loc[bloco.index, 'Distância (km)'] = data['distances'][0]
            df.loc[bloco.index, 'Tempo estimado (min)'] = [t / 60 for t in data['durations'][0]]

        time.sleep(SLEEP_TIME)
    return df

def carregar_geojson(geojson_path):
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson = json.load(f)

    cidades, coordenadas = [], []
    for feature in geojson['features']:
        cidades.append(feature['properties']['name'])
        centroide = shape(feature['geometry']).centroid
        coordenadas.append((centroide.y, centroide.x))  # Latitude, Longitude
    return cidades, coordenadas

def detectar_cidade_mais_proxima(origem_coords, cidades, coordenadas):
    lat_origem, lon_origem = map(float, origem_coords.split(','))
    dist_cidades = [np.linalg.norm(np.subtract((lat_origem, lon_origem), (lat, lon))) for lat, lon in coordenadas]
    return cidades[np.argmin(dist_cidades)]

def construir_matriz_distancia(coordenadas):
    n = len(coordenadas)
    matriz = np.full((n, n), np.inf)
    for i in range(n):
        matriz[i, i] = 0.0
    for i in range(n):
        for j in range(i+1, n):
            matriz[i, j] = matriz[j, i] = np.linalg.norm(np.subtract(coordenadas[i], coordenadas[j]))
    return matriz

def obter_cidades_proximas(cidade_origem, cidades, matriz):
    entrada_usuario_normalizada = normalizar(cidade_origem)
    cidades_normalizadas = [normalizar(c) for c in cidades]
    origem_idx = cidades_normalizadas.index(entrada_usuario_normalizada)
    distancias, _ = dijkstra(matriz, directed=False, indices=origem_idx, return_predecessors=True)
    resultado = pd.DataFrame({'Cidade': cidades, 'Distância (km)': distancias}).sort_values(by='Distância (km)')
    return resultado['Cidade'].head(25).tolist()

def identificar_soro(animal):
    mapa = {
        "escorpiao": "ESCORPIÔNICO",
        "aranha marrom": "ARACNÍDICO",
        "aranha armadeira": "ARACNÍDICO",
        "aranha viuva-negra": "ARACNÍDICO",
        "taturana": "LONÔMICO",
        "cobra jararaca": "BOTRÓPICO",
        "cobra surucucu": "LAQUÉTICO",
        "cobra cascavel": "CROTÁLICO",
        "cobra coral": "ELAPÍDICO",
    }
    return mapa.get(normalizar(animal))

def gerar_mapa_folium(postos_validos, origem_coords):
    mapa = folium.Map(location=[-23.5, -46.6], zoom_start=7)
    for _, posto in postos_validos.iterrows():
        folium.Marker(
            location=[posto['latitude'], posto['longitude']],
            icon=folium.Icon(color='red', icon='plus', prefix='fa'),
            popup=f"{posto['Unidade de Saúde']} - {posto['Cidade']}"
        ).add_to(mapa)

    lat_origem, lon_origem = map(float, origem_coords.split(','))
    folium.Marker(
        location=[lat_origem, lon_origem],
        icon=folium.Icon(color='blue', icon='home', prefix='fa'),
        popup="Origem"
    ).add_to(mapa)
    return mapa

def processar_acidente(endereco_origem, animal, modo_transporte, geojson_path, caminho_csv):
    origem_coords = geocodificar_origem(endereco_origem)
    cidades, coordenadas = carregar_geojson(geojson_path)
    cidade_origem = detectar_cidade_mais_proxima(origem_coords, cidades, coordenadas)
    matriz = construir_matriz_distancia(coordenadas)
    cidades_proximas = obter_cidades_proximas(cidade_origem, cidades, matriz)
    soro_necessario = identificar_soro(animal)
    if not soro_necessario:
        return {"erro": "Animal não reconhecido"}

    # valida se algum posto tem o soro necessário
    try:
        df_postos = carregar_postos_soro(caminho_csv, soro_necessario)
    except ValueError as e:
        return {"erro": str(e)}

    if df_postos.empty:
        return {"erro": f"Nenhum posto encontrado com o soro {soro_necessario}"}

    df_postos['Cidade'] = df_postos['Cidade'].apply(normalizar)
    cidades_proximas = [normalizar(cidade) for cidade in cidades_proximas]
    postos_filtrados = df_postos[df_postos['Cidade'].isin(cidades_proximas)]

    modo_api = {'carro': 'driving', 'caminhando': 'walking', 'bicicleta': 'bicycling'}.get(normalizar(modo_transporte), 'driving')
    postos_resultado = calcular_distancias_com_blocos(postos_filtrados, origem_coords, modo_api)

    postos_resultado = postos_resultado.copy()
    postos_resultado['Distância (km)'] = pd.to_numeric(postos_resultado['Distância (km)'], errors='coerce')
    postos_validos = postos_resultado.dropna(subset=['Distância (km)'])
    if postos_validos.empty:
        return {"erro": "Nenhum posto com distância válida encontrada"}

    posto_mais_proximo = postos_validos.nsmallest(1, 'Distância (km)').iloc[0].to_dict()
    top_10 = postos_validos.nsmallest(10, 'Distância (km)')[[
        'Unidade de Saúde', 'Endereço', 'Cidade', 'Telefone', 'Tipos de Soro', 'Distância (km)', 'Tempo estimado (min)'
    ]].to_dict(orient='records')

    #mapa = gerar_mapa_folium(postos_validos, origem_coords)

    # Adicione esta linha no final da função processar_acidente:
    return {
        "cidade_origem": cidade_origem,
        "posto_mais_proximo": posto_mais_proximo,
        "top_10_postos": top_10,
        "origem_coords": origem_coords,  # <--- adicionada aqui
    }
