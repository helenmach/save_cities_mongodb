from pymongo import MongoClient
import psycopg2 as pg
import pandas as pd
import hashlib
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import unidecode
import requests

register_adapter(dict, Json)

def get_mongo_connection_data():
    client = MongoClient(
        "")
    return client

def get_postgreSQL_connection_data():
    connection = pg.connect(
            host="",
            port="",
            dbname="",
            user="",
            password=""
        )
    return connection


def find_mongo_city():

    db = get_mongo_connection_data()
    projection = {'code': 1, 'state': 1, 'StateId': 1}
    city_details = db['Nome_do_DB']['Nome_da_collection'].find({}, projection)
    return pd.DataFrame(list(city_details)) #transformar a lista em tabela


def select_postgreSQL_city_table():
    postgreDbData = get_postgreSQL_connection_data()
    cursor = postgreDbData.cursor()
    cursor.execute('SELECT "id", "name" FROM "Nome_da_tabela"', )
    city_json = cursor.fetchall()

    all_city_polygon = []
    for city in city_json:
        location = {
            'city_Id': city[0],
            'name': city[1],
        }
        all_city_polygon.append(location)

    # Convertendo para DataFrame
    city_df = pd.DataFrame(all_city_polygon)
    return city_df

def find_City_details(city_details, city_id):
    # Filtrar os valores nulos em 'city_Id' em city_id
    city_id_filtered = city_id.dropna(subset=['city_Id'])
    city_details['code'] = city_details['code'].fillna(-1).astype(int).astype(str).replace('-1', '', regex=True)
    
    
    city_filtered = city_id_filtered[~city_id_filtered['city_Id'].astype(str).isin(city_details['code'])]
    
    return city_filtered

def api_ibge ():
    url_municipios = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

    # Fazendo a requisição GET para obter todos os municípios
    response = requests.get(url_municipios)

    # Verificando se a requisição foi bem-sucedida (código 200)
    if response.status_code == 200:
        municipios = response.json()  # Convertendo a resposta para formato JSON

        reorganizados = []
        for municipio in municipios:
            dados = {
                'code': municipio['id'],
                'names': municipio['nome'],
                'mesorregiao': municipio['microrregiao']['mesorregiao']['nome'],
                'inter_regiao': municipio['regiao-imediata']['regiao-intermediaria']['nome'],
                'state': municipio['microrregiao']['mesorregiao']['UF']['nome'],
                'state_abbreviation': municipio['microrregiao']['mesorregiao']['UF']['sigla'],
                'region': municipio['microrregiao']['mesorregiao']['UF']['regiao']['nome'],
                'region_abbreviation': municipio['microrregiao']['mesorregiao']['UF']['regiao']['sigla']
            }
            reorganizados.append(dados)

        # Criando um DataFrame a partir dos dados reorganizados
        ibge_data = pd.DataFrame(reorganizados)

        ibge_data['names'] = ibge_data['names'].apply(lambda x: unidecode.unidecode(x).upper())
        ibge_data['mesorregiao'] = ibge_data['mesorregiao'].apply(lambda x: unidecode.unidecode(x).upper())
        ibge_data['inter_regiao'] = ibge_data['inter_regiao'].apply(lambda x: unidecode.unidecode(x).upper())
        ibge_data['state'] = ibge_data['state'].apply(lambda x: unidecode.unidecode(x).upper())
        ibge_data['region'] = ibge_data['region'].apply(lambda x: unidecode.unidecode(x).upper())
        ibge_data['region'] = ibge_data['region'].apply(lambda x: unidecode.unidecode(x).upper())
        print(ibge_data)  # Exibindo o DataFrame dos municípios
    else:
        print(f"Erro ao obter os dados. Código de status: {response.status_code}")

    return ibge_data


def find_city_data():
    db = get_mongo_connection_data()
    city_details = find_mongo_city()
    all_city_polygon = select_postgreSQL_city_table()
    connection = get_postgreSQL_connection_data()
    
    all_city = find_City_details(city_details, all_city_polygon)

    ibge_data = api_ibge()
    all_city['city_Id'] = all_city['city_Id'].astype(int)
    city_filtered_data = ibge_data[ibge_data['code'].isin(all_city['city_Id'])]
    
    unique_states = city_filtered_data['state'].unique()
    # Filtrar city_details onde 'state' está presente em unique_states
    filtered_details = city_details[city_details['state'].isin(unique_states)]
    # Mapear 'StateId' com base em 'state'
    state_id_mapping = dict(zip(filtered_details['state'], filtered_details['StateId']))
        
    city_filtered_data['Id'] = city_filtered_data['code'].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
    city_filtered_data['StateId'] = city_filtered_data['state'].map(state_id_mapping)
    city_filtered_data['code'] = city_filtered_data['code'].astype(int)
    
    print(city_filtered_data)
   
    collection = db['Nome_do_DB']['Nome_da_collection']

    for index, row in city_filtered_data.iterrows():
        city_data = {
            'code':row['code'],
            'names': row['names'],
            'Id': row['Id'],
            'mesorregiao':  row['mesorregiao'],
            'inter_regiao': row['inter_regiao'],
            'state': row['state'],
            'state_abbreviation': row['state_abbreviation'],
            'region': row['region'],
            'region_abbreviation': row['region_abbreviation'],
            'StateId': row['StateId']
        }
        
        collection.insert_one(city_data)
    
    return True

find_city_data()