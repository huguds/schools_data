###################################################### Bibliotecas #################################################################################
import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

###################################################### VARIAVEIS #################################################################################

# Lista com o nome das colunas para extrair os dados do JSON
column_names = [
"NCESSCH", "SURVYEAR", "STABR", "LEA_NAME", "SCH_NAME",
    "LSTREET1", "LCITY", "VIRTUAL", "SCHOOL_LEVEL",
    "TOTAL", "AM", "AS_", "BL", "HP", "HI", "TR", "WH"
]

# Dicionário de mapeamento de colunas
column_mapping = {
    "NCESSCH": "school_id",
    "SURVYEAR": "survey_year",
    "STABR": "state_code",
    "LEA_NAME": "agency_name",
    "SCH_NAME": "school_name",
    "LSTREET1": "street",
    "LCITY": "city",
    "VIRTUAL": "virtual_school",
    "SCHOOL_LEVEL": "school_level",
    "TOTAL": "total_students_all_grades",
    "AM": "ai_an_total",
    "AS_": "asian_total",
    "BL": "black_total",
    "HP": "hawaiian_pacific_islander_total",
    "HI": "hispanic_total",
    "TR": "two_or_more_races_total",
    "WH": "white_total"
}

###################################################### Funções #################################################################################

# Cria um dicionário de colunas vazias com base na lista de nome de colunas fornecidas para criar o DataFrame
def create_column_data_dict(column_names):
    """
    Cria um dicionário de colunas vazias para armazenar os valores das escolas.
    Parâmetros:
        column_names (list): Lista de nomes de colunas.
    Retorna:
        dict: Dicionário com nomes de colunas como chaves e listas vazias como valores.
    """
    column_data = {column_name: [] for column_name in column_names}
    return column_data

# Preenche cada coluna com os seus respectivos dados
def append_values_in_dict(data, column_names):
    """
    Preenche o dicionário column_data com os valores correspondentes das colunas para cada escola.
    
    Parâmetros:
        data (list): Dados brutos das escolas em formato de lista de dicionários.
        column_names (list): Lista de nomes das colunas desejadas no dataframe final.
    
    Retorna:
        dict: Dicionário preenchido com os valores das colunas.
    """
    column_data = create_column_data_dict(column_names)
    for dicts_school_info in data:
        for column_name in column_names:
            if column_name == "id":
                column_data[column_name].append(dicts_school_info[column_name])
            else:
                column_data[column_name].append(dicts_school_info['properties'][column_name])
    return column_data

# Cria o DataFrame de acordo com os dados obtidos da API
def create_data_frame():
    """
    Cria o dataframe final a partir dos dados obtidos da API e das listas de nomes das colunas.
    Retorna:
        DataFrame: O dataframe final preenchido com os valores das escolas.
    """
    url = "https://services1.arcgis.com/Ua5sjt3LWTPigjyD/arcgis/rest/services/School_Characteristics_Current/FeatureServer/3/query?outFields=*&where=1%3D1&f=geojson"
    response = requests.get(url)
    if response.status_code == 200:
        school_data = response.json().get('features')
        column_data = append_values_in_dict(school_data, column_names)
        # Cria o DataFrame diretamente a partir da lista de dicionários
        df = pd.DataFrame(column_data)
        return df

# Renomear as colunas do DataFrame para nome mais legíveis
def rename_columns(df, column_mapping):
    """
    Renomeia as colunas do DataFrame de acordo com o mapeamento fornecido.
    Parâmetros:
        df (DataFrame): O DataFrame que terá suas colunas renomeadas.
        column_mapping (dict): Um dicionário que mapeia os nomes das colunas atuais para os novos nomes.
    Retorna:
        DataFrame: O DataFrame com as colunas renomeadas.
    """
    return df.rename(columns=column_mapping)

###################################################### MAIN CODE #################################################################################

# Criar o DataFrame
df = create_data_frame()

# Renomear as colunas do DataFrame
df = rename_columns(df, column_mapping)

#################################################### Tratamento das colunas ####################################################################

# Lista de todas as colunas númericas
numeric_columns = [
    "school_id",
    "total_students_all_grades", "ai_an_total", "asian_total", "black_total",
    "hawaiian_pacific_islander_total", "hispanic_total", "two_or_more_races_total", "white_total"
]

# Tratamento para passar as colunas númericas que vem como tipo float para inteiro
for col in numeric_columns:
    # Converte as colunas para tipo numérico
    df[col] = pd.to_numeric(df[col]).fillna(0).astype('int64')

################################################## Salvar os dados no BigQuery ################################################################

# Cria um cliente do BigQuery
client = bigquery.Client()

# Definir o id do projeto e o id do conjunto de dados criado no BigQuery
project_id = 'voltaic-charter-394503'
dataset_id = 'schools_information'

# Nome da tabela no BigQuery (sem o caminho completo)
table_name = 'schools_information_data'

# Cria uma tabela no BigQuery
table_id = f"{project_id}.{dataset_id}.{table_name}"

# Cria um schema para a tabela do BigQuery a partir dos tipos de dados das colunas do DataFrame
schema = []

for column_name, data_type in zip(df.columns, df.dtypes):
    if "int" in str(data_type):
        bq_data_type = bigquery.SchemaField(column_name, "INTEGER")
    elif "float" in str(data_type):
        bq_data_type = bigquery.SchemaField(column_name, "FLOAT")
    elif "datetime" in str(data_type):
        bq_data_type = bigquery.SchemaField(column_name, "TIMESTAMP")
    else:
        bq_data_type = bigquery.SchemaField(column_name, "STRING")
    schema.append(bq_data_type)

# Verifica se o conjunto de dados já existe
dataset_ref = client.dataset(dataset_id)
try:
    client.get_dataset(dataset_ref)
    print(f"O conjunto de dados {dataset_id} já existe.")
except NotFound:
    # Cria o conjunto de dados no BigQuery
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)
    print(f"O conjunto de dados {dataset_id} foi criado com sucesso.")

# Cria uma tabela no BigQuery (se ainda não existir)
table_ref = dataset_ref.table(table_name)
try:
    client.get_table(table_ref)
    print(f"A tabela {table_name} já existe. Os dados serão adicionados a ela.")
except NotFound:
    # Cria a tabela no BigQuery
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f"A tabela {table_name} foi criada com sucesso.")

# Carrega os dados do DataFrame para a tabela no BigQuery
job = client.load_table_from_dataframe(df, table_ref)
job.result()
print(f"Os dados foram carregados com sucesso na tabela {table_name} no BigQuery.")
