# as funções que serão utilizadas no Flow.

import os
import re
import pandas as pd
from prefect import task
import requests
import pandas as pd

import psycopg2

# import schedule
from bs4 import BeautifulSoup

from utils import log, log_and_propagate_error
from dotenv import load_dotenv
load_dotenv()

@task
def download_new_data() -> dict:
    """
    Baixa dados recentes de terceirizados da Controladoria Geral da União
      https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados
    e retorna um texto em formato CSV.

    Returns:
        dict: Dicionário com chaves sendo o mês referente, e valores sendo um dicionário contendo bytes do conteúdo baixado, e o tipo do arquivo.
    """
    data = {}
    URL = os.getenv("URL_FOR_DATA_DOWNLOAD")
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Ache os dados mais recentes disponíveis
    headers = soup.find_all('h3')
    if(len(headers) > 0):
        header = headers[0]
        year = header.get_text()
        ul = header.find_next('ul')
        if ul:
            links = ul.find_all('a')
            if(len(links) > 0):
                link = links[0]

                month_text = link.get_text()
                # Checagem se já temos os dados do mês referente no Banco de Dados
                    #   if yes -> log and reschedule for ~1day
                    #   if no -> keep going and reschedule for ~4 months in the end
                if month_text in ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho",
                                   "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]:
                    file_url = link['href']
                    for attempt in range(2):  # Caso download falhe, tentativa de recaptura imediata
                        response = requests.get(file_url)
                        if response.status_code == 200:
                            log(f'Dados referentes ao mês de {month_text} baixados com sucesso!')

                            content_type = response.headers.get('Content-Type', '')
                            if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
                                'text/csv' in content_type or \
                                'application/vnd.ms-excel' in content_type:

                                # Salve o arquivo baixado, sua extensão e ano referente para tratamento posterior
                                file_extension = 'xlsx' if 'spreadsheetml.sheet' in content_type else 'csv'
                                data[month_text] = {'content': response.content, 'type': file_extension, 'year': year}
                                break

                        else: # Caso download falhe, tentativa de recaptura imediata.
                            log(f"Tentativa {attempt + 1}: Falha ao baixar dados referentes à {month_text}/{year}. Status code: {response.status_code}")

                            if attempt == 1:
                                error = f"Falha ao baixar dados referentes à {month_text}/{year} após tentativa(s) de recaptura."
                                log_and_propagate_error(error, data)
                                
        return data

@task
def save_raw_data_locally(rawData: dict) -> dict:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        rawData (dict): Dicionário com chaves sendo o mês referente, e valores sendo um dicionário contendo bytes do conteúdo baixado, e o tipo do arquivo.
    
    Returns:
        dict: Dicionário contendo chaves-valores:
            {
                'saved_files': caminhos dos arquivos locais salvos (list de strings),
                'error': Possíveis erros propagados (string)
            }
    """
    saved_files = []

    # Crie o diretório no padrão de particionamento Hive
    for month_text, content in rawData.items(): 
        download_dir = os.path.join('downloads/', f"year={content['year']}/")
        os.makedirs(download_dir, exist_ok=True)
        file_path = os.path.join(download_dir, f"{month_text}.{content['type']}")
        log('Diretótio para armazenar localmente os dados no padrão de particionamento Hive criado com sucesso!')

        # Salve localmente os dados baixados
        with open(file_path, 'wb') as file:
            file.write(content['content'])
            log(f"Dados salvos localmente em {file_path} com sucesso!")
        saved_files.append(file_path)

    return saved_files

@task
def parse_data_into_dataframes(rawFilePaths: dict) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas, para facilitar sua manipulação.

    Args:
        rawFilePaths (dict): Dicionário contendo chaves-valores:
            {
                'saved_files': caminhos dos arquivos locais salvos (list de strings),
                'error': Possíveis erros propagados (string)
            }

    Returns:
        dict: Dicionário com chaves sendo meses do ano, e valores sendo pd.DataFrames com o conteudo referente ao mês da chave.
             Podendo conter chave error com erros propagados
    """
    parsedData = {}
    for filePath in rawFilePaths: 

        # Extrai ano e mês de caminho do arquivo
        year, month = extract_year_month_from_path(filePath)

        # Determine o tipo do arquivo RAW local e leia o conteúdo
        if filePath.endswith('.xlsx'):
            try:
                df = pd.read_excel(filePath, engine='openpyxl')
            except Exception as e:
                error = f"Falha ao interpretar como .xlsx os dados RAW {filePath}: {e}"
                log_and_propagate_error(error, parsedData)
                continue
        elif filePath.endswith('.csv'):
            try:
                df = pd.read_csv(filePath)
            except Exception as e:
                error = f"Falha ao interpretar como .csv os dados RAW {filePath}: {e}"
                log_and_propagate_error(error, parsedData)
                continue

        # Organizar dados tratados na memória principal
        # clean_key = re.sub(r'[^a-zA-Z0-9]', '_', filePath)
        parsedData[filePath] = {}
        parsedData[filePath]['year'] = year
        parsedData[filePath]['month'] = month
        parsedData[filePath]['dataframe'] = df

    log("Dados convertidos em DataFrames com sucesso!")
    return parsedData

@task
def save_parsed_data_as_csv_locally(parsedData: dict) -> dict:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        parsedData (dict): Dicionário com chaves sendo os caminhos dos arquivos locais já tratados, e valores sendo dicionários contendo chaves-valores:
            {
                'content': o conteúdo (pd.DataFrame),
                'month': o mês referente (string),
                'year': o ano referente (string).
            }
        
    Returns:
        dict: Dicionário contendo chaves-valores:
            {
                'saved_files': caminhos para CSV locais, já tratados (list de strings),
                'error': Possíveis erros propagados (string)
            }
    """
    saved_files = {
        saved_files: []
    }

    # Crie o diretório no padrão de particionamento Hive
    for filePath, data in parsedData.items(): 
        # Salve localmente os dados baixados
        parsedFilePath = f'{filePath}_parsed.csv'
        data['dataframe'].to_csv(parsedFilePath, index=False)
        log(f"Dados salvos localmente em {parsedFilePath} com sucesso!")
        saved_files['saved_files'].append(filePath)

    return saved_files


@task
def upload_csv_to_database(files: list) -> None:
    """
    Faz o Upload dos arquivos CSV para o banco de dados PostgreSQL.

    Args:
        files (list): Lista contendo os nomes dos arquivos CSV locais, já tratados.
    """
    # Informações referentes à conexão com PostgreSQL vindas das variáveis de ambiente .env
    print(os.getenv("DB_NAME"))
    # conn = psycopg2.connect(
    #     host=os.getenv("DB_HOST"),
    #     port=os.getenv("DB_PORT"),
    #     dbname=os.getenv("DB_NAME"),
    #     user=os.getenv("DB_USER"),
    #     password=os.getenv("DB_PASSWORD")
    # )
    # cur = conn.cursor()

    # for file in files:
    #     try:
    #         df = pd.read_csv(file)
    #         table_name = os.path.splitext(os.path.basename(file))[0]
    #         create_table_query = psycopg2.sql.SQL("""
    #             CREATE TABLE IF NOT EXISTS {table} (
    #                 {columns}
    #             )
    #         """).format(
    #             table=psycopg2.sql.Identifier(table_name),
    #             columns=psycopg2.sql.SQL(', ').join([
    #                 psycopg2.sql.SQL('{} {}').format(
    #                     psycopg2.sql.Identifier(col), psycopg2.sql.SQL('TEXT')
    #                 ) for col in df.columns
    #             ])
    #         )
    #         cur.execute(create_table_query)
    #         conn.commit()

    #         # Inserting data
    #         for index, row in df.iterrows():
    #             insert_query = psycopg2.sql.SQL("""
    #                 INSERT INTO {table} ({fields})
    #                 VALUES ({values})
    #             """).format(
    #                 table=psycopg2.sql.Identifier(table_name),
    #                 fields=psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, df.columns)),
    #                 values=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(df.columns))
    #             )
    #             cur.execute(insert_query, list(row))

    #         conn.commit()
    #     except Exception as e:
    #         log(f"Falha no upload do arquivo {file} para o PostgreSQL: {e}")
    #         conn.rollback()

    # cur.close()
    # conn.close()

@task
def upload_logs_to_database() -> dict:
    log('@TODO')


# @task
# def download_all_available_data() -> dict:
    """
    Baixa dados de terceirizados da Controladoria Geral da União de todos os anos
      https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados
    e retorna um texto em formato CSV.

    Returns:
        dict: Dicionário com chaves sendo f"{mes}_{ano}", e valores sendo um dicionário contendo o conteúdo baixado, e o tipo do arquivo.
    """

    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    files = {}

    # Ache as listas anuais com links para download de dados
    headers = soup.find_all('h3')
    for header in headers:

        # Colete o ano do cabeçário da lista
        year = header.get_text()
        ul = header.find_next('ul')
        if ul:
            links = ul.find_all('a')
            for link in links:

                # Colete os meses disponíveis na lista do ano
                month_text = link.get_text()
                if month_text in months:
                    file_url = link['href']

                    for attempt in range(2):  # Caso download falhe, tentativa de recaptura imediata

                        # Baixe os dados contidos no link do mês
                        response = requests.get(file_url)
                        if response.status_code == 200:
                            log(f'Dados referentes ao mês de {month_text} do ano {year} baixados com sucesso!')

                            content_type = response.headers.get('Content-Type', '')
                            if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
                                'text/csv' in content_type or \
                                'application/vnd.ms-excel' in content_type:

                                # Salve o arquivo baixado, sua extensão e ano referente para tratamento posterior
                                file_extension = 'xlsx' if 'spreadsheetml.sheet' in content_type else 'csv'
                                files[f"{month_text}_{year}"] = {'content': response.content, 'type': file_extension, 'year': year}
                                break

                        else: # Caso download falhe, tentativa de recaptura imediata.
                            log(f"Tentativa {attempt + 1}: Falha ao baixar dados referentes à {month_text}/{year}. Status code: {response.status_code}")

                            if attempt == 1:
                                log_and_propagate_error(f"Falha ao baixar dados referentes à {month_text}/{year} após tentativa(s) de recaptura.",
                                                        files)

    return files

# Extrai ano e mês de caminho do arquivo através de expressões regulares.
def extract_year_month_from_path(filePath):
    match = re.search(r'year=(\d{4})/(\w+)\.', filePath)
    if match:
        year = match.group(1)
        month = match.group(2)
        return year, month