# as funções que serão utilizadas no Flow.

import os
import re
import pandas as pd
from prefect import task
import requests
import pandas as pd

import psycopg2
from psycopg2 import sql

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
    rawData = {}
    URL = os.getenv("URL_FOR_DATA_DOWNLOAD")
    response = requests.get(URL)
    # Log site fora do ar
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
                    for attempt in range(3):  # Caso download falhe, duas tentativas de recaptura imediata
                        response = requests.get(file_url)
                        if response.status_code == 200:
                            log(f'Dados referentes ao mês de {month_text} baixados com sucesso!')

                            content_type = response.headers.get('Content-Type', '')
                            if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
                                'text/csv' in content_type or \
                                'application/vnd.ms-excel' in content_type:

                                # Salve o arquivo baixado, sua extensão e ano referente para tratamento posterior
                                file_extension = 'xlsx' if 'spreadsheetml.sheet' in content_type else 'csv'
                                rawData[month_text] = {'content': response.content, 'type': file_extension, 'year': year}
                                break

                            # else log formato não reconhecido

                        else: # Caso download falhe, tentativa de recaptura imediata.
                            log(f"Tentativa {attempt + 1}: Falha ao baixar dados referentes à {month_text}/{year}. Status code: {response.status_code}")

                            if attempt == 2:
                                error = f"Falha ao baixar dados referentes à {month_text}/{year} após tentativa(s) de recaptura. Status code: {response.status_code}"
                                log_and_propagate_error(error, rawData)
        # log erros não encontrou                    
        return rawData

@task
def save_raw_data_locally(rawData: dict) -> dict:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        rawData (dict): Dicionário com chaves sendo o mês referente, e valores sendo um dicionário contendo bytes do conteúdo baixado, e o tipo do arquivo.
    
    Returns:
        dict: Dicionário contendo chaves-valores:
                'rawFilePaths': caminhos dos arquivos locais salvos (list de strings),
                'error': Possíveis erros propagados (string)
    """
    rawFilePaths = {
        'rawFilePaths': []
    }
    # Crie os diretórios no padrão de particionamento Hive
    try:
        for month_text, content in rawData.items(): 
            download_dir = os.path.join('adm_terceirizados_fed_local/', f"year={content['year']}/")
            os.makedirs(download_dir, exist_ok=True)
            file_path = os.path.join(download_dir, f"month={month_text}.{content['type']}".lower())
        log('Diretótio para armazenar localmente os dados no padrão de particionamento Hive criado com sucesso!')
    except Exception as e:
        error = f"Falha ao criar diretótios para armazenar localmente dados referentes à {month_text}/{content['year']}. {e}"
        log_and_propagate_error(error, rawFilePaths)

    # Salve localmente os dados baixados
    try:
        with open(file_path, 'wb') as file:
            file.write(content['content'])
            rawFilePaths['rawFilePaths'].append(file_path)
            log(f"Dados salvos localmente em {file_path} com sucesso!")
    except Exception as e:
        error = f"Falha ao salvar os dados crus referentes à {month_text}/{content['year']} localmente. {e}"
        log_and_propagate_error(error, rawFilePaths)

    return rawFilePaths

@task
def parse_data_into_dataframes(rawFilePaths: dict) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas, para facilitar sua manipulação.

    Args:
        rawFilePaths (dict): Dicionário contendo chaves-valores:
                'saved_files': caminhos dos arquivos locais salvos (list de strings),
                'error': Possíveis erros propagados (string)

    Returns:
        dict: Dicionário com chaves sendo meses do ano, e valores sendo pd.DataFrames com o conteudo referente ao mês da chave.
             Podendo conter chave error com erros propagados
    """
    parsedData = {}
    for filePath in rawFilePaths['rawFilePaths']: 
        parsedData[filePath] = {}

        # Extrai ano e mês de caminho do arquivo
        year, month = extract_year_month_from_path(filePath)
        parsedData[filePath]['year'] = year
        parsedData[filePath]['month'] = month
        # Determine o tipo do arquivo RAW local e leia o conteúdo
        if filePath.endswith('.xlsx'):
            try:
                df = pd.read_excel(filePath, engine='openpyxl')
                parsedData[filePath]['dataframe'] = df
                log("Dados crus .xlsx convertidos em DataFrames com sucesso!")
            except Exception as e:
                error = f"Falha ao interpretar como .xlsx os dados crus {filePath}: {e}"
                log_and_propagate_error(error, parsedData)
                continue
        elif filePath.endswith('.csv'):
            try:
                df = pd.read_csv(filePath)
                parsedData[filePath]['dataframe'] = df
                log("Dados crus .csv convertidos em DataFrames com sucesso!")
            except Exception as e:
                error = f"Falha ao interpretar como .csv os dados crus {filePath}: {e}"
                log_and_propagate_error(error, parsedData)
                continue

    return parsedData

@task
def save_parsed_data_as_csv_locally(parsedData: dict) -> dict:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        parsedData (dict): Dicionário com chaves sendo os caminhos dos arquivos locais crus,
          e valores sendo dicionários contendo chaves-valores:
                'content': o conteúdo (pd.DataFrame),
                'month': o mês referente (string),
                'year': o ano referente (string).
        
    Returns:
        dict: Dicionário contendo chaves-valores:
                'parsedFilePaths': caminhos para CSV locais, já tratados (list de strings),
                'error': Possíveis erros propagados (string)
    """
    parsedFilePaths = {
        'parsedFilePaths': []
    }
    try:
        for filePath, data in parsedData.items(): 
            parsedFilePath = f'{filePath}_parsed.csv'.lower()
            data['dataframe'].to_csv(parsedFilePath, index=False)
            log(f"Dados tratados salvos localmente em {parsedFilePath} com sucesso!")
            parsedFilePaths['parsedFilePaths'].append(filePath)
    except Exception as e:
        error = f"Falha ao salvar dados localmente como .csv {filePath}: {e}"
        log_and_propagate_error(error, parsedData)

    return parsedFilePaths


@task
def upload_csv_to_database(parsedFilePaths: dict) -> dict:
    """
    Faz o Upload dos arquivos CSV para o banco de dados PostgreSQL.

    Args:
        parsedFilePaths (dict): Dicionário contendo chaves-valores:
                'parsedFilePaths': caminhos para CSV locais, já tratados (list de strings),
                'error': Possíveis erros propagados (string)
    
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': Nome das tabelas atualizadas no banco de dados,
                'error': Possíveis erros propagados (string)
    
    """
    status = {
        'tables': {}
    }

    # Informações referentes à conexão com PostgreSQL vindas das variáveis de ambiente .env
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()

    for file in parsedFilePaths['parsedFilePaths']:
        try:
            # Extraia ano e mês de caminho do arquivo
            year, month = extract_year_month_from_path(file)
            tableName = f"year={year}/month={month}".lower()

            # Leia o arquivo e crie uma tabela no PostgresSQL caso não exista
            df = pd.read_csv(file)
            createTableQuery = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {columns}
                )
            """).format(
                table= sql.Identifier(tableName),
                columns=sql.SQL(', ').join([
                    sql.SQL('{} {}').format(
                        sql.Identifier(col), sql.SQL('TEXT')
                    ) for col in df.columns
                ])
            )
            cur.execute(createTableQuery)
            conn.commit()

            # Insere dados
            for index, row in df.iterrows():
                insertValuesQuery = sql.SQL("""
                    INSERT INTO {table} ({fields})
                    VALUES ({values})
                """).format(
                    table=sql.Identifier(tableName),
                    fields=sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                    values=sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
                )
                cur.execute(insertValuesQuery, list(row))
            conn.commit()
            
            status['tables'].append(tableName)

        except Exception as e:
            error = f"Falha no upload do arquivo {file} para o PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            conn.rollback()

    cur.close()
    conn.close()

    return status

@task
def upload_logs_to_database() -> dict:
    log('@TODO')

@task
def rename_columns_following_style_manual() -> dict:
    log('@TODO')

@task
def set_columns_types(newColumns) -> dict:
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
    match = re.search(r'year=(\d{4})/month=(\w+)', filePath)
    if match:
        year = match.group(1)
        month = match.group(2)
        return year, month
    return None, None