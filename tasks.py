# as funções que serão utilizadas no Flow.

import os
import re
import pandas as pd
from prefect import task
import requests
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2 import sql

# import schedule

from utils import log, log_and_propagate_error


@task
def download_new_cgu_terceirizados_data() -> dict:
    f"""
    Baixa os Dados Abertos mais recentes dos Terceirizados de Órgãos Federais,
      disponibilizado pela Controladoria Geral da União.

    Returns:
        dict: Dicionário contendo chaves-valores:
                'rawData': Conteúdo do arquivo (bytes),
                ?'error': Possíveis erros propagados (string)
    """
    rawData = {}
    try:
        # Acesse o portal de dados públicos da CGU
        URL = os.getenv("URL_FOR_DATA_DOWNLOAD")
        response = requests.get(URL)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Encontre o link de download com os dados mais recentes
        headers = soup.find_all('h3')
        if(len(headers) > 0):
            header = headers[0]
            year = header.get_text()
            ul = header.find_next('ul')
            if ul:
                links = ul.find_all('a')
                if(len(links) > 0):
                    link = links[0]
                    monthText = link.get_text()
                    # Cheque se já temos essa informação desse mês/ano (redis?),
                    #   se sim break e fast re-schedule (~1 dia), lançamento de dados da cgu atrasado.,
                    #   se não continue a pipeline e re-schedule ~4 meses.
                    file_url = link['href']

                    # Caso download falhe, duas tentativas de recaptura imediata
                    for attempt in range(3):  
                        response = requests.get(file_url)
                        if response.status_code == 200:
                            log(f'Dados referentes ao mês de {monthText} baixados com sucesso!')

                            # Salve o arquivo baixado, sua extensão e ano referente para tratamento posterior
                            content_type = response.headers.get('Content-Type', '')
                            if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
                                'text/csv' in content_type or \
                                'application/vnd.ms-excel' in content_type:
                                file_extension = 'xlsx' if 'spreadsheetml.sheet' in content_type else 'csv'
                                rawData['rawData'] = {'content': response.content, 'type': file_extension, 'year': year}
                                break
                            else:
                                raise ValueError('Formato de arquivo cru fora do esperado (.csv, .xlsx).')

                        else: # Caso download falhe, tentativa de recaptura imediata.
                            log(f"Tentativa {attempt + 1}: Falha ao baixar dados referentes à {monthText}/{year}. Status code: {response.status_code}")
                            if attempt == 2:
                                error = f"Falha ao baixar dados referentes à {monthText}/{year} após tentativa(s) de recaptura. Status code: {response.status_code}"
                                log_and_propagate_error(error, rawData)

    except Exception as e:
        error = f"Falha ao baixar os dados crus mais recentes de {URL}. {e}"
        log_and_propagate_error(error, rawData)
                        
    return rawData

@task
def save_raw_data_locally(rawData: dict) -> dict:
    """
    Salva os dados crus localmente.

    Args:
        dict: Dicionário contendo chaves-valores:
                'rawData': Conteúdo do arquivo (bytes),
                ?'error': Possíveis erros propagados (string)    
    Returns:
        dict: Dicionário contendo chaves-valores:
                'rawFilePaths': caminhos dos arquivos locais salvos (list de strings),
                ?'error': Possíveis erros propagados (string)
    """
    rawFilePaths = {
        'rawFilePaths': []
    }
    # Crie os diretórios no padrão de particionamento Hive
    try:
        DB_NAME = os.getenv("DB_NAME")
        for key, content in rawData.items(): 
            download_dir = os.path.join(f'{DB_NAME}_local/', f"year={content['year']}/")
            os.makedirs(download_dir, exist_ok=True)
            filePath = os.path.join(download_dir, f"raw_data.{content['type']}".lower())
            log(filePath)
        log('Diretótio para armazenar localmente os dados crus criado com sucesso!')
    except Exception as e:
        error = f"Falha ao criar diretótios locais para armazenar os dados crus. {e}"
        log_and_propagate_error(error, rawFilePaths)

    # Salve localmente os dados baixados
    try:
        with open(filePath, 'wb') as file:
            file.write(content['content'])
            rawFilePaths['rawFilePaths'].append(filePath)
            log(f"Dados salvos localmente em {filePath} com sucesso!")
    except Exception as e:
        error = f"Falha ao salvar os dados crus localmente. {e}"
        log_and_propagate_error(error, rawFilePaths)

    return rawFilePaths

@task
def parse_data_into_dataframes(rawFilePaths: dict) -> pd.DataFrame:
    """
    Transforma os dados crus em um DataFrame.

    Args:
        rawFilePaths (dict): Dicionário contendo chaves-valores:
                'rawFilePaths': caminhos dos arquivos locais salvos (list de strings),
                ?'error': Possíveis erros propagados (string)

    Returns:
        dict: Dicionário com chaves sendo os caminhos dos arquivos locais crus, e valores
          sendo dicionários contendo chaves-valores:
                'content': pd.DataFrame,
                ?'error': Possíveis erros propagados (string)
    """
    parsedData = {}
    for rawfilePath in rawFilePaths['rawFilePaths']: 
        parsedData[rawfilePath] = {}

        # Determine o tipo do arquivo RAW local e leia o conteúdo
        if rawfilePath.endswith('.xlsx'):
            try:
                df = pd.read_excel(rawfilePath, engine='openpyxl')
                parsedData[rawfilePath]['dataframe'] = df
                log("Dados crus .xlsx convertidos em DataFrames com sucesso!")
            except Exception as e:
                error = f"Falha ao interpretar como .xlsx os dados crus {rawfilePath}: {e}"
                log_and_propagate_error(error, parsedData)
        elif rawfilePath.endswith('.csv'):
            try:
                df = pd.read_csv(rawfilePath)
                parsedData[rawfilePath]['dataframe'] = df
                log("Dados crus .csv convertidos em DataFrames com sucesso!")
            except Exception as e:
                error = f"Falha ao interpretar como .csv os dados crus {rawfilePath}: {e}"
                log_and_propagate_error(error, parsedData)
        else:
            raise ValueError('Formato de arquivo cru fora do esperado (.csv, .xlsx).')

    return parsedData

@task
def save_parsed_data_as_csv_locally(parsedData: dict) -> dict:
    """
    Salva DataFrames em um arquivo CSV local.

    Args:
        dict: Dicionário com chaves sendo os caminhos dos arquivos locais crus, e valores
          sendo dicionários contendo chaves-valores:
                'content': pd.DataFrame,
                ?'error': Possíveis erros propagados (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'parsedFilePaths': caminhos para CSV locais (list de strings),
                ?'error': Possíveis erros propagados (string)
    """
    parsedFilePaths = {
        'parsedFilePaths': []
    }
    try:
        for rawFilePath, data in parsedData.items(): 
            parsedFilePath = f'{rawFilePath}_parsed.csv'.lower()
            data['dataframe'].to_csv(parsedFilePath, index=False)
            log(f"Dados tratados em CSV salvos localmente em {parsedFilePath} com sucesso!")
            parsedFilePaths['parsedFilePaths'].append(parsedFilePath)
    except Exception as e:
        error = f"Falha ao salvar dados tratados localmente como .csv {rawFilePath}: {e}"
        log_and_propagate_error(error, parsedData)

    return parsedFilePaths

@task
def upload_csv_to_database(parsedFilePaths: dict, tableName: str) -> dict:
    """
    Faz o upload dos arquivos tratados, localizados em parsedFilePaths,
        para a tabela tableName no banco de dados PostgreSQL.

    Args:
        dict: Dicionário contendo chaves-valores:
                'parsedFilePaths': caminhos para CSV locais (list de strings),
                ?'error': Possíveis erros propagados (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': Nome das tabelas atualizadas no banco de dados,
                ?'error': Possíveis erros propagados (string)
    """
    status = {
        'tables': []
    }
    # Conecte com o PostgresSQL
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()
    except Exception as e:
        error = f"Falha ao conectar com o PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        conn.rollback()
        cur.close()
        conn.close()
        return status

    for parsedFile in parsedFilePaths['parsedFilePaths']:
        # Leia o arquivo
        try:
            df = pd.read_csv(parsedFile) # low_memory=False
        except Exception as e:
            error = f"Falha ao ler o arquivo {parsedFile}: {e}"
            log_and_propagate_error(error, status)
            conn.rollback()
            cur.close()
            conn.close()
            return status
        
        # Crie a tabela tableName no PostgresSQL, caso não exista
        try:
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
            log(createTableQuery)
            cur.execute(createTableQuery)
            conn.commit()
        except Exception as e:
            error = f"Falha ao criar tabela {tableName} no PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            conn.rollback()
            cur.close()
            conn.close()
            return status

        # Insere os dados tratados na tabela tableName
        try:
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

            log(f"Feito upload de dados do arquivo {parsedFile} no PostgresSQL com sucesso!")
            status['tables'].append(tableName)
        except Exception as e:
            error = f"Falha ao inserir dados do arquivo {parsedFile} na tabela {tableName} no PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            conn.rollback()
            cur.close()
            conn.close()
            return status

    cur.close()
    conn.close()
    return status

@task
def upload_logs_to_database(logFilePath: str) -> dict:
    """
    Faz o upload dos logs da pipeline para o PostgresSQL.

    Args:
        logFilePath: Caminho para o arquivo de log (string)    
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': Nome das tabelas atualizadas no banco de dados,
                ?'error': Possíveis erros propagados (string)
    """
    status = {
        'tables': []
    }
    tableName = os.getenv("LOGS_TABLE_NAME")

    # Conecte com o PostgresSQL
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()
    except Exception as e:
        error = f"Falha ao conectar com o PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        conn.rollback()
        cur.close()
        conn.close()
        return status

    try:
        # Crie a tabela de logs no PostgreSQL, caso não exista
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                log_id SERIAL PRIMARY KEY,
                log_content TEXT,
                log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(table=sql.Identifier(tableName))
        cur.execute(create_table_query)
        conn.commit()
    except Exception as e:
        error = f"Falha ao criar tabela de {tableName} no PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        conn.rollback()
        cur.close()
        conn.close()
        return status
    
    # Leia o arquivo de log e insira os registros na tabela
    try:
        with open(logFilePath, 'r') as file:
            for line in file:
                insert_log_query = sql.SQL("""
                    INSERT INTO {table} (log_content)
                    VALUES (%s)
                """).format(table=sql.Identifier(tableName))
                cur.execute(insert_log_query, [line])
        conn.commit()
    except Exception as e:
        error = f"Falha ao inserir logs na tabela de {tableName} no PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        conn.rollback()
        cur.close()
        conn.close()
        return status
    
    log(f"Feito upload de logs do arquivo {logFilePath} na tabela {tableName} PostgresSQL com sucesso!")
    status['tables'].append("logs")

    return status

@task
def rename_columns_following_style_manual() -> dict:
    log('@TODO')

@task
def set_columns_types() -> dict:
    log('@TODO')


# @task
# def download_all_available_data() -> dict:
    # """
    # Baixa dados de terceirizados da Controladoria Geral da União de todos os anos
    #   https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados
    # e retorna um texto em formato CSV.

    # Returns:
    #     dict: Dicionário com chaves sendo f"{mes}_{ano}", e valores sendo um dicionário contendo o conteúdo baixado, e o tipo do arquivo.
    # """

    # response = requests.get(URL)
    # soup = BeautifulSoup(response.content, 'html.parser')
    # files = {}

    # # Ache as listas anuais com links para download de dados
    # headers = soup.find_all('h3')
    # for header in headers:

    #     # Colete o ano do cabeçário da lista
    #     year = header.get_text()
    #     ul = header.find_next('ul')
    #     if ul:
    #         links = ul.find_all('a')
    #         for link in links:

    #             # Colete os meses disponíveis na lista do ano
    #             month_text = link.get_text()
    #             if month_text in months:
    #                 file_url = link['href']

    #                 for attempt in range(2):  # Caso download falhe, tentativa de recaptura imediata

    #                     # Baixe os dados contidos no link do mês
    #                     response = requests.get(file_url)
    #                     if response.status_code == 200:
    #                         log(f'Dados referentes ao mês de {month_text} do ano {year} baixados com sucesso!')

    #                         content_type = response.headers.get('Content-Type', '')
    #                         if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
    #                             'text/csv' in content_type or \
    #                             'application/vnd.ms-excel' in content_type:

    #                             # Salve o arquivo baixado, sua extensão e ano referente para tratamento posterior
    #                             file_extension = 'xlsx' if 'spreadsheetml.sheet' in content_type else 'csv'
    #                             files[f"{month_text}_{year}"] = {'content': response.content, 'type': file_extension, 'year': year}
    #                             break

    #                     else: # Caso download falhe, tentativa de recaptura imediata.
    #                         log(f"Tentativa {attempt + 1}: Falha ao baixar dados referentes à {month_text}/{year}. Status code: {response.status_code}")

    #                         if attempt == 1:
    #                             log_and_propagate_error(f"Falha ao baixar dados referentes à {month_text}/{year} após tentativa(s) de recaptura.",
    #                                                     files)

    # return files

# Extrai ano e mês de caminho do arquivo através de expressões regulares.
def extract_year_month_from_path(filePath):
    match = re.search(r'year=(\d{4})/month=(\w+)', filePath)
    if match:
        year = match.group(1)
        month = match.group(2)
        return year, month
    return None, None

# tableName = f"{tableName}_{re.match(r"^[^.]*", parsedFile)}" # Gere o nome da tabela único por arquivo baixado