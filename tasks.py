# as funções que serão utilizadas no Flow.
import logging
import os
import subprocess

import pandas as pd
from bs4 import BeautifulSoup
import requests

from prefect import task
from prefect.engine.state import Failed
from prefect.triggers import all_finished
import psycopg2
from psycopg2 import sql

from dotenv import load_dotenv
load_dotenv()
# import schedule

from utils import (
    log,
    log_and_propagate_error
)

@task
def download_new_cgu_terceirizados_data(cleanStart: dict) -> dict:
    f"""
    Baixa os Dados Abertos mais recentes dos Terceirizados de Órgãos Federais,
      disponibilizado pela Controladoria Geral da União.

    Args:
        dict: Dicionário contendo chaves-valores:
                'logFilePath': caminho dos arquivo local de log (string),
                ?'error': Possíveis erros propagados (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'rawData': Dicionário contendo chaves-valores:
                    'content': Conteúdo do arquivo (bytes),
                    'type': Extensão do arquivo (.csv, .xlsx),
                    'year': Ano do arquivo para particionamento,
                ?'error': Possíveis erros propagados (string)    
    """
    if isinstance(cleanStart, Failed): return Failed(result=cleanStart)
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
                            # Salve o arquivo baixado, sua extensão e ano referente para tratamento posterior
                            content_type = response.headers.get('Content-Type', '')
                            if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
                                'text/csv' in content_type or \
                                'application/vnd.ms-excel' in content_type:
                                file_extension = 'xlsx' if 'spreadsheetml.sheet' in content_type else 'csv'
                                rawData['rawData'] = {'content': response.content, 'type': file_extension, 'year': year}
                                break
                            else:
                                raise ValueError('Formato do arquivo cru fora do esperado (.csv, .xlsx).')
                        else:
                            log(f"Tentativa {attempt + 1}: Falha ao baixar dados referentes à {monthText}/{year}. Status code: {response.status_code}")
                            if attempt == 2:
                                error = f"Falha ao baixar dados referentes à {monthText}/{year} após tentativa(s) de recaptura. Status code: {response.status_code}"
                                log_and_propagate_error(error, rawData)
    except Exception as e:
        error = f"Falha ao baixar os dados crus mais recentes de {URL}. {e}"
        log_and_propagate_error(error, rawData)

    if 'errors' in rawData: return Failed(result=rawData)
    log(f'Dados referentes ao mês de {monthText} baixados com sucesso!')
    return rawData

@task
def save_raw_data_locally(rawData: dict) -> dict:
    """
    Salva os dados crus localmente.

    Args:
        dict: Dicionário contendo chaves-valores:
                'rawData': Dicionário contendo chaves-valores:
                    'content': Conteúdo do arquivo (bytes),
                    'type': Extensão do arquivo (.csv, .xlsx),
                    'year': Ano do arquivo para particionamento.,
                ?'error': Possíveis erros propagados (string)    
    Returns:
        dict: Dicionário contendo chaves-valores:
                'rawFilePaths': caminhos dos arquivos locais salvos (list de strings),
                ?'error': Possíveis erros propagados (string)
    """
    if isinstance(rawData, Failed): return Failed(result=rawData)
    rawFilePaths = { 'rawFilePaths': [] }

    # Crie os diretórios no padrão de particionamento por ano
    try:
        DB_NAME = os.getenv("DB_NAME")
        for _key, content in rawData.items(): 
            download_dir = os.path.join(f'{DB_NAME}_local/', f"year={content['year']}/")
            os.makedirs(download_dir, exist_ok=True)
            filePath = os.path.join(download_dir, f"raw_data.{content['type']}".lower())
        log(f'Diretótio para armazenar localmente os dados crus {filePath} criado com sucesso!')
    except Exception as e:
        error = f"Falha ao criar diretótios locais para armazenar os dados crus. {e}"
        log_and_propagate_error(error, rawFilePaths)

    # Salve localmente os dados baixados
    try:
        with open(filePath, 'wb') as file:
            file.write(content['content'])
    except Exception as e:
        error = f"Falha ao salvar os dados crus localmente. {e}"
        log_and_propagate_error(error, rawFilePaths)

    if 'errors' in rawFilePaths: return Failed(result=rawFilePaths)
    log(f"Dados salvos localmente em {filePath} com sucesso!")
    rawFilePaths['rawFilePaths'].append(filePath)
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
    if isinstance(rawFilePaths, Failed): return Failed(result=rawFilePaths)
    parsedData = {}

    for rawfilePath in rawFilePaths['rawFilePaths']: 
        parsedData[rawfilePath] = {}

        # Determine o tipo do arquivo RAW local e leia o conteúdo
        if rawfilePath.endswith('.xlsx'):
            try:
                df = pd.read_excel(rawfilePath, engine='openpyxl')
                log("Dados crus .xlsx convertidos em DataFrames com sucesso!")
                parsedData[rawfilePath]['dataframe'] = df
            except Exception as e:
                error = f"Falha ao interpretar como .xlsx os dados crus {rawfilePath}: {e}"
                log_and_propagate_error(error, parsedData)

        elif rawfilePath.endswith('.csv'):
            try:
                df = pd.read_csv(rawfilePath)
                log("Dados crus .csv convertidos em DataFrames com sucesso!")
                parsedData[rawfilePath]['dataframe'] = df
            except Exception as e:
                error = f"Falha ao interpretar como .csv os dados crus {rawfilePath}: {e}"
                log_and_propagate_error(error, parsedData)
        else:
            raise ValueError('Formato de arquivo cru fora do esperado (.csv, .xlsx).')

    if 'errors' in parsedData: return Failed(result=parsedData)
    log(f"Dados interpretados localmente em como DataFrame com sucesso!")
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
                'parsedFilePaths': [caminhos para CSV locais (strings)],
                ?'error': Possíveis erros propagados (string)
    """
    if isinstance(parsedData, Failed): return Failed(result=parsedData)
    parsedFilePaths = { 'parsedFilePaths': [] }
    try:
        for rawFilePath, data in parsedData.items(): 
            parsedFilePath = f'{rawFilePath}_parsed.csv'.lower()
            data['dataframe'].to_csv(parsedFilePath, index=False)
    except Exception as e:
        error = f"Falha ao salvar dados tratados localmente como .csv {rawFilePath}: {e}"
        log_and_propagate_error(error, parsedData)

    if 'error' in parsedFilePaths: return Failed(result=parsedFilePaths)
    log(f"Dados tratados em CSV salvos localmente em {parsedFilePath} com sucesso!")
    parsedFilePaths['parsedFilePaths'].append(parsedFilePath)
    return parsedFilePaths

@task
def upload_csv_to_database(parsedFilePaths: dict, tableName: str) -> dict:
    """
    Faz o upload dos arquivos tratados, localizados em parsedFilePaths,
        para a tabela tableName no banco de dados PostgreSQL.

    Args:
        dict: Dicionário contendo chaves-valores:
                'parsedFilePaths': [Caminhos para CSV locais (strings)],
                ?'error': Possíveis erros propagados (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': [Nome das tabelas atualizadas no banco de dados (strings)],
                ?'error': Possíveis erros propagados (string)
    """
    if isinstance(parsedFilePaths, Failed): return Failed(result=parsedFilePaths)
    status = {'tables': [] }

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
                )""").format(
                table= sql.Identifier(tableName),
                columns=sql.SQL(', ').join([
                    sql.SQL('{} {}').format(
                        sql.Identifier(col), sql.SQL('TEXT')
                    ) for col in df.columns
                ])
            )
            cur.execute(createTableQuery)
            conn.commit()
            log(f"Tabela {tableName} criada no PostgresSQL com sucesso!")
        except Exception as e:
            error = f"Falha ao criar tabela {tableName} no PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            conn.rollback()
            cur.close()
            conn.close()
            return status

        # Insere os dados tratados na tabela tableName
        try:
            for _index, row in df.iterrows():
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
        except Exception as e:
            error = f"Falha ao inserir dados do arquivo {parsedFile} na tabela {tableName} no PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            conn.rollback()
            cur.close()
            conn.close()
            return status
        
    cur.close()
    conn.close()

    if 'error' in status: return Failed(result=status)
    log(f"Feito upload de dados do arquivo {parsedFile} no PostgresSQL com sucesso!")
    status['tables'].append(tableName)
    return status

@task(trigger=all_finished)
def upload_logs_to_database(status: dict, logFilePath: str, tableName: str) -> dict:
    """
    Faz o upload dos logs da pipeline para o PostgresSQL.

    Args:
        status: Dicionário contendo chaves-valores:
                'tables': [Nome das tabelas atualizadas no banco de dados (strings)],
                ?'error': Possíveis erros propagados (string)
        logFilePath: Caminho para o arquivo de log (string)
        tableName: Nome da tabela de log no PostgresSQL (string)    
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': Nome das tabelas atualizadas no banco de dados,
                ?'error': Possíveis erros propagados (string)
    """
    logStatus = { 'tables': [] }
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
        log_and_propagate_error(error, logStatus)
        conn.rollback()
        cur.close()
        conn.close()
        return logStatus

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
        log_and_propagate_error(error, logStatus)
        conn.rollback()
        cur.close()
        conn.close()
        return logStatus
    
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
        log_and_propagate_error(error, logStatus)
        conn.rollback()
        cur.close()
        conn.close()
        return logStatus
    
    if "error" in logStatus: return Failed(result=logStatus)
    log(f"Feito upload de logs do arquivo {logFilePath} na tabela {tableName} PostgresSQL com sucesso!")
    logStatus['tables'].append(tableName)
    return logStatus
        
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


@task
def setup_log_file(logFilePath: str) -> dict:
    """
    Configura o arquivo de log.
    Args:
        logFilePath: Caminho para o arquivo de log (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'logFilePath': Caminho para o arquivo de log (string),
                ?'error': Possíveis erros propagados (string)
    """
    logs = {}

    try:
        logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s - %(name)s | %(message)s',
                    handlers=[logging.FileHandler(logFilePath)])
    except Exception as e:
        error = f"Falha na configuração do arquivo de log {logFilePath}: {e}"
        log_and_propagate_error(error, logs)
    
    if "error" in logs: return Failed(result=logs)
    log(f'Configuração do arquivo de log {logFilePath} realizada com sucesso.')
    logs['logFilePath'] = logFilePath
    return logs

@task
def clean_log_file(logFilePath: dict) -> dict:
    """
    Limpa o arquivo de log.
    Args:
        logFilePath: Dicionário contendo chaves-valores:
                        'logFilePath': Caminho para o arquivo de log (string),
                        ?'error': Possíveis erros propagados (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'logFilePath': Caminho para o arquivo de log (string),
                ?'error': Possíveis erros propagados (string)
    """
    
    if isinstance(logFilePath, Failed): return Failed(result=logFilePath)
    cleanStart = {}

    try:
        path = logFilePath['logFilePath']
        with open(path, 'w') as file:
            pass
    except Exception as e:
        error = f"Falha na limpeza do arquivo de log local {path}: {e}"
        log_and_propagate_error(error, cleanStart)

    if "error" in cleanStart: return Failed(result=cleanStart)
    log(f'Limpeza do arquivo de log local {path} realizada com sucesso.')
    cleanStart['logFilePath'] = path
    return cleanStart

@task
def run_dbt_test():
    dbtResult = {}
    originalDir = os.getcwd()
    DB_NAME = os.getenv("DB_NAME")

    dbtDir = "/dbt"
    try:
        os.chdir(f'{originalDir}/{dbtDir}')
        result = subprocess.run(["dbt", "run"
        , "--vars", f"database: {DB_NAME}"], capture_output=True, text=True)
        log(result)
        if result.returncode != 0:
            raise Exception(result.stderr)
    except Exception as e:
        error = f"Falha na transformação (DBT): {e}"
        log_and_propagate_error(error, dbtResult)
    finally:
        os.chdir(originalDir)
    
    if "error" in dbtResult: return Failed(result=dbtResult)
    log(f'Transformação realizada com sucesso.')
    dbtResult['result'] = result
    return dbtResult
