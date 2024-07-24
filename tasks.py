import psycopg2
import logging
import requests
import os
import subprocess
import pandas as pd
from prefect import task
from prefect.engine.signals import FAIL
from prefect.engine.state import Failed
from prefect.triggers import all_finished

from bs4 import BeautifulSoup
from psycopg2 import sql
from dotenv import load_dotenv
from utils import (
    download_file,
    get_file_extension,

    log,
    log_and_propagate_error,
    create_table,
    create_log_table,
    clean_table,
    insert_data,
    insert_log_data,
    connect_to_postgresql
)
load_dotenv()

# as Tarefas (@tasks) que serão utilizadas nos Flows.

@task
def setup_log_file(logFilePath: str) -> dict:
    """
    Configura o arquivo de log dos flows.

    Args:
        logFilePath: Caminho para o arquivo de log (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'logFilePath': Caminho para o arquivo de log (string),
                ?'error': Possíveis erros propagados (string)
    """
    logs = {}

    # Salve os logs do prefect + custom no arquivo .txt em logFilePath
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
    Limpa o arquivo de log para começar um novo flow.

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

    # Abra o arquivo logFilePath em modo escrita ('w'), apagando-o
    try:
        path = logFilePath['logFilePath']
        with open(path, 'w') as _file:
            pass
    except Exception as e:
        error = f"Falha na limpeza do arquivo de log local {path}: {e}"
        log_and_propagate_error(error, cleanStart)

    if "error" in cleanStart: return Failed(result=cleanStart)
    log(f'Limpeza do arquivo de log local {path} realizada com sucesso.')
    cleanStart['logFilePath'] = path
    return cleanStart

@task
def download_new_cgu_terceirizados_data(cleanStart: dict) -> dict:
    """
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
        # Acesse as variáveis de ambiente em busca da url atualizada do portal da CGU
        URL = os.getenv("URL_FOR_DATA_DOWNLOAD")
        DOWNLOAD_ATTEMPTS = int(os.getenv("DOWNLOAD_ATTEMPTS"))
    except Exception as e:
        error = f"Falha ao acessar variáveis de ambiente. {e}"
        log_and_propagate_error(error, rawData)

    try:
        # Capture o link de download através do portal de dados públicos da CGU
        def fetch_download_url_from_dados_abertos_cgu(url):
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            headers = soup.find_all('h3')
            if headers:
                header = headers[0]
                year = header.get_text()
                ul = header.find_next('ul')
                if ul:
                    links = ul.find_all('a')
                    if links:
                        link = links[0]
                        monthText = link.get_text()
                        file_url = link['href']
                        return file_url, year, monthText
            raise Exception("No valid download link found")
        file_url, year, monthText = fetch_download_url_from_dados_abertos_cgu(URL)
        log(f"Link {file_url} para dados crus capturado do portal de Dados Abertos da \Controladoria Geral da União com sucesso!")
    except Exception as e:
        error = f"Falha ao capturar link para dados crus capturado do portal de Dados Abertos da Controladoria Geral da União.\
                Possível mudança de layout. {e}"
        log_and_propagate_error(error, rawData)

    try:
        # Baixe o arquivo no link e armazene na memória principal
        content, content_type = download_file(file_url, DOWNLOAD_ATTEMPTS, monthText, year)
        rawData['rawData'] = {
            'content': content,
            'type': get_file_extension(content_type),
            'year': year
        }
    except Exception as e:
        error = f"Falha ao baixar os dados do link {file_url}. Foram realizadas {DOWNLOAD_ATTEMPTS} tentativas. {e}"
        log_and_propagate_error(error, rawData)
        
    if 'errors' in rawData: 
        return Failed(result=rawData)
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
        for _key, content in rawData.items(): 
            download_dir = os.path.join(f'adm_cgu_terceirizados_local/', f"year={content['year']}/")
            os.makedirs(download_dir, exist_ok=True)
            log(f'Diretório para armazenar localmente os dados crus {download_dir} criado com sucesso!')
            filePath = os.path.join(download_dir, f"raw_data.{content['type']}".lower())
            log(f'Arquivo para armazenar localmente os dados crus {filePath} criado com sucesso!')
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
        # Determine o tipo do arquivo cru e leia seu o conteúdo como pd.DataFrame
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
    log(f"Dados interpretados localmente como DataFrame com sucesso!")
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

    try:
        # Conecte com o PostgreSQL
        conn, cur = connect_to_postgresql()
        log(f"Conectado com PostgreSQL com sucesso!")
    except Exception as e:
        conn.rollback(); cur.close(); conn.close()
        error = f"Falha ao conectar com o PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        return status
    

    for parsedFile in parsedFilePaths['parsedFilePaths']:
        try: 
            # Leia o arquivo
            df = pd.read_csv(parsedFile) # low_memory=False
        except Exception as e:
            conn.rollback(); cur.close(); conn.close()
            error = f"Falha ao ler o arquivo {parsedFile}: {e}"
            log_and_propagate_error(error, status)
            return status
        
        try:
            # Crie a tabela tableName no PostgreSQL, caso não exista
            create_table(cur, conn, df, tableName)
            log(f"Tabela {tableName} criada no PostgreSQL com sucesso!")
        except Failed as e:
            conn.rollback(); cur.close(); conn.close()
            error = f"Falha ao criar tabela {tableName} no PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            return status

        try:
            # Limpe a tabela
            clean_table(cur, conn, tableName)
            log(f"Tabela {tableName} limpa no PostgreSQL com sucesso!")
        except Exception as e:
            conn.rollback(); cur.close(); conn.close()
            error = f"Falha ao limpar tabela {tableName} no PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            return status

        try:
            # Insere os dados tratados na tabela tableName
            log(f"Inserindo {df.shape[0]} linhas em {tableName}...")
            insert_data(cur, conn, df, tableName)
            log(f"Dados inseridos em {tableName} com sucesso!")
        except Exception as e:
            conn.rollback(); cur.close(); conn.close()
            error = f"Falha ao inserir dados do arquivo {parsedFile} na tabela {tableName} no PostgreSQL: {e}"
            log_and_propagate_error(error, status)
            return status
        
    cur.close(); conn.close()
    if 'error' in status: return Failed(result=status)
    log(f"Feito upload de dados do arquivo {parsedFile} no PostgreSQL com sucesso!")
    status['tables'].append(tableName)
    return status

@task(trigger=all_finished)
def upload_logs_to_database(status: dict, logFilePath: str, tableName: str) -> dict:
    """
    Faz o upload dos logs da pipeline para o PostgreSQL.

    Args:
        status: Dicionário contendo chaves-valores:
                'tables': [Nome das tabelas atualizadas no banco de dados (strings)],
                ?'error': Possíveis erros propagados (string)
        logFilePath: Caminho para o arquivo de log (string)
        tableName: Nome da tabela de log no PostgreSQL (string)    
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': Nome das tabelas atualizadas no banco de dados,
                ?'error': Possíveis erros propagados (string)
    """
    logStatus = { 'tables': [] }

    try:
        # Conecte com o PostgreSQL
        conn, cur = connect_to_postgresql()
        log(f"Conectado com PostgreSQL com sucesso!")
    except Exception as e:
        error = f"Falha ao conectar com o PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        conn.rollback(); cur.close(); conn.close()
        return status

    try:
        # Crie a tabela de logs tableName no PostgreSQL, caso não exista
        create_log_table(cur, conn, tableName)
        log(f"Tabela de logs {tableName} criada no PostgreSQL com sucesso!")
    except Exception as e:
        error = f"Falha ao criar tabela de {tableName} no PostgreSQL: {e}"
        log_and_propagate_error(error, logStatus)
        conn.rollback(); cur.close(); conn.close()
        return logStatus
    
    try:
        # Leia o arquivo de log e insira os registros na tabela
        insert_log_data(conn,cur,tableName,logFilePath)
        log(f"Dados de logs do Flow inseridos em {tableName} com sucesso!")
    except Exception as e:
        error = f"Falha ao inserir logs na tabela de {tableName} no PostgreSQL: {e}"
        log_and_propagate_error(error, logStatus)
        conn.rollback(); cur.close(); conn.close()
        return logStatus
    
    if "error" in logStatus: return Failed(result=logStatus)
    log(f"Feito upload do arquivo de log local {logFilePath} na tabela {tableName} PostgreSQL com sucesso!")
    logStatus['tables'].append(tableName)
    return logStatus
        
@task
def run_dbt(cleanStart: dict) -> dict:
    """
    Realiza transformações com DBT no schema staging do PostgreSQL:
        1: standard_null: Define padrão de variáveis Nulas por coluna,
        2: casted: Define tipos das colunas

    Args:
        cleanStart: Dicionário contendo chaves-valores:
                'logFilePath': Caminho para o arquivo de log (string),
                ?'error': Possíveis erros propagados (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': Nome das tabelas atualizadas no banco de dados,
                ?'error': Possíveis erros propagados (string)
    """
    if isinstance(cleanStart, Failed): return Failed(result=cleanStart)
    dbtResult = {}

    try:
        # Acesse as variáveis de ambiente
        originalDir = os.getcwd()
        dbtDir = "/dbt"
        DB_NAME = os.getenv("DB_NAME")
    except Exception as e:
        error = f"Falha ao acessar variáveis de ambiente. {e}"
        log_and_propagate_error(error, dbtResult)

    try:
        os.chdir(f'{originalDir}/{dbtDir}')
        result = subprocess.run(["dbt", "run"
        , "--vars", f"database: {DB_NAME}"], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(result.stderr)
    except Exception as e:
        error = f"Falha na transformação (DBT): {e} {result} "
        log_and_propagate_error(error, dbtResult)
    finally:
        os.chdir(originalDir)
    
    if "error" in dbtResult: return Failed(result=dbtResult)
    log(f'Transformação realizada com sucesso. {result.stdout}')
    dbtResult['result'] = result
    return dbtResult


# @task
# def download_all_available_data() -> dict:
    # """

@task
def scheduled_capture_completed(flowName: str) -> bool:
    """
    @task de Sucesso para Flow de Captura
    """
    print("Capture flow completed successfully, triggering materialize flow.")
    return True
