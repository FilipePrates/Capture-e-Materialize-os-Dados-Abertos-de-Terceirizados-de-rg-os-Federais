"""
Modulo com as Tarefas (@tasks) para os Flows
"""
import os
import psycopg2
import logging
import requests
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
    log,
    log_and_fail_task,
    download_file,
    create_table,
    create_log_table,
    clean_table,
    insert_data,
    insert_log_data,
    connect_to_postgresql,
    get_most_recent_raw_data_download_links,
    get_historic_raw_data_download_links,
    determine_dbt_models_to_run
)
load_dotenv()

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

    # Salve os logs do prefect e utils.log() no arquivo .txt em logFilePath
    try:
        logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s - %(name)s | %(message)s',
                    handlers=[logging.FileHandler(logFilePath)])
    except Exception as e:
        error = f"Falha na configuração do arquivo de log {logFilePath}: {e}"
        log_and_fail_task(error, logs)
    
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
        log(f' <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> ')
    except Exception as e:
        error = f"Falha na limpeza do arquivo de log local {path}: {e}"
        log_and_fail_task(error, cleanStart)

    if "error" in cleanStart: return Failed(result=cleanStart)
    log(f'Limpeza do arquivo de log local {path} realizada com sucesso.')
    cleanStart['logFilePath'] = path
    return cleanStart

@task
def download_cgu_terceirizados_data(cleanStart: dict, historic: bool = False) -> dict:
    """
    Baixa os Dados Abertos dos Terceirizados de Órgãos Federais,
    disponibilizado pela Controladoria Geral da União.

    Args:
        cleanStart (dict): Dicionário contendo chaves-valores:
            'logFilePath': caminho dos arquivo local de log (string),
            ?'error': Possíveis erros propagados (string)
        historic (bool): Flag para definir se deve baixar todos os dados históricos (True)
            ou apenas os dados mais recentes (False). Default é False.
    Returns:
        dict: Dicionário contendo chaves-valores:
            'rawData': Lista contendo dicionários com chaves-valores:
                'content': Conteúdo do arquivo (bytes),
                'type': Extensão do arquivo (.csv, .xlsx),
                'year': Ano do arquivo para particionamento,
            ?'error': Possíveis erros propagados (string)
    """
    if isinstance(cleanStart, Failed):
        return Failed(result=cleanStart)
    rawData = { 'rawData' : [] }

    try:
        # Acesse as variáveis de ambiente em busca da URL atualizada do portal da CGU
        URL = os.getenv("URL_FOR_DATA_DOWNLOAD")
        DOWNLOAD_ATTEMPTS = int(os.getenv("DOWNLOAD_ATTEMPTS"))
    except Exception as e:
        error = f"Falha ao acessar variáveis de ambiente. {e}"
        log_and_fail_task(error, rawData)

    try:
        # Função para capturar links de download através do portal de dados públicos da CGU
        rawDataLinks = []
        response = requests.get(URL)
        soup = BeautifulSoup(response.content, 'html.parser')
        if historic:
            get_historic_raw_data_download_links(soup,rawDataLinks)
        else:
            get_most_recent_raw_data_download_links(soup,rawDataLinks)   
        if len(rawDataLinks) == 0:
            raise ValueError('Zero link de download encontrado!')
        log(f"{len(rawDataLinks)} links para dados brutos capturados do portal de Dados Abertos da Controladoria Geral da União com sucesso!")
    except Exception as e:
        error = f"""Falha ao capturar links para dados brutos do portal de Dados Abertos da Controladoria Geral da União {URL}.\n
                    Possível mudança de layout. {e}"""
        log_and_fail_task(error, rawData)

    for rawDataFile in rawDataLinks:
        try:
            # Baixe o arquivo no link e armazene na memória principal
            log(f"Realizando o download do arquivo... \n {rawDataFile['file_url']}")
            content, file_extension = download_file(rawDataFile['file_url'],
                                                   DOWNLOAD_ATTEMPTS,
                                                     rawDataFile['monthText'],
                                                       rawDataFile['year'])
            rawData['rawData'].append({
                'content': content,
                'type': file_extension,
                'year': rawDataFile['year']
            })
        except Exception as e:
            error = f"Falha ao baixar os dados. Foram realizadas {DOWNLOAD_ATTEMPTS} tentativas. {e}"
            log_and_fail_task(error, rawData)

    if 'errors' in rawData:
        return Failed(result=rawData)
    log(f'Dados referentes a {len(rawData.get("rawData", []))} meses baixados com sucesso!')
    return rawData

@task
def save_raw_data_locally(rawData: dict) -> dict:
    """
    Salva os dados brutos localmente em adm_cgu_terceirizados_local/ particionado por ano.

    Args:
        rawData (dict): Dicionário contendo chaves-valores:
            'rawData': Lista de dicionários contendo chaves-valores:
                'content': Conteúdo do arquivo (bytes),
                'type': Extensão do arquivo (.csv, .xlsx),
                'year': Ano do arquivo para particionamento.
            ?'error': Possíveis erros propagados (string)
    Returns:
        dict: Dicionário contendo chaves-valores:
            'rawFilePaths': Caminhos dos arquivos locais salvos (list de strings),
            ?'error': Possíveis erros propagados (string)
    """
    if isinstance(rawData, Failed):
        return Failed(result=rawData)
    rawFilePaths = {'rawFilePaths': []}

    # Crie os diretórios no padrão de particionamento por ano
    try:
        for i, content in enumerate(rawData['rawData']):
            if len(content['year']) != 4 :
                log(f"Dados de ano inválido {content['year']}! - Ignorando.")
                continue
            download_dir = os.path.join('adm_cgu_terceirizados_local', f"year={content['year']}")
            os.makedirs(download_dir, exist_ok=True)
            filePath = os.path.join(download_dir, f"raw_data_{i}.{content['type']}".lower())
            rawFilePaths['rawFilePaths'].append(filePath)
    except Exception as e:
        error = f"Falha ao criar diretórios locais para armazenar os dados brutos. {e}"
        log_and_fail_task(error, rawFilePaths)
    log(f'Arquivos e diretórios para armazenar localmente os dados brutos criados com sucesso!')

    # Salve localmente os dados baixados
    try:
        for index, content in enumerate(rawData['rawData']):
            with open(rawFilePaths['rawFilePaths'][index], 'wb') as file:
                file.write(content['content'])
            log(f"Dados salvos localmente em {rawFilePaths['rawFilePaths'][index]} com sucesso!")
    except Exception as e:
        error = f"Falha ao salvar os dados brutos localmente. {e}"
        log_and_fail_task(error, rawFilePaths)
    
    if 'error' in rawFilePaths:
        return Failed(result=rawFilePaths)
    log("Dados salvos localmente com sucesso!")
    log(rawFilePaths)
    return rawFilePaths

@task
def parse_data_into_dataframes(rawFilePaths: dict, lenient: bool) -> dict:
    """
    Transforma os dados brutos em um DataFrame.

    Args:
        rawFilePaths (dict): Dicionário contendo chaves-valores:
            'rawFilePaths': Caminhos dos arquivos locais salvos (list de strings),
            ?'error': Possíveis erros propagados (string),
        lenient (bool): Indica se Falha a Tarefa caso um dos arquivos falhe
    Returns:
        dict: Dicionário com chaves sendo os caminhos dos arquivos locais brutos, e valores
            sendo dicionários contendo chaves-valores:
            'dataframe': pd.DataFrame,
            ?'error': Possíveis erros propagados (string)
    """
    if isinstance(rawFilePaths, Failed):
        return Failed(result=rawFilePaths)
    
    parsedData = {}

    for rawfilePath in rawFilePaths['rawFilePaths']:
        parsedData[rawfilePath] = {}
        if rawfilePath.endswith('.xlsx'):
            try:
                df = pd.read_excel(rawfilePath, engine='openpyxl')
                log(f"dados brutos {rawfilePath} convertidos em pd.DataFrame com sucesso!")
                parsedData[rawfilePath]['content'] = df
            except Exception as e:
                error = f"Falha ao interpretar {rawfilePath} como pd.DataFrame: {e} \nArquivo {rawfilePath} corrompido."
                if lenient:
                        log(error)
                        del parsedData[rawfilePath]
                        continue
                else:
                    log_and_fail_task(error)
                    return parsedData

        elif rawfilePath.endswith('.csv'):
            try:
                df = pd.read_csv(rawfilePath)
                log(f"dados brutos {rawfilePath} convertidos em pd.DataFrame com sucesso!")
                parsedData[rawfilePath]['content'] = df
            except Exception as e:
                try:
                    df = pd.read_csv(rawfilePath, delimiter=';')
                    log(f"dados brutos {rawfilePath} convertidos em pd.DataFrame com sucesso!")
                    parsedData[rawfilePath]['content'] = df
                except Exception as e:
                    error = f"Falha ao interpretar {rawfilePath} como pd.DataFrame: {e} \nArquivo {rawfilePath} corrompido."
                    if lenient:
                            log(error)
                            del parsedData[rawfilePath]
                            continue
                    else:
                        log_and_fail_task(error)
                        return parsedData
        else:
            log(f"Formato de arquivo bruto fora do esperado (.csv, .xlsx) para o arquivo {rawfilePath}.")
            del parsedData[rawfilePath]
            continue

    log("Dados interpretados localmente como DataFrame com sucesso!")
    return parsedData

@task
def save_data_as_csv_locally(parsedData: dict, lenient: bool) -> dict:
    """
    Salva DataFrames em um arquivo CSV local.

    Args:
        parsedData (dict): Dicionário com chaves sendo os caminhos dos arquivos locais brutos, e valores
          sendo dicionários contendo chaves-valores:
                'content': Dados (pd.DataFrame),
                ?'error': Possíveis erros propagados (string),
        lenient (bool): Indica se Falha a Tarefa caso um dos arquivos falhe
    Returns:
        dict: Dicionário contendo chaves-valores:
                'parsedFilePaths': [caminhos para CSV locais (strings)],
                ?'error': Possíveis erros propagados (string)
    """
    if isinstance(parsedData, Failed): return Failed(result=parsedData)
    parsedFilePaths = { 'parsedFilePaths': [] }

    # Para cada arquivo:
    for rawFilePath, data in parsedData.items(): 
        # Salve-o como .csv
        try:
            if 'content' not in data.keys(): raise ValueError(f'{parsedFilePath} sem contúdo para salvar!')
            parsedFilePath = f'{rawFilePath}_parsed.csv'.lower()
            data['content'].to_csv(parsedFilePath, index=False)
            parsedFilePaths['parsedFilePaths'].append(parsedFilePath)
        except Exception as e:
            error = f"Falha ao salvar localmente {parsedFilePath} como CSV: {e} \n Arquivo {parsedFilePath} corrompido."
            if lenient:
                    log(error)
                    continue
            else:
                log_and_fail_task(error)
                return parsedFilePaths


    if 'error' in parsedFilePaths: return Failed(result=parsedFilePaths)
    log(f"Dados tratados em CSV salvos localmente com sucesso!")
    return parsedFilePaths

@task
def upload_csv_to_database(parsedFilePaths: dict, tableName: str, lenient: bool) -> dict:
    """
    Faz o upload dos arquivos tratados, localizados em parsedFilePaths,
        para a tabela tableName no banco de dados PostgreSQL.

    Args:
        parsedFilePaths (dict): Dicionário contendo chaves-valores:
                'parsedFilePaths': [Caminhos para CSV locais (strings)],
                ?'error': Possíveis erros propagados (string)
        tableName (string): Nome da tabela no PostgreSQL para receber os dados,
        lenient (bool): Indica se Falha a Tarefa caso um dos arquivos falhe no upload.
    Returns:
        dict: Dicionário contendo chaves-valores:
                'inserts': [Dicionário contendo chaves-valores]:
                    'tableName': Nome da tabela no PostgreSQL que recebeu os dados (string),
                    'localFilePath': Caminho do arquivo inserido (string),
                ?'error': Possíveis erros propagados (string)
    """
    if isinstance(parsedFilePaths, Failed): return Failed(result=parsedFilePaths)
    status = {'inserts': [], 'totalInsertedLines': 0 }

    # Conecte com o PostgreSQL
    try:
        conn, cur = connect_to_postgresql()
        log(f"Conectado com PostgreSQL com sucesso!")
    except Exception as e:
        error = f"Falha ao conectar com o PostgreSQL: {e}"
        conn.rollback(); cur.close(); conn.close()
        log_and_fail_task(error, status)
        return status
    
    # Crie a tabela tableName no PostgreSQL, caso não exista
    try:
        df = pd.read_csv(parsedFilePaths['parsedFilePaths'][0]) # Padrão de colunas escolhido do primeiro arquivo (mais recente)
        create_table(cur, conn, df, tableName)
        log(f"Tabela {tableName} criada no PostgreSQL com sucesso!")
    except Failed as e:
        error = f"Falha ao criar tabela {tableName} no PostgreSQL: {e}"
        conn.rollback(); cur.close(); conn.close()
        log_and_fail_task(error)
        return status
    
    # Limpe a tabela
    try:
        clean_table(cur, conn, tableName)
        log(f"Tabela {tableName} limpa no PostgreSQL com sucesso!")
    except Exception as e:
        error = f"Falha ao limpar tabela {tableName} no PostgreSQL: {e}"
        conn.rollback(); cur.close(); conn.close()
        log_and_fail_task(error)
        return status

    # Para cada arquivo:
    for fileNumber, parsedFilePath in enumerate(parsedFilePaths['parsedFilePaths'], start=1):
        # Leia-o
        try: 
            df = pd.read_csv(parsedFilePath) # low_memory=False
        except Exception as e:
            error = f"Falha ao ler o arquivo {parsedFilePath}: {e} \n Arquivo {parsedFilePath} corrompido."
            if lenient:
                log(error)
                continue
            else:
                conn.rollback(); cur.close(); conn.close()
                log_and_fail_task(error)
                return status
            
        # Insira seus dados na tabela tableName
        try:
            log(f"Inserindo {df.shape[0]} novas linhas do arquivo {parsedFilePath} em {tableName}...")
            insert_data(cur, conn, df, tableName)
            status['inserts'].append({'tableName':tableName, 'localFilePath': parsedFilePath })
            status['totalInsertedLines'] += df.shape[0]
            log(f"Dados inseridos em {tableName} com sucesso!")
        except Exception as e:
            conn.rollback(); cur.close(); conn.close()
            error = f"Falha ao inserir dados do arquivo {parsedFilePath} na tabela {tableName} no PostgreSQL: {e}"
            if lenient:
                log(error)
                continue
            else:
                conn.rollback(); cur.close(); conn.close()
                log_and_fail_task(error)
                return status

        
    cur.close(); conn.close()
    if 'error' in status: return Failed(result=status)
    log(f"Feito upload de dados de {status['totalInsertedLines']} linha(s) de {fileNumber} arquivo(s) na tabela {tableName} no PostgreSQL com sucesso!")
    return status

@task(trigger=all_finished)
def upload_logs_to_database(status: dict, logFilePath: str, tableName: str) -> dict:
    """
    Faz o upload dos logs da pipeline para o PostgreSQL.        

    Args:
    status (dict): Dicionário contendo chaves-valores:
                'inserts': [Dicionário contendo chaves-valores]:
                    'tableName': Nome da tabela no PostgreSQL que recebeu os dados (string),
                    'localFilePath': Caminho do arquivo inserido (string),
                ?'error': Possíveis erros propagados (string),
    logFilePath: Caminho do arquivo de log do Flow  (string),
    tableName: Nome da tabela no PostgreSQL que recebeu os dados (string)
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
        log_and_fail_task(error, status)
        conn.rollback(); cur.close(); conn.close()
        return status

    try:
        # Crie a tabela de logs tableName no PostgreSQL, caso não exista
        create_log_table(cur, conn, tableName)
        log(f"Tabela de logs {tableName} criada no PostgreSQL com sucesso!")
    except Exception as e:
        error = f"Falha ao criar tabela de {tableName} no PostgreSQL: {e}"
        log_and_fail_task(error, logStatus)
        conn.rollback(); cur.close(); conn.close()
        return logStatus
    
    try:
        log(f' <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> ')
        # Leia o arquivo de log e insira os registros na tabela
        insert_log_data(conn,cur,tableName,logFilePath)
        log(f"Dados de logs do Flow inseridos em {tableName} com sucesso!")
    except Exception as e:
        error = f"Falha ao inserir logs na tabela de {tableName} no PostgreSQL: {e}"
        log_and_fail_task(error, logStatus)
        conn.rollback(); cur.close(); conn.close()
        return logStatus
    
    if "error" in logStatus: return Failed(result=logStatus)
    log(f"Feito upload do arquivo de log local {logFilePath} na tabela {tableName} PostgreSQL com sucesso!")
    logStatus['tables'].append(tableName)
    return logStatus

@task 
def run_dbt(cleanStart: dict, historic: bool, publish:bool = False) -> dict:
    """
    Realiza transformações com DBT no schema staging do PostgreSQL:
    Args:
        cleanStart: Dicionário contendo chaves-valores:
                'logFilePath': Caminho para o arquivo de log (string),
                ?'error': Possíveis erros propagados (string)
        historic (bool): Flag para definir se deve baixar todos os dados históricos (True)
            ou apenas os dados mais recentes (False).
        publish (bool): Flag para definir se deve executar transformação para marts (True)
            ou apenas staging (False). Padrão False.
    Returns:
        dict: Dicionário contendo chaves-valores:
                'tables': Nome das tabelas atualizadas no banco de dados,
                ?'error': Possíveis erros propagados (string)
    """
    if isinstance(cleanStart, Failed):
        return Failed(result=cleanStart)
    dbtResult = {}
    models_to_run = determine_dbt_models_to_run(historic, publish)

    try:
        # Acesse as variáveis de ambiente
        originalDir = os.getcwd()
        dbtDir = "/dbt"
        DB_NAME = os.getenv("DB_NAME")
    except Exception as e:
        error = f"Falha ao acessar variáveis de ambiente. {e}"
        log_and_fail_task(error, dbtResult)
        return Failed(result=dbtResult)

    try:
        os.chdir(f'{originalDir}/{dbtDir}')
        result = subprocess.run([
            "dbt", "run", 
            "--vars", f"database: {DB_NAME}",
            "--select", models_to_run,
        ], capture_output=True, text=True, bufsize=1)
        
        if result.returncode != 0:
            raise Exception(result.stderr)
        dbtResult['result'] = result
    except Exception as e:
        error = f"Falha na transformação (DBT): {e} {result}"
        log_and_fail_task(error, dbtResult)
        return Failed(result=dbtResult)
    finally:
        os.chdir(originalDir)
    
    log(f'Transformação realizada com sucesso. {result.stdout}')
    return dbtResult


def check_flow_state(capture_flow_state):
    # Not working
    # if capture_flow_state.is_successful():
    #     return "success"
    # return "retry"
    return "success"