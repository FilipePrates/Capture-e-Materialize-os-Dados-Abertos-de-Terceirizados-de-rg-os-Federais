"""
Modulo com as funções auxiliares para os Flows
"""


import prefect
import psycopg2
import requests
import os

from prefect import Client
from prefect.engine.signals import FAIL
from prefect.engine.state import Failed
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.agent.local import LocalAgent

from bs4 import BeautifulSoup
from datetime import timedelta, datetime
from psycopg2 import sql
from prefect.agent.local import LocalAgent
# 

def log(message) -> None:
    """Ao ser chamada dentro de um Flow, realiza um log da message"""
    prefect.context.logger.info(f"\n <> {message}")

def log_and_propagate_error(message, returnObj) -> None:
    """Log e propaga falhas por erros"""
    returnObj['error'] = message
    log(message)
    raise FAIL(result=returnObj)

def start_agent():
    """Começa um Agente Prefect local no processo"""
    agent = LocalAgent()
    agent.start()

def connect_to_postgresql():
    """Conecta à base de dados PostgreSQL"""
    conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
    cur = conn.cursor()
    return conn, cur

def create_table(cur, conn, df, tableName):
    """Cria uma tabela tableName com as colunas do pd.DataFrame df no PostgreSQL"""
    createTableQuery = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            {columns}
        )""").format(
        table=sql.Identifier(tableName),
        columns=sql.SQL(', ').join([
            sql.SQL('{} {}').format(
                sql.Identifier(col), sql.SQL('TEXT')
            ) for col in df.columns
        ])
    )
    cur.execute(createTableQuery)
    conn.commit()

def create_log_table(cur, conn, tableName):
    """Cria uma tabela tableName com colunas padrão de log no PostgreSQL"""
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            log_id SERIAL PRIMARY KEY,
            log_content TEXT,
            log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """).format(table=sql.Identifier(tableName))
    cur.execute(create_table_query)
    conn.commit()

def insert_data(cur, conn, df, tableName):
    """Insere os dados do pd.DataFrame df na tabela tableName no PostgreSQL"""
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

def insert_log_data(conn, cur, tableName, logFilePath):
    """Insere os dados do arquivo de log em logFilePath na tabela tableName no PostgreSQL"""
    with open(logFilePath, 'r') as file:
        for line in file:
            insert_log_query = sql.SQL("""
                INSERT INTO {table} (log_content)
                VALUES (%s)
            """).format(table=sql.Identifier(tableName))
            cur.execute(insert_log_query, [line])
    conn.commit()

def clean_table(cur, conn, tableName):
    """Insere os dados do arquivo de log em logFilePath na tabela tableName no PostgreSQL"""
    cleanTableQuery = sql.SQL("""
        DELETE FROM {table}
    """).format(
        table=sql.Identifier(tableName)
    )
    cur.execute(cleanTableQuery)
    conn.commit()

def download_file(file_url, attempts, monthText, year):
    for attempt in range(attempts):
        response = requests.get(file_url)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            return response.content, content_type
        else:
            log(f"""Tentativa {attempt +1}: Falha ao baixar dados referentes à {monthText}/{year}. \n
                Status code: {response.status_code}""")
    raise Exception(f"Falha ao baixar dados do link {file_url}.")

def get_file_extension(content_type):
    if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type:
        return 'xlsx'
    elif 'text/csv' in content_type or 'application/vnd.ms-excel' in content_type:
        return 'csv'
    else:
        raise ValueError('Formato do arquivo cru fora do esperado (.csv, .xlsx).')

def standard_schedule__adm_cgu_terceirizados():
    """Determina o cronograma padrão de disponibilização
    de novos dados da Controladoria Geral da União"""
    # client = Client()
    # flow_runs = client.get_flow_run_info()
    flow_runs = [] # Fake last success

    if not flow_runs:
        # Sem flows prévios, realize primeira captura
        return [CronClock(start_date=datetime.now(),
                           cron="0 0 1 */4 *")]

    last_run = flow_runs[0]
    if last_run['state'] == 'Success':
        # Se útimo flow teve sucesso, então programar próximo flow para daqui a 4 meses
        next_run_time = last_run['start_time'] + timedelta(days=4*30+1)
        return [CronClock(start_date=next_run_time.strftime("%M %H %d %m *"),
                          cron="0 0 1 */4 *")]
    else:
        # se obteve Fail tente novamente no dia seguinte (possivelmente dados novos ainda não disponíceis)
        next_run_time = last_run['start_time'] + timedelta(days=1)
        return [CronClock(start_date=next_run_time.strftime("%M %H %d %m *"),
                          cron="0 0 1 */4 *")]

