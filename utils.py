import prefect
import psycopg2
import os

from prefect import Client
from prefect.engine.signals import FAIL
from prefect.engine.state import Failed
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from datetime import timedelta, datetime
from psycopg2 import sql

# as funções auxiliares que serão utilizadas nos Flows.

def log(message) -> None:
    """Ao ser chamada dentro de um Flow, realiza um log da message"""
    prefect.context.logger.info(f"\n{message}")

def log_and_propagate_error(message, returnObj) -> None:
    returnObj['error'] = message
    log(message)
    raise FAIL(result=returnObj)

def create_table(cur, conn, df, tableName, status):
    try:
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
        log(f"Tabela {tableName} criada no PostgresSQL com sucesso!")
    except Exception as e:
        error = f"Falha ao criar tabela {tableName} no PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        conn.rollback(); cur.close(); conn.close()
        raise Failed(error)

def insert_data(cur, conn, df, tableName, parsedFile, status):
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
        conn.rollback(); cur.close(); conn.close()
        raise Failed(error)

def clean_raw_table(cur, conn, tableName, status):
    try:
        cleanTableQuery = sql.SQL("""
            DELETE FROM {table}
        """).format(
            table=sql.Identifier(tableName)
        )
        cur.execute(cleanTableQuery)
        conn.commit()
        log(f"Tabela {tableName} limpa no PostgresSQL com sucesso!")
    except Exception as e:
        error = f"Falha ao limpar a tabela {tableName} no PostgreSQL: {e}"
        log_and_propagate_error(error, status)
        conn.rollback(); cur.close(); conn.close()
        raise Failed(error)
    
def connect_to_postgresql():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        error = f"Falha ao conectar com o PostgreSQL: {e}"
        raise Failed(error)

def cronograma_padrao_cgu_terceirizados():
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

