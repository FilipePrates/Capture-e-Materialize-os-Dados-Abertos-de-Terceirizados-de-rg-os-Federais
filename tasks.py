# as funções que serão utilizadas no Flow.

from io import BytesIO
import os
import pandas as pd
from prefect import task
import requests
import pandas as pd
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

# import schedule
from bs4 import BeautifulSoup

from utils import log

# URL da página que contém os Dados
URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"

# Possíveis valores de texto dos links com Dados.
# months = ["Janeiro", "Maio"]
months = ["Janeiro", "Maio", "Setembro"]
# months = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

@task
def download_data() -> str:
    """
    Baixa dados de terceirizados da Controladoria Geral da União
      https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados
    e retorna um texto em formato CSV.

    Returns:
        dict: Dicionário com chaves sendo os meses do ano, e valores sendo bytes contendo o conteúdo baixado.
    """
    CURRENT_YEAR = datetime.now().year
    DOWNLOAD_DIR = f"downloads/year={CURRENT_YEAR}"
    # Garante que o diretório local para que armazenará os dados existe, se não, o cria.
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    files = {}
    
    # Clique nos links para download de cada arquivo disponível no site (HOW TO ONLY GET NEW DATA?)
    for month in months:
        link = soup.find('a', text=month)
        if link:
            file_url = link['href']
            print(file_url)
            response = requests.get(file_url)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type:
                    files[month] = {'content': response.content, 'type': 'xlsx'}
                    log(f'Dados referentes ao mês {month} em formato .xlsx baixados com sucesso!')
                elif 'text/csv' in content_type or 'application/vnd.ms-excel' in content_type:
                    files[month] = {'content': response.content, 'type': 'csv'}
                    log(f'Dados referentes ao mês {month} em formato .csv baixados com sucesso!')
                else:
                    log(f"Dados fora do tipo esperado (.xlsx ou .csv) para mês {month}: {content_type}")
            else:
                log(f"Falha ao baixar dados refêrentes à {month} de {CURRENT_YEAR}.") 
    return files

@task
def parse_data(files: dict) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas, para facilitar sua manipulação.

    Args:
        data (fict): Dicionário com chaves sendo os meses do ano, e valores sendo bytes conténdo o conteúdo baixado.

    Returns:
        dict: Dicionário com chaves sendo os meses do ano, e valores sendo pd.DataFrames com o conteudo referente ao mês da chave.
    """
    parsed_data = {}

    for month, file_info in files.items():
        try:
            if not file_info['content']:
                print(f"No content found for {month}")
                continue

            if file_info['type'] == 'xlsx':
                df = pd.read_excel(BytesIO(file_info['content']), engine='openpyxl')
            elif file_info['type'] == 'csv':
                try:
                    df = pd.read_csv(BytesIO(file_info['content']))
                except pd.errors.ParserError as e:
                    log(f"Falha ao tratar os dados referentes ao mês {month}: {e}")
                    # Tentar ler os dados com outra configuração possível de arquivos CSV
                    try:
                        df = pd.read_csv(BytesIO(file_info['content']), delimiter=';', error_bad_lines=False)
                    except Exception as e2:
                        log(f"Falha na segunda tentativa de tratar os dados referentes ao mês {month}: {e2}")
                        continue

            parsed_data[month] = df

        except Exception as e:
            log(f"Falha ao tratar os dados referentes ao mês {month}: {e}")

    log("Dados convertidos em DataFrames com sucesso!")
    return parsed_data

@task
def save_as_csv_locally(parsedData: dict) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        parsedData (dict): Dicionário com chaves sendo os meses do ano, e valores sendo pd.DataFrames com o conteudo referente ao mês da chave.
    
    Returns:
        list: Lista contendo os nomes dos arquivos CSV locais, já tratados.

    """
    CURRENT_YEAR = datetime.now().year
    DOWNLOAD_DIR = f"downloads/year={CURRENT_YEAR}"
    saved_files = []

    for month, dataframe in parsedData.items(): 
        filename = f"{DOWNLOAD_DIR}/month={month}.csv"
        dataframe.to_csv(filename, index=False)
        saved_files.append(filename)
    log(f"Dados salvos localmente em {DOWNLOAD_DIR}/month={month}.csv com sucesso!")
    return saved_files

@task
def upload_csv_to_database(files: list) -> None:
    """
    Faz o Upload dos arquivos CSV para o banco de dados PostgreSQL.

    Args:
        files (list): Lista contendo os nomes dos arquivos CSV locais, já tratados.
    """
# PostgreSQL connection details from environment variables
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()

    for file in files:
        try:
            df = pd.read_csv(file)
            table_name = os.path.splitext(os.path.basename(file))[0]
            create_table_query = psycopg2.sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {columns}
                )
            """).format(
                table=psycopg2.sql.Identifier(table_name),
                columns=psycopg2.sql.SQL(', ').join([
                    psycopg2.sql.SQL('{} {}').format(
                        psycopg2.sql.Identifier(col), psycopg2.sql.SQL('TEXT')
                    ) for col in df.columns
                ])
            )
            cur.execute(create_table_query)
            conn.commit()

            # Inserting data
            for index, row in df.iterrows():
                insert_query = psycopg2.sql.SQL("""
                    INSERT INTO {table} ({fields})
                    VALUES ({values})
                """).format(
                    table=psycopg2.sql.Identifier(table_name),
                    fields=psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, df.columns)),
                    values=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(df.columns))
                )
                cur.execute(insert_query, list(row))

            conn.commit()
        except Exception as e:
            log(f"Falha no upload do arquivo {file} para o PostgreSQL: {e}")
            conn.rollback()

    cur.close()
    conn.close()