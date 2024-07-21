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
months = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

@task
def download_new_data() -> dict:
    """
    Baixa dados de terceirizados da Controladoria Geral da União do Ano atual
      https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados
    e retorna um texto em formato CSV.

    Returns:
        dict: Dicionário com chaves sendo o mês referente, e valores sendo um dicionário contendo o conteúdo baixado, e o tipo do arquivo.
    """
    CURRENT_YEAR = datetime.now().year
    
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    files = {}

    # Ache a lista com links para download de dados referente ao ano atual
    headers = soup.find_all('h3')
    for header in headers:
        if str(header.get_text()) == str(CURRENT_YEAR):
            ul = header.find_next('ul')
            if ul:
                links = ul.find_all('a')
                for link in links:

                    # Colete os meses disponíveis na lista
                    month_text = link.get_text()

                    # Baixe os dados contidos nos links de cada mês
                    if month_text in months:
                        file_url = link['href']

                        for attempt in range(2):  # Caso download falhe, tentativa de recaptura imediata
                            response = requests.get(file_url)
                            if response.status_code == 200:
                                log(f'Dados referentes ao mês {month_text} baixados com sucesso!')

                                content_type = response.headers.get('Content-Type', '')
                                if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
                                    'text/csv' in content_type or \
                                    'application/vnd.ms-excel' in content_type:

                                    # Salve o arquivo baixado, sua extensão e ano referente para tratamento posterior
                                    file_extension = 'xlsx' if 'spreadsheetml.sheet' in content_type else 'csv'
                                    files[month_text] = {'content': response.content, 'type': file_extension, 'year': year}
                                    break

                            else: # Caso download falhe, tentativa de recaptura imediata.
                                log(f"Tentativa {attempt + 1}: Falha ao baixar dados referentes à {month_text}/{year}. Status code: {response.status_code}")

                                if attempt == 1:
                                    log(f"Falha ao baixar dados referentes à {month_text}/{year} após tentativa(s) de recaptura.")

        return files

@task
def download_all_available_data() -> dict:
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
                            log(f'Dados referentes ao mês {month_text} do ano {year} baixados com sucesso!')

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
                                log(f"Falha ao baixar dados referentes à {month_text}/{year} após tentativa(s) de recaptura.")

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

    for dataId, file_info in files.items():
        try:
            if not file_info['content']:
                print(f"No content found for {dataId}")
                continue

            if file_info['type'] == 'xlsx':
                df = pd.read_excel(BytesIO(file_info['content']), engine='openpyxl')
            elif file_info['type'] == 'csv':
                try:
                    df = pd.read_csv(BytesIO(file_info['content']))
                except pd.errors.ParserError as e:
                    log(f"Falha ao tratar os dados referentes ao mês {dataId}: {e}")
                    # Tentar ler os dados com outra configuração possível de arquivos CSV
                    try:
                        df = pd.read_csv(BytesIO(file_info['content']), delimiter=';', error_bad_lines=False)
                    except Exception as e2:
                        log(f"Falha na segunda tentativa de tratar os dados referentes ao mês {dataId}: {e2}")
                        continue

            parsed_data[dataId]["content"] = df

        except Exception as e:
            log(f"Falha ao tratar os dados referentes ao mês {dataId}: {e}")

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
    saved_files = []

    # Crie o diretório no padrão de particionamento Hive
    for dataId, content in parsedData.items(): 
        download_dir = os.path.join('downloads/', f"year={dataId["year"]}/", f"month={month_text}")
        os.makedirs(download_dir, exist_ok=True)
        file_path = os.path.join(download_dir, f"{month_text}.{file_extension}")
        log('Diretótio para armazenar localmente os dados no padrão de particionamento Hive criado!')

        # Salve localmente os dados baixados
        with open(file_path, 'wb') as file:
            file.write(response.content)
            log(f'Dados referentes ao mês {month_text} do ano {year} em formato {file_extension} baixados com sucesso!')
        files.append(file_path)



    for month, dataframe in parsedData.items(): 
        filename = f"{DOWNLOAD_DIR}/month={month}.csv"
        dataframe.to_csv(filename, index=False)
        saved_files.append(filename)
    log(f"Dados salvos localmente em {DOWNLOAD_DIR}/month={month}.csv com sucesso!")
    print(saved_files)

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