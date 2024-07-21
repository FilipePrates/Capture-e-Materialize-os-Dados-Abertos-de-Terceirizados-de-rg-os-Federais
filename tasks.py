# as funções que serão utilizadas no Flow.

from io import StringIO

import os
import pandas as pd
from prefect import task
import requests
import pandas as pd
from datetime import datetime

# import schedule
from bs4 import BeautifulSoup

from utils import log

# URL da página que contém os Dados
URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"

# Possíveis valores de texto dos links com Dados.
# months = ["Janeiro", "Maio", "Setembro"]
months = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

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
            if response.status_code == 200 and response.headers.get('Content-Type', '').startswith('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):
                files[month] = response.content
            else:
                print(f"Failed to download a valid Excel file for {month}.")
    log('Dados baixados com sucesso!')
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

    for month, content in files.items():
        df = pd.read_excel(pd.io.common.BytesIO(content), engine='openpyxl')
        parsed_data[month] = df

    log("Dados convertidos em DataFrames com sucesso!")
    return parsed_data

@task
def save_as_csv_locally(parsedData: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame do Pandas.
    """
    for month, dataframe in parsedData.items(): 
        dataframe.to_csv(f"month={month}.csv", index=False)
    log("Dados salvos em report.csv com sucesso!")