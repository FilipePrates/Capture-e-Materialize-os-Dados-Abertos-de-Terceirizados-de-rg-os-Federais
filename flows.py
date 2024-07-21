# o Flow propriamente dito.

from prefect import Flow, Parameter

from tasks import (
    download_data,
    parse_data,
    save_as_csv_locally,
    upload_csv_to_database
)

with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Captura",
          code_owners = ["filipe"]) as capture:
    data = download_data()
    dataframes = parse_data(data)
    filenames = save_as_csv_locally(dataframes)
    # filenames = ['downloads/year=2024/month=Janeiro.csv', 'downloads/year=2024/month=Maio.csv']
    upload_csv_to_database(filenames)

with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Materialização",
          code_owners = ["filipe"]) as materialization:
    # data = download_data()
    # dataframes = parse_data(data)
    # filenames = save_as_csv_locally(dataframes)
    filenames = ['downloads/year=2024/month=Janeiro.csv', 'downloads/year=2024/month=Maio.csv']
    upload_csv_to_database(filenames)