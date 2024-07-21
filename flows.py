# o Flow propriamente dito.

from prefect import Flow, Parameter

from tasks import (
    download_new_data,
    save_raw_data_locally,
    parse_data_into_dataframes,
    save_parsed_data_as_csv_locally,
    upload_csv_to_database,
    upload_logs_to_database
)

with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Captura") as capture:
    # EXTRACT #
    raw_data = download_new_data()
    raw_filepaths = save_raw_data_locally(raw_data)
    
    # CLEAN #
    dataframes = parse_data_into_dataframes(raw_filepaths)
    filenames = save_parsed_data_as_csv_locally(dataframes)

    # LOAD #
    # filenames = ['downloads/year=2024/month=Janeiro.csv', 'downloads/year=2024/month=Maio.csv']
    status = upload_csv_to_database(filenames)
    # upload_logs_to_database(status)

with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Materialização") as materialization:
    # data = download_data()
    # dataframes = parse_data(data)
    # filenames = save_as_csv_locally(dataframes)
    filenames = ['downloads/year=2024/month=Janeiro.csv', 'downloads/year=2024/month=Maio.csv']
    upload_csv_to_database(filenames)