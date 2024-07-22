# o Flow propriamente dito.

from prefect import Flow, Parameter

from tasks import (
    download_new_data,
    save_raw_data_locally,
    parse_data_into_dataframes,
    save_parsed_data_as_csv_locally,
    upload_csv_to_database,
    upload_logs_to_database,

    rename_columns_following_style_manual,
    set_columns_types
)

# Executar captura e materialização a cada ~4 meses.
with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Captura") as capture:
    # # SETUP #


    # # EXTRACT #
    rawData = download_new_data()
    rawFilePaths = save_raw_data_locally(rawData)
    
    # # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths)
    parsedFilePaths = save_parsed_data_as_csv_locally(parsedData)

    # LOAD #
    # filenames = ['downloads/year=2024/month=Maio.xlsx_parsed.csv']
    status = upload_csv_to_database(parsedFilePaths)
    # upload_logs_to_database(status)

    # # DEPLOY? #

with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Materialização") as materialize:
    # renomear colunas no estilo
    # garantir tipagens
    new_columns = rename_columns_following_style_manual()
    status = set_columns_types(new_columns)

# Executar captura e materialização de dados históricos sem necessidade, a princípio, de re-execução posterior
with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Captura dos Dados Históricos") as captureAll:
    # # EXTRACT #
    raw_data = download_new_data()
    raw_filepaths = save_raw_data_locally(raw_data)
    
    # # CLEAN #
    dataframes = parse_data_into_dataframes(raw_filepaths)
    filenames = save_parsed_data_as_csv_locally(dataframes)

    # LOAD #
    # filenames = ['downloads/year=2024/month=Maio.xlsx_parsed.csv']
    status = upload_csv_to_database(filenames)
    # upload_logs_to_database(status)

with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Materialização dos Dados Históricos") as materializeAll:
    # renomear colunas no estilo
    # garantir tipagens
    new_columns = rename_columns_following_style_manual()
    status = set_columns_types(new_columns)
