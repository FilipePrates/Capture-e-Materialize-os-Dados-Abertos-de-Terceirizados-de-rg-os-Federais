# o Flow propriamente dito.

from prefect import Flow, Parameter
from tasks import (
    setup_log_file,
    clean_log_file,

    download_new_cgu_terceirizados_data,
    save_raw_data_locally,
    parse_data_into_dataframes,
    save_parsed_data_as_csv_locally,
    upload_csv_to_database,
    upload_logs_to_database,

    run_dbt
)

# Executar Captura e Materialização a cada ~4 meses.
with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Captura") as capture:
    # # SETUP #
    logFilePath = setup_log_file("logs.txt")
    cleanStart = clean_log_file(logFilePath)

    # # EXTRACT #
    rawData = download_new_cgu_terceirizados_data(cleanStart)
    # @TODO branch: if already had downloaded - reschedule for tomorrow
    rawFilePaths = save_raw_data_locally(rawData)
    
    # # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths)
    parsedFilePaths = save_parsed_data_as_csv_locally(parsedData)

    # LOAD #
    status = upload_csv_to_database(parsedFilePaths, "raw")
    logStatus = upload_logs_to_database(status, "logs.txt", "logs__capture")


with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Materialização (DBT)") as materialize:
    # # SETUP #
    logFilePath = setup_log_file("logs.txt")
    cleanStart = clean_log_file(logFilePath)

    # TRANSFORM #
    columns = run_dbt(cleanStart)
    # columns = rename_columns_following_style_manual()

    #LOAD
    logStatus = upload_logs_to_database(columns, "logs.txt", "logs__materialize")

# Executar captura de dados históricos
# with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Captura dos Dados Históricos") as captureAll:
#     # # SETUP #
#     # # EXTRACT #
#     #rawData = download_historic_cgu_terceirizados_data
#     rawFilePaths = save_raw_data_locally(rawData)
    
#     # # CLEAN #
#     parsedData = parse_data_into_dataframes(rawFilePaths)
#     parsedFilePaths = save_parsed_data_as_csv_locally(parsedData)

#     # LOAD #
#     status = upload_csv_to_database(parsedFilePaths, "historico")
#     logStatus = upload_logs_to_database(status)

#     # # DEPLOY? #


# with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Materialização dos Dados Históricos") as materializeAll:
#    # columns = rename_columns_following_style_manual("historico")
#    status = set_columns_types("123")