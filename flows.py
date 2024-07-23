# o Flow propriamente dito.

from prefect import Flow, Parameter

from tasks import (
    download_new_cgu_terceirizados_data,
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
    rawData = download_new_cgu_terceirizados_data()
    rawFilePaths = save_raw_data_locally(rawData)
    
    # rawFilePaths = {'rawFilePaths':['cgu_terceirizados_local/year=2024/month=maio.xlsx']}
    # # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths)
    parsedFilePaths = save_parsed_data_as_csv_locally(parsedData)

    # LOAD #
    status = upload_csv_to_database(parsedFilePaths, "raw")
    logStatus = upload_logs_to_database("flow_logs.txt")
    

with Flow("Dados Abertos de Terceirizados de Órgãos Federais - Materialização (DBT)") as materialize:
    columns = set_columns_types()
    columns = rename_columns_following_style_manual()


# # Executar captura e materialização de dados históricos sem necessidade, a princípio, de re-execução posterior
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