"""
Modulo com os Schedules para os Flows
"""

from prefect import Flow
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
with Flow("Captura dos Dados Abertos de Terceirizados de Órgãos Federais") as capture:
    # SETUP #
    logFilePath = setup_log_file("logs/logs__capture.txt")
    cleanStart = clean_log_file(logFilePath)
    # EXTRACT #
    rawData = download_new_cgu_terceirizados_data(cleanStart)
    rawFilePaths = save_raw_data_locally(rawData)
    # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths)
    parsedFilePaths = save_parsed_data_as_csv_locally(parsedData)
    # LOAD #
    status = upload_csv_to_database(parsedFilePaths, "raw")
    logStatus = upload_logs_to_database(status, "logs/logs__capture.txt", "logs__capture")


with Flow("Materialização dos Dados Abertos de Terceirizados de Órgãos Federais") as materialize:
    # SETUP #
    logFilePath = setup_log_file("logs/logs__materialize.txt")
    cleanStart = clean_log_file(logFilePath)
    # TRANSFORM #
    columns = run_dbt(cleanStart)
    # LOAD #
    logStatus = upload_logs_to_database(columns, "logs/logs__materialize.txt", "logs__materialize")

capture.register(project_name="adm_cgu_terceirizados")
materialize.register(project_name="adm_cgu_terceirizados")


# Executar Captura e Materialização Histórica uma vez.
with Flow("Captura Histórica dos Dados Abertos de Terceirizados de Órgãos Federais") as historic_capture:
    # SETUP #
    logFilePath = setup_log_file("logs/logs__historic_capture.txt")
    cleanStart = clean_log_file(logFilePath)
    # EXTRACT #
    rawData = download_new_cgu_terceirizados_data(cleanStart)
    rawFilePaths = save_raw_data_locally(rawData)
    # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths)
    parsedFilePaths = save_parsed_data_as_csv_locally(parsedData)
    # LOAD #
    status = upload_csv_to_database(parsedFilePaths, "raw")
    logStatus = upload_logs_to_database(status, "logs/logs__historic_capture.txt", "logs__historic_capture")

with Flow("Materialização Histórica dos Dados Abertos de Terceirizados de Órgãos Federais") as historic_materialize:
    # SETUP #
    logFilePath = setup_log_file("logs/logs__historic_materialize.txt")
    cleanStart = clean_log_file(logFilePath)
    # TRANSFORM #
    columns = run_dbt(cleanStart)
    # LOAD #
    logStatus = upload_logs_to_database(columns, "logs/logs__historic_materialize.txt", "logs__historic_materialize")

historic_capture.register(project_name="adm_cgu_terceirizados")
historic_materialize.register(project_name="adm_cgu_terceirizados")

## Flow de Schedule definido em ./schedules.py ##
