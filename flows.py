# -*- coding: utf-8 -*-
"""
Modulo com os Schedules para os Flows
"""
from prefect import Flow
from tasks import (
    setup_log_file,
    clean_log_file,
    download_cgu_terceirizados_data,
    save_raw_data_locally,
    parse_data_into_dataframes,
    save_data_as_csv_locally,
    upload_csv_to_database,
    upload_logs_to_database,
    run_dbt
)

## Gere o Cronograma (Schedule) dos Flows executando 'python ./run/scheduler.py' ##

with Flow("Captura dos Dados") as capture:
    # SETUP #
    logFilePath = clean_log_file("logs/logs__capture.txt")
    cleanStart = setup_log_file(logFilePath)
    # EXTRACT #
    rawData = download_cgu_terceirizados_data(cleanStart, historic=False)
    rawFilePaths = save_raw_data_locally(rawData, lenient=False)
    # DEBUG help:
    # rawFilePaths = {'rawFilePaths': ['adm_cgu_terceirizados_local/year=2024/raw_data_0.xlsx']}
    # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths, lenient=False)
    parsedFilePaths = save_data_as_csv_locally(parsedData, lenient=False)
    # LOAD #
    status = upload_csv_to_database(parsedFilePaths, "raw", lenient=False)
    logStatus = upload_logs_to_database(status, "logs/logs__capture.txt", "logs__capture")


with Flow("Materialização dos Dados") as materialize:
    # SETUP #
    logFilePath = clean_log_file("logs/logs__materialize.txt")
    cleanStart = setup_log_file(logFilePath)
    # TRANSFORM #
    columns = run_dbt(cleanStart, historic=False, publish=True)
    # LOAD #
    logStatus = upload_logs_to_database(columns, "logs/logs__materialize.txt", "logs__materialize")


# Executar Captura e Materialização Histórica uma vez.
with Flow("Captura dos Dados Históricos") as historic_capture:
    # SETUP #
    logFilePath = clean_log_file("logs/logs__historic_capture.txt")
    cleanStart = setup_log_file(logFilePath)
    # EXTRACT #
    rawData = download_cgu_terceirizados_data(cleanStart, historic=True)
    rawFilePaths = save_raw_data_locally(rawData, lenient=True)
    # DEBUG help:
    # rawFilePaths = {'rawFilePaths': ['adm_cgu_terceirizados_local/year=2024/raw_data_0.xlsx', 'adm_cgu_terceirizados_local/year=2024/raw_data_1.xlsx', 'adm_cgu_terceirizados_local/year=2023/raw_data_2.csv', 'adm_cgu_terceirizados_local/year=2023/raw_data_3.csv', 'adm_cgu_terceirizados_local/year=2023/raw_data_4.csv', 'adm_cgu_terceirizados_local/year=/raw_data_5.csv', 'adm_cgu_terceirizados_local/year=/raw_data_6.csv', 'adm_cgu_terceirizados_local/year=/raw_data_7.csv', 'adm_cgu_terceirizados_local/year=2022/raw_data_8.csv', 'adm_cgu_terceirizados_local/year=2022/raw_data_9.csv', 'adm_cgu_terceirizados_local/year=2022/raw_data_10.csv', 'adm_cgu_terceirizados_local/year=2021/raw_data_11.csv', 'adm_cgu_terceirizados_local/year=2021/raw_data_12.csv', 'adm_cgu_terceirizados_local/year=2021/raw_data_13.csv', 'adm_cgu_terceirizados_local/year=2020/raw_data_14.csv', 'adm_cgu_terceirizados_local/year=2020/raw_data_15.csv', 'adm_cgu_terceirizados_local/year=2020/raw_data_16.csv', 'adm_cgu_terceirizados_local/year=2019/raw_data_17.csv', 'adm_cgu_terceirizados_local/year=2019/raw_data_18.csv', 'adm_cgu_terceirizados_local/year=2019/raw_data_19.csv']}
    # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths, lenient=True)
    parsedFilePaths = save_data_as_csv_locally(parsedData, lenient=True)
    # LOAD #
    status = upload_csv_to_database(parsedFilePaths, "historic_raw", lenient=True)
    logStatus = upload_logs_to_database(status, "logs/logs__historic_capture.txt", "logs__historic_capture")

with Flow("Materialização dos Dados Históricos") as historic_materialize:
    # SETUP #
    logFilePath = clean_log_file("logs/logs__historic_materialize.txt")
    cleanStart = setup_log_file(logFilePath)
    # TRANSFORM #
    columns = run_dbt(cleanStart, historic=True, publish=True)
    # LOAD #
    logStatus = upload_logs_to_database(columns, "logs/logs__historic_materialize.txt", "logs__historic_materialize")


historic_capture.register(project_name="adm_cgu_terceirizados")
historic_materialize.register(project_name="adm_cgu_terceirizados")
capture.register(project_name="adm_cgu_terceirizados")
materialize.register(project_name="adm_cgu_terceirizados")

## Gere o Flow de Schedule executando python ./run/scheduler.py ##
