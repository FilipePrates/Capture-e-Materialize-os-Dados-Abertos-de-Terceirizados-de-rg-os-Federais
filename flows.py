# o Flow propriamente dito.
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
    # # SETUP #
    logFilePath = setup_log_file("logs/logs__capture.txt")
    cleanStart = clean_log_file(logFilePath)

    # # EXTRACT #
    rawData = download_new_cgu_terceirizados_data(cleanStart)
    rawFilePaths = save_raw_data_locally(rawData)
    
    # # CLEAN #
    parsedData = parse_data_into_dataframes(rawFilePaths)
    parsedFilePaths = save_parsed_data_as_csv_locally(parsedData)

    # LOAD #
    status = upload_csv_to_database(parsedFilePaths, "raw")
    logStatus = upload_logs_to_database(status, "logs/logs__capture.txt", "logs__capture")


with Flow("Materialização dos Dados Abertos de Terceirizados de Órgãos Federais") as materialize:
    # # SETUP #
    logFilePath = setup_log_file("logs/logs__materialize.txt")
    cleanStart = clean_log_file(logFilePath)

    # TRANSFORM #
    columns = run_dbt(cleanStart)

    #LOAD
    logStatus = upload_logs_to_database(columns, "logs/logs__materialize.txt", "logs__materialize")

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

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from datetime import timedelta, datetime
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run
)

# A cada 10 minutos -- demo
schedule = Schedule(
    clocks=[IntervalClock(timedelta(minutes=5))]
)
with Flow("Cronograma Padrão seguindo a Disponibilização dos Dados Abertos pela Controladoria Geral da União",
           schedule=schedule) as schedule:
    # Captura
    capture_flow_run = create_flow_run(
        flow_name="Captura dos Dados Abertos de Terceirizados de Órgãos Federais",
        project_name="adm_cgu_terceirizados")
    capture_flow_state = wait_for_flow_run(capture_flow_run, raise_final_state=True)

    # Materialização
    materialize_flow_run = create_flow_run(
        flow_name="Materialização dos Dados Abertos de Terceirizados de Órgãos Federais",
        project_name="adm_cgu_terceirizados"
    )
    
# Registra Flows no Project do Prefect http://localhost:8080
capture.register(project_name="adm_cgu_terceirizados")
materialize.register(project_name="adm_cgu_terceirizados")
schedule.register(project_name="adm_cgu_terceirizados")