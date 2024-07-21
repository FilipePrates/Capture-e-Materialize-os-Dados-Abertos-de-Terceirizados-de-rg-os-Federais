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
    #     # EXTRACT #
    # raw_status = get_raw(url)

    # raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # # CLEAN #
    # treated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps(
    #     status=raw_status, timestamp=timestamp, version=version
    # )

    # treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # # LOAD #
    # error = bq_upload(
    #     dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
    #     table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
    #     filepath=treated_filepath,
    #     raw_filepath=raw_filepath,
    #     partitions=partitions,
    #     status=treated_status,
    # )

    # upload_logs_to_bq(
    #     dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
    #     parent_table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
    #     error=error,
    #     timestamp=timestamp,
    # )

    # EXTRACT #
    raw_data = download_new_data()
    raw_filepath = save_raw_data_locally(raw_data)
    # CLEAN #
    dataframes = parse_data_into_dataframes(raw_filepath)
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