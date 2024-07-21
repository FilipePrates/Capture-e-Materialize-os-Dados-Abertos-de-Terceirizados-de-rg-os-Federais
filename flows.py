# o Flow propriamente dito.

from prefect import Flow, Parameter

from tasks import (
    download_data,
    parse_data,
    save_as_csv_locally,
    upload_csv_to_database
)

with Flow("Users report") as flow:
    # Tasks
    data = download_data()
    dataframes = parse_data(data)
    filenames = save_as_csv_locally(dataframes)
    upload_csv_to_database(filenames)