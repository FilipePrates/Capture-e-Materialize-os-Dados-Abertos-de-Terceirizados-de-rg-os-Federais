# o Flow propriamente dito.

from prefect import Flow, Parameter

from tasks import (
    download_data,
    parse_data,
    save_as_csv_locally,
)

with Flow("Users report") as flow:
    # Tasks
    data = download_data()
    dataframes = parse_data(data)
    save_as_csv_locally(dataframes)