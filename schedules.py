"""
Modulo com os Schedules para os Flows
"""
from datetime import datetime, timedelta
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock
from pytz import timezone

TIMEZONE = "America/Sao_Paulo"
START_YEAR = 2024

every_4_months_starting_may = Schedule(
    clocks=[
        CronClock(
            cron="0 0 1 5/4 *",
            start_date=datetime(START_YEAR, 5, 1, 0, 0, tzinfo=timezone(TIMEZONE)),
        ),
    ]
)

every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(START_YEAR, 1, 1, 0, 0, tzinfo=timezone(TIMEZONE)),
        ),
    ]
)

every_10_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=10),
            start_date=datetime(START_YEAR, 1, 1, 0, 0, 0, tzinfo=timezone(TIMEZONE)),
        ),
    ]
)

every_5_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(START_YEAR, 1, 1, 0, 0, 0, tzinfo=timezone(TIMEZONE)),
        ),
    ]
)

every_minute = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(START_YEAR, 1, 1, 0, 0, 0, tzinfo=timezone(TIMEZONE)),
            
        ),
    ]
)