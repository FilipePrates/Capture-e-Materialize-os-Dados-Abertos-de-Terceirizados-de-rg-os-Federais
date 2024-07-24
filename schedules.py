from prefect import Flow
import threading
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from datetime import timedelta, datetime
from utils import start_agent, log
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run
)
from prefect.agent.local import LocalAgent
import threading

def start_schedule_flow():
    # DEMO: de 5 em 5 minutoes , Produção: de 4 em 4 meses com start_date=datetime("1/5/2024")
    schedule = Schedule(
        clocks=[IntervalClock(timedelta(minutes=5))]
    )

    # Flow de Cronograma pipilene adm_cgu_terceirizados
    with Flow("Cronograma Padrão seguindo a Disponibilização dos Dados Abertos pela Controladoria Geral da União", schedule=schedule) as schedule:
        # Captura
        log(f"Criando Run de Flow de Captura para daqui à {timedelta(minutes=5)}.")
        capture_flow_run = create_flow_run(
            flow_name="Captura dos Dados Abertos de Terceirizados de Órgãos Federais",
            project_name="adm_cgu_terceirizados"
        )
        log("Run de Flow de Captura criada.")

        capture_flow_state = wait_for_flow_run(capture_flow_run, raise_final_state=True)

        log(f"Criando Run de Flow de Materialização.")
        # Materialização
        materialize_flow_run = create_flow_run(
            flow_name="Materialização dos Dados Abertos de Terceirizados de Órgãos Federais",
            project_name="adm_cgu_terceirizados"
        )
        log("Run de Flow de Materialização criada.")

    schedule.register(project_name="adm_cgu_terceirizados")

# Start the scheduling flow in a separate thread
schedule_thread = threading.Thread(target=start_schedule_flow)
schedule_thread.start()

# Start the Prefect agent in the main thread
agent = LocalAgent()
agent.start()