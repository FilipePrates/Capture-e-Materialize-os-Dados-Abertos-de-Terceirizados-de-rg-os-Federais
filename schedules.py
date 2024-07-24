from prefect import Flow
import threading
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock #, CronClock
from datetime import timedelta #, datetime
from utils import start_agent
from tasks import check_flow_state
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run
)
import threading

def start_schedule_flow():
    # DEMO: de 5 em 5 minutoes , 
    schedule = Schedule(
        clocks=[IntervalClock(timedelta(minutes=5))]
    )
    # para Produção:
    #      de 4 em 4 meses,
    #      começando dia 1 de Maio (ou Setembro ou Janeiro - o mais recente deles).
    #   schedule = Schedule(
    #       clocks=[
    #           IntervalClock(interval=timedelta(days=1)),
    #            CronClock(cron="0 0 1 */4 *", start_date=datetime(2024, 1, 5))
    #       ]
    #   )

    # Flow de Cronograma para a pipilene do projeto adm_cgu_terceirizados
    with Flow("Cronograma Padrão seguindo a Disponibilização dos Dados Abertos pela Controladoria Geral da União", schedule=schedule) as scheduleFlow:

        # Captura
        print(f" <> Criando Run de Flow de Captura para daqui à {schedule.clocks[0]}.")
        captureFlowRun = create_flow_run(
            flow_name="Captura dos Dados Abertos de Terceirizados de Órgãos Federais",
            project_name="adm_cgu_terceirizados"
        )
        captureFlowState = wait_for_flow_run(captureFlowRun, raise_final_state=True)
        print(" <>  Run de Flow de Captura criada!")

        # Materialização
        print(f" <> Criando Run de Flow de Materialização.")
        materializeFlowRun = create_flow_run(
            flow_name="Materialização dos Dados Abertos de Terceirizados de Órgãos Federais",
            project_name="adm_cgu_terceirizados"
        )
        materializeFlowState = wait_for_flow_run(materializeFlowRun, raise_final_state=True)
        print(" <>  Run de Flow de Materialização criada!")
        
        # Caso de falha no Flow, intervalo curto para recaptura
        if check_flow_state(captureFlowState) == "retry" or check_flow_state(materializeFlowState) == "retry" :
            schedule.clocks[0] = IntervalClock(interval=timedelta(days=1))

        print(" <>   <>   <>   <>   <>   <>   <>   <>   <>   <>   <>   <>   <> ")
        print(" <>                                                          <> ")
        print(" <>   Cronograma de Flows Criado!                            <> ")
        print(" <>                                   Visite localhost:8080  <> ")
        print(" <>                                        para acompanhar!  <> ")
        print(" <>                                                          <> ")
        print(" <>   <>   <>   <>   <>   <>   <>   <>   <>   <>   <>   <>   <> ")

    scheduleFlow.register(project_name="adm_cgu_terceirizados")

# Executando o Flow de Cronograma.
schedule_thread = threading.Thread(target=start_schedule_flow)
schedule_thread.start()

# Execute o Agente
start_agent()

