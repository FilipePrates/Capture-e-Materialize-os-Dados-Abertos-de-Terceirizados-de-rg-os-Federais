import sys
import os
from prefect import Flow
import threading
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock #, CronClock
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run
)
from datetime import timedelta
# Adicionando módulos do diretório pai
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
from schedules import (
    every_5_minutes,
    every_4_months_starting_may
)
from utils import start_agent
from tasks import check_flow_state

def start_schedule_flow(schedule, flowName):

    # Flow de Cronograma para a pipilene do projeto adm_cgu_terceirizados
    with Flow(flowName, schedule=schedule) as scheduleFlow:

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

version = input('Gostaria do progromaga de DEMO ou o de Produção? (d/p)')
if version in ['d','D','demo','Demo','DEMO','Demonstração','0'] :
    demoFlow = start_schedule_flow(every_5_minutes, "Cronograma Demonstrativo")
    schedule_thread = threading.Thread(target=demoFlow)
    schedule_thread.start()

    # Execute o Agente para Demonstração
    start_agent()

elif version in ['p','P','prod','Prod','produção','Produção','1']:
    prodFlow = start_schedule_flow(every_4_months_starting_may,
                                   "Cronograma Padrão seguindo a Disponibilização dos Dados Abertos pela Controladoria Geral da União")
    schedule_thread = threading.Thread(target=prodFlow)
    schedule_thread.start()