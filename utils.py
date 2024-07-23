import prefect
from prefect import Client
from prefect.engine.signals import FAIL
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from datetime import timedelta, datetime

# as funções auxiliares que serão utilizadas nos Flows.

def log(message) -> None:
    """Ao ser chamada dentro de um Flow, realiza um log da message"""
    prefect.context.logger.info(f"\n{message}")

def log_and_propagate_error(message, returnObj) -> None:
    returnObj['error'] = message
    log(message)
    raise FAIL(result=returnObj)

def cronograma_padrao_cgu_terceirizados():
    """Determina o cronograma padrão de disponibilização
    de novos dados da Controladoria Geral da União"""
    # client = Client()
    # flow_runs = client.get_flow_run_info()
    flow_runs = [] # Fake last success

    if not flow_runs:
        # Sem flows prévios, realize primeira captura
        return [CronClock(start_date=datetime.now(),
                           cron="0 0 1 */4 *")]

    last_run = flow_runs[0]
    if last_run['state'] == 'Success':
        # Se útimo flow teve sucesso, então programar próximo flow para daqui a 4 meses
        next_run_time = last_run['start_time'] + timedelta(days=4*30+1)
        return [CronClock(start_date=next_run_time.strftime("%M %H %d %m *"),
                          cron="0 0 1 */4 *")]
    else:
        # se obteve Fail tente novamente no dia seguinte (possivelmente dados novos ainda não disponíceis)
        next_run_time = last_run['start_time'] + timedelta(days=1)
        return [CronClock(start_date=next_run_time.strftime("%M %H %d %m *"),
                          cron="0 0 1 */4 *")]

