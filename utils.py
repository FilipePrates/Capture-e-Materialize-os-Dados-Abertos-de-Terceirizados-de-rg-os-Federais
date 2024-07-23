# as funções auxiliares que serão utilizadas no Flow.

import prefect
from prefect.engine.signals import FAIL

def log(message) -> None:
    """Ao ser chamada dentro de um Flow, realiza um log da message"""
    prefect.context.logger.info(f"\n{message}")

def log_and_propagate_error(message, returnObj) -> None:
    returnObj['error'] = message
    log(message)
    raise FAIL(result=returnObj)