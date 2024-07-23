# as funções auxiliares que serão utilizadas no Flow.

import prefect
import os

def log(message) -> None:
    """Ao ser chamada dentro de um Flow, realiza um log da message"""
    prefect.context.logger.info(f"\n{message}")

def log_and_propagate_error(message, returnObj) -> None:
    returnObj['error'] = message
    log(message)

def clean_log_file(logFilePath: str) -> None:
    """
    Limpa o arquivo de log.
    """
    try:
        logFilePath = os.getenv("LOGS_TABLE_NAME")
        with open(logFilePath, 'w') as file:
            pass
        log(f"Created a new empty log file: {logFilePath}")
    except Exception as e:
        log(f"Failed to clean {logFilePath}: {e}")