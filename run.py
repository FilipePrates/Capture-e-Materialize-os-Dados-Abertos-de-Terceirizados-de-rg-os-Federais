from flows import capture, materialize
import logging
# Registro de logs do Prefect em arquivo .txt para posterior upload
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s - %(name)s | %(message)s',
                    handlers=[
                        logging.FileHandler("flow_logs.txt"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger("prefect")

capture.run()
# materialization.run()