# Desafio Engenheiro de Dados @ Escritório de Dados
# Capture e Materialize os Dados Abertos de Terceirizados de Órgãos Federais

## Execute:

Configure ambiente virtual python e variáveis de ambiente necessárias:

0. :
   ```sh
   python -m venv orchestrator && source orchestrator/bin/activate && cp .env.example .env
   ```

Baixe os requisitos e levante o Servidor Prefect:

1. :
   ```sh
   pip install -r requirements/start.txt && prefect server start
   ```

espere até a arte em ASCII:
```sh
                                         WELCOME TO

_____  _____  ______ ______ ______ _____ _______    _____ ______ _______      ________ _____
|  __ \|  __ \|  ____|  ____|  ____/ ____|__   __|  / ____|  ____|  __ \ \    / /  ____|  __ \
| |__) | |__) | |__  | |__  | |__ | |       | |    | (___ | |__  | |__) \ \  / /| |__  | |__) |
|  ___/|  _  /|  __| |  __| |  __|| |       | |     \___ \|  __| |  _  / \ \/ / |  __| |  _  /
| |    | | \ \| |____| |    | |___| |____   | |     ____) | |____| | \ \  \  /  | |____| | \ \
|_|    |_|  \_\______|_|    |______\_____|  |_|    |_____/|______|_|  \_\  \/   |______|_|  \_\

Visit http://localhost:8080 to get started
```

Em outro terminal:

2. :
   ```sh
   prefect create project adm_cgu_terceirizados
   ```
3. :
   ```sh
   python ./run/capture.py
   python ./run/materialize.py
   python ./run/historic_capture.py
   python ./run/historic_materialized.py
   ```
   ```sh
   pip install -r requirements/results.txt
   python ./run/results.py
   ```


#### Ou então, rode o Bash Script:

Permita execução dos scripts:

0. :
   ```sh
   sudo chmod +x start.sh scripts/stop.sh && scripts/stop.sh
   ```
Execute:

1. :
   ```sh
   ./start.sh
   ```

Acompanhe a Captura e Materialização dos Dados:

2. :
   Visite [htpt://localhost:8050/ (Dash App)](http://localhost:8050/) no seu browser
    para visualizar algumas das tabelas resultantes dos Flows iniciais armazenadas no PostgreSQL.

### App Dash (localhost:8050) para visualizar tabelas do PostgreSQL
![dash_visualization_staging_transformed](images/dash_visualization_staging_transformed.png)

### Para parar o Servidor e Agente(s) Prefect

0. :
   ```sh
   sudo chmod +x scripts/stop.sh
   ```

1. :
   ```sh
   ./scripts/stop.sh
   ```

### Para programar Schedule (Cronograma) de Captura

1. :
   ```sh
   python ./run/scheduler.py
   ```
2. :
   Visite [http://localhost:8080/ (Prefect Server Dashboard)](http://localhost:8080/) no seu browser para acompanhar o cronograma de Flows.

### Dashboard Prefect (localhost:8080) para acompanhar Scheduler e Flows
![prefect_dashboard_capture_flow_visualization](images/prefect_dashboard_capture_flow_visualization.png)

#### Opção 3: Rode dentro de um container Docker (WIP)

Permita execução dos scripts necessários e configuração docker:

0. :
   ```sh
   sudo chmod +x scripts/docker_start.sh && scripts/stop.sh && scripts/stop.sh && sudo docker service start
   ```

Construa a imagem docker:

1. : 
   ```sh
   docker build -t adm_cgu_terceirizados_pipeline .
   ```
   <!-- É esperado que "Installing build dependencies: finished with status 'done'" e "Running setup install for numpy" demore um pouquinho. -->

Rode a imagem docker:

2. : 
   ```sh
   docker run -it --privileged -v /var/run/docker.sock:/var/run/docker.sock -p 8080:8080 -p 4200:4200 -p 8050:8050 adm_cgu_terceirizados_pipeline
   ```

### Conectar diretamente ao PostgreSQL:

Na camada com o Servidor Prefect em execução:

1. : 
   ```
   docker exec -it $(docker ps | grep 'postgres:11' | awk '{print $1}') bash
   ```
2. :
   ```sh
   psql -U prefect -d prefect_server -W
   ```
3. :
Escreva a senha: "test-password"

### #help
<!-- ###
caso:
   Caso utilizando sistema operacional Windows:

0. :
   Caso utilizando sistema operacional Windows - utilize através do WSL. -->


###
caso:
   ```sh
   Error: [Errno 2] No such file or directory: 'path/orchestrator/bin/python'
   ```

1. :
   ```sh
   rm -rf "orchestrator"
   ```

caso:
```sh
   (orchestrator) user@machine:~/path$ start.sh
   Pulling postgres ... done
   Pulling hasura   ... done
   Pulling graphql  ... done
   Pulling apollo   ... done
   Pulling towel    ... done
   Pulling ui       ... done
   Starting tmp_postgres_1 ... error

   ERROR: for tmp_postgres_1  Cannot start service postgres: network $ID not found

   ERROR: for postgres  Cannot start service postgres: network $ID not found
   ERROR: Encountered errors while bringing up the project.
   ```
1. :
   ```sh
   docker network prune -f
   ```

   se erro permanecer, limpe todos os processos relacionados com a pipeline:
1. 
   ```sh
   ./scripts/stop.sh
   ```