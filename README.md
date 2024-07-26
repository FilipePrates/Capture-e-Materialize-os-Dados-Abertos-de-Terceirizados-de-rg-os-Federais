# Desafio Engenheiro de Dados @ Escritório de Dados
# Capture e Materialize os Dados Abertos de Terceirizados de Órgãos Federais

## Execute:

Permissões e limpeza do sistema host:

0. :
   ```sh
   sudo chmod +x start.sh docker_start.sh stop.sh
   ```
1. :
   ```sh
   ./stop.sh
   ```

#### Opção 1: Rode Localmente com Bash Script
1. 
   ```sh
   ./start.sh
   ```

#### Opção 2: Rode dentro de um container Docker
1. :
   ```sh
   ./stop.sh
   ```
2. : 
   ```sh
   docker build -t adm_cgu_terceirizados_pipeline .
   ```
   <!-- É esperado que "Installing build dependencies: finished with status 'done'" e "Running setup install for numpy" demore um pouquinho. -->
3. : 
   ```sh
   docker run -it --privileged -v /var/run/docker.sock:/var/run/docker.sock -p 8080:8080 -p 4200:4200 -p 8050:8050 adm_cgu_terceirizados_pipeline
   ```

   Acompanhe a Captura e Materialização dos Dados:

2. :
   Visite [htpt://localhost:8050/ (Dash App)](http://localhost:8050/) no seu browser
    para visualizar algumas das tabelas resultantes dos Flows iniciais armazenadas no PostgreSQL.

3. :
   ```sh
   python ./run/scheduler.py
   ```
4.  :
   Visite [http://localhost:8080/ (Prefect Server Dashboard)](http://localhost:8080/) no seu browser
    para acompanhar o cronograma de Flows.


#### Opção 3: Rode localmente e de forma manual
1. :
   ```sh
   python -m venv orchestrator
   ```

2. :
   ```sh
   source orchestrator/bin/activate
   ```

3. :
   ```sh
   pip install -r requirements/start.txt
   ```

4. :
   ```sh
   cp .env.example .env
   ```

5. :
   ```sh
   prefect server start
   ```
   e espere até a arte em ASCII:
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

7. :
   ```sh
   prefect create project adm_cgu_terceirizados
   ```
8. :
   ```sh
   python ./run/capture.py
   ```
   ```sh
   python ./run/materialize.py
   ```
   ```sh
   python ./run/scheduler.py
   ```
   ```sh
   pip install -r requirements/results.txt
   python ./run/results.py
   ```

### Para parar o Servidor e Agente(s) Prefect

0. :
   ```sh
   sudo chmod +x stop.sh
   ```

1. :
   ```sh
   ./stop.sh
   ```
   
### Para visualizar os dados após Captura e Materialização

Com o Servidor Prefect local rodando:

0. :
   ```sh
    pip install -r requirements/results.txt
   ```

1. :
   ```sh
    python ./run/results.py
   ```

2. :
   Visite [http://localhost:8050/ (Dash App)](http://localhost:8050/) no seu browser.

## localhost:8050
### App Dash para visualizar tabelas do PostgreSQL
![dash_visualization_staging_transformed](images/dash_visualization_staging_transformed.png)


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


### Para programar Schedule (Cronograma) de Captura

1. :
   ```sh
   python ./run/scheduler.py
   ```
2. :
   Visite [http://localhost:8080/ (Prefect Dashboard)](http://localhost:8080/) no seu browser.

## localhost:8080
### Dashboard Prefect para acompanhar Scheduler e Flows
![prefect_dashboard_capture_flow_visualization](images/prefect_dashboard_capture_flow_visualization.png)

### #help
caso:
```sh
   (orchestrator) user@machine:~/path$ ./start.sh
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
   ./stop.sh
   ```

###
caso:
   Problemas ao executar shell scripts:

0. :
   Caso utilizando sistema operacional Windows - tente através do WSL.

1. :
   ```sh
   sudo chmod +x start.sh
   ```   
2. :
   ```sh
   sudo chmod +x stop.sh
   ```   

###
caso:
```sh
   Error: [Errno 2] No such file or directory: 'path/orchestrator/bin/python'
   ```
1. :
   ```sh
   rm -rf "orchestrator"
   ```
##
by Filipe Prates