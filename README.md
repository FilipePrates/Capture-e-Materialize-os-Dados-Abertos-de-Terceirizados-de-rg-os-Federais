# Desafio Engenheiro de Dados @ Escritório de Dados
# Capture e Materialize os Dados Abertos de Terceirizados de Órgãos Federais

### Opção 1:
1. :
   ```sh
   chmod +x start.sh
   ```
2. :
   ```sh
   ./start.sh
   ```
<!-- ![prefect_server_and_agents_ready](images/prefect_server_and_agents_ready.png) -->

3. :
   Visite [http://localhost:8080/](http://localhost:8080/) no seu browser para acompanhar os Flows.

### Opção 2:
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
   pip install -r requirements.txt
   ```

4. :
   ```sh
   cp .env.example .env
   ```

5. :
   ```sh
   prefect server start
   ```
Em outra janela do terminal:

6. : 
   ```sh
   prefect agent local start --label default
   ```
Em outra janela do terminal:

7. :
   ```sh
   prefect create project cgu_terceirizados
   ```
8. :
   ```sh
   python ./capture.py
   ```
   ```sh
   python ./materialize.py
   ```
   ```sh
   python ./schedules.py
   ```
<!-- ![capture_flow_log_in_prefect_server](images/capture_flow_log_in_prefect_server.png)
![materialize_flow_log_in_vscode_terminal](images/materialize_flow_log_in_vscode_terminal.png) -->

### Para visualizar os dados após Captura e Materialização

Com o Servidor Prefect local rodando:

1. :
   ```sh
   python ./materialize.py
   ```

2. :
   ```sh
   pip install -r requirements__view_results.txt
   ```

3. :
   ```sh
    python ./view_results.py
   ```

4. :
   Visite [http://localhost:8050/](http://localhost:8050/) no seu browser.
<!-- ![dash_visualization_staging_transformed](images/dash_visualization_staging_transformed.png) -->

### Para parar o Servidor e Agente Prefect

0. :
   ```sh
   chmod +x stop.sh
   ```
1. :
   ```sh
   ./stop.sh
   ```
ou,:

1. :
   ```sh
   prefect server stop
   ```
2. : 
   ```sh
   pkill -f 'prefect agent local start'
   ```
ou ainda, de maneira mais exigente:

1. :
   ```sh
   chmod +x clear.sh
   ```
2. : 
   ```sh
   ./clear.sh
   ```

### Conectar diretamente ao PostgreSQL:

Com o Servidor Prefect local rodando:

1. : 
   ```sh
   docker exec -it $(docker ps | grep 'postgres:11' | awk '{print $1}') bash
   ```
2. :
   ```sh
   psql -U prefect -d prefect_server -W
   ```
3. :
Escreva a senha: "test-password"

## 
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
1. :
   ```sh
   chmod +x clean.sh
   ```
2. :
   ```sh
   ./clean.sh
   ```
##
caso:
```sh
   Error: [Errno 2] No such file or directory: 'path/orchestrator/bin/python'
   ```
1. :
   ```sh
   rmdir orchestrator
   ```
#

by Filipe