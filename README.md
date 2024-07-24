# Desafio Engenheiro de Dados @ Escritório de Dados
# Capture e Materialize os Dados Abertos de Terceirizados de Órgãos Federais

### Opção 1:
1. :
   ```sh
   ./start.sh
   ```

2. :
   Acompanhe a Captura e Inicialização Inicial.

3. :
   Visite [htpt://localhost:8050/ (Dash App)](http://localhost:8050/) no seu browser
    para visualizar algumas das tabelas resultantes dos Flows iniciais já no PostgreSQL.

3. :
   ```sh
   python ./schedules.py
   ```
4.  :
   Visite [http://localhost:8080/ (Prefect Server Dashboard)](http://localhost:8080/) no seu browser
    para acompanhar o cronograma de Flows.

### Opção 2:
Manual:
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
   prefect create project adm_cgu_terceirizados
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
   Visite [http://localhost:8050/ (Dash App)](http://localhost:8050/) no seu browser.
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

## help
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

##
caso:
   Problemas ao executar shell shcripts:
1. :
   ```sh
   sudo chmod +x start.sh
   ```   
1. :
   ```sh
   sudo chmod +x stop.sh
   ```   

##
caso:
```sh
   Error: [Errno 2] No such file or directory: 'path/orchestrator/bin/python'
   ```
1. :
   ```sh
   rm -rf "orchestrator"
   ```

by Filipe