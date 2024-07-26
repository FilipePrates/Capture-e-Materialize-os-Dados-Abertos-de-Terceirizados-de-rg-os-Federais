<!-- # Desafio Engenheiro de Dados @ Escritório de Dados -->
# Capture e Materialize os Dados Abertos de Terceirizados de Órgãos Federais

### Flow de Captura de Dados
**SETUP**:
   (🧹) Limpar Arquivo de Log -> (🔧) Configurar Arquivo de Log ->

**EXTRACT**:
   -> (⬇️) Baixar Dados -> (🧠) Salvar Dados Brutos em Memória ->

**CLEAN**:
   -> (🔍) Interpretar Dados em DataFrames -> (📥) Salvar Dados como CSVs Localmente ->

**LOAD**:
   -> (📦) Carregar CSVs para o Banco de Dados brutos -> (⬆️) Carregar Logs para o Banco de Dados

### Flow de Materialização dos Dados

**SETUP**:
   (🧹) Limpar Arquivo de Log  -> (🔧) Configurar Arquivo de Log ->

**TRANSFORM (DBT)**:
   -> (📦) staging.raw (Dados Brutos) -> (🧼) staging.cleaned (Dados com valor nulo padrão) -> 
    (📝) staging.renamed (Colunas renomeadas seguindo manuais de estilo do [Escritório de Dados](https://docs.dados.rio/guia-desenvolvedores/manual-estilo/#nome-e-ordem-das-colunas) e [Base dos Dados](https://basedosdados.github.io/mais/style_data/)) -> (🔶) staging.transformed (Colunas com tipos definidos.) ->

**LOAD**:
   -> (⬆️) Carregar Logs para o Banco de Dados

---

Configure ambiente virtual python, variáveis de ambiente necessárias, e baixe os requerimentos do sistema:

0. :
   ```sh
   python -m venv orchestrator && source orchestrator/bin/activate && cp .env.example .env && pip install -r requirements/start.txt
   ```

#### Execute o Servidor Prefect dentro de um container Docker local

1. : 
   ```sh
   docker build -t terceirizados_pipeline .
   ```
   ou
   ```sh
   sudo docker buildx create --name builder
   sudo docker buildx build . --tag terceirizados_pipeline
   ```
2. : 
   ```sh
   docker run -it --privileged -v /var/run/docker.sock:/var/run/docker.sock -p 8080:8080 -p 8050:8050 -p 4200:4200 terceirizados_pipeline
   ```

3. :
   O Servidor Prefect está online!

```sh
                                         WELCOME TO

_____  _____  ______ ______ ______ _____ _______    _____ ______ _______      ________ _____
|  __ \|  __ \|  ____|  ____|  ____/ ____|__   __|  / ____|  ____|  __ \ \    / /  ____|  __ \
| |__) | |__) | |__  | |__  | |__ | |       | |    | (___ | |__  | |__) \ \  / /| |__  | |__) |
|  ___/|  _  /|  __| |  __| |  __|| |       | |     \___ \|  __| |  _  / \ \/ / |  __| |  _  /
| |    | | \ \| |____| |    | |___| |____   | |     ____) | |____| | \ \  \  /  | |____| | \ \
|_|    |_|  \_\______|_|    |______\_____|  |_|    |_____/|______|_|  \_\  \/   |______|_|  \_\

```

Em outro terminal, execute as funcionalidades do serviço:

2. :
   ```
   prefect server create-tenant --name tenant && prefect create project adm_cgu_terceirizados
   ```
   ```
   python ./run/capture.py && python ./run/materialize.py && python ./run/historic_capture.py && python ./run/historic_materialize.py
   ```

Em um terceiro terminal, visualize os resultados:

3. :
   ```sh
   source orchestrator/bin/activate && pip install -r requirements/results.txt
   ```
4. :
   ```sh
   python ./run/results.py
   ```

### App Dash (localhost:8050) para visualizar tabelas do PostgreSQL
![dash_visualization_staging_transformed](images/dash_visualization_staging_historic_transformed.png)

---
### Programe Cronograma para Captura :

1. :
   ```sh
   source orchestrator/bin/activate && python ./run/scheduler.py
   ```

A Captura e Materialização dos dados mais recentes é programada para ocorrer **a cada 4 meses, começando em Maio**. Se ocorrer uma falha no Flow, uma nova tentativa ocorre diaramente até ser bem sucedida.

### Dashboard Prefect (localhost:8080) para acompanhar os Flows:
![prefect_dashboard_capture_flow_visualization](images/prefect_dashboard_capture_flow_visualization.png)

### Funcionalidades:
- **Captura dos dados mais recentes** (`python run/capture.py`)
- **Materialização dos dados mais recentes** (`python run/materialize.py`)
- **Captura dos dados históricos** - Todos os dados já disponibilizados (`python run/historic_capture.py`)
- **Materialização dos dados históricos** (`python run/historic_materialize.py`)
- **Scheduler** - Definição de cronograma de execução de flows Prefect de captura e materialização (`python run/scheduler.py`)
- **Results** - App Dash para visualizar tabelas resultantes armazenadas no banco de dados PostgreSQL (`python run/results.py`)


#### Alternativamente, através de Bash Script:

0. :
   ```sh
   sudo chmod +x start.sh stop.sh && stop.sh
   ```
1. :
   ```sh
   ./start.sh
   ```

#### Para parar o Servidor e Agente(s) Prefect

0. :
   ```sh
   sudo chmod +x stop.sh
   ```

1. :
   ```sh
   ./stop.sh
   ```

### Conectar diretamente ao PostgreSQL:

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
   ./stop.sh
   ```

###

caso:
&nbsp; Sistema Operacional host seja Windows:

1. :
   Tente através do WSL.
