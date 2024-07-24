# Desafio Engenheiro de Dados @ Escritório de Dados
# Capture e Materialize os Dados Abertos de Terceirizados de Órgãos Federais
### Opção 1:
0. :
   ```sh
   pip install -r requirements.txt
   ```
1. :
   ```sh
   chmod +x start.sh
   ```
2. :
   ```sh
   ./start.sh
   ```

### Para visualizar os resultados

1. :
   ```sh
   pip install -r requirements__view_results.txt
   ```
2. :
   ```sh
   python ./view_results.py
   ```
3. :
   Visite [http://localhost:8050/](http://localhost:8050/) no seu browser.


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
   prefect server start
   ```
Em outra janela do terminal:

5. : 
   ```sh
   prefect agent local start --label default
   ```
Em outra janela do terminal:

6. :
   ```sh
   prefect create project cgu_terceirizados
   ```
7. :
   ```sh
   python ./run.py
   ```

### Para visualizar os resultados

1. :
   ```sh
   pip install -r requirements__view_results.txt
   ```
2. :
   ```sh
   python ./view_results.py
   ```
3. :
   Visite [http://localhost:8050/](http://localhost:8050/) no seu browser.

### Para parar o Servidor e Agente Prefect

1. :
   ```sh
   prefect server stop
   ```
2. : 
   ```sh
   pkill -f 'prefect agent local start'
   ```

### Conectar ao PostgreSQL:
1. :
   ```sh
   prefect server stop
   ```
2. : 
   ```sh
   docker exec -it $(docker ps | grep 'postgres:11' | awk '{print $1}') bash
   ```
3. :
   ```sh
   psql -U prefect -d prefect_server -W
   ```
4. :
Escreva a senha: "test-password"
