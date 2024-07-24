# Desafio Escritório de Dados
## Capture e Materialize os Dados Abertos de Terceirizados de Órgãos Federais 
### Opção 1:
1. :
   ```sh
   chmod +x start.sh
   ```
2. :
   ```sh
   ./start.sh
   ```

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
### Para parar o Servidor e Agente Prefect
1.   ```sh
   prefect server stop
   ```
2.   ```sh
   pkill -f 'prefect agent local start'
   ```