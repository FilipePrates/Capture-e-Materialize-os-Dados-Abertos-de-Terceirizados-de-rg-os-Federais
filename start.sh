# Activate the virtual environment
source orchestrator/bin/activate

# Crie arquivo local de variáveis de ambiente
cp .env.example .env

# Start do Servidor Prefect
echo "<> Start Servidor Prefect..."
prefect server start &

sleep 15
# echo "Começando os Flows em 15 seconds..."
# sleep 10

# Start do(s) Agente(s) Prefect
echo "<> Start Agente(s) Prefect..."
prefect agent local start --label default &
prefect agent local start --label default &

echo "<> Começando os Flows em 5 seconds..."
sleep 5
echo "<> Começando os Flows"

# Crie o projeto Prefect (cgu_terceirizados)
echo "<> Criando o projeto Prefect..."
prefect create project cgu_terceirizados || echo "Project 'cgu_terceirizados' already exists"

# Começe a Captura Inicial
echo "<> Começando Captura incial..."
python ./capture.py &

# sleep 50 # delay captura inicial 

# # Começe a Materialização Inicial
# echo "Começando Materialização incial..."
# python ./materialize.py &

# Execute o Cronograma de Flows
echo "<> Executando Cronograma de Flows..."
python ./run.py &
