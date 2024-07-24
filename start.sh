# Activate the virtual environment
source orchestrator/bin/activate

# docker network ls
# docker network prune

# Start the Prefect server
echo "Starting Prefect server..."
prefect server start &

# Wait a few seconds to ensure the server starts properly
sleep 15
# echo "Começando os Flows em 15 seconds..."
# sleep 10

# Start the Prefect agent
echo "Starting Prefect agents..."
prefect agent local start --label default &
prefect agent local start --label default &

echo "Começando os Flows em 5 seconds..."
sleep 5
echo "Começando os Flows"

# Create the Prefect project (if not already created)
echo "Creating Prefect project..."
prefect create project cgu_terceirizados || echo "Project 'cgu_terceirizados' already exists"

# Run the Prefect flow
echo "Running Prefect flow..."
python ./run.py