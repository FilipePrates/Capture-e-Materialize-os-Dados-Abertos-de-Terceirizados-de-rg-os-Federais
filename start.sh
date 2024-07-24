# Activate the virtual environment
source orchestrator/bin/activate

# Start the Prefect server
echo "Starting Prefect server..."
prefect server start &

# Wait a few seconds to ensure the server starts properly
sleep 10

# Start the Prefect agent
echo "Starting Prefect agent..."
prefect agent local start --label default &

# Create the Prefect project (if not already created)
echo "Creating Prefect project..."
prefect create project cgu_terceirizados || echo "Project 'cgu_terceirizados' already exists"

# Run the Prefect flow
echo "Running Prefect flow..."
python ./run.py