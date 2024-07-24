# Exit script on any error
set -e

# Function to install and start pgAdmin using Docker
install_and_start_pgadmin() {
    echo "Installing and starting pgAdmin using Docker..."
    docker run -d \
        --name pgadmin4 \
        -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
        -e PGADMIN_DEFAULT_PASSWORD="admin" \
        -p 5050:80 \
        dpage/pgadmin4
}

# Check if Docker is installed
if ! command -v docker > /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if pgAdmin container is already running
if [ "$(docker ps -q -f name=pgadmin4)" ]; then
    echo "pgAdmin container is already running."
else
    install_and_start_pgadmin
fi

# Activate the virtual environment
echo "Activating virtual environment..."
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

# Print instructions to access pgAdmin
echo "pgAdmin is now accessible at http://localhost:5050"
echo "Login with email: admin@admin.com and password: admin"

# Keeping the script running
echo "Setup complete. Services are running."
wait