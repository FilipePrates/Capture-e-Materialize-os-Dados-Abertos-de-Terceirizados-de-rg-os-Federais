kill_processes() {
    # echo "Killing all Python processes..."
    # pkill -f python

    echo "Killing all Docker containers..."
    docker stop $(docker ps -q)
    docker rm $(docker ps -a -q)

    echo "Killing all PostgreSQL processes..."
    pkill -f postgres

    echo "Killing all Prefect processes..."
    pkill -f 'prefect agent'
    pkill -f 'prefect server'
    pkill -f prefect

    echo "Cleaning up Docker networks..."
    docker network prune -f

    echo "All specified processes have been killed."
}

# Trap errors and interrupts to ensure cleanup
trap 'kill_processes' ERR SIGINT

# Run the cleanup function
kill_processes