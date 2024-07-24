kill_processes() {
    # echo "Killing all Python processes..."
    # pkill -f python

    echo " <> Parando e Removendo containers Docker..."
    containers=$(docker ps -a -q)
    if [[ ! -z "$containers" ]]; then
        docker stop $containers
        docker rm $(docker ps -a -q)
    fi
    echo " <> Podando networks do Docker..."
    docker network prune -f

    echo " <> Parando processos do PostgreSQL..."
    pkill -f postgres

    echo " <> Parando processos do Prefect..."
    pkill -f 'prefect agent'
    pkill -f 'prefect server'
    pkill -f prefect

    echo " <> Parando processo do Dash..."
    stop_dash_process() {
        pid=$(lsof -t -i:8050)
        if [ -n "$pid" ]; then
            kill $pid
            if kill -0 $pid > /dev/null 2>&1; then
                echo " <>  <> kill -9 (force kill)"
                kill -9 $pid
            fi
        fi
    }
    stop_dash_process

    echo " <> Parando processo do Prefect Dashboard..."
    stop_dash_process() {
        pid=$(lsof -t -i:8080)
        if [ -n "$pid" ]; then
            kill $pid
            if kill -0 $pid > /dev/null 2>&1; then
                echo " <>  <> kill -9 (force kill)"
                kill -9 $pid
            fi
        fi
    }
    stop_dash_process

    echo "Todos os processos relevantes foram parados. <> 
    Até a próxima."
}

# Trap errors and interrupts to ensure cleanup
trap 'kill_processes' ERR SIGINT

# Run the cleanup function
kill_processes