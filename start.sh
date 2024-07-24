#!/bin/bash

# Confirmar execução de Servidor Prefect e
# Captura e Materialização dos Dados Abertos de Terceirizados de Órgãos Federais

# Pergunte ao usuário se deseja baixar requisitos para realizar captura
read -p "<> Deseja criar ambiente virtual e baixar os requisitos:
prefect & dbt-core & dbt-postgres & requests & bs4 & pandas & openpyxl & datetime & psycopg2-binary & python-dotenv
<> para realizar a captura e materialização dos Dados Abertos de Terceirizados de Órgãos Federais <> ? (y/n): " run
if [[ "$run" == "y" || "$run" == "Y" || "$run" == "yes" || "$run" == "Yes" || "$run" == "s" || "$run" == "S" || "$run" == "sim" || "$run" == "Sim" ]]; then

    echo " <> Começando."
    # Remova o diretório 'orchestrator', se ele existir, evitando conflitos
    if [ -d "orchestratorB" ]; then
        rm -rf "orchestratorB"
    fi
    # Crie o ambiente virtual python do orquestrador prefect
    python -m venv orchestrator
    # Ative o ambiente virtual
    source orchestrator/bin/activate
    # Instale os requisitos
    pip install -r requirements.txt
    # Crie arquivo local de variáveis de ambiente
    cp .env.example .env
    # Start do Servidor Prefect
    echo " <> Start Servidor Prefect..."
    prefect server start &
    total_seconds=25
    echo " <> Começando os Flows em $total_seconds segundos..."
    for ((i=total_seconds; i>0; i--))
    do
        echo " <> Começando os Flows em $i segundos..."
        sleep 1
    done
    # Start do(s) Agente(s) Prefect
    echo " <> Start Agente(s) Prefect..."
    prefect agent local start --label default &
    # prefect agent local start --label default &
    # Crie o projeto Prefect (cgu_terceirizados)
    echo " <> Criando o projeto Prefect..."
    prefect create project cgu_terceirizados || echo "Project 'cgu_terceirizados' already exists"
    sleep 2
    echo " <> Começando os Flows!..."
    # Começe a Captura Inicial
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> Começando Captura incial!... <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <> "
    python ./capture.py
    echo " <> Captura incial finalizada!"

    # Começe a Materialização Inicial
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> Começando Materialização incial!...  <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> ..."
    python ./materialize.py
    echo " <> Materialização incial finalizada!"

    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> Resultados armazenados no PostgreSQL!... <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "

    echo " <> Configurando visualização..."
    # Espera input para baixar requisitos mostra de resultados
    # Instale os requisitos
    pip install -r requirements__view_results.txt
    # Pare qualquer processo estudando a porta local 8050
    stop_dash_process() {
        pid=$(lsof -t -i:8050)
        if [ -n "$pid" ]; then
            echo " <> Parando processo externo $pid devido à conflitos de porta..."
            kill $pid
            if kill -0 $pid > /dev/null 2>&1; then
                echo " <>  <> kill -9 (force kill)"
                kill -9 $pid
            fi

            echo " <> Configuração de visualização finalizada!"
        else
            echo " <> Configuração de visualização finalizada!"
        fi
    }
    stop_dash_process

    # Rode o app Dash para visualização básica dos dados no PostgreSQL
    python ./view_results.py &

    while true; do
        echo "<> Visualize os resultados! Visite localhost:8050 no browser de sua escolha."
        sleep 5
    done

    # Pergunte ao usuário se deseja baixar requisitos para visualizar resultados

    # Execute o Cronograma de Flows
    # echo "<> Executando Cronograma de Flows..."
    # python ./run.py &
fi
