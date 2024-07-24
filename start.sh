#!/bin/bash

# Confirmar execução de Servidor Prefect e
# Captura e Materialização dos Dados Abertos de Terceirizados de Órgãos Federais

# Pergunte ao usuário se deseja baixar requisitos para realizar captura
read -p "<> Deseja criar ambiente virtual e baixar os requisitos:
prefect & dbt-core & dbt-postgres & requests & bs4 & pandas & openpyxl & datetime & psycopg2-binary & python-dotenv
<> para realizar a captura e materialização dos Dados Abertos de Terceirizados de Órgãos Federais <> ? (y/n): " run
if [[ "$run" == "y" || "$run" == "Y" || "$run" == "yes" || "$run" == "Yes" || "$run" == "s" || "$run" == "S" || "$run" == "sim" || "$run" == "Sim" ]]; then

    echo " <> Começando."
    # Crie o ambiente virtual python do orquestrador prefect
    rmdir orchestrator
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
    echo " <> Começando os Flows"
    # Começe a Captura Inicial
    echo " <> Começando Captura incial..."
    python ./capture.py
    echo " <> Captura incial finalizada..."
    # Começe a Materialização Inicial
    echo " <> Começando Materialização incial..."
    python ./materialize.py
    echo " <> Materialização incial finalizada..."

    echo " <> Resultados prontos!..."
    # Espera input para baixar requisitos mostra de resultados
    while true; do
        echo " <> Deseja criar ambiente virtual e baixar os requisitos:
         prefect & dbt-core & dbt-postgres & requests & bs4 & pandas & openpyxl & datetime & psycopg2-binary & python-dotenv
         para realizar a captura e materialização dos Dados Abertos de Terceirizados de Órgãos Federais ? (y/n):"
        read -t 1 -n 1 view_results
        if [[ "$view_results" == "y" || "$view_results" == "Y" || "$view_results" == "yes" || "$view_results" == "Yes" || "$view_results" == "s" || "$view_results" == "S" || "$view_results" == "sim" || "$view_results" == "Sim" ]]; then
            echo " <> Começando"

            pip install sqlalchemy dash

            echo "<> Visualizando Resultados! Visite localhost:8050 no browser de sua escolha."
            python ./view_results.py &
            break
        fi
        if [[ "$view_results" == "n" || "$view_results" == "N" || "$view_results" == "no" || "$view_results" == "No" || "$view_results" == "nao" || "$view_results" == "Não" || "$view_results" == "não" || "$view_results" == "NAO" ]]; then
            echo "Ok.. <>"
            break
        fi
    done

    # Pergunte ao usuário se deseja baixar requisitos para visualizar resultados

    # Execute o Cronograma de Flows
    # echo "<> Executando Cronograma de Flows..."
    # python ./run.py &
fi
