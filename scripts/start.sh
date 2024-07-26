#!/bin/bash
# Script de Start para Captura e Materialização dos Dados Abertos de Terceirizados de Órgãos Federais
# by Filipe for Escritório de Dados.

# Pergunte ao usuário se deseja baixar requisitos para realizar captura
read -p "<> Deseja criar ambiente virtual e baixar os requisitos?
<> para realizar a captura e materialização dos Dados Abertos de Terceirizados de Órgãos Federais <> ? (y/n): " run
if [[ "$run" == "y" || "$run" == "Y" || "$run" == "yes" || "$run" == "Yes" || "$run" == "s" || "$run" == "S" || "$run" == "sim" || "$run" == "Sim" ]]; then

    echo " <> Começando."
    # Remova o diretório 'orchestrator', se ele existir, evitando conflitos
    if [ -d "orchestrator" ]; then
        rm -rf "orchestrator"
    fi
    # Configure o ambiente virtual python para o orquestrador Servidor Prefect
    python -m venv orchestrator
    source orchestrator/bin/activate
    pip install -r requirements/start.txt
    cp .env.example .env

    # Levante o Servidor Prefect
    echo " <> Start Servidor Prefect..."
    prefect backend server
    prefect server start &

    total_seconds=25 # Aumente este número caso Servidor não esteja ainda preparado quando se tenta criar o projeto
    for ((i=total_seconds; i>0; i--))
    do
        echo " <> Começando os Flows em $i segundos..."
        sleep 1
    done

    # Crie o projeto Prefect
    echo " <> Criando o projeto Prefect (adm_cgu_terceirizados)..."
    prefect create project adm_cgu_terceirizados || echo "Project 'adm_cgu_terceirizados' already exists"
    sleep 2
    echo " <> Começando os Flows!..."

    # Realize a Captura Inicial
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> Começando Captura incial!... <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <> "
    python ./run/capture.py
    echo " <> Captura incial finalizada!"

    # Realize a Materialização Inicial
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> Começando Materialização incial!...  <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    python ./run/materialize.py
    echo " <> Materialização incial finalizada!"
    
    # Comemore objetivo concluído
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> Resultados armazenados no PostgreSQL!... <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    sleep 3

    # Redirecione para visualização em Dash
    echo " <> Configurando visualização..."
    pip install -r requirements/results.txt
    stop_port_8050_process() {
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
    stop_port_8050_process
    python ./run/results.py &

    show_view_results() {
        echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
        echo " <>                                                                              <> "
        echo " <>  Visualize os resultados!                                                    <> "
        echo " <>                             Visite localhost:8050 no browser de sua escolha! <> "
        echo " <>                                                                              <> "
        echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
        echo " <>                                                                              <> "
        echo " <>     \"python ./run/scheduler.py\" para programar as próximas capturas.       <> "
        echo " <>                                                                              <> "
        echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    }
    show_view_results
    sleep 10
    # Realize a Captura Histórica
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>   <> "
    echo " <> Começando Captura Histórica!...   <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>   <> "
    python ./run/historic_capture.py
    echo " <> Captura Histórica finalizada!"
    show_view_results
    sleep 10
    # Realize a Materialização Histórica
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    echo " <> Começando Materialização Histórica!...   <> "
    echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
    python ./run/historic_materialize.py
    echo " <> Materialização Histórica finalizada!"

fi
