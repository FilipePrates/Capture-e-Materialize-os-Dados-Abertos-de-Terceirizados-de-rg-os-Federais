#!/bin/bash
# Script de Start para Captura e Materialização dos Dados Abertos de Terceirizados de Órgãos Federais
# by Filipe for Escritório de Dados.

echo " <> Start Servidor Prefect..."
prefect backend server
docker network create prefect-server
# Start Prefect Server
prefect server start &

# docker-compose up postgres &
# sleep 1
# # Wait until postgres is fully up and healthy
# docker-compose up hasura &
# docker-compose up graphql &
# docker-compose up apollo &

# total_seconds=90 # Increase this to give the server more time to start
# for ((i=total_seconds; i>0; i--))
# do
#     echo " <> Começando os Flows em $i segundos..."
#     sleep 1
# done

# # Check if the Prefect server is running

# echo " <> Servidor Prefect iniciado!"

# # Create the Prefect project
# echo " <> Criando o projeto Prefect (adm_cgu_terceirizados)..."
# if prefect create project adm_cgu_terceirizados; then
#     echo " <> Projeto criado com sucesso!"
# else
#     echo " <> Projeto já existe ou falha ao criar projeto"
# fi

# echo " <> Começando os Flows!..."

# # Realize a Captura Inicial
# echo " <>  <>  <>  <>  <>  <>  <>  <>  <> "
# echo " <> Começando Captura incial!... <> "
# echo " <>  <>  <>  <>  <>  <>  <>  <>  <> "
# python ./run/capture.py
# echo " <> Captura incial finalizada!"

# # Realize a Materialização Inicial
# echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
# echo " <> Começando Materialização incial!...  <> "
# echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
# python ./run/materialize.py
# echo " <> Materialização incial finalizada!"

# # Comemore objetivo concluído
# echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
# echo " <> Resultados armazenados no PostgreSQL!... <> "
# echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
# sleep 3

# # Redirecione para visualização em Dash
# echo " <> Configurando visualização..."
# stop_dash_process() {
#     pid=$(lsof -t -i:8050)
#     if [ -n "$pid" ]; then
#         echo " <> Parando processo externo $pid devido à conflitos de porta..."
#         kill $pid
#         if kill -0 $pid > /dev/null 2>&1; then
#             echo " <>  <> kill -9 (force kill)"
#             kill -9 $pid
#         fi

#         echo " <> Configuração de visualização finalizada!"
#     else
#         echo " <> Configuração de visualização finalizada!"
#     fi
# }
# stop_dash_process
# python ./run/results.py &
# while true; do
#     echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
#     echo " <>                                                                              <> "
#     echo " <>  Visualize os resultados!                                                    <> "
#     echo " <>                             Visite localhost:8050 no browser de sua escolha! <> "
#     echo " <>                                                                              <> "
#     echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
#     echo " <>                                                                              <> "
#     echo " <>     \"python ./run/scheduler.py\" para programar as próximas capturas.       <> "
#     echo " <>                                                                              <> "
#     echo " <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <>  <> "
#     sleep 5
# done
