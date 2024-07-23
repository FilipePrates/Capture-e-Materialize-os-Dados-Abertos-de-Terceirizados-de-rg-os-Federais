# Desafio Escritório de Dados
## 
1. :
   ```sh
   python -m venv orchestrator
   ```
2. :
   ```sh
   source orchestrator/bin/activate
   ```
3. :
   ```sh
   pip install -r requirements.txt
   ```
4. :
   ```sh
   prefect server start
   ```
Em outra janela do terminal:
5. : 
   ```sh
   prefect agent local start --label default
   ```
Em outra janela do terminal:
5. :
   ```sh
   python ./run.py
   ```


## Rode em containers com docker
1. Garanta que tenha docker e docker-compose no seu sistema:
   ```sh
   sudo apt-get install -y docker.io
   sudo systemctl start docker
   sudo systemctl enable docker
   sudo groupadd docker
   sudo usermod -aG docker $USER
   newgrp docker
   groups $USER

   sudo apt-get install -y docker-compose
   ```
   (ou?)

   sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
   sudo chmod +x /usr/local/bin/docker-compose

2. Construa a imagem e execute os containers
   ```sh
   docker-compose up --build
   ```

### Project Orchestrator Setup

Follow these steps to set up your project environment and run the application:

1. :
   ```sh
   python -m venv orchestrator
   ```


### Config Prefect Server
1. :
   ```sh
   prefect backend server
   ```

2. :
   ```sh
   prefect server start
   ```