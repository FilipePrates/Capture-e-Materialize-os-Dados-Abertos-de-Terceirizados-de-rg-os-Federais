# Stage 1: Build the environment
FROM python:3.10-slim AS build

# Install system dependencies
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Install Prefect and other dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy .env.example to .env
COPY .env.example /app/.env

# Stage 2: Runtime environment
FROM python:3.10-slim

# Copy Python dependencies from the build stage
COPY --from=build /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=build /usr/local/bin/prefect /usr/local/bin/prefect

# Copy .env file
COPY --from=build /app/.env /app/.env

# Prefect server start command
CMD ["prefect", "server", "start"]

# Expose the necessary ports for Prefect Server
EXPOSE 4200 8080 8081