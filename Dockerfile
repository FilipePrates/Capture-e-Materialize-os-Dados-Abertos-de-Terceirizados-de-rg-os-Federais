# Use the python:3.9-slim image as the base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install pip, curl, docker-compose, and development packages
RUN apt-get update && \
    apt-get install -y python3-pip libpq-dev gcc libffi-dev curl docker-compose && \
    apt-get clean

# Install the dependencies
RUN pip3 install --no-cache-dir -r requirements/start.txt
RUN pip3 install --no-cache-dir -r requirements/results.txt

# Expose necessary ports
EXPOSE 8080 4200 8050

# Set environment variables for Prefect server
ENV PREFECT__BACKEND=server
ENV PREFECT__SERVER__HOST=http://localhost

# Start the Prefect server and agent
CMD prefect backend server && docker network create prefect-server && prefect server start