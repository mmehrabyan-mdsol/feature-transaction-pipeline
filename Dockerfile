FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /feature-transaction-pipeline

# Install build essentials for psycopg2 and other native deps
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy dependency files first
COPY requirements.txt pyproject.toml ./
COPY data /feature-transaction-pipeline/data
# Upgrade pip/setuptools/wheel
RUN pip install --upgrade pip setuptools wheel

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# Copy source code
COPY ./src /feature-transaction-pipeline/src

# Fix PYTHONPATH so Dagster can find your code
ENV PYTHONPATH=/feature-transaction-pipeline/src

EXPOSE 4003

# Run Dagster gRPC server
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4003", "--module-name", "feature_transaction_pipeline.definitions"]
