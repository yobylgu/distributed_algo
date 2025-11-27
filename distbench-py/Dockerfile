# Dockerfile for distbench-py framework
# Provides a containerized environment for running distributed algorithms

FROM python:3.11-slim

# Install system dependencies needed for Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY distbench/ ./distbench/
COPY README.md GUIDE.md ./

# Install uv package manager
RUN pip install --no-cache-dir uv

# Install project dependencies using uv
RUN uv sync --frozen

# Set Python path to include the app directory
ENV PYTHONPATH=/app:$PYTHONPATH

# Default command (can be overridden in docker-compose)
# Expects NODE_ID, CONFIG_FILE, ALGORITHM environment variables
CMD uv run distbench \
    -c ${CONFIG_FILE:-/config/dolev.yaml} \
    -a ${ALGORITHM:-dolev} \
    --mode network \
    --id ${NODE_ID} \
    -v
