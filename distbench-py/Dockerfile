FROM python:3.11-slim

# Install uv for faster package management
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY distbench/ ./distbench/

# Install dependencies
RUN uv pip install --system -e .

# Default command (will be overridden by docker-compose)
CMD ["python", "-m", "distbench.main"]
