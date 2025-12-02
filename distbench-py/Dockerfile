FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 distbench

WORKDIR /app

COPY pyproject.toml README.md* ./

COPY distbench ./distbench
COPY algorithms ./algorithms

RUN pip install --no-cache-dir -e .

COPY distbench ./distbench
COPY algorithms ./algorithms

RUN chown -R distbench:distbench /app

USER distbench
ENTRYPOINT ["python", "-m", "distbench.main"]
