FROM python:3.11-slim

RUN pip install uv

WORKDIR /app

COPY . /app

RUN uv pip install --system -e .

EXPOSE 5000-5009

CMD ["distbench", "help"]
