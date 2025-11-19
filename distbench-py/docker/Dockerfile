FROM python:3.11-slim

# Disable uv’s resolver for PyPI
ENV UV_NO_INDEX=1

# Install uv itself
RUN pip install uv

WORKDIR /app

# Copy the entire project
COPY . /app

# Install the project WITHOUT talking to the internet
RUN uv pip install --system .

CMD ["bash"]
