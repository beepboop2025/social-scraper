FROM python:3.12-slim

WORKDIR /app

# Install system deps for spaCy and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements-platform.txt .
RUN pip install --no-cache-dir -r requirements-platform.txt
RUN python -m spacy download en_core_web_sm

COPY . .

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
