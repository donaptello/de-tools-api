FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip

RUN apt-get update && apt-get install -y \
    gcc \
    libpq5 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt --timeout 300

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8567"]