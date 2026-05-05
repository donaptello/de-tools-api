FROM python:3.10-slim-buster

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip

RUN apt-get update

RUN apt-get -y install gcc libpq5 libpq-dev

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt --timeout 300

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--port 8567"]