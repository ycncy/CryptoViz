FROM python:3.10-alpine

WORKDIR /app

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY src src/
COPY config config/

COPY scheduled.py ./
COPY coin_market_cap_api_worker.py ./