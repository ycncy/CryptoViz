FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./src /app/src

ENV PYTHONPATH="/app/src:${PYTHONPATH}"

CMD ["python", "src/workers/coin_market_cap_api_worker.py"]
