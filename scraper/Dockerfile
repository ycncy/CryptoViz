FROM python:3.10

WORKDIR /scraper

RUN apt-get update \
    && pip install --upgrade pip \
    && apt-get install -y git build-essential

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY src src/
COPY scheduled.py ./