-- Activer l'extension TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Créer la table raw_crypto_data
CREATE TABLE raw_crypto_data (
    currency           VARCHAR(10)              NOT NULL,
    timestamp          TIMESTAMP WITH TIME ZONE NOT NULL,
    source             VARCHAR(50),
    name               VARCHAR(255),
    circulating_supply DOUBLE PRECISION,
    total_supply       DOUBLE PRECISION,
    max_supply         DOUBLE PRECISION,
    price              DOUBLE PRECISION,
    volume_24          DOUBLE PRECISION,
    market_cap         DOUBLE PRECISION,
    percent_change_1h  DOUBLE PRECISION,
    percent_change_24h DOUBLE PRECISION,
    percent_change_7d  DOUBLE PRECISION,
    percent_change_30d DOUBLE PRECISION,
    percent_change_60d DOUBLE PRECISION,
    percent_change_90d DOUBLE PRECISION,
    percent_change_1y  DOUBLE PRECISION,
    PRIMARY KEY (currency, timestamp)
);

-- Créer la table hypertable pour raw_crypto_data
SELECT create_hypertable('raw_crypto_data', 'timestamp');

-- Créer la table crypto_data_history
CREATE TABLE crypto_data_history (
    record_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    name            TEXT                     NOT NULL,
    symbol          TEXT                     NOT NULL,
    price           NUMERIC,
    volume          NUMERIC,
    market_cap      NUMERIC
);

-- Créer la table hypertable pour crypto_data_history
SELECT create_hypertable('crypto_data_history', 'record_datetime');