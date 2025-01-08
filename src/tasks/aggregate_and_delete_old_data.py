from sqlalchemy import text

from config.logging import logger


def aggregate_old_data(session):
    try:
        aggregate_query = """
        INSERT INTO crypto_data_history (record_datetime, name, symbol, price, volume, market_cap, source)
        SELECT
            time_bucket('1 hour', timestamp) + interval '1 hour' AS hour_bucket,
            name,
            currency AS symbol,
            price,
            volume_24 AS volume,
            market_cap,
            source
        FROM (
                 SELECT
                     DISTINCT ON (time_bucket('1 hour', timestamp), currency)
                     timestamp,
                     name,
                     currency,
                     price,
                     volume_24,
                     market_cap,
                     source
                 FROM
                     raw_crypto_data
                 WHERE
                     timestamp >= NOW() - INTERVAL '1 day'
                 ORDER BY
                     time_bucket('1 hour', timestamp), currency, timestamp DESC
             ) AS subquery
        ORDER BY
            hour_bucket;

        INSERT INTO ohlc_history (record_datetime, name, symbol, open, high, low, close)
        SELECT
            time_bucket('1 hour', timestamp) + interval '1 hour' AS hour_bucket,
            name,
            currency AS symbol,
            FIRST(price) WITHIN GROUP (ORDER BY timestamp ASC) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            LAST(price) WITHIN GROUP (ORDER BY timestamp ASC) AS close
        FROM
            raw_crypto_data
        WHERE
            timestamp >= NOW() - INTERVAL '1 day'
        GROUP BY
            hour_bucket, name, currency
        ORDER BY
            hour_bucket, name, currency;
        """

        session.execute(text(aggregate_query))
        session.commit()

        logger.info("Data aggregated successfully for daily intervals.")

    except Exception as e:
        session.rollback()
        logger.info(f"Error executing job: {e}")

    finally:
        session.close()


def delete_old_data(session):
    try:
        delete_query = """
        DELETE FROM raw_crypto_data
        WHERE timestamp < NOW() - INTERVAL '14 day'
        
        DELETE FROM raw_ohlc_data
        WHERE timestamp < NOW() - INTERVAL '14 day'
        """

        result = session.execute(text(delete_query))
        rows_deleted = result.rowcount

        session.commit()

        logger.info(f"Deleted {rows_deleted} rows older than 14 days.")

    except Exception as e:
        session.rollback()
        logger.error(f"Error deleting old data: {e}")

    finally:
        session.close()