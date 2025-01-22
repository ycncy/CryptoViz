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

        WITH aggregated_data AS (
            SELECT
                time_bucket('1 hour', record_datetime) + interval '1 hour' AS hour_bucket,
                name,
                symbol,
                MIN(record_datetime) AS first_time,
                MAX(record_datetime) AS last_time,
                MAX(high) AS high,
                MIN(low) AS low
            FROM
                raw_ohlc_data
            WHERE
                record_datetime >= NOW() - INTERVAL '1 day'
            GROUP BY
                hour_bucket, name, symbol
        ),
        first_last_data AS (
            SELECT DISTINCT ON (time_bucket('1 hour', record_datetime), name, symbol)
                time_bucket('1 hour', record_datetime) + interval '1 hour' AS hour_bucket,
                name,
                symbol,
                record_datetime,
                open,
                close
            FROM
                raw_ohlc_data
            WHERE
                record_datetime >= NOW() - INTERVAL '1 day'
            ORDER BY
                time_bucket('1 hour', record_datetime), name, symbol, record_datetime ASC
        )
        INSERT INTO ohlc_history (record_datetime, name, symbol, open, high, low, close)
        SELECT
            agg.hour_bucket,
            agg.name,
            agg.symbol,
            COALESCE(fl.open, 0) AS open,
            agg.high,
            agg.low,
            COALESCE(fl.close, 0) AS close
        FROM
            aggregated_data agg
        LEFT JOIN first_last_data fl
        ON
            agg.name = fl.name AND
            agg.symbol = fl.symbol AND
            (fl.record_datetime = agg.first_time OR fl.record_datetime = agg.last_time)
        ORDER BY
            agg.hour_bucket, agg.name, agg.symbol;
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
        WHERE timestamp < NOW() - INTERVAL '14 day';
        
        DELETE FROM raw_ohlc_data
        WHERE record_datetime < NOW() - INTERVAL '14 day'
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