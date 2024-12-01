from sqlalchemy import text

from config.logging import logger


def aggregate_and_delete_old_data(session):
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
        """

        session.execute(text(aggregate_query))

        rows_deleted = 1
        while rows_deleted > 0:
            delete_query = """
            DELETE FROM raw_crypto_data
            WHERE timestamp >= NOW() - INTERVAL '1 day'
            """

            result = session.execute(text(delete_query))
            rows_deleted = result.rowcount

            session.commit()

            logger.info(f"Deleted {rows_deleted} rows in this batch.")

        logger.info("All old data deleted successfully.")

        logger.info("Data aggregated and old data deleted successfully.")

    except Exception as e:
        session.rollback()
        logger.info(f"Error executing job: {e}")

    finally:
        session.close()
