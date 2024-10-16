import os

import psycopg2
from kafka_worker import KafkaWorker


class CoinMarketCapAPIWorker(KafkaWorker):
    def __init__(self):
        super().__init__(topic_name="raw_data.coin_market_cap.api")
        self.conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host="gaia-dev.msc-projects.me",
            port="5432"
        )
        self.cursor = self.conn.cursor()

    def create_table(self):
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS coin_market_cap (
            currency VARCHAR(10),
            name VARCHAR(255),
            circulating_supply FLOAT,
            total_supply FLOAT,
            max_supply FLOAT,
            price FLOAT,
            volume_24 FLOAT,
            market_cap FLOAT,
            percent_change_1h FLOAT,
            percent_change_24h FLOAT,
            percent_change_7d FLOAT,
            percent_change_30d FLOAT,
            percent_change_60d FLOAT,
            percent_change_90d FLOAT,
            percent_change_1y FLOAT,
            timestamp TIMESTAMP
        )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def process_data(self, raw_data):
        print("Processing data from raw_data.coin_market_cap.api")
        data_timestamp = raw_data['timestamp']

        for coin_data in raw_data["data"]:
            try:
                insert_query = '''
                INSERT INTO coin_market_cap (currency, name, circulating_supply, total_supply, max_supply, price, volume_24, market_cap, percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d, percent_change_1y, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
                '''
                values = (
                    coin_data["symbol"],
                    str(coin_data["name"]),
                    float(coin_data["circulating_supply"]) if coin_data["circulating_supply"] is not None else None,
                    float(coin_data["total_supply"]) if coin_data["total_supply"] is not None else None,
                    float(coin_data["max_supply"]) if coin_data["max_supply"] is not None else None,
                    float(coin_data["quotes"][0]["price"]),
                    float(coin_data["quotes"][0]["volume_24h"]),
                    float(coin_data["quotes"][0]["market_cap"]),
                    float(coin_data["quotes"][0]["percent_change_1h"]),
                    float(coin_data["quotes"][0]["percent_change_24h"]),
                    float(coin_data["quotes"][0]["percent_change_7d"]),
                    float(coin_data["quotes"][0]["percent_change_30d"]),
                    float(coin_data["quotes"][0]["percent_change_60d"]),
                    float(coin_data["quotes"][0]["percent_change_90d"]),
                    float(coin_data["quotes"][0]["percent_change_1y"]),
                    data_timestamp
                )

                self.cursor.execute(insert_query, values)
                self.conn.commit()
                print(f"Inserted data from raw_data.coin_market_cap.api for currency: {coin_data['symbol']}")
            except Exception as e:
                print(f"Failed to insert data for currency: {coin_data['symbol']}, error: {str(e)}")
                self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    print("Starting CoinMarketCapAPIWorker")
    worker = CoinMarketCapAPIWorker()
    worker.create_table()
    worker.read_from_kafka()
    worker.close()
