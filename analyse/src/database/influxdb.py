from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDBConnector:
    def __init__(self, url, token, org, bucket):
        self.client = InfluxDBClient(url=url, token=token)
        self.org = org
        self.bucket = bucket
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write_data(self, data):
        self.write_api.write(bucket=self.bucket, org=self.org, record=data)
