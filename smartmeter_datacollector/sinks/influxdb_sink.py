from configparser import SectionProxy
from dataclasses import dataclass

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from ..smartmeter.meter_data import MeterDataPoint
from .data_sink import DataSink


@dataclass
class InfluxdbConfig:
    url: str
    token: str
    org: str
    bucket: str

    @staticmethod
    def from_sink_config(config: SectionProxy) -> "InfluxdbConfig":
        influxdb_config = InfluxdbConfig(
            config.get("url"),
            config.get("token"),
            config.get("org"),
            config.get("bucket")
        )
        return influxdb_config


class InfluxdbSink(DataSink):
    def __init__(self, config: InfluxdbConfig) -> None:
        self._client = InfluxDBClient(
            url=config.url,
            token=config.token,
            org=config.org
        )

        self._bucket = config.bucket
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, data_point: MeterDataPoint) -> None:
        try:
            point = Point(data_point.type.identifier).field(data_point.type.unit, data_point.value)
            self._write_api.write(bucket=self._bucket, org=self._client.org, record=point)
        except:
            print("Error submitting locally")
