import logging
from configparser import SectionProxy
from dataclasses import dataclass
import requests

from PyP100 import PyP110

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from ..smartmeter.meter_data import MeterDataPoint
from .data_sink import DataSink

# Disable "too general exception" warning
# pylint: disable=W0718

LOGGER = logging.getLogger("sink")


@dataclass
class InfluxdbP110Config:
    url: str
    token: str
    org: str
    bucket: str

    tapo_ip: str
    tapo_user: str
    tapo_pw: str

    hc_url: str

    @staticmethod
    def from_sink_config(config: SectionProxy) -> "InfluxdbP110Config":
        influxdb_config = InfluxdbP110Config(
            config.get("url"),
            config.get("token"),
            config.get("org"),
            config.get("bucket"),
            config.get("tapo_ip"),
            config.get("tapo_user"),
            config.get("tapo_pw"),
            config.get("hc_url"),
        )

        return influxdb_config


class InfluxdbP110Sink(DataSink):
    def __init__(self, config: InfluxdbP110Config) -> None:
        self._client = InfluxDBClient(
            url=config.url,
            token=config.token,
            org=config.org,
        )

        self._org = config.org
        self._bucket = config.bucket
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

        self._tapo_ip = config.tapo_ip
        self._tapo_user = config.tapo_user
        self._tapo_pw = config.tapo_pw

        self._hc_url = config.hc_url

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, data_point: MeterDataPoint) -> None:
        try:
            point = Point(data_point.type.identifier)
            point.tag("unit", data_point.type.unit)
            point.field("value", data_point.value)
            point.time(data_point.timestamp)

            self._write_api.write(bucket=self._bucket, org=self._org, record=point)

            if data_point.type.identifier == "ACTIVE_POWER_P":
                self.send_smartplug_data(data_point.timestamp)
                self.send_hc_ping()

        except Exception as e:
            LOGGER.error("Error submitting smartmeter data to InfluxDB: %s", e)

    def send_smartplug_data(self, timestamp):
        try:
            p110 = PyP110.P110(self._tapo_ip, self._tapo_user, self._tapo_pw)
            usage_dict = p110.getEnergyUsage()
        except Exception as e:
            LOGGER.error("Failed to communicate with device %s: %s", p110.address, e)
            return

        if usage_dict["current_power"] is None or usage_dict["today_energy"] is None:
            LOGGER.error(
                "Some data is missing: power=%d, energy=%d",
                usage_dict["current_power"],
                usage_dict["today_energy"],
            )
            return

        current_power = int(usage_dict["current_power"] / 1000)
        today_energy = int(usage_dict["today_energy"])

        point1 = (
            Point("SOLAR_PANEL")
            .tag("unit", "W")
            .field("value", int(current_power))
            .time(timestamp)
        )
        point2 = (
            Point("SOLAR_PANEL_DAILY_ENERGY")
            .tag("unit", "Wh")
            .field("value", int(today_energy))
            .time(timestamp)
        )

        try:
            self._write_api.write(bucket=self._bucket, org=self._org, record=point1)
            self._write_api.write(bucket=self._bucket, org=self._org, record=point2)
        except Exception as e:
            LOGGER.error("Error submitting smartplug data to InfluxDB: %s", e)

    def send_hc_ping(self):
        try:
            r = requests.get(self._hc_url, timeout=1)

            if r.status_code != 200:
                raise Warning(f"returned {r} (should be 200 OK)")

        except Exception as e:
            LOGGER.error("Pinging Healthchecks.io failed: %s", e)
