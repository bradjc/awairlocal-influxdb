#!/usr/bin/env python3

import arrow
import datetime
import re
import sys
import time

import influxdb
from influxdb import line_protocol
import requests


CONFIG_FILE_PATH = "/etc/swarm-gateway/awairlocal.conf"
INFLUX_CONFIG_FILE_PATH = "/etc/swarm-gateway/influx.conf"

# Get awair device info
awair_config = {}
with open(CONFIG_FILE_PATH) as f:
    for l in f:
        fields = l.split("=")
        if len(fields) == 2:
            k = fields[0].strip()
            v = fields[1].strip()

            if k == "ipaddress":
                ips = v.split(",")
                ips = [ip.strip() for ip in ips]
                awair_config[k] = ips
            else:
                awair_config[k] = v

# Get influxDB config.
influx_config = {}
with open(INFLUX_CONFIG_FILE_PATH) as f:
    for l in f:
        fields = l.split("=")
        if len(fields) == 2:
            influx_config[fields[0].strip()] = fields[1].strip()


class AwairHttp:
    url_metadata = "http://{ipaddr}/settings/config/data"
    url_data = "http://{ipaddr}/air-data/latest"

    def __init__(self, ipaddr):
        self._ipaddr = ipaddr

    def get_metadata(self):
        url = self.url_metadata.format(ipaddr=self._ipaddr)
        r = requests.get(url)

        if r.status_code != 200:
            print("Could not get metadata for {}".format(self._ipaddr))
            return None

        data = r.json()

        device_id = data["wifi_mac"].lower()
        device_id = re.sub(r"\W+", "", device_id)

        metadata = {"awair_device_uuid": data["device_uuid"], "device_id": device_id}

        # We keep power status from this request to send as data.
        measurements = {}

        # If power data is available from the device, store it
        if "power-status" in data:
            if "battery" in data["power-status"]:
                measurements["battery_%"] = data["power-status"]["battery"]
            if "plugged" in data["power-status"]:
                measurements["plugged"] = data["power-status"]["plugged"]

        return (metadata, measurements)

    def get_data(self):
        url = self.url_data.format(ipaddr=self._ipaddr)
        r = requests.get(url)

        if r.status_code != 200:
            print("Could not get data for {}".format(self._ipaddr))
            return None

        data = r.json()

        mapping = {
            "score": "awair_score",
            "temp": "Temperature_°C",
            "humid": "Humidity_%",
            "co2": "co2_ppm",
            "voc": "voc_ppb",
            "pm25": "pm2.5_μg/m3",
            "lux": "Illumination_lx",
            "spl_a": "spl_a",
        }

        timestamp = data["timestamp"]

        out = {}

        # Save the measured data with correctly mapped names.
        for k, v in data.items():
            if k in mapping:
                out[mapping[k]] = v

        metadata = {
            "received_time": timestamp,
            "receiver": "awairlocal-influxdb",
            "gateway_id": "bunjee",
        }

        return (metadata, out)


# Data fetcher
fetchers = []
for ip_addr in awair_config["ipaddress"]:
    fetchers.append(AwairHttp(ip_addr))

# Influx DB client
client = influxdb.InfluxDBClient(
    influx_config["url"],
    influx_config["port"],
    influx_config["username"],
    influx_config["password"],
    influx_config["database"],
    ssl=True,
    gzip=True,
    verify_ssl=True,
)

counter = 1
general_metadata = None
general_data = None
saved = {}

points = []

while True:
    for fetcher in fetchers:
        if counter == 1:
            # On first item get the base metadata.
            general_metadata, general_data = fetcher.get_metadata()
            saved[fetcher] = {
                "general_metadata": general_metadata,
                "general_data": general_data,
            }

        # On every iteration get the sensor data.
        metadata, data = fetcher.get_data()

        # Create points and add to array.
        saved[fetcher]["general_metadata"].update(metadata)
        tags = general_metadata
        ts = time.time_ns()
        d = saved[fetcher]["general_data"].update(data)
        for k, v in data.items():
            point = {
                "measurement": k,
                "fields": {"value": v},
                "tags": tags,
                "time": ts,
            }
            points.append(point)

    # On the 6th actually publish and reset
    if counter % 6 == 0:
        # resp = client.write_points(points)
        # print("wrote points")

        data = {"points": points}

        data = influxdb.line_protocol.make_lines(data, None).encode("utf-8")
        params = {
            "db": client._database,
            "u": influx_config["username"],
            "p": influx_config["password"],
        }
        headers = client._headers.copy()

        client.request(
            url="gateway/write",
            method="POST",
            params=params,
            data=data,
            expected_response_code=204,
            headers=headers,
        )
        print("wrote 1 minute of awair data")

        counter = 1
        points = []

        time.sleep(10)
        continue

    counter += 1
    time.sleep(10)
