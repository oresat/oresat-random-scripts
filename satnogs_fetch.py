#!/usr/bin/env python3
"""Script to fetch all data from satnogs db."""

import csv
import re
from zlib import crc32
from argparse import ArgumentParser
from time import sleep
from typing import Any

import requests

from oresat_configs import OreSatConfig, Mission


def get_data(data: list[dict[str, Any]], token: str, satellite: str) -> None:
    response = requests.get(
        'https://db.satnogs.org/api/telemetry/',
        headers={"Authorization": f"Token {token}"},
        params={
            'format': 'json',
            'is_decoded': 'true',
            'satellite': satellite,
        },
    )
    while True:
        if response.status_code == requests.codes.OK:
            content = response.json()
            results = content['results']
            data.extend(results)
            print(
                f'Retrieved {len(results):2} telemetry packets ({len(data):5} total), '
                f'from {results[0]["timestamp"]} to {results[-1]["timestamp"]}'
            )
            cursor = content['next']
            if cursor is None:
                break
        elif response.status_code == requests.codes.TOO_MANY_REQUESTS:
            detail = response.json()['detail']
            print(detail)
            m = re.fullmatch(
                r'Request was throttled. Expected available in (?P<time>\d+) seconds.', detail
            )
            if m is None:
                print('Unexpected response')
                break
            sleep(int(m['time']))
        else:
            print('Unexpected SatNOGS DB response:', response)
            print(response.json())
            break
        # Not documented as far as I can find but looking at the SatNOGS DB source we are limited
        # to 6/minute requests. We could burst and then hit the throttle response but this seems
        # more polite?
        sleep(10)
        response = requests.get(
            cursor,
            headers={"Authorization": f"Token {token}"},
        )


def parse_data(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    beacon_def = OreSatConfig(Mission.ORESAT0_5).beacon_def
    beacons = []
    for r in data:
        packet = {
            'timestamp': r['timestamp'],
            'observation_id': r['observation_id'],
            'observer': r['observer'],
            'station_id': r['station_id'],
            'app_source': r['app_source'],
        }
        msg = bytes.fromhex(r['frame'])
        body = msg[16:-4]  # ax.25 header : body : crc
        crc = int.from_bytes(msg[-4:], 'little')

        if crc32(body, 0) != crc:
            print(f"Packet in obseration {r['observation_id']} ({r['timestamp']}) has invalid CRC")
            continue

        offset = 0
        for obj in beacon_def:
            try:
                size = obj.STRUCT_TYPES[obj.data_type].size
            except KeyError:
                # For strings and the like
                size = len(obj.value)
            value = obj.decode_raw(body[offset : offset + size])
            offset += size

            name = obj.name
            if hasattr(obj.parent, 'name'):
                name = f"{obj.parent.name}_{name}"

            if obj.bit_definitions:
                for k, bits in obj.bit_definitions.items():
                    packet[f'{name}_{k.lower()}'] = bool(value & (1 << bits[0]))
            else:
                if obj.unit:
                    name += f' ({obj.unit})'
                value = obj.value_descriptions.get(value, value)
                packet[name] = value
        packet['crc32'] = crc
        beacons.append(packet)
    return beacons


if __name__ == "__main__":
    parser = ArgumentParser(
        description=(
            "Download all beacon data from SatNOGS, decode it, and save it to a csv. "
            "Due to rate limits beacons are fetched at 150/minute"
        ),
        epilog="Ctrl+c during beacon download will write out beacons fetched to that point",
    )
    parser.add_argument(
        "token",
        help=(
            "Your personal SatNOGS DB API token. Find your token by creating an account at "
            "db.satnogs.org"
        ),
    )
    parser.add_argument(
        "-f", "--file", help="CSV file name. Default '%(default)s'", default="beacons.csv"
    )
    parser.add_argument(
        "-s",
        "--satellite",
        help="Satellite NORAD ID. Default %(default)s (OreSat0.5)",
        default=60525,
    )
    args = parser.parse_args()

    data: list[dict[str, Any]] = []
    try:
        get_data(data, args.token, args.satellite)
    except KeyboardInterrupt:
        pass
    beacons = parse_data(data)

    with open(args.file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=beacons[0].keys())
        writer.writeheader()
        for beacon in beacons:
            writer.writerow(beacon)
