#!/usr/bin/env python3
"""Quick script to fetch all data from satnogs db."""

import zlib
from argparse import ArgumentParser
from time import sleep

import canopen
import requests
from oresat_configs import OreSatConfig, OreSatId

OUT_FILE = "beacons.csv"
SAT_ID = "DKCD-1609-0567-7056-3922"  # OreSat0.5
URL = (
    f"https://db.satnogs.org/api/telemetry/?format=json&is_decoded=true&sat_id={SAT_ID}"
)

DATA_TYPE_SIZE = {
    canopen.objectdictionary.datatypes.BOOLEAN: 1,
    canopen.objectdictionary.datatypes.INTEGER8: 1,
    canopen.objectdictionary.datatypes.INTEGER16: 2,
    canopen.objectdictionary.datatypes.INTEGER32: 4,
    canopen.objectdictionary.datatypes.UNSIGNED8: 1,
    canopen.objectdictionary.datatypes.UNSIGNED16: 2,
    canopen.objectdictionary.datatypes.UNSIGNED32: 4,
    canopen.objectdictionary.datatypes.REAL32: 4,
    canopen.objectdictionary.datatypes.REAL64: 8,
    canopen.objectdictionary.datatypes.INTEGER64: 8,
    canopen.objectdictionary.datatypes.UNSIGNED64: 8,
}

beacon_def = OreSatConfig(OreSatId.ORESAT0_5).beacon_def

data = []


def get_data(url: str, token: str):
    response = requests.get(url, headers={"Authorization": f"Token {token}"})
    r_json = response.json()
    if response.status_code == 401:
        print(r_json["detail"])
        return

    if "results" not in r_json:
        print(r_json["detail"], "\n")
        tmp = r_json["detail"].split(" ")
        sleep(int(tmp[-2]) + 1)
        return get_data(url, token)

    for r in r_json["results"]:
        row = f'{r["timestamp"]},{r["observation_id"]},{r["observer"]},'
        row += f'{r["station_id"]},{r["app_source"]},'
        frame = r["frame"]
        msg = bytes.fromhex(frame)

        crc32_calc = zlib.crc32(msg[16:-4], 0).to_bytes(4, "little")
        if crc32_calc != msg[-4:]:
            print("invalid crc32\n")
            continue

        offset = 16  # skip ax25 header
        for obj in beacon_def:
            size = DATA_TYPE_SIZE.get(obj.data_type, 0)
            if size == 0:
                size = len(obj.value)
            value = obj.decode_raw(msg[offset : offset + size])
            if obj.bit_definitions:
                for i in obj.bit_definitions.values():
                    row += f"{bool(value & (1 << i))},"
            else:
                value = obj.value_descriptions.get(value, value)
                row += f"{value},"
            offset += size
        row += f'{int.from_bytes(msg[-4:], "little")}\n'
        print(len(data) + 1, row)
        data.append(row)

    sleep(0.2)  # don't abuse the api
    if r_json["next"]:
        get_data(r_json["next"], token)


def main():
    parser = ArgumentParser(
        "download all beacon data from satnogs, decode it, and save it to a csv"
    )
    parser.add_argument("token", help="satnogs db api token")
    args = parser.parse_args()

    try:
        get_data(URL, args.token)
    except KeyboardInterrupt:
        pass

    header = "timestamp,observation_id,observer,station_id,app_source,"
    for obj in beacon_def:
        name = obj.name
        if not isinstance(obj.parent, canopen.ObjectDictionary):
            name = f"{obj.parent.name}_{name}"
        if obj.bit_definitions:
            for v in obj.bit_definitions.keys():
                header += f"{obj.name}_{v.lower()},"
        else:
            if obj.unit:
                name += f" ({obj.unit})"
            header += f"{name},"
    header += "crc32\n"

    lines = [header] + list(reversed(data))

    with open(OUT_FILE, "w") as f:
        f.writelines(lines)


if __name__ == "__main__":
    main()
