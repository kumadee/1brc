import argparse
import os
import sys


def calc_avg(file: str):
    stations = {}
    with open(file, "r") as measurements:
        for line in measurements:
            try:
                name, tmp = line.rstrip("\n").split(";")
                temp = float(tmp)
            except ValueError:
                pass
            if name not in stations:
                stations[name] = {"min": temp,
                                  "max": temp, "sum": temp, "count": 1}
            else:
                station = stations[name]
                if station["min"] > temp:
                    station["min"] = temp
                if station["max"] < temp:
                    station["max"] = temp
                station["sum"] += temp
                station["count"] += 1
    for name in sorted(stations):
        station = stations[name]
        avg = station["sum"] / station["count"]
        print(name, station["min"], "{:.1f}".format(
            avg), station["max"], sep=";")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Do 1brc challenge")
    parser.add_argument(
        "-f", "--file", help="path of the measurements.txt", required=True)
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print("FileNotFound:", args.file)
        sys.exit(1)

    calc_avg(args.file)
