import argparse
# import aiofiles
# import asyncio
import os
import sys
import polars as pl
import platform

"""
async def async_calc_avg(file: str):
    stations = {}
    async with aiofiles.open(file, 'r') as measurements:
        async for line in measurements:
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
    return stations
"""


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
                stations[name] = [temp, temp, temp, 1]
            else:
                station = stations[name]
                if station[0] > temp:
                    station[0] = temp
                if station[1] < temp:
                    station[1] = temp
                station[2] += temp
                station[3] += 1
    return stations


def solution(file: str, is_async: bool):
    stations = {}
    if is_async:
        stations = pl.scan_csv(
            file, has_header=False, separator=";", new_columns=['station', 'temp']
        ).filter(pl.col('station').is_not_null()).group_by('station').agg(
            pl.col('temp').min().alias('min'),
            pl.col('temp').mean().alias('mean'),
            pl.col('temp').max().alias('max')).sort('station').collect()
        python_impl = platform.python_implementation().lower()
        stations.write_csv(
            f"out-{python_impl}.txt", include_header=False, separator=";", float_precision=1)
    else:
        stations = calc_avg(file)
        for name in sorted(stations):
            station = stations[name]
            avg = station[2] / station[3]
            print(name, station[0], "{:.1f}".format(
                avg), station[1], sep=";")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Do 1brc challenge")
    parser.add_argument(
        "-f", "--file", help="path of the measurements.txt", required=True)
    parser.add_argument('--is-async', help="Do IO asynchronously",
                        action="store_true")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print("FileNotFound:", args.file)
        sys.exit(1)

    solution(args.file, args.is_async)
