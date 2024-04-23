import argparse
import os
import sys
import polars as pl
import platform


def solution(file: str, is_lazy: bool):
    stations = None
    columns = ['station', 'temp']
    if is_lazy:
        stations = pl.scan_csv(
            file, has_header=False, separator=";", new_columns=columns
        ).filter(pl.col('station').is_not_null()).group_by('station').agg(
            pl.col('temp').min().alias('min'),
            pl.col('temp').mean().alias('mean'),
            pl.col('temp').max().alias('max')).sort('station').collect()
    else:
        stations = pl.read_csv(
            file, has_header=False, separator=";", new_columns=columns
        ).filter(pl.col('station').is_not_null()).group_by('station').agg(
            pl.col('temp').min().alias('min'),
            pl.col('temp').mean().alias('mean'),
            pl.col('temp').max().alias('max')).sort('station')
    python_impl = platform.python_implementation().lower()
    suffix = "-lazy" if is_lazy else ""
    stations.write_csv(
        f"out-{python_impl}{suffix}.txt", include_header=False, separator=";", float_precision=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Do 1brc challenge with polars")
    parser.add_argument(
        "-f", "--file", help="path of the measurements.txt", required=True)
    parser.add_argument('--is-lazy', help="Use lazy API instead of eager API.",
                        action="store_true")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print("FileNotFound:", args.file)
        sys.exit(1)

    solution(args.file, args.is_lazy)
