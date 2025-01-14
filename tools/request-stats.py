#!/usr/bin/env python3
from __future__ import annotations
from collections import defaultdict
import json
import statistics
import sys


def main() -> None:
    workers_to_reqtimes = defaultdict(list)
    for fpath in sys.argv[1:]:
        with open(fpath) as fp:
            data = json.load(fp)
        for trav in data["traversals"]:
            workers = trav["workers"]
            for field in ("file_request_times", "request_times"):
                for duration in trav.get(field, []):
                    secs = duration["secs"] + duration["nanos"] / 1_000_000_000
                    workers_to_reqtimes[workers].append(secs)
    for workers, times in sorted(workers_to_reqtimes.items()):
        qty = len(times)
        minimum = min(times)
        maximum = max(times)
        mean = statistics.fmean(times)
        stddev = statistics.stdev(times, xbar=mean)
        median = statistics.median(times)
        q1, _, q3 = statistics.quantiles(times)
        print(f"- Workers: {workers}")
        print(f"  Qty: {qty}")
        print(f"  Min: {minimum}")
        print(f"  Q1:  {q1}")
        print(f"  Med: {median}")
        print(f"  Q3:  {q3}")
        print(f"  Max: {maximum}")
        print(f"  Avg: {mean}")
        print(f"  StdDev: {stddev}")
        print()


if __name__ == "__main__":
    main()
