#!/usr/bin/env python3
"""Longer paired checks of adverse initial results, with exclusive locks."""

import argparse
import fcntl
from pathlib import Path
import time

from run_benchmarks import Campaign, sha


def main():
    coordination = Path("/home/rob/projects/splinterdb/.cache/projects-20261011/coordination")
    with (coordination / "benchmark.lock").open("rb") as benchmark_lock, \
            (coordination / "machine.lock").open("rb") as machine_lock:
        fcntl.flock(benchmark_lock, fcntl.LOCK_EX)
        fcntl.flock(machine_lock, fcntl.LOCK_EX)
        args = argparse.Namespace(mode="repeat", seconds=15, cpus="0-11")
        campaign = Campaign(args)
        campaign.manifest["followup_sha256"] = sha(__file__)
        campaign.manifest["selection"] = "Adverse initial results: common long scans, disabled long scans, nonlexical short scans, logged random overwrites"
        order = ["main", "candidate", "candidate", "main", "main", "candidate"]
        cases = [
            ("resident-common", 0, ["warm", "range:uniform:10000@8"]),
            ("resident-common-disabled", 0, ["warm", "range:uniform:10000@8", "range:uniform:10000@1"]),
            ("resident-nonlexical", 0, ["warm", "range:uniform:1@8"]),
            ("resident-random", 1, ["warm", "ow:uniform@8"]),
        ]
        campaign.manifest["order"] = order
        campaign.manifest["cases"] = cases
        campaign.save()
        for case, wal, phases in cases:
            for index, profile in enumerate(order):
                campaign.query(case, profile, index, phases, wal)
        campaign.manifest.update(complete=True, finished=time.time())
        campaign.save()


if __name__ == "__main__":
    main()
