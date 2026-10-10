# Branch-versus-main benchmark comparison

This directory builds the same benchmark sources against pinned copies of the
current commit and `main`, runs paired comparisons, and produces a single report
with standalone SVG graphs.  It needs only the C toolchain and Python's standard
library.

```sh
OUT=.cache/comparison-$(date +%Y%m%d)
benchmarks/comparison/build.sh "$OUT"
python3 benchmarks/comparison/run.py --out "$OUT" --deadline 07:20
python3 benchmarks/comparison/report.py --out "$OUT"
```

The runner uses CPUs 0-11, two ABBA repetitions, 20-second core phases, a
180-second warm-up plus 300-second measurement for large random uniform and
Zipfian overwrites, and reserves 30 minutes before the deadline for reporting.
All are command-line options. Results and exact commands are recorded under
`$OUT/results`; database images are deleted after each comparison cell.

The enhanced suite includes:

- 100 ms insertion-stall sampling during an 8-inserter/8-reader workload;
- paced open-loop inserts at several offered loads, recording service and
  response latency (response latency includes scheduling backlog);
- durable-barrier characterization on the audited branch; and
- deterministic, untimed post-overwrite value checks in `kvbench`.

The report defines positive percentages as improvements by the audited branch.
A major regression requires at least two paired repetitions, a median loss of
at least 10%, and a loss of at least 5% in every pair.  Single-pair results are
explicitly preliminary.

The extended large/random overwrite phases run as one block per version. The
runner unlinks each version's image when its block completes, avoiding the need
to retain two sparse images that can each grow by tens of GiB. Repetition order
reverses the version blocks. If an operator has to exclude a harness-caused
process failure from an interrupted or manually managed run,
`$OUT/results/exclusions.json` may map the exact manifest run name to a reason.
The report lists these separately and preserves the raw manifest.

For a completed campaign, optional run-specific interpretation can be placed in
`$OUT/results/analysis.md`. The report generator includes it under the executive
summary while keeping all computed tables, classifications, and graphs derived
from the recorded CSV and manifest data.
