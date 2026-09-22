"""Turn results/results.csv into the markdown table and chart used in the README."""
import csv
import statistics
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
LABELS = {
    "01_sql": "Straight SQL",
    "02_py_sequential": "Python, executemany",
    "03_py_copy": "Python, COPY",
    "04_multiproc": "Multiprocessing",
    "05_celery": "Celery",
    "06_airflow": "Airflow",
    "07_spark": "Spark",
}


def load(slices: int) -> dict[str, list[dict]]:
    """Latest 3 OK runs per loader at this slice count.

    Taking the tail matters: a loader re-benchmarked after a code change leaves its
    older rows in the file, and mixing them would average two different implementations.
    """
    rows = defaultdict(list)
    with (HERE / "results.csv").open() as f:
        for r in csv.DictReader(f):
            if int(r["slices"]) == slices and r["status"] == "OK":
                rows[r["loader"]].append(r)
    return {k: v[-3:] for k, v in rows.items()}


def table(slices: int) -> str:
    rows = load(slices)
    out = ["| loader | load (s) | constraints (s) | total (s) | rows/s | runs |",
           "|---|---:|---:|---:|---:|---:|"]
    stats = []
    for name in LABELS:
        if name not in rows:
            continue
        loads = [float(r["load_s"]) for r in rows[name]]
        cons = [float(r["constraints_s"]) for r in rows[name]]
        entries = int(rows[name][0]["playlist_tracks"])
        med_load = statistics.median(loads)
        stats.append((name, med_load, statistics.median(cons), entries / med_load, len(loads)))
    for name, ld, cn, rps, n in sorted(stats, key=lambda s: s[1]):
        out.append(f"| {LABELS[name]} | {ld:.1f} | {cn:.1f} | {ld + cn:.1f} | {rps:,.0f} | {n} |")
    return "\n".join(out)


if __name__ == "__main__":
    import sys
    print(table(int(sys.argv[1]) if len(sys.argv) > 1 else 100))
