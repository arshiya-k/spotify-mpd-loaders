"""Render the benchmark chart used in the README."""
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
LABELS = {
    "02_py_sequential": "Python\nexecutemany",
    "01_sql": "Straight SQL",
    "06_airflow": "Airflow",
    "07_spark": "Spark",
    "05_celery": "Celery ×8",
    "03_py_copy": "Python\nCOPY",
    "04_multiproc": "Multiprocessing ×8",
}


def main(slices: int = 100) -> None:
    load, cons = defaultdict(list), defaultdict(list)
    with (HERE / "results.csv").open() as f:
        for r in csv.DictReader(f):
            if int(r["slices"]) == slices and r["status"] == "OK":
                load[r["loader"]].append(float(r["load_s"]))
                cons[r["loader"]].append(float(r["constraints_s"]))
    load = {k: v[-3:] for k, v in load.items()}      # latest 3 only -- see summarize.load()
    cons = {k: v[-3:] for k, v in cons.items()}

    names = sorted(load, key=lambda n: -statistics.median(load[n]))
    y = range(len(names))
    l = [statistics.median(load[n]) for n in names]
    c = [statistics.median(cons[n]) for n in names]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(y, l, color="#378ADD", label="load")
    ax.barh(y, c, left=l, color="#B5D4F4", label="constraints + indexes")
    span = max(a + b for a, b in zip(l, c))
    for i, (a, b) in enumerate(zip(l, c)):
        ax.text(a + b + span * 0.015, i, f"{a + b:.0f}s total", va="center",
                fontsize=9, color="#444")
    ax.set_xlim(0, span * 1.18)          # room for the labels

    ax.set_yticks(list(y), [LABELS.get(n, n) for n in names], fontsize=9)
    ax.set_xlabel("seconds (median of 3 runs)")
    entries = {100: "6.7M", 1000: "66.3M"}.get(slices, "")
    ax.set_title(f"Loading {slices:,} slices -- {slices * 1000:,} playlists, "
                 f"{entries} track entries -- into Postgres", fontsize=11)
    ax.legend(frameon=False, fontsize=9, loc="upper right")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(axis="x", alpha=0.25)
    ax.set_axisbelow(True)
    fig.tight_layout()
    fig.savefig(HERE / f"benchmark_{slices}.png", dpi=150)
    print(f"wrote results/benchmark_{slices}.png")


if __name__ == "__main__":
    import sys
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 100)
