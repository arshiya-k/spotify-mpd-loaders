"""Trigger the mpd_load DAG via Airflow's REST API and block until the run finishes.

This is the harness-facing half. In a real deployment nothing calls this -- the
scheduler starts runs on a timer, and humans watch the UI.
"""
import os
import sys
import time
from datetime import datetime, timezone

import urllib.request
import json

AIRFLOW = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
POLL_S = 2


def api(method: str, path: str, body: dict | None = None, token: str | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{AIRFLOW}{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def main() -> None:
    n = os.environ.get("MPD_SLICES") or "1000"
    token = api("POST", "/auth/token", {"username": "admin", "password": "admin"})["access_token"]

    api("PATCH", "/api/v2/dags/mpd_load", {"is_paused": False}, token)

    run_id = f"harness__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    api("POST", "/api/v2/dags/mpd_load/dagRuns",
        {"dag_run_id": run_id, "conf": {"slices": int(n)}, "logical_date": None}, token)
    print(f"  triggered {run_id} (slices={n})")

    t0 = time.perf_counter()
    while True:
        state = api("GET", f"/api/v2/dags/mpd_load/dagRuns/{run_id}", token=token)["state"]
        if state in ("success", "failed"):
            break
        time.sleep(POLL_S)

    print(f"  run {state} in {time.perf_counter() - t0:.1f}s")
    if state != "success":
        sys.exit(f"DAG run {run_id} {state} -- see http://localhost:8080")


if __name__ == "__main__":
    main()
