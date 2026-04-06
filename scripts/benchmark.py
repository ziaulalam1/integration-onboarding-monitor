"""
Throughput benchmark for Integration Onboarding Monitor.

Simulates 20 customers each sending events during onboarding.
Measures p50/p95 latency and throughput. Writes artifacts to reports/.

Usage:
    python scripts/benchmark.py
"""

import json
import statistics
import sys
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent.parent))
from fastapi.testclient import TestClient
from main import app

REPORTS = Path(__file__).parent.parent / "reports"
REPORTS.mkdir(exist_ok=True)

N = 1000
CUSTOMERS = [f"cust-{i:03d}" for i in range(20)]
EVENTS = [
    "account_created",
    "data_synced",
    "config_updated",
    "webhook_registered",
    "error_occurred",
]

PAYLOAD = {"records": 50, "schema_version": "1.0"}

client = TestClient(app)

print(f"Benchmarking {N} requests across {len(CUSTOMERS)} customers...")

latencies = []
for i in range(N):
    cust = CUSTOMERS[i % len(CUSTOMERS)]
    event = EVENTS[i % len(EVENTS)]
    body = {"event_type": event, "payload": PAYLOAD, "source": "webhook"}

    t0 = time.perf_counter()
    r = client.post(f"/integrations/{cust}/events", json=body)
    latencies.append((time.perf_counter() - t0) * 1000)  # ms

    if r.status_code != 201:
        print(f"FAIL: unexpected status {r.status_code}", file=sys.stderr)
        sys.exit(1)

latencies_sorted = sorted(latencies)
p50 = statistics.median(latencies)
p95 = latencies_sorted[int(0.95 * N)]
total_time_s = sum(latencies) / 1000
throughput = N / total_time_s

results = {
    "requests": N,
    "customers": len(CUSTOMERS),
    "p50_ms": round(p50, 3),
    "p95_ms": round(p95, 3),
    "throughput_rps": round(throughput, 1),
}

print(json.dumps(results, indent=2))
(REPORTS / "results.json").write_text(json.dumps(results, indent=2))

# Chart
fig, ax = plt.subplots(figsize=(8, 4))
ax.hist(latencies, bins=50, color="#4a90d9", edgecolor="white")
ax.axvline(p50, color="green", linestyle="--", linewidth=1.5, label=f"p50 = {p50:.2f}ms")
ax.axvline(p95, color="red", linestyle="--", linewidth=1.5, label=f"p95 = {p95:.2f}ms")
ax.set_xlabel("Latency (ms)")
ax.set_ylabel("Requests")
ax.set_title(f"Integration Monitor -- {N} requests, {len(CUSTOMERS)} customers")
ax.legend()
fig.tight_layout()
fig.savefig(REPORTS / "throughput.png", dpi=120)

print(f"\nPASS -- p50={p50:.3f}ms  p95={p95:.3f}ms  throughput={throughput:.0f} req/s")
print(f"Artifacts -> {REPORTS}/results.json, {REPORTS}/throughput.png")
