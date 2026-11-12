#!/usr/bin/env python3
"""Create reproducible latency/scalability metrics and a standalone SVG graph."""
import json, os, statistics, sys, time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); sys.path.insert(0, os.path.join(ROOT, "ml"))
from recommender import recommend

def percentile(values, q):
    values = sorted(values); return values[min(len(values) - 1, int(round((len(values) - 1) * q)))]
def chart(points, path):
    width, height, left, bottom = 920, 470, 70, 55; max_y = max(max(p["p95_ms"] for p in points), 1); xstep = (width - left - 30) / max(1, len(points) - 1)
    def xy(i, value): return left + i * xstep, height - bottom - (value / max_y) * (height - bottom - 35)
    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="Recommendation latency by candidate dataset size"><style>text{{font-family:system-ui,sans-serif;fill:#24323a}}.grid{{stroke:#dce4e6}}.line{{fill:none;stroke:#1f6f62;stroke-width:3}}.dot{{fill:#d07a39}}</style><rect width="100%" height="100%" fill="#f7f8f6"/><text x="{left}" y="25" font-size="20" font-weight="700">Recommendation latency and scalability</text>']
    for tick in range(5):
        y = height - bottom - tick * (height - bottom - 35) / 4; value = max_y * tick / 4; lines.append(f'<line class="grid" x1="{left}" x2="{width-25}" y1="{y:.1f}" y2="{y:.1f}"/><text x="8" y="{y+4:.1f}" font-size="12">{value:.1f} ms</text>')
    path_d = " ".join(("M" if i == 0 else "L") + " %.1f %.1f" % xy(i, p["p95_ms"]) for i, p in enumerate(points)); lines.append(f'<path class="line" d="{path_d}"/>')
    for i, p in enumerate(points):
        x, y = xy(i, p["p95_ms"]); lines.append(f'<circle class="dot" cx="{x:.1f}" cy="{y:.1f}" r="5"><title>{p["candidate_count"]} candidates: p95 {p["p95_ms"]:.2f} ms</title></circle><text x="{x-15:.1f}" y="{height-22}" font-size="12">{p["candidate_count"]}</text>')
    lines.append(f'<text x="{width/2-60}" y="{height-3}" font-size="12">candidate profiles</text></svg>'); open(path, "w", encoding="utf-8").write("\n".join(lines))

def recovery_chart(seconds, path):
    width, height = 620, 300; bar = min(500, max(12, seconds * 70)); lines = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="Raft leader recovery time"><style>text{{font-family:system-ui,sans-serif;fill:#24323a}}</style><rect width="100%" height="100%" fill="#f7f8f6"/><text x="35" y="38" font-size="20" font-weight="700">Raft leader recovery</text><text x="35" y="75" font-size="14">Time to elect a replacement leader</text><rect x="35" y="115" width="500" height="50" rx="8" fill="#dce4e6"/><rect x="35" y="115" width="{bar:.1f}" height="50" rx="8" fill="#d07a39"/><text x="35" y="205" font-size="34" font-weight="700">{seconds:.3f} s</text><text x="35" y="230" font-size="13">measured process-level recovery</text></svg>']; open(path, "w", encoding="utf-8").write("\n".join(lines))

def main():
    rows = json.load(open(os.path.join(ROOT, "ml", "data", "processed", "roommate_clean.json"), encoding="utf-8")); points = []
    for size in [100, 500, 1000, 5000, len(rows)]:
        samples = []
        for _ in range(10):
            started = time.perf_counter(); recommend(rows[0], rows[:size], 10); samples.append((time.perf_counter() - started) * 1000)
        points.append({"candidate_count": size, "p50_ms": round(percentile(samples, .50), 3), "p95_ms": round(percentile(samples, .95), 3), "throughput_per_second": round(1000 / statistics.mean(samples), 2)})
    fault_path = os.path.join(ROOT, "results", "process_fault_results.json")
    result = {"recommendation_latency": points, "fault_recovery": json.load(open(fault_path)) if os.path.exists(fault_path) else {"status": "run ds/process_fault_test.py first"}}
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True); open(os.path.join(ROOT, "results", "evaluation_metrics.json"), "w").write(json.dumps(result, indent=2)); chart(points, os.path.join(ROOT, "results", "latency-scalability.svg"));
    if "recovery_seconds" in result["fault_recovery"]: recovery_chart(result["fault_recovery"]["recovery_seconds"], os.path.join(ROOT, "results", "raft-recovery.svg"))
    print(json.dumps(result, indent=2))
if __name__ == "__main__": main()
