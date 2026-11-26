#!/usr/bin/env python3
"""Clean the downloaded roommate dataset and produce local artifacts."""
import argparse, csv, json, os, statistics

from generate_dataset import generate

REQUIRED = ["user_id", "age", "location", "budget_min", "budget_max", "sleep_schedule", "smoking", "drinking", "cleanliness", "noise_level", "food_preference", "pets", "study_habit", "interests", "gender_preference", "room_type_preference"]

def clean(src, out_csv, out_json, stats_path):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    rows, seen = [], set()
    if not os.path.exists(src):
        generate(src)
        print(f"source_missing=generated_deterministic_dataset path={src}")
    with open(src, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing = [x for x in REQUIRED if x not in (reader.fieldnames or [])]
        if missing: raise ValueError("Missing columns: " + ", ".join(missing))
        for raw in reader:
            uid = raw["user_id"].strip()
            if not uid or uid in seen: continue
            try:
                row = {k: raw.get(k, "").strip() for k in reader.fieldnames}
                row["user_id"] = uid; row["age"] = int(row["age"])
                row["budget_min"] = int(row["budget_min"]); row["budget_max"] = int(row["budget_max"])
                row["cleanliness"] = int(row["cleanliness"]); row["noise_level"] = int(row["noise_level"])
                if row["budget_min"] > row["budget_max"]: continue
            except (TypeError, ValueError): continue
            seen.add(uid); rows.append(row)
    if not rows:
        raise ValueError("No valid rows found in input dataset")
    fields = list(rows[0])
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
    with open(out_json, "w", encoding="utf-8") as fh: json.dump(rows, fh)
    report = {"rows": len(rows), "columns": fields, "locations": len({r["location"] for r in rows}), "avg_budget_min": round(statistics.mean(r["budget_min"] for r in rows), 2), "avg_budget_max": round(statistics.mean(r["budget_max"] for r in rows), 2)}
    with open(stats_path, "w", encoding="utf-8") as fh: json.dump(report, fh, indent=2)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    ml_root = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(); ap.add_argument("--input", default=os.path.join(ml_root, "data/raw/roommate_dataset.csv")); ap.add_argument("--output-dir", default=os.path.join(ml_root, "data/processed")); a = ap.parse_args()
    clean(a.input, os.path.join(a.output_dir, "roommate_clean.csv"), os.path.join(a.output_dir, "roommate_clean.json"), os.path.join(a.output_dir, "summary.json"))
