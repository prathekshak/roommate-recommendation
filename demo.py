#!/usr/bin/env python3
import json, os, subprocess, sys
ROOT=os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.join(ROOT,"ml")); from recommender import recommend

def main():
    subprocess.run([sys.executable, os.path.join(ROOT, "ml/preprocess.py")], check=True)
    with open(os.path.join(ROOT,"ml/data/processed/roommate_clean.json"), encoding="utf-8") as f: rows=json.load(f)
    print("preprocessed_profiles=", len(rows))
    print("sample_profile=", rows[0]["user_id"])
    print("top_matches=", json.dumps(recommend(rows[0], rows, 5), indent=2))
    print("\nStart the distributed API with ./run.sh; then check http://127.0.0.1:8201/health")

if __name__ == "__main__": main()
