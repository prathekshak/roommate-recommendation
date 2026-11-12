#!/usr/bin/env python3
"""Explainable, deterministic roommate compatibility scoring."""
def _set(value): return {x.strip() for x in (value or "").split("|") if x.strip()}
def _yes(v): return v in {"okay_with_pets", "has_pets"}

DEFAULT_WEIGHTS = {"location": .16, "budget": .16, "sleep": .12, "study": .10, "food": .08, "smoking": .08, "drinking": .06, "pets": .06, "cleanliness": .10, "noise": .08, "interests": .06}

def score(a, b, config=None):
    weights = dict(DEFAULT_WEIGHTS); weights.update((config or {}).get("weights", {}))
    parts = []
    if a.get("location") == b.get("location"): parts.append((weights["location"], "same location"))
    amin, amax = int(a["budget_min"]), int(a["budget_max"]); bmin, bmax = int(b["budget_min"]), int(b["budget_max"])
    overlap = max(0, min(amax, bmax) - max(amin, bmin)) / max(1, max(amax, bmax) - min(amin, bmin)); parts.append((weights["budget"] * overlap, "overlapping budget" if overlap else "different budget"))
    if a.get("sleep_schedule") == b.get("sleep_schedule"): parts.append((weights["sleep"], "matching sleep schedule"))
    if a.get("study_habit") == b.get("study_habit"): parts.append((weights["study"], "matching study habit"))
    if a.get("food_preference") == b.get("food_preference") or "no_preference" in {a.get("food_preference"), b.get("food_preference")}: parts.append((weights["food"], "compatible food preference"))
    if a.get("smoking") == b.get("smoking"): parts.append((weights["smoking"], "matching smoking preference"))
    if a.get("drinking") == b.get("drinking"): parts.append((weights["drinking"], "matching drinking preference"))
    if a.get("pets") == b.get("pets") or (_yes(a.get("pets")) and _yes(b.get("pets"))): parts.append((weights["pets"], "compatible pet preference"))
    for field, key, label in [("cleanliness", "cleanliness", "cleanliness"), ("noise_level", "noise", "noise level")]: parts.append((weights[key] * (1 - abs(int(a[field]) - int(b[field])) / 4), "similar " + label))
    common = _set(a.get("interests")) & _set(b.get("interests")); parts.append((weights["interests"] * min(1, len(common) / 2), "shared interests" if common else "few shared interests"))
    return {"score": round(100 * sum(x[0] for x in parts), 2), "reasons": [label for value, label in parts if value >= 0.04], "shared_interests": sorted(common)}

def recommend(profile, candidates, limit=10, config=None):
    results = []
    for candidate in candidates:
        if candidate.get("user_id") == profile.get("user_id"): continue
        pref = profile.get("gender_preference", "any")
        if pref != "any" and candidate.get("gender") != pref: continue
        results.append({"user_id": candidate["user_id"], "location": candidate.get("location"), **score(profile, candidate, config)})
    return sorted(results, key=lambda x: (-x["score"], x["user_id"]))[:limit]
