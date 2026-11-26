#!/usr/bin/env python3
"""Generate a deterministic, dependency-free roommate dataset for local demos.

The project accepts the original public CSV when it is available.  This
generator keeps a fresh clone reproducible without requiring a network
download or committing a large generated data file to the repository.
"""
import argparse
import csv
import os
import random


FIELDS = [
    "user_id", "age", "location", "budget_min", "budget_max",
    "sleep_schedule", "smoking", "drinking", "cleanliness", "noise_level",
    "food_preference", "pets", "study_habit", "interests",
    "gender_preference", "room_type_preference",
]

LOCATIONS = ["Kathmandu", "San Jose", "Austin", "Boston", "Chicago", "Seattle", "New York"]
SLEEPS = ["early_bird", "flexible", "night_owl"]
SMOKING = ["non_smoker", "occasional", "smoker"]
DRINKING = ["non_drinker", "social", "drinker"]
FOOD = ["no_preference", "vegetarian", "vegan", "non_vegetarian"]
PETS = ["okay_with_pets", "no_pets", "not_okay_with_pets"]
STUDY = ["quiet", "background_noise_ok", "group_study", "rarely_studies_at_home"]
GENDERS = ["female", "male", "non_binary"]
ROOMS = ["private", "shared", "studio"]
INTERESTS = ["Music", "Hiking", "Cooking", "Gaming", "Reading", "Sports", "Travel"]


def generate(path, rows=10000, seed=249):
    rng = random.Random(seed)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(rows):
            minimum = rng.randrange(7000, 22001, 500)
            maximum = minimum + rng.randrange(2000, 10001, 500)
            interests = rng.sample(INTERESTS, rng.randint(1, 3))
            writer.writerow({
                "user_id": str(100000 + index),
                "age": rng.randint(18, 35),
                "location": rng.choice(LOCATIONS),
                "budget_min": minimum,
                "budget_max": maximum,
                "sleep_schedule": rng.choice(SLEEPS),
                "smoking": rng.choice(SMOKING),
                "drinking": rng.choice(DRINKING),
                "cleanliness": rng.randint(1, 5),
                "noise_level": rng.randint(1, 5),
                "food_preference": rng.choice(FOOD),
                "pets": rng.choice(PETS),
                "study_habit": rng.choice(STUDY),
                "interests": "|".join(interests),
                "gender_preference": rng.choice(["any", *GENDERS]),
                "room_type_preference": rng.choice(ROOMS),
            })


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=os.path.join(os.path.dirname(__file__), "data/raw/roommate_dataset.csv"))
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=249)
    args = parser.parse_args()
    generate(args.output, args.rows, args.seed)
    print(f"generated_rows={args.rows} seed={args.seed} output={args.output}")
