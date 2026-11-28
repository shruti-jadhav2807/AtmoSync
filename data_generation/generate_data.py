import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

FIELDS = [
    "timestamp_utc",
    "container_id",
    "product",
    "route",
    "temperature_c",
    "humidity_pct",
    "vibration_g",
]

CONTAINERS = [
    ("C001", "avocado", "Pune-Mumbai"),
    ("C002", "tomato", "Nashik-Delhi"),
    ("C003", "mango", "Mumbai-Bengaluru"),
    ("C004", "spinach", "Pune-Hyderabad"),
    ("C005", "banana", "Nagpur-Chennai"),
]

def generate_data(output_file, hours=48, seed=7):
    random.seed(seed)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_time = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=hours - 1)

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
        writer.writeheader()

        for hour in range(hours):
            timestamp = (
                start_time + timedelta(hours=hour)
            ).isoformat(timespec="seconds").replace("+00:00", "Z")

            for index, (container_id, product, route) in enumerate(CONTAINERS):
                # Synthetic readings for software testing only.
                temperature = [5.0, 7.0, 6.0, 4.0, 5.5][index]
                humidity = [65, 72, 68, 75, 66][index]
                vibration = [2.1, 2.8, 2.4, 1.9, 2.3][index]

                # Add a gradual temperature and humidity increase to C002.
                drift = max(0, hour - hours * 0.55)
                if index == 1:
                    temperature += drift * 0.12
                    humidity += drift * 0.17

                # Add small random variation to each reading.
                temperature += random.uniform(-0.8, 0.8)
                humidity += random.uniform(-5, 5)
                vibration += random.uniform(-0.5, 0.8)

                writer.writerow({
                    "timestamp_utc": timestamp,
                    "container_id": container_id,
                    "product": product,
                    "route": route,
                    "temperature_c": round(temperature, 2),
                    "humidity_pct": round(humidity, 2),
                    "vibration_g": round(vibration, 2),
                })

    print(f"Created {hours * len(CONTAINERS)} readings in {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic AtmoSync sensor data."
    )
    parser.add_argument("--output", default="data/sensor_readings.csv")
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    if args.hours < 1:
        parser.error("--hours must be at least 1")

    generate_data(args.output, args.hours, args.seed)