import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


AIRLINE_PROFILES = {
    "Turkish Airlines": {
        "traffic_weight": 1.08,
        "delay_multiplier": 0.62,
        "company_bias": 0.42,
        "punctual_rate": 0.23,
        "early_rate": 0.12,
        "cancel_rate": 0.012,
        "divert_rate": 0.006,
    },
    "Qatar Airways": {
        "traffic_weight": 0.88,
        "delay_multiplier": 0.74,
        "company_bias": 0.58,
        "punctual_rate": 0.18,
        "early_rate": 0.10,
        "cancel_rate": 0.014,
        "divert_rate": 0.007,
    },
    "Emirates": {
        "traffic_weight": 0.92,
        "delay_multiplier": 0.78,
        "company_bias": 0.62,
        "punctual_rate": 0.17,
        "early_rate": 0.09,
        "cancel_rate": 0.015,
        "divert_rate": 0.007,
    },
    "Lufthansa": {
        "traffic_weight": 0.92,
        "delay_multiplier": 0.83,
        "company_bias": 0.66,
        "punctual_rate": 0.16,
        "early_rate": 0.08,
        "cancel_rate": 0.018,
        "divert_rate": 0.008,
    },
    "Alaska Airlines": {
        "traffic_weight": 0.82,
        "delay_multiplier": 0.88,
        "company_bias": 0.70,
        "punctual_rate": 0.15,
        "early_rate": 0.08,
        "cancel_rate": 0.019,
        "divert_rate": 0.009,
    },
    "Delta": {
        "traffic_weight": 1.15,
        "delay_multiplier": 0.92,
        "company_bias": 0.74,
        "punctual_rate": 0.13,
        "early_rate": 0.07,
        "cancel_rate": 0.021,
        "divert_rate": 0.010,
    },
    "Southwest": {
        "traffic_weight": 1.12,
        "delay_multiplier": 0.98,
        "company_bias": 0.78,
        "punctual_rate": 0.12,
        "early_rate": 0.06,
        "cancel_rate": 0.023,
        "divert_rate": 0.011,
    },
    "United": {
        "traffic_weight": 1.08,
        "delay_multiplier": 1.05,
        "company_bias": 0.82,
        "punctual_rate": 0.11,
        "early_rate": 0.06,
        "cancel_rate": 0.024,
        "divert_rate": 0.012,
    },
    "American Airlines": {
        "traffic_weight": 1.10,
        "delay_multiplier": 1.08,
        "company_bias": 0.86,
        "punctual_rate": 0.10,
        "early_rate": 0.05,
        "cancel_rate": 0.026,
        "divert_rate": 0.012,
    },
    "JetBlue": {
        "traffic_weight": 0.86,
        "delay_multiplier": 1.15,
        "company_bias": 0.90,
        "punctual_rate": 0.09,
        "early_rate": 0.05,
        "cancel_rate": 0.029,
        "divert_rate": 0.014,
    },
    "Frontier": {
        "traffic_weight": 0.76,
        "delay_multiplier": 1.28,
        "company_bias": 1.02,
        "punctual_rate": 0.07,
        "early_rate": 0.04,
        "cancel_rate": 0.034,
        "divert_rate": 0.016,
    },
    "Spirit": {
        "traffic_weight": 0.74,
        "delay_multiplier": 1.34,
        "company_bias": 1.08,
        "punctual_rate": 0.06,
        "early_rate": 0.04,
        "cancel_rate": 0.038,
        "divert_rate": 0.017,
    },
}

COMPANY_DELAY_REASONS = [
    ("Airline", 0.13, 8, 65),
    ("Late Aircraft", 0.22, 12, 115),
    ("Maintenance", 0.20, 18, 150),
    ("Crew Scheduling", 0.10, 12, 95),
    ("Baggage Handling", 0.07, 5, 55),
    ("Ground Operations", 0.08, 6, 75),
    ("Technical Inspection", 0.07, 22, 175),
    ("Fueling", 0.04, 5, 45),
]

EXTERNAL_DELAY_REASONS = [
    ("Weather", 0.34, 20, 220),
    ("Air Traffic Control", 0.28, 10, 125),
    ("Air System", 0.18, 8, 95),
    ("Security", 0.08, 5, 55),
    ("Passenger Issue", 0.07, 5, 45),
]

AIRCRAFT_TYPES = [
    "Boeing 737",
    "Boeing 747",
    "Boeing 777",
    "Boeing 787",
    "Airbus A220",
    "Airbus A320",
    "Airbus A321",
    "Airbus A330",
    "Airbus A350",
    "Embraer E190",
]

DATETIME_COLUMNS = [
    "ScheduledDeparture",
    "ActualDeparture",
    "ScheduledArrival",
    "ActualArrival",
]


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M")
    except ValueError:
        return None


def format_dt(value):
    return value.strftime("%Y-%m-%d %H:%M") if value else ""


def weighted_choice(items):
    values = [item[0] for item in items]
    weights = [item[1] for item in items]
    return random.choices(values, weights=weights, k=1)[0]


def choose_airline():
    items = [(airline, profile["traffic_weight"]) for airline, profile in AIRLINE_PROFILES.items()]
    airline = weighted_choice(items)
    return airline, AIRLINE_PROFILES[airline]


def airport_risk_factor(origin):
    if not origin:
        return 1.0
    code_score = sum(ord(char) for char in origin)
    return 0.84 + ((code_score % 33) / 100)


def sample_delay_minutes(min_delay, max_delay, multiplier, origin):
    mode = min_delay + ((max_delay - min_delay) * random.uniform(0.20, 0.55))
    triangular_delay = random.triangular(min_delay, max_delay, mode)
    long_tail = random.lognormvariate(2.45, 0.58)
    blended_delay = (triangular_delay * 0.78) + (long_tail * 0.22)
    delay = blended_delay * multiplier * airport_risk_factor(origin) * random.uniform(0.82, 1.18)
    return round(delay)


def choose_delay(profile, origin):
    roll = random.random()

    if roll < profile["punctual_rate"]:
        return 0, "None"

    if roll < profile["punctual_rate"] + profile["early_rate"]:
        return random.randint(-20, -1), "Early Arrival"

    company_probability = 0.56 * profile["company_bias"]
    reason_pool = COMPANY_DELAY_REASONS if random.random() < company_probability else EXTERNAL_DELAY_REASONS
    reason = weighted_choice(reason_pool)
    _, _, min_delay, max_delay = next(item for item in reason_pool if item[0] == reason)

    delay = sample_delay_minutes(min_delay, max_delay, profile["delay_multiplier"], origin)

    if reason in {"Weather", "Maintenance", "Technical Inspection"} and random.random() < 0.08:
        delay += round(random.lognormvariate(4.25, 0.45))

    return min(420, max(1, delay)), reason


def update_times(row, delay):
    scheduled_departure = parse_dt(row.get("ScheduledDeparture", ""))
    scheduled_arrival = parse_dt(row.get("ScheduledArrival", ""))

    if scheduled_departure:
        row["ActualDeparture"] = format_dt(scheduled_departure + timedelta(minutes=delay))

    if scheduled_arrival:
        arrival_noise = random.randint(-5, 15)
        row["ActualArrival"] = format_dt(scheduled_arrival + timedelta(minutes=delay + arrival_noise))


def generate(input_path, output_path, limit=None, seed=42):
    random.seed(seed)
    input_path = Path(input_path)
    output_path = Path(output_path)

    with input_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        fieldnames = reader.fieldnames

        if not fieldnames:
            raise ValueError("Input CSV header could not be read.")

        with output_path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames)
            writer.writeheader()

            written = 0
            for row in reader:
                airline, profile = choose_airline()
                delay, reason = choose_delay(profile, row.get("Origin", ""))

                row["Airline"] = airline
                row["FlightNumber"] = str(random.randint(100, 9999))
                row["DelayMinutes"] = str(delay)
                row["DelayReason"] = "" if reason in {"None", "Early Arrival"} else reason
                row["Cancelled"] = "True" if random.random() < profile["cancel_rate"] else "False"
                row["Diverted"] = "True" if random.random() < profile["divert_rate"] else "False"
                row["AircraftType"] = random.choice(AIRCRAFT_TYPES)
                row["TailNumber"] = f"N{random.randint(10000, 99999)}"

                if row["Cancelled"] == "True":
                    row["ActualDeparture"] = ""
                    row["ActualArrival"] = ""
                else:
                    update_times(row, delay)

                for column in DATETIME_COLUMNS:
                    row[column] = row.get(column, "")

                writer.writerow(row)
                written += 1

                if limit and written >= limit:
                    break

    print(f"Synthetic CSV created: {output_path} ({written:,} rows)")


def main():
    parser = argparse.ArgumentParser(description="Generate a richer synthetic flight delay CSV.")
    parser.add_argument("--input", default="flight_delays.csv", help="Source CSV path.")
    parser.add_argument("--output", default="flight_delays_synthetic.csv", help="Output CSV path.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for quick tests.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible output.")
    args = parser.parse_args()

    generate(args.input, args.output, args.limit, args.seed)


if __name__ == "__main__":
    main()
