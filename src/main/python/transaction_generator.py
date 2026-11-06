
import csv
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_DIR = Path("data/raw/generated")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ACCOUNTS = [f"ACC{i:03}" for i in range(101, 121)]

LOCATIONS = [
    "Bengaluru",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Chennai",
    "Pune",
]

TRANSACTION_TYPES = ["UPI", "CARD", "BANK_TRANSFER"]

FIELDS = [
    "transaction_id",
    "account_id",
    "amount",
    "timestamp",
    "location",
    "transaction_type",
]


def generate_normal_transaction():
    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
        "account_id": random.choice(ACCOUNTS),
        "amount": round(random.uniform(100, 10000), 2),
        "timestamp": (
            datetime.now()
            - timedelta(minutes=random.randint(0, 1440))
        ).isoformat(timespec="seconds"),
        "location": random.choice(LOCATIONS),
        "transaction_type": random.choice(TRANSACTION_TYPES),
    }


def generate_suspicious_transactions():
    account = random.choice(ACCOUNTS)
    transactions = []

    # Three large transactions from different cities.
    for i, location in enumerate(random.sample(LOCATIONS, 3)):
        transactions.append({
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
            "account_id": account,
            "amount": round(random.uniform(50000, 150000), 2),
            "timestamp": (
                datetime.now() + timedelta(minutes=i * 2)
            ).isoformat(timespec="seconds"),
            "location": location,
            "transaction_type": "BANK_TRANSFER",
        })

    return transactions


def generate_batch(batch_size=20):
    transactions = [
        generate_normal_transaction()
        for _ in range(batch_size)
    ]

    # Inject suspicious activity occasionally.
    if random.random() < 0.4:
        transactions.extend(generate_suspicious_transactions())

    random.shuffle(transactions)

    # Timestamp + UUID ensures filenames remain unique across restarts.
    batch_id = (
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:6]}"
    )

    filename = OUTPUT_DIR / f"batch_{batch_id}.csv"
    temp_filename = OUTPUT_DIR / f"batch_{batch_id}.tmp"

    # Write to a temporary file first.
    with open(temp_filename, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(transactions)

    # Rename only after writing is complete.
    temp_filename.replace(filename)

    print(
        f"Generated {filename} "
        f"({len(transactions)} transactions)"
    )


def main():
    print("Transaction generator started.")
    print(f"Output directory: {OUTPUT_DIR}")

    try:
        while True:
            generate_batch()
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nTransaction generation stopped.")


if __name__ == "__main__":
    main()

