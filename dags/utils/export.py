import csv
from datetime import datetime
import os

def save_to_csv(btc_usd, usd_inr, btc_inr, output="/usr/local/airflow/data/btc_rates.csv"):
    os.makedirs(os.path.dirname(output), exist_ok=True)
    write_header = not os.path.exists(output)

    with open(output, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["timestamp", "btc_usd", "usd_inr", "btc_inr"])
        writer.writerow([datetime.utcnow().isoformat(), btc_usd, usd_inr, btc_inr])
