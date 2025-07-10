from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import requests
import csv
import os

# 🪙 Fetch Bitcoin price in USD
def fetch_price(ti):
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    price_usd = data['bitcoin']['usd']
    print(f"🪙 Bitcoin Price in USD: ${price_usd}")
    ti.xcom_push(key='price_usd', value=price_usd)

# 💱 Fetch USD to INR exchange rate
def fetch_exchange_rate(ti):
    url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(url)

    print(f"🌐 Status Code: {response.status_code}")
    print(f"📦 Raw Response: {response.text}")

    try:
        data = response.json()
        rate = data['rates']['INR']   # ✅ Fixed key
        print(f"💱 USD to INR Rate: ₹{rate}")
        ti.xcom_push(key='usd_to_inr', value=rate)
    except Exception as e:
        print("❌ Failed to parse exchange rate.")
        print(f"🔧 Error: {e}")
        raise

# 🧮 Convert and log results to CSV
def convert_and_log(ti):
    usd = ti.xcom_pull(key='price_usd', task_ids='fetch_price')
    rate = ti.xcom_pull(key='usd_to_inr', task_ids='fetch_exchange_rate')
    price_inr = usd * rate
    IST = timezone(timedelta(hours=5, minutes=30))
    timestamp = datetime.now(IST).isoformat()

    print(f"✅ Final Bitcoin Price (INR): ₹{price_inr:.2f}")

    # 📁 Save to CSV
    file_dir = "/usr/local/airflow/files"
    os.makedirs(file_dir, exist_ok=True)
    file_path = os.path.join(file_dir, "btc_price_inr.csv")

    try:
        # Write header only if file does not exist
        write_header = not os.path.exists(file_path)
        with open(file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            if write_header:
                writer.writerow(["Timestamp", "Price (USD)", "Rate (USD→INR)", "Price (INR)"])
            writer.writerow([timestamp, usd, rate, round(price_inr, 2)])
        print(f"📝 Data written to: {file_path}")
    except Exception as e:
        print(f"❌ Failed to write to CSV: {e}")

# 🗓️ Define the DAG
with DAG(
    dag_id="btc_price_live_inr",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["api", "crypto", "currency"]
) as dag:

    fetch_price_task = PythonOperator(
        task_id="fetch_price",
        python_callable=fetch_price
    )

    fetch_exchange_task = PythonOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_exchange_rate
    )

    convert_and_log_task = PythonOperator(
        task_id="convert_and_log",
        python_callable=convert_and_log
    )

    # Set dependencies
    [fetch_price_task, fetch_exchange_task] >> convert_and_log_task
