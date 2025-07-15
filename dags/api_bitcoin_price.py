from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def fetch_price(ti):
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    price_usd = data['bitcoin']['usd']
    print(f"🪙 Bitcoin Price in USD: ${price_usd}")
    ti.xcom_push(key='price_usd', value=price_usd)

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

def convert_and_log(ti):
    usd = ti.xcom_pull(key='price_usd', task_ids='fetch_price')
    rate = ti.xcom_pull(key='usd_to_inr', task_ids='fetch_exchange_rate')
    price_inr = usd * rate
    print(f"✅ Final Bitcoin Price (INR): ₹{price_inr:.2f}")

with DAG(
    dag_id="btc_price_live_inr",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["api", "crypto", "currency"]
) as dag:

    fetch_price = PythonOperator(
        task_id="fetch_price",
        python_callable=fetch_price
    )

    fetch_exchange = PythonOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_exchange_rate
    )

    convert = PythonOperator(
        task_id="convert_and_log",
        python_callable=convert_and_log
    )

    # Set dependencies: both fetch tasks → convert
    [fetch_price, fetch_exchange] >> convert
