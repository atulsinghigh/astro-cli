from utils.api import get_usd_to_inr

def run(ti):
    rate = get_usd_to_inr()
    print(f"💱 USD/INR: {rate}")
    ti.xcom_push(key="usd_inr", value=rate)
