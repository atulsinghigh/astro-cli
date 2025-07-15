from utils.export import save_to_csv

def run(ti):
    btc_usd = ti.xcom_pull(task_ids='fetch_btc_usd', key='btc_usd')
    usd_inr = ti.xcom_pull(task_ids='fetch_usd_inr', key='usd_inr')
    final_rate = btc_usd * usd_inr
    save_to_csv(btc_usd, usd_inr, final_rate)
    print(f"✅ BTC/INR: ₹{final_rate}")
