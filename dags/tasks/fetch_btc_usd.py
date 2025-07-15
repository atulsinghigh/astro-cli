from utils.api import get_btc_price_usd

def run(ti):
    btc_price = get_btc_price_usd()
    print(f"₿ BTC/USD: {btc_price}")
    ti.xcom_push(key="btc_usd", value=btc_price)