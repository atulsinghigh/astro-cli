import requests

def get_btc_price_usd():
    url = "https://api.coindesk.com/v1/bpi/currentprice/USD.json"
    response = requests.get(url).json()
    return float(response["bpi"]["USD"]["rate"].replace(",", ""))

def get_usd_to_inr():
    url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(url).json()
    return response["rates"]["INR"]
