from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# -------------------- CONFIG -------------------- #
AMADEUS_CLIENT_ID = "ArzcnzI6Cywe2mgJ1X9v6myxULgjF8vB"
AMADEUS_CLIENT_SECRET = "dKCvpkUq5R7FFDOd"
ORIGIN = "DEL"
DESTINATION = "BOM"
DEPARTURE_DATE = "2025-08-15"
CURRENCY = "INR"
PRICE_THRESHOLD = 4000  # INR
# ------------------------------------------------ #

def get_amadeus_token(client_id, client_secret):
    url = "https://test.api.amadeus.com/v1/security/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json().get("access_token")

def check_flight_price_amadeus():
    try:
        token = get_amadeus_token(AMADEUS_CLIENT_ID, AMADEUS_CLIENT_SECRET)

        headers = {
            "Authorization": f"Bearer {token}"
        }
        params = {
            "originLocationCode": ORIGIN,
            "destinationLocationCode": DESTINATION,
            "departureDate": DEPARTURE_DATE,
            "adults": 1,
            "currencyCode": CURRENCY,
            "max": 5
        }

        url = "https://test.api.amadeus.com/v2/shopping/flight-offers"
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if "data" in data:
            offers = data['data']
            print(f"\n🛫 Flight offers from {ORIGIN} to {DESTINATION} on {DEPARTURE_DATE}:\n")
            for idx, offer in enumerate(offers, start=1):
                price = offer['price']['total']
                airline = offer['validatingAirlineCodes'][0]
                itinerary = offer['itineraries'][0]
                segments = itinerary['segments']
                departure_time = segments[0]['departure']['at']
                departure_airport = segments[0]['departure']['iataCode']
                arrival_time = segments[-1]['arrival']['at']
                arrival_airport = segments[-1]['arrival']['iataCode']
                num_stops = len(segments) - 1

                print(f"🔹 Offer #{idx}")
                print(f"   ✈️ Airline: {airline}")
                print(f"   💰 Price: ₹{price}")
                print(f"   🕒 Departure: {departure_airport} at {departure_time}")
                print(f"   🛬 Arrival: {arrival_airport} at {arrival_time}")
                print(f"   🔁 Stops: {num_stops}\n")

        else:
            print("No flight offers found in the response.")

    except Exception as e:
        print(f"❌ Error fetching flight details: {e}")


        url = "https://test.api.amadeus.com/v2/shopping/flight-offers"
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if "data" in data:
            offers = data['data']
            print(f"\n🛫 Flight offers from {ORIGIN} to {DESTINATION} on {DEPARTURE_DATE}:\n")
            for offer in offers:
                price = offer['price']['total']
                print(f"💰 ₹{price}")
                if float(price) < PRICE_THRESHOLD:
                    print("🚨 Good Deal Found!")
        else:
            print("No data found in Amadeus response.")

    except Exception as e:
        print(f"❌ Error fetching flight offers: {e}")

# -------------------- AIRFLOW DAG -------------------- #

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id="amadeus_flight_price_tracker",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # Change as needed
    catchup=False,
    default_args=default_args,
    tags=["amadeus", "flights", "tracker"]
) as dag:

    track_price = PythonOperator(
        task_id="check_flight_price_amadeus",
        python_callable=check_flight_price_amadeus
    )

    track_price