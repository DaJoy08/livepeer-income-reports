import os
import requests
from datetime import datetime

AUTH_TOKEN = os.getenv("GRAPH_AUTH_TOKEN")
SUBGRAPH_URL = os.getenv(
    "SUBGRAPH_URL",
    "https://gateway.thegraph.com/api/subgraphs/id/FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC",
)
WALLET_ADDRESS = os.getenv(
    "WALLET_ADDRESS", "0x455A304A1D3342844f4Dd36731f8da066eFdd30B"
)
START_DATE = os.getenv("START_DATE", "2025-01-01 00:00:00")
END_DATE = os.getenv("END_DATE", "2025-05-27 00:00:00")

if not AUTH_TOKEN:
    raise EnvironmentError("AUTH_TOKEN environment variable is required but not set.")


def to_timestamp(date_string, date_format="%Y-%m-%d %H:%M:%S"):
    dt = datetime.strptime(date_string, date_format)
    return int(dt.timestamp())


def fetch_eth_price_cryptocompare(timestamp):
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {"fsym": "ETH", "tsym": "EUR", "limit": 1, "toTs": timestamp}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data["Data"]["Data"][-1]["close"]
    else:
        raise Exception(
            f"Failed to fetch ETH price: {response.status_code}, Response: {response.text}"
        )


def fetch_earnings_claimed_events_paginated(delegator, start_timestamp, end_timestamp):
    results = []
    skip = 0
    has_more = True

    while has_more:
        query = f"""
        {{
          earningsClaimedEvents(
            where: {{
              delegator: "{delegator.lower()}",
              timestamp_gte: {start_timestamp},
              timestamp_lt: {end_timestamp}
            }},
            first: 100,
            skip: {skip}
          ) {{
            fees
            timestamp
            round {{
              id
            }}
          }}
        }}
        """

        headers = {
            "Authorization": f"Bearer {AUTH_TOKEN}",
            "Content-Type": "application/json",
        }

        response = requests.post(SUBGRAPH_URL, json={"query": query}, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if "data" in data and "earningsClaimedEvents" in data["data"]:
                events = data["data"]["earningsClaimedEvents"]
                results.extend(events)
                if len(events) < 100:
                    has_more = False
                else:
                    skip += 100
            else:
                raise Exception(
                    "Unexpected response structure: Missing 'earningsClaimedEvents' key."
                )
        else:
            raise Exception(
                f"Query failed with status code {response.status_code}: {response.text}"
            )

    return results


def sum_delegator_fees(data):
    total_fees = 0
    for event in data:
        total_fees += float(event.get("fees", 0))
    return total_fees


if __name__ == "__main__":
    delegator = WALLET_ADDRESS
    start_timestamp = to_timestamp(START_DATE)
    end_timestamp = to_timestamp(END_DATE)

    print("=" * 70)
    print(f"Fetching Delegator Fee Income for Wallet: {delegator}")
    print(f"Time Period: {START_DATE} to {END_DATE}")
    print("=" * 70)

    try:
        events = fetch_earnings_claimed_events_paginated(
            delegator, start_timestamp, end_timestamp
        )
        total_fees = sum_delegator_fees(events)

        eth_price_eur = fetch_eth_price_cryptocompare(end_timestamp)

        print(f"Total Fees Claimed: {total_fees}")
        print(f"ETH Price on {END_DATE} in EUR: {eth_price_eur}")
        print(f"Total Fees in EUR: {total_fees * eth_price_eur}")
    except Exception as e:
        print(f"Error: {e}")
