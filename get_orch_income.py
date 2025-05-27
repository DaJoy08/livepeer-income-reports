import os
import requests
from datetime import datetime

ORCH_ADDRESS = os.getenv("ORCH_ADDRESS", "0x5bdeedca9c6346b0ce6b17ffa8227a4dace37039")
AUTH_TOKEN = os.getenv("GRAPH_AUTH_TOKEN")
SUBGRAPH_URL = os.getenv(
    "SUBGRAPH_URL",
    "https://gateway.thegraph.com/api/subgraphs/id/FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC",
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


def fetch_lpt_price_cryptocompare(timestamp):
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {"fsym": "LPT", "tsym": "EUR", "limit": 1, "toTs": timestamp}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data["Data"]["Data"][-1]["close"]
    else:
        raise Exception(
            f"Failed to fetch LPT price: {response.status_code}, Response: {response.text}"
        )


def fetch_ticket_events_paginated(recipient, start_timestamp, end_timestamp):
    results = []
    skip = 0
    has_more = True

    while has_more:
        query = f"""
        {{
          winningTicketRedeemedEvents(
            where: {{ 
              recipient: "{recipient}", 
              timestamp_gte: {start_timestamp}, 
              timestamp_lt: {end_timestamp} 
            }},
            first: 100,
            skip: {skip}
          ) {{
            id
            transaction {{
              id
            }}
            timestamp
            round {{
              id
            }}
            sender {{
              id
            }}
            faceValue
            faceValueUSD
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
            if "data" in data and "winningTicketRedeemedEvents" in data["data"]:
                events = data["data"]["winningTicketRedeemedEvents"]
                results.extend(events)
                if len(events) < 100:
                    has_more = False
                else:
                    skip += 100
            else:
                raise Exception(
                    "Unexpected response structure: Missing 'winningTicketRedeemedEvents' key."
                )
        else:
            raise Exception(
                f"Query failed with status code {response.status_code}: {response.text}"
            )

    return results


def sum_orch_fees(data):
    total_fees = 0
    total_ETH = 0
    for event in data:
        total_fees += float(event.get("faceValueUSD", 0))
        total_ETH += float(event.get("faceValue", 0))
    return total_fees, total_ETH


def fetch_reward_events_paginated(delegate, start_timestamp, end_timestamp):
    results = []
    skip = 0
    has_more = True

    while has_more:
        query = f"""
        {{
          rewardEvents(
            where: {{ 
              delegate: "{delegate}", 
              timestamp_gte: {start_timestamp}, 
              timestamp_lt: {end_timestamp} 
            }},
            first: 100,
            skip: {skip}
          ) {{
            transaction {{
              gasUsed
              gasPrice
              blockNumber
              timestamp
              id
            }}
            round {{
              id
            }}
            rewardTokens
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
            if "data" in data and "rewardEvents" in data["data"]:
                events = data["data"]["rewardEvents"]
                results.extend(events)
                if len(events) < 100:
                    has_more = False
                else:
                    skip += 100
            else:
                raise Exception(
                    "Unexpected response structure: Missing 'rewardEvents' key."
                )
        else:
            raise Exception(
                f"Query failed with status code {response.status_code}: {response.text}"
            )

    return results


def sum_rewards(data):
    total_rewards = 0
    for event in data:
        total_rewards += float(event.get("rewardTokens", 0))
    return total_rewards


if __name__ == "__main__":
    recipient = ORCH_ADDRESS
    start_timestamp = to_timestamp(START_DATE)
    end_timestamp = to_timestamp(END_DATE)

    print("=" * 70)
    print(f"Orchestrator Address: {recipient}")
    print(f"Time Period: {START_DATE} to {END_DATE}")
    print("=" * 70)

    try:
        data = fetch_ticket_events_paginated(recipient, start_timestamp, end_timestamp)
        total_fees, total_ETH = sum_orch_fees(data)
        final_fees = total_fees * 0.5
        final_ETH = total_ETH * 0.5

        reward_data = fetch_reward_events_paginated(
            recipient, start_timestamp, end_timestamp
        )
        total_rewards = sum_rewards(reward_data)
        final_rewards = total_rewards * 0.1

        eth_price_eur = fetch_eth_price_cryptocompare(end_timestamp)
        lpt_price_eur = fetch_lpt_price_cryptocompare(end_timestamp)

        print(f"Total Orch Fees in USD: {total_fees}")
        print(f"Total Orch Fees in ETH: {total_ETH}")
        print(f"Total Rewards (LPT): {total_rewards}")
        print(" ")
        print(f"Total Orch Fees in USD (after 50% cut): {final_fees}")
        print(f"Total Orch Fees in ETH (after 50% cut): {final_ETH}")
        print(f"Total Rewards (after 10% cut): {final_rewards}")
        print(f"ETH Price on {END_DATE} in EUR: {eth_price_eur}")
        print(f"LPT Price on {END_DATE} in EUR: {lpt_price_eur}")
        print(f"Total LPT Value in EUR: {final_rewards * lpt_price_eur}")
        print(f"Total Orch Fees in EUR: {final_ETH * eth_price_eur}")
    except Exception as e:
        print(f"Error: {e}")
