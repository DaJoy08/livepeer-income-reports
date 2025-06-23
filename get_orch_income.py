"""Retrieve and export orchestrator income data for tax reporting as a CSV file.

Subgraph Improvements:
    - Fix gas logic (https://github.com/livepeer/subgraph/pull/164).
    - Add rewardValueUSD field to rewardEvents query to avoid fetching prices.
"""

import os
import sys
from datetime import datetime, timezone

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from web3 import Web3
import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import requests
from tabulate import tabulate
from tqdm import tqdm

GRAPH_TOKEN = os.getenv("GRAPH_AUTH_TOKEN")
GRAPH_ID = os.getenv("GRAPH_ID", "FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC")
ARB_RPC_URL = os.getenv("ARB_RPC_URL", "https://arb1.arbitrum.io/rpc")

if not GRAPH_TOKEN:
    raise EnvironmentError(
        "GRAPH_AUTH_TOKEN environment variable is required but not set."
    )

GRAPHQL_ENDPOINT = (
    f"https://gateway.thegraph.com/api/{GRAPH_TOKEN}/subgraphs/id/{GRAPH_ID}"
)

TRANSPORT = RequestsHTTPTransport(url=GRAPHQL_ENDPOINT, verify=True, retries=3)
GRAPHQL_CLIENT = Client(transport=TRANSPORT, fetch_schema_from_transport=True)

ARB_CLIENT = Web3(Web3.HTTPProvider(ARB_RPC_URL, request_kwargs={"timeout": 60}))


REWARD_EVENTS_QUERY = gql(
    """
query RewardEvents($orchestrator: String!, $startTimestamp: Int!, $endTimestamp: Int!) {
  rewardEvents(
    where: {
      delegate: $orchestrator,
      timestamp_gte: $startTimestamp,
      timestamp_lte: $endTimestamp
    }
    orderBy: round__startBlock
    orderDirection: asc
  ) {
    id
    timestamp
    transaction {
        id
        gasPrice
        gasUsed
    }
    rewardTokens
    round {
      id
      pools(where: { delegate: $orchestrator }) {
        rewardCut
      }
    }
  }
}
"""
)
POOL_DATA_QUERY = gql(
    """
query PoolData($poolId: String!) {
  pool(id: $poolId) {
    rewardCut
  }
}
"""
)
WINNING_TICKET_REDEEMED_EVENTS_QUERY = gql(
    """
    query WinningTicketRedeemedEvents(
        $recipient: String!,
        $startTimestamp: Int!,
        $endTimestamp: Int!
    ) {
      winningTicketRedeemedEvents(
        where: {
          recipient: $recipient,
          timestamp_gte: $startTimestamp,
          timestamp_lte: $endTimestamp
        }
        orderBy: timestamp
        orderDirection: asc
      ) {
        id
        timestamp
        transaction {
          id
          gasPrice
          gasUsed
        }
        faceValue
        round {
          id
          pools(where: { delegate: $recipient }) {
            feeShare
          }
        }
      }
    }
    """
)


def human_to_unix_time(human_time: str, time_format: str = "%Y-%m-%d %H:%M:%S") -> int:
    """Convert a human-readable time to a Unix timestamp.

    Args:
        human_time: The human-readable time (e.g., "2025-06-22 14:30:00").
        time_format: The format of the input time string (default: "%Y-%m-%d %H:%M:%S").

    Returns:
        The Unix timestamp.
    """
    try:
        dt = datetime.strptime(human_time, time_format)
        return int(dt.timestamp())
    except ValueError as e:
        raise ValueError(f"Invalid time format: {e}")


def create_arbiscan_url(transaction_id: str) -> str:
    """Create a URL for the Arbiscan transaction.

    Args:
        transaction_id: The transaction ID.

    Returns:
        A string representing the Arbiscan URL for the transaction.
    """
    return f"https://arbiscan.io/tx/{transaction_id}"


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def fetch_reward_events(
    orchestrator: str, start_timestamp: int, end_timestamp: int, page_size: int = 100
) -> list[object]:
    """Fetch reward events for a given orchestrator within a specified time range, with
    pagination.

    Args:
        orchestrator: The orchestrator address.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.
        page_size: The number of results to fetch per page (default: 100).

    Returns:
        A list of all reward events.
    """
    all_events = []
    skip = 0
    while True:
        variables = {
            "orchestrator": orchestrator,
            "startTimestamp": start_timestamp,
            "endTimestamp": end_timestamp,
            "first": page_size,
            "skip": skip,
        }
        try:
            response = GRAPHQL_CLIENT.execute(
                REWARD_EVENTS_QUERY, variable_values=variables
            )
            events = response.get("rewardEvents", [])
            all_events.extend(events)

            if len(events) < page_size:
                break
            skip += page_size
        except Exception as e:
            print(f"Error while fetching reward events: {e}")
            break
    return all_events


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def fetch_fee_events(
    recipient: str, start_timestamp: int, end_timestamp: int, page_size: int = 100
) -> list[object]:
    """Fetch fee events for a given recipient within a specified time range.

    Args:
        recipient: The recipient address.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.
        page_size: The number of results to fetch per page (default: 100).

    Returns:
        A list of all fee events.
    """
    all_events = []
    skip = 0
    while True:
        variables = {
            "recipient": recipient,
            "startTimestamp": start_timestamp,
            "endTimestamp": end_timestamp,
            "first": page_size,
            "skip": skip,
        }
        try:
            response = GRAPHQL_CLIENT.execute(
                WINNING_TICKET_REDEEMED_EVENTS_QUERY, variable_values=variables
            )
            events = response.get("winningTicketRedeemedEvents", [])
            all_events.extend(events)

            if len(events) < page_size:
                break
            skip += page_size
        except Exception as e:
            print(f"Error while fetching fee events: {e}")
            break
    return all_events


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def fetch_gas_used_from_rpc(transaction_id: str) -> int:
    """Retrieve the actual gas used for a transaction using Web3 and Arbitrum RPC.

    Args:
        transaction_id: The transaction ID.

    Returns:
        The gas used for the transaction.
    """
    receipt = ARB_CLIENT.eth.get_transaction_receipt(transaction_id)
    return receipt["gasUsed"]


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def fetch_crypto_price(
    crypto_symbol: str, target_currency: str, unix_timestamp: int
) -> float:
    """Fetch the historical price of a cryptocurrency in a specific currency at a
    specific timestamp using the CryptoCompare API.

    Args:
        crypto_symbol: The cryptocurrency symbol (e.g., "ETH", "LPT").
        target_currency: The target currency symbol (e.g., "EUR", "USD").
        unix_timestamp: The Unix timestamp for the desired historical price.

    Returns:
        The price of the cryptocurrency in the target currency.
    """
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {
        "fsym": crypto_symbol,
        "tsym": target_currency,
        "limit": 1,
        "toTs": unix_timestamp,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data["Data"]["Data"][-1]["close"]


def process_reward_events(reward_events: list, currency: str) -> pd.DataFrame:
    """Process reward events and create a DataFrame with relevant information.

    Args:
        reward_events: A list of reward events.
        currency: The currency for the reward values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the reward data.
    """
    rows = []
    for event in tqdm(reward_events, desc="Processing reward events", unit="event"):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        transaction = create_arbiscan_url(event["transaction"]["id"])
        pool_reward = float(event["rewardTokens"])
        reward_cut = int(event["round"]["pools"][0]["rewardCut"]) / 10**6
        orchestrator_reward = reward_cut * pool_reward
        transaction_type = "reward cut"
        gas_price = int(event["transaction"]["gasPrice"])

        try:
            gas_used = fetch_gas_used_from_rpc(event["transaction"]["id"])
        except Exception as e:
            print(f"Error fetching gas used for transaction {transaction}: {e}")
            sys.exit(1)
        gas_cost_eth = gas_price * gas_used / 10**18

        try:
            lpt_price = fetch_crypto_price("LPT", currency, event["timestamp"])
            eth_price = fetch_crypto_price("ETH", currency, event["timestamp"])
        except Exception as e:
            print(f"Error fetching crypto prices: {e}")
            sys.exit(1)

        gas_cost_currency = gas_cost_eth * eth_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": event["round"]["id"],
                "transaction": transaction,
                "transaction type": transaction_type,
                "currency": "LPT",
                "pool reward": pool_reward,
                "reward cut": reward_cut,
                "orch reward": orchestrator_reward,
                "gas price (Wei)": gas_price,
                "gas used (Wei)": gas_used,
                "gas cost (ETH)": gas_cost_eth,
                f"ETH price ({currency})": eth_price,
                f"LPT price ({currency})": lpt_price,
                f"gas cost ({currency})": gas_cost_currency,
                f"orch reward value ({currency})": orchestrator_reward * lpt_price,
            }
        )
    return pd.DataFrame(rows)


def process_fee_events(fee_events: list, currency: str) -> pd.DataFrame:
    """Process fee events and create a DataFrame with relevant information.

    Args:
        fee_events: A list of fee events.
        currency: The currency for the fee values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the fee data.
    """
    rows = []
    for event in tqdm(fee_events, desc="Processing fee events", unit="event"):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        transaction = create_arbiscan_url(event["transaction"]["id"])
        face_value = float(event["faceValue"])
        fee_share = int(event["round"]["pools"][0]["feeShare"]) / 10**6
        orch_fee = fee_share * face_value
        transaction_type = "fee cut"
        gas_price = int(event["transaction"]["gasPrice"])

        try:
            gas_used = fetch_gas_used_from_rpc(event["transaction"]["id"])
        except Exception as e:
            print(f"Error fetching gas used for transaction {transaction}: {e}")
            sys.exit(1)
        gas_cost_eth = gas_price * gas_used / 10**18

        try:
            eth_price = fetch_crypto_price("ETH", currency, event["timestamp"])
        except Exception as e:
            print(f"Error fetching crypto prices: {e}")
            sys.exit(1)

        gas_cost_currency = gas_cost_eth * eth_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": event["round"]["id"],
                "transaction": transaction,
                "transaction type": transaction_type,
                "currency": "ETH",
                "face value": face_value,
                "fee share": fee_share,
                "orch fee": orch_fee,
                "gas price (Wei)": gas_price,
                "gas used (Wei)": gas_used,
                "gas cost (ETH)": gas_cost_eth,
                f"ETH price ({currency})": eth_price,
                f"gas cost ({currency})": gas_cost_currency,
                f"orch fee value ({currency})": orch_fee * eth_price,
            }
        )
    return pd.DataFrame(rows)


if __name__ == "__main__":
    print("== Orchestrator Income Data Exporter ==")

    start_time = input("Enter data range start (YYYY-MM-DD HH:MM:SS): ")
    start_timestamp = human_to_unix_time(start_time)
    end_time = input("Enter data range end (YYYY-MM-DD HH:MM:SS): ")
    end_timestamp = human_to_unix_time(end_time)
    recipient = input("Enter orchestrator address: ").lower()
    currency = input("Enter currency (default: EUR): ").upper() or "EUR"

    print("\nFetching reward call events...")
    reward_events = fetch_reward_events(recipient, start_timestamp, end_timestamp)
    if not reward_events:
        print("No reward events found for the specified orchestrator and time range.")
        sys.exit(0)
    else:
        print(f"Found {len(reward_events)} reward events.")

    print("Processing reward calls...")
    reward_data = process_reward_events(reward_events, currency)

    print("Fetching fee redeem events...")
    fee_events = fetch_fee_events(recipient, start_timestamp, end_timestamp)
    if not fee_events:
        print("No fee events found for the specified recipient and time range.")
    else:
        print(f"Found {len(fee_events)} fee events.")

    print("Processing fee events...")
    fee_data = process_fee_events(fee_events, currency)

    combined_data = pd.concat([reward_data, fee_data], ignore_index=True)

    print(f"\nOverview ({start_time} - {end_time}):")
    total_orchestrator_reward = reward_data["orch reward"].sum()
    total_orchestrator_reward_value = reward_data[
        f"orch reward value ({currency})"
    ].sum()
    total_orchestrator_fees = fee_data["recipient fee"].sum()
    total_orchestrator_fees_value = fee_data[f"recipient fee value ({currency})"].sum()
    total_gas_cost = combined_data[f"gas cost ({currency})"].sum()
    overview_table = [
        ["Total Orchestrator Reward (LPT)", f"{total_orchestrator_reward:.3f} LPT"],
        [
            f"Total Orchestrator Reward ({currency})",
            f"{total_orchestrator_reward_value:.3f} {currency}",
        ],
        ["Total Orchestrator Fees (ETH)", f"{total_orchestrator_fees:.3f} ETH"],
        [
            f"Total Orchestrator Fees ({currency})",
            f"{total_orchestrator_fees_value:.3f} {currency}",
        ],
        [f"Total Gas Cost ({currency})", f"{total_gas_cost:.3f} {currency}"],
    ]
    print(tabulate(overview_table, headers=["Metric", "Value"], tablefmt="grid"))

    print("\nStoring data in CSV format...")
    csv_file = "orchestrator_income.csv"
    combined_data.to_csv(csv_file, index=False)
    print(f"CSV file '{csv_file}' created successfully!")
