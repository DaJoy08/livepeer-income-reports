"""Retrieve and export orchestrator income data for tax reporting as a CSV file.


TODO:
    - Add LPT, ETH in out transacftions.
    - Add compounding rewards.
"""

import os
import sys
from datetime import datetime, timezone

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from web3 import Web3
import pandas as pd
from pandas import ExcelWriter
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import requests
from tabulate import tabulate
from tqdm import tqdm
from typing import Callable

tqdm.pandas()

GRAPH_TOKEN = os.getenv("GRAPH_AUTH_TOKEN")
ARBISCAN_API_KEY_TOKEN = os.getenv("ARBISCAN_API_KEY_TOKEN")
CRYPTO_COMPARE_API_KEY = os.getenv("CRYPTO_COMPARE_API_KEY", "")
GRAPH_ID = os.getenv("GRAPH_ID", "FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC")
ARB_RPC_URL = os.getenv("ARB_RPC_URL", "https://arb1.arbitrum.io/rpc")

if not GRAPH_TOKEN:
    raise EnvironmentError(
        "GRAPH_AUTH_TOKEN environment variable is required but not set."
    )
if not ARBISCAN_API_KEY_TOKEN:
    raise EnvironmentError(
        "ARBISCAN_API_KEY_TOKEN environment variable is required but not set."
    )

GRAPHQL_ENDPOINT = (
    f"https://gateway.thegraph.com/api/{GRAPH_TOKEN}/subgraphs/id/{GRAPH_ID}"
)
ARBISCAN_ENDPOINT = "https://api.etherscan.io/v2/api"

TRANSPORT = RequestsHTTPTransport(url=GRAPHQL_ENDPOINT, verify=True, retries=3)
GRAPHQL_CLIENT = Client(transport=TRANSPORT, fetch_schema_from_transport=True)

ARB_CLIENT = Web3(Web3.HTTPProvider(ARB_RPC_URL, request_kwargs={"timeout": 60}))


REWARD_EVENTS_QUERY = gql(
    """
query RewardEvents(
  $orchestrator: String!
  $startTimestamp: Int!
  $endTimestamp: Int!
  $first: Int!
  $skip: Int!
) {
  rewardEvents(
    where: {
      delegate: $orchestrator
      timestamp_gte: $startTimestamp
      timestamp_lte: $endTimestamp
    }
    first: $first
    skip: $skip
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
WINNING_TICKET_REDEEMED_EVENTS_QUERY = gql(
    """
query WinningTicketRedeemedEvents(
  $recipient: String!
  $startTimestamp: Int!
  $endTimestamp: Int!
  $first: Int!
  $skip: Int!
) {
  winningTicketRedeemedEvents(
    where: {
      recipient: $recipient
      timestamp_gte: $startTimestamp
      timestamp_lte: $endTimestamp
    }
    first: $first
    skip: $skip
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
TRANSFER_BOND_EVENTS_QUERY = gql(
    """
query TransferBondEvents(
  $oldDelegator: String!
  $startTimestamp: Int!
  $endTimestamp: Int!
) {
  transferBondEvents(
    where: {
      oldDelegator: $oldDelegator
      timestamp_gte: $startTimestamp
      timestamp_lte: $endTimestamp
    }
  ) {
    timestamp
    amount
    round {
      id
    }
    oldDelegator {
      id
    }
    newDelegator {
      id
    }
    transaction {
      id
      gasPrice
      gasUsed
    }
  }
}
"""
)


CSV_COLUMN_ORDER = [
    "timestamp",
    "transaction hash",
    "transaction url",
    "direction",
    "transaction type",
    "currency",
    "amount",
    "value (EUR)",
    "price (EUR)",
    "gas cost (EUR)",
    "gas cost (ETH)",
    "round",
    "pool reward",
    "reward cut",
    "face value",
    "fee share",
]


def filter_transactions_by_sender(
    df: pd.DataFrame, wallet_address: str
) -> pd.DataFrame:
    """Filter transactions where the wallet address is the sender.

    Args:
        df: A Pandas DataFrame containing transaction data.
        wallet_address: The wallet address to filter transactions.

    Returns:
        A DataFrame of filtered transactions where the wallet is the sender.
    """
    return df[df["from"].str.lower() == wallet_address.lower()]


def add_gas_cost_information(df: pd.DataFrame, currency: str = "EUR") -> pd.DataFrame:
    """Add gas cost information (in ETH and specified currency) to a DataFrame.

    Args:
        df (pd.DataFrame): A Pandas DataFrame containing transaction data with columns
            'gasPrice', 'gasUsed', and 'timestamp'.
        currency (str): The target currency for conversion (default: "EUR").

    Returns:
        pd.DataFrame: The updated DataFrame with gas cost information added.
    """
    df = df.copy()

    def calculate_gas_cost_currency(row):
        """Calculate gas cost in the specified currency."""
        try:
            eth_price = fetch_crypto_price("ETH", currency, int(row["timeStamp"]))
            return row["gas cost (ETH)"] * eth_price
        except Exception as e:
            print(f"Error fetching ETH price for transaction at index {row.name}: {e}")
            return 0

    if not all(col in df.columns for col in ["gasPrice", "gasUsed", "timeStamp"]):
        raise ValueError(
            "Missing required columns: 'gasPrice', 'gasUsed', or 'timeStamp'."
        )

    df["gas cost (ETH)"] = (
        df["gasPrice"].astype(float) * df["gasUsed"].astype(float)
    ) / 10**18
    df[f"gas cost ({currency})"] = df.progress_apply(
        lambda row: calculate_gas_cost_currency(row), axis=1
    )

    return df


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
        unix_time = int(dt.timestamp())
        if unix_time > int(datetime.now().timestamp()):
            raise ValueError(
                "The provided time is in the future. Please provide a valid past time."
            )
        return unix_time
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
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def get_block_number_by_timestamp(timestamp: int, closest: str = "before") -> int:
    """Fetch the block number for a given timestamp using the Arbiscan API.

    Args:
        timestamp: The Unix timestamp.
        closest: Whether to fetch the block closest 'before' or 'after' the timestamp.

    Returns:
        The block number corresponding to the timestamp.
    """
    params = {
        "chainid": 42161,
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": timestamp,
        "closest": closest,
        "apikey": ARBISCAN_API_KEY_TOKEN,
    }
    try:
        response = requests.get(ARBISCAN_ENDPOINT, params=params)
        response.raise_for_status()
        data = response.json()

        if data["status"] == "1":
            return int(data["result"])
        else:
            raise Exception(f"Error fetching block number: {data['message']}")
    except Exception as e:
        print(f"Error fetching block number for timestamp {timestamp}: {e}")
        sys.exit(1)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
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
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_fee_events(
    recipient: str, start_timestamp: int, end_timestamp: int, page_size: int = 100
) -> list[object]:
    """Fetch fee events for a given recipient within a specified time range, with
    pagination.

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
    wait=wait_exponential(multiplier=1, min=1, max=60),
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
    wait=wait_exponential(multiplier=1, min=1, max=60),
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
        "api_key": CRYPTO_COMPARE_API_KEY,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data["Data"]["Data"][-1]["close"]


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_transfer_bond_events(
    old_delegator: str, start_timestamp: int, end_timestamp: int, page_size: int = 100
) -> list[object]:
    """Fetch transfer bond events for a given old delegator with pagination.

    Args:
        old_delegator: The address of the old delegator.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.
        page_size: The number of results to fetch per page (default: 100).

    Returns:
        A list of transfer bond events.
    """
    all_events = []
    skip = 0
    while True:
        variables = {
            "oldDelegator": old_delegator,
            "startTimestamp": start_timestamp,
            "endTimestamp": end_timestamp,
            "first": page_size,
            "skip": skip,
        }
        try:
            response = GRAPHQL_CLIENT.execute(
                TRANSFER_BOND_EVENTS_QUERY, variable_values=variables
            )
            events = response.get("transferBondEvents", [])
            all_events.extend(events)

            if len(events) < page_size:
                break
            skip += page_size
        except Exception as e:
            print(f"Error while fetching transfer bond events: {e}")
            break
    return all_events


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_transactions(
    address: str,
    start_block: int,
    end_block: int,
    sort: str = "asc",
    action: str = "txlist",
) -> list:
    """Fetch transactions for a given address on the Arbitrum (Layer 2) chain using the Arbiscan API.

    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number.
        end_block: The ending block number.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').
        action: The type of transaction to fetch (e.g., 'txlist', 'tokentx', 'txlistinternal').

    Returns:
        A list of transactions.
    """
    all_transactions = []
    processed_hashes = set()
    max_records = 1000  # Free tier limit
    current_start_block = start_block

    while current_start_block <= end_block:
        params = {
            "chainid": 42161,
            "module": "account",
            "action": action,
            "address": address,
            "startblock": current_start_block,
            "endblock": end_block,
            "page": 1,
            "offset": max_records,
            "sort": sort,
            "apikey": ARBISCAN_API_KEY_TOKEN,
        }

        try:
            response = requests.get(ARBISCAN_ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json()

            if data["status"] == "1":
                transactions = data["result"]

                # Filter out duplicate transactions and add to final list.
                new_transactions = [
                    tx for tx in transactions if tx["hash"] not in processed_hashes
                ]
                all_transactions.extend(new_transactions)
                processed_hashes.update(tx["hash"] for tx in new_transactions)

                # If limit hit on Arbitrum chain, set next start_block to last block - 1.
                if len(transactions) == max_records:
                    last_block_number = int(transactions[-1]["blockNumber"])
                    current_start_block = last_block_number - 1
                else:
                    break
            elif data["status"] == "0" and data["message"] == "No transactions found":
                break
            else:
                raise Exception(f"Error fetching transactions: {data['message']}")
        except Exception as e:
            print(f"Error fetching transactions: {e}")
            break

    return all_transactions


def fetch_arb_transactions(
    address: str, start_block: int, end_block: int, sort: str = "asc"
) -> list:
    """Fetch normal transactions for a given address on the Arbitrum chain.
    
    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number.
        end_block: The ending block number.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').
    Returns:
        A list of normal transactions.
    """
    return fetch_transactions(address, start_block, end_block, sort, action="txlist")


def fetch_arb_token_transactions(
    address: str, start_block: int, end_block: int, sort: str = "asc"
) -> list:
    """Fetch token transactions for a given address on the Arbitrum chain.
    
    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number.
        end_block: The ending block number.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').
    
    Returns:
        A list of token transactions.
    """
    return fetch_transactions(address, start_block, end_block, sort, action="tokentx")


def fetch_arb_internal_transactions(
    address: str, start_block: int, end_block: int, sort: str = "asc"
) -> list:
    """Fetch internal transactions for a given address on the Arbitrum chain.
    
    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number.
        end_block: The ending block number.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').
    
    Returns:
        A list of internal transactions.
    """
    return fetch_transactions(address, start_block, end_block, sort, action="txlistinternal")


def fetch_arb_transactions_with_timestamps(
    address: str, start_timestamp: int, end_timestamp: int, sort: str = "asc"
) -> list:
    """Fetch normal transactions for a given address within a specified time range.

    Args:
        address: The wallet address to fetch transactions for.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').

    Returns:
        A list of normal transactions.
    """
    start_block = get_block_number_by_timestamp(start_timestamp)
    end_block = get_block_number_by_timestamp(end_timestamp)
    return fetch_arb_transactions(address, start_block, end_block, sort)


def fetch_arb_token_transactions_with_timestamps(
    address: str, start_timestamp: int, end_timestamp: int, sort: str = "asc"
) -> list:
    """Fetch token transactions for a given address within a specified time range.
    
    Args:
        address: The wallet address to fetch transactions for.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').
    
    Returns:
        A list of token transactions.
    """
    start_block = get_block_number_by_timestamp(start_timestamp)
    end_block = get_block_number_by_timestamp(end_timestamp)
    return fetch_arb_token_transactions(address, start_block, end_block, sort)


def fetch_arb_internal_transactions_with_timestamps(
    address: str, start_timestamp: int, end_timestamp: int, sort: str = "asc"
) -> list:
    """Fetch internal transactions for a given address within a specified time range.
    
    Args:
        address: The wallet address to fetch transactions for.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').
    
    Returns:
        A list of internal transactions.
    """
    start_block = get_block_number_by_timestamp(start_timestamp)
    end_block = get_block_number_by_timestamp(end_timestamp)
    return fetch_arb_internal_transactions(address, start_block, end_block, sort)


def fetch_all_transactions(
    address: str, start_timestamp: int, end_timestamp: int
) -> pd.DataFrame:
    """Fetch all normal, internal, and token transactions for a given address and store
    them in a DataFrame.

    Args:
        address: The wallet address to fetch transactions for.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.

    Returns:
        A Pandas DataFrame containing all transactions with consistent fields.
    """
    # Fetch normal transactions.
    print("Fetching normal transactions...")
    normal_transactions = fetch_arb_transactions_with_timestamps(
        address, start_timestamp, end_timestamp, sort="asc"
    )
    normal_df = pd.DataFrame(normal_transactions)
    if not normal_df.empty:
        print(f"Found {len(normal_df)} normal transactions.")
    else:
        print("No normal transactions found for the specified address and time range.")

    # Fetch token transactions.
    print("Fetching token transactions...")
    token_transactions = fetch_arb_token_transactions_with_timestamps(
        address, start_timestamp, end_timestamp, sort="asc"
    )
    token_df = pd.DataFrame(token_transactions)
    if not token_df.empty:
        print(f"Found {len(token_df)} token transactions.")
    else:
        print("No token transactions found for the specified address and time range.")

    # Fetch internal transactions.
    print("Fetching internal transactions...")
    internal_transactions = fetch_arb_internal_transactions_with_timestamps(
        address, start_timestamp, end_timestamp, sort="asc"
    )
    internal_df = pd.DataFrame(internal_transactions)
    if not internal_df.empty:
        print(f"Found {len(internal_df)} internal transactions.")
    else:
        print(
            "No internal transactions found for the specified address and time range."
        )

    combined_df = pd.concat([normal_df, token_df, internal_df], ignore_index=True)
    if not combined_df.empty:
        print(f"Total transactions fetched: {len(combined_df)}")
    else:
        print("No transactions found.")

    return combined_df


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
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        pool_reward = float(event["rewardTokens"])
        reward_cut = int(event["round"]["pools"][0]["rewardCut"]) / 10**6
        orchestrator_reward = reward_cut * pool_reward
        transaction_type = "reward cut"

        lpt_price = fetch_crypto_price("LPT", currency, event["timestamp"])
        value_currency = orchestrator_reward * lpt_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": event["round"]["id"],
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": transaction_type,
                "direction": "incoming",
                "currency": "LPT",
                "pool reward": pool_reward,
                "reward cut": reward_cut,
                "amount": orchestrator_reward,
                f"price ({currency})": lpt_price,
                f"value ({currency})": value_currency,
                "source function": "rewardWithHint",
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
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        face_value = float(event["faceValue"])
        fee_share = int(event["round"]["pools"][0]["feeShare"]) / 10**6
        orch_fee = (1 - fee_share) * face_value
        transaction_type = "fee cut"

        eth_price = fetch_crypto_price("ETH", currency, event["timestamp"])
        value_currency = orch_fee * eth_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": event["round"]["id"],
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": transaction_type,
                "direction": "incoming",
                "currency": "ETH",
                "face value": face_value,
                "fee share": fee_share,
                "amount": orch_fee,
                f"price ({currency})": eth_price,
                f"value ({currency})": value_currency,
                "source function": "redeemWinningTicket",
            }
        )
    return pd.DataFrame(rows)


def process_transfer_bond_events(
    transfer_bond_events: list, currency: str
) -> pd.DataFrame:
    """Process transfer bond events and create a DataFrame with relevant information.

    Args:
        transfer_bond_events: A list of transfer bond events.
        currency: The currency for the reward values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the transfer bond data.
    """
    rows = []
    for event in tqdm(
        transfer_bond_events, desc="Processing transfer bond events", unit="event"
    ):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        amount = float(event["amount"])
        transaction_type = "reward transfer"
        transaction_category = "outward"

        lpt_price = fetch_crypto_price("LPT", currency, event["timestamp"])
        value_currency = amount * lpt_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": event["round"]["id"],
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": transaction_type,
                "direction": transaction_category,
                "from": event["oldDelegator"]["id"],
                "to": event["newDelegator"]["id"],
                "currency": "LPT",
                "amount": amount,
                f"price ({currency})": lpt_price,
                f"value ({currency})": value_currency,
                "source function": "transferBond",
            }
        )
    return pd.DataFrame(rows)


def merge_gas_info(
    data: pd.DataFrame, gas_info_df: pd.DataFrame, currency: str
) -> pd.DataFrame:
    """Merge gas cost information into a DataFrame if it's not empty.

    Args:
        data: A Pandas DataFrame containing transaction data.
        gas_info_df: A DataFrame containing gas cost information.
        currency: The currency for the gas cost values (e.g., "EUR", "USD

    Returns:
        A DataFrame with gas cost information merged, or the original DataFrame if empty.
    """
    if not data.empty:
        return data.merge(
            gas_info_df[
                ["transaction hash", "gas cost (ETH)", f"gas cost ({currency})"]
            ],
            on="transaction hash",
            how="left",
        )
    return data


def fetch_and_process_events(
    fetch_func: Callable[[str, int, int], list],
    process_func: Callable[[list, str], pd.DataFrame],
    event_name: str,
) -> pd.DataFrame:
    """Fetch and process blockchain events for a given orchestrator and time range.

    Args:
        fetch_func: A callable function to fetch events. It should accept orchestrator address,
            start timestamp, and end timestamp as arguments and return a list of events.
        process_func: A callable function to process the fetched events. It should accept a list
            of events and a currency string as arguments and return a Pandas DataFrame.
        event_name: A string representing the name of the event being processed (e.g., "reward events").

    Returns:
        A Pandas DataFrame containing the processed event data. If no events are found, returns an empty DataFrame.
    """
    print(f"\nFetching {event_name}...")
    events = fetch_func(orchestrator, start_timestamp, end_timestamp)
    if events:
        print(f"Found {len(events)} {event_name}.")
        print(f"Processing {event_name}...")
        return process_func(events, currency)
    else:
        print(f"No {event_name} found for the specified orchestrator and time range.")
        return pd.DataFrame()


def infer_function_name(row: pd.Series, transactions_df: pd.DataFrame) -> str:
    """Infer the function name for a transaction based on other transactions with the
    same hash or the functionName field.

    Args:
        row: A Pandas Series representing a transaction row.
        transactions_df: A DataFrame containing all transactions.

    Returns:
        The inferred function name, or None if it cannot be determined.
    """
    if "functionName" in row and pd.notna(row["functionName"]):
        return row["functionName"].split("(")[0]

    # Look for another transaction with the same hash.
    matching_transaction = transactions_df[transactions_df["hash"] == row["hash"]]
    if not matching_transaction.empty:
        function_name = matching_transaction.iloc[0].get("functionName", "")
        if pd.notna(function_name):
            return function_name.split("(")[0]
    return None


def retrieve_token_and_eth_transfers(
    transactions_df: pd.DataFrame, wallet_address: str, currency: str
) -> pd.DataFrame:
    """Retrieve incoming/outgoing token (LPT) and ETH transfers, including their price
    in the specified currency, and infer missing function names.

    Args:
        transactions_df: A Pandas DataFrame containing all transactions.
        wallet_address: The wallet address to filter transactions for.
        currency: The target currency for conversion (e.g., "EUR").

    Returns:
        A DataFrame with categorized token and ETH transfers, including their price in
        the specified currency.
    """
    # Ensure 'tokenSymbol' exists in the DataFrame.
    if "tokenSymbol" not in transactions_df.columns:
        transactions_df["tokenSymbol"] = None

    wallet_address = wallet_address.lower()
    processed_rows = []

    def process_transactions(transactions, transaction_category, token_symbol):
        """Helper function to process transactions."""
        for _, row in transactions.iterrows():
            timestamp = datetime.fromtimestamp(
                int(row["timeStamp"]), tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")
            price = fetch_crypto_price(token_symbol, currency, int(row["timeStamp"]))
            amount = float(row["value"]) / 10**18  # Convert wei to ETH
            function_name = infer_function_name(row, transactions_df)
            processed_rows.append(
                {
                    "timestamp": timestamp,
                    "transaction hash": row["hash"],
                    "transaction url": create_arbiscan_url(row["hash"]),
                    "transaction type": "transfer",
                    "direction": transaction_category,
                    "currency": token_symbol,
                    "amount": amount,
                    f"price ({currency})": price,
                    f"value ({currency})": amount * price,
                    "source function": function_name,
                }
            )

    # Process LPT transactions.
    process_transactions(
        transactions_df[
            (transactions_df["tokenSymbol"] == "LPT")
            & (transactions_df["to"].str.lower() == wallet_address)
        ],
        "incoming",
        "LPT",
    )
    process_transactions(
        transactions_df[
            (transactions_df["tokenSymbol"] == "LPT")
            & (transactions_df["from"].str.lower() == wallet_address)
        ],
        "outgoing",
        "LPT",
    )

    # Process ETH transactions.
    process_transactions(
        transactions_df[
            (
                (transactions_df["tokenSymbol"] == "ETH")
                | (transactions_df["value"].astype(float) > 0)
            )
            & (transactions_df["to"].str.lower() == wallet_address)
        ],
        "incoming",
        "ETH",
    )
    process_transactions(
        transactions_df[
            (
                (transactions_df["tokenSymbol"] == "ETH")
                | (transactions_df["value"].astype(float) > 0)
            )
            & (transactions_df["from"].str.lower() == wallet_address)
        ],
        "outgoing",
        "ETH",
    )

    return pd.DataFrame(processed_rows)


if __name__ == "__main__":
    print("== Orchestrator Income Data Exporter ==")

    start_time = input("Enter data range start (YYYY-MM-DD HH:MM:SS): ")
    start_timestamp = human_to_unix_time(start_time)
    end_time = input("Enter data range end (YYYY-MM-DD HH:MM:SS): ")
    end_timestamp = human_to_unix_time(end_time)
    orchestrator = input("Enter orchestrator address: ").lower()
    if not orchestrator:
        print("Orchestrator address is required.")
        sys.exit(1)
    currency = input("Enter currency (default: EUR): ").upper() or "EUR"

    reward_data = fetch_and_process_events(
        fetch_reward_events, process_reward_events, "reward events"
    )
    fee_data = fetch_and_process_events(
        fetch_fee_events, process_fee_events, "fee events"
    )
    transfer_bond_data = fetch_and_process_events(
        fetch_transfer_bond_events, process_transfer_bond_events, "transfer bond events"
    )

    print("\nFetch all wallet transactions...")
    transactions_df = fetch_all_transactions(
        orchestrator, start_timestamp, end_timestamp
    )

    print("Filter transactions by sending address...")
    transactions_with_gas_info_df = filter_transactions_by_sender(
        transactions_df, orchestrator
    )

    print("\nAdd gas cost information to transactions")
    transactions_with_gas_info_df = add_gas_cost_information(
        transactions_with_gas_info_df, currency
    )

    transactions_with_gas_info_df.rename(
        columns={"hash": "transaction hash"}, inplace=True
    )

    print("Calculate total gas fees paid by orchestrator...")
    total_gas_cost = transactions_with_gas_info_df["gas cost (ETH)"].sum()
    total_gas_cost_eur = transactions_with_gas_info_df[f"gas cost ({currency})"].sum()

    print("Merging gas information into processed data...")
    reward_data = merge_gas_info(reward_data, transactions_with_gas_info_df, currency)
    fee_data = merge_gas_info(fee_data, transactions_with_gas_info_df, currency)
    transfer_bond_data = merge_gas_info(
        transfer_bond_data, transactions_with_gas_info_df, currency
    )

    combined_data = pd.concat([reward_data, fee_data, transfer_bond_data])

    print(f"\nOverview ({start_time} - {end_time}):")
    total_orchestrator_reward = reward_data["amount"].sum()
    total_orchestrator_reward_value = reward_data[f"value ({currency})"].sum()
    total_orchestrator_fees = fee_data["amount"].sum()
    total_orchestrator_fees_value = fee_data[f"value ({currency})"].sum()
    overview_table = [
        ["Total Orchestrator Reward (LPT)", f"{total_orchestrator_reward:.4f} LPT"],
        [
            f"Total Orchestrator Reward ({currency})",
            f"{total_orchestrator_reward_value:.4f} {currency}",
        ],
        ["Total Orchestrator Fees (ETH)", f"{total_orchestrator_fees:.4f} ETH"],
        [
            f"Total Orchestrator Fees ({currency})",
            f"{total_orchestrator_fees_value:.4f} {currency}",
        ],
        ["Total Gas Cost (ETH)", f"{total_gas_cost:.4f} ETH"],
        [f"Total Gas Cost ({currency})", f"{total_gas_cost_eur:.4f} {currency}"],
    ]
    print(tabulate(overview_table, headers=["Metric", "Value"], tablefmt="grid"))

    print("\nRetrieve token and ETH transfers...")
    token_and_eth_transfers = retrieve_token_and_eth_transfers(
        transactions_df, orchestrator, currency
    )

    print("Add missing gas cost information to token and ETH transfers...")
    token_and_eth_transfers = merge_gas_info(
        token_and_eth_transfers, transactions_with_gas_info_df, currency
    )

    print("Merging token and ETH transfers with reward, fee, and transfer bond data...")
    combined_df = pd.concat(
        [token_and_eth_transfers, reward_data, fee_data, transfer_bond_data],
        ignore_index=True,
    ).sort_values(by="timestamp")
    combined_df = combined_df[CSV_COLUMN_ORDER]

    overview_df = pd.DataFrame(overview_table, columns=["Metric", "Value"])

    print("\nExporting data to Excel with two tabs...")
    with ExcelWriter("orchestrator_income.xlsx") as writer:
        overview_df.to_excel(writer, sheet_name="overview", index=False)
        combined_df.to_excel(writer, sheet_name="transactions", index=False)

    print("Excel export completed.")
