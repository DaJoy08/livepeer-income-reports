"""Retrieve and export orchestrator income data for tax reporting as a CSV file.

Subgraph Improvements:
    - Fix gas logic (https://github.com/livepeer/subgraph/pull/164).
    - Add rewardValueUSD field to rewardEvents query to avoid fetching prices.

TODO:
    - Add compounding rewards.
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

    def calculate_gas_cost_currency(row):
        """Calculate gas cost in the specified currency."""
        try:
            eth_price = fetch_crypto_price("ETH", currency, int(row["timeStamp"]))
            return row["gas cost (ETH)"] * eth_price
        except Exception as e:
            print(f"Error fetching ETH price for transaction {row['hash']}: {e}")
            return 0

    df["gas cost (ETH)"] = (
        df["gasPrice"].astype(float) * df["gasUsed"].astype(float)
    ) / 10**18
    df[f"gas cost ({currency})"] = df.progress_apply(
        calculate_gas_cost_currency, axis=1
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
def fetch_arb_transactions(
    address: str, start_block: int = 0, end_block: int = 99999999, sort: str = "asc"
) -> list:
    """Fetch transactions for a given address on the Arbitrum (Layer 2) chain using the
    Arbiscan API, with pagination.

    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number (default: 0).
        end_block: The ending block number (default: 99999999).
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').

    Returns:
        A list of all unique transactions on the Arbitrum chain.
    """
    all_transactions = []
    processed_hashes = set()
    max_records = 1000  # Free tier limit
    current_start_block = start_block

    while current_start_block <= end_block:
        params = {
            "chainid": 42161,
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": current_start_block,
            "endblock": end_block,
            "page": 1,
            "offset": max_records,
            "sort": sort,
            "apikey": ARBISCAN_API_KEY_TOKEN,
        }

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
    return all_transactions


def fetch_arb_transactions_with_timestamps(
    address: str, start_timestamp: int, end_timestamp: int, sort: str = "asc"
) -> list:
    """Fetch transactions for a given address on the Arbitrum (Layer 2) chain using
    timestamps.

    Args:
        address: The wallet address to fetch transactions for.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.

    Returns:
        A list of ETH transactions on the Arbitrum chain within the specified time
        range.
    """
    start_block = get_block_number_by_timestamp(start_timestamp)
    end_block = get_block_number_by_timestamp(end_timestamp)
    return fetch_arb_transactions(address, start_block, end_block, sort)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_arb_token_transactions(
    address: str, start_block: int = 0, end_block: int = 99999999, sort: str = "asc"
) -> list:
    """Fetch token transactions for a given address on the Arbitrum (Layer 2) chain
    using the Arbiscan API, with pagination.

    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number (default: 0).
        end_block: The ending block number (default: 99999999).
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').

    Returns:
        list: A list of  token transactions.
    """
    all_transactions = []
    processed_hashes = set()
    max_records = 1000  # Free tier limit
    current_start_block = start_block

    while current_start_block <= end_block:
        params = {
            "chainid": 42161,
            "module": "account",
            "action": "tokentx",
            "address": address,
            "startblock": current_start_block,
            "endblock": end_block,
            "page": 1,
            "offset": max_records,
            "sort": sort,
            "apikey": ARBISCAN_API_KEY_TOKEN,
        }

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
            raise Exception(f"Error fetching LPT transactions: {data['message']}")
    return all_transactions


def fetch_arb_token_transactions_with_timestamps(
    address: str, start_timestamp: int, end_timestamp: int, sort: str = "asc"
) -> list:
    """Fetch token transactions for a given address on the Arbitrum (Layer 2) chain
    using timestamps.

    Args:
        address: The wallet address to fetch transactions for.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.

    Returns:
        A list of token transactions within the specified time range.
    """
    start_block = get_block_number_by_timestamp(start_timestamp)
    end_block = get_block_number_by_timestamp(end_timestamp)
    return fetch_arb_token_transactions(address, start_block, end_block, sort)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_arb_internal_transactions(
    address: str, start_block: int = 0, end_block: int = 99999999, sort: str = "asc"
) -> list:
    """Fetch internal transactions for a given address on the Arbitrum (Layer 2) chain
    using the Arbiscan API, with pagination.

    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number (default: 0).
        end_block: The ending block number (default: 99999999).
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').

    Returns:
        list: A list of internal transactions.
    """
    all_transactions = []
    processed_hashes = set()
    max_records = 1000  # Free tier limit
    current_start_block = start_block

    while current_start_block <= end_block:
        params = {
            "chainid": 42161,
            "module": "account",
            "action": "txlistinternal",
            "address": address,
            "startblock": current_start_block,
            "endblock": end_block,
            "page": 1,
            "offset": max_records,
            "sort": sort,
            "apikey": ARBISCAN_API_KEY_TOKEN,
        }

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
            raise Exception(f"Error fetching internal transactions: {data['message']}")
    return all_transactions


def fetch_arb_internal_transactions_with_timestamps(
    address: str, start_timestamp: int, end_timestamp: int, sort: str = "asc"
) -> list:
    """Fetch internal transactions for a given address on the Arbitrum (Layer 2) chain
    using timestamps.

    Args:
        address: The wallet address to fetch transactions for.
        start_timestamp: The start timestamp in Unix format.
        end_timestamp: The end timestamp in Unix format.

    Returns:
        A list of internal transactions within the specified time range.
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
        A Pandas DataFrame containing all transactions.
    """
    print("Fetch normal transactions...")
    normal_transactions = fetch_arb_transactions_with_timestamps(
        address, start_timestamp, end_timestamp, sort="asc"
    )

    print("Fetch token transactions...")
    token_transactions = fetch_arb_token_transactions_with_timestamps(
        address, start_timestamp, end_timestamp, sort="asc"
    )

    print("Fetch internal transactions...")
    internal_transactions = fetch_arb_internal_transactions_with_timestamps(
        address, start_timestamp, end_timestamp, sort="asc"
    )

    all_transactions = normal_transactions + token_transactions + internal_transactions
    df = pd.DataFrame(all_transactions)

    return df


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
                "transaction category": "inward",
                "currency": "LPT",
                "pool reward": pool_reward,
                "reward cut": reward_cut,
                "amount": orchestrator_reward,
                "gas price (Wei)": gas_price,
                "gas used (Wei)": gas_used,
                "gas cost (ETH)": gas_cost_eth,
                f"ETH price ({currency})": eth_price,
                f"LPT price ({currency})": lpt_price,
                f"gas cost ({currency})": gas_cost_currency,
                f"value ({currency})": orchestrator_reward * lpt_price,
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
        orch_fee = (1 - fee_share) * face_value
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
                "transaction category": "inward",
                "currency": "ETH",
                "face value": face_value,
                "fee share": fee_share,
                "amount": orch_fee,
                "gas price (Wei)": gas_price,
                "gas used (Wei)": gas_used,
                "gas cost (ETH)": gas_cost_eth,
                f"ETH price ({currency})": eth_price,
                f"gas cost ({currency})": gas_cost_currency,
                f"value ({currency})": orch_fee * eth_price,
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
        transaction = create_arbiscan_url(event["transaction"]["id"])
        amount = float(event["amount"])
        transaction_type = "reward transfer"
        transaction_category = "outward"
        gas_price = int(event["transaction"]["gasPrice"])

        try:
            gas_used = fetch_gas_used_from_rpc(event["transaction"]["id"])
        except Exception as e:
            print(f"Error fetching gas data for transaction {transaction}: {e}")
            sys.exit(1)
        gas_cost_eth = gas_price * gas_used / 10**18

        try:
            lpt_price = fetch_crypto_price("LPT", currency, event["timestamp"])
            eth_price = fetch_crypto_price("ETH", currency, event["timestamp"])
        except Exception as e:
            print(f"Error fetching ETH price: {e}")
            sys.exit(1)

        gas_cost_currency = gas_cost_eth * eth_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": event["round"]["id"],
                "transaction": transaction,
                "transaction type": transaction_type,
                "transaction category": transaction_category,
                "from": event["oldDelegator"]["id"],
                "to": event["newDelegator"]["id"],
                "currency": "LPT",
                "amount": amount,
                "gas price (Wei)": gas_price,
                "gas used (Wei)": gas_used,
                "gas cost (ETH)": gas_cost_eth,
                f"ETH price ({currency})": eth_price,
                f"LPT price ({currency})": lpt_price,
                f"gas cost ({currency})": gas_cost_currency,
                f"value ({currency})": amount * lpt_price,
            }
        )
    return pd.DataFrame(rows)


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

    print("\nFetching reward call events...")
    reward_events = fetch_reward_events(orchestrator, start_timestamp, end_timestamp)
    if not reward_events:
        print("No reward events found for the specified orchestrator and time range.")
        sys.exit(0)
    else:
        print(f"Found {len(reward_events)} reward events.")
    print("Processing reward calls...")
    reward_data = process_reward_events(reward_events, currency)

    print("Fetching fee redeem events...")
    fee_events = fetch_fee_events(orchestrator, start_timestamp, end_timestamp)
    if not fee_events:
        print("No fee events found for the specified orchestrator and time range.")
    else:
        print(f"Found {len(fee_events)} fee events.")
    print("Processing fee events...")
    fee_data = process_fee_events(fee_events, currency)

    print("Fetching transfer bond events...")
    transfer_bond_events = fetch_transfer_bond_events(
        orchestrator, start_timestamp, end_timestamp
    )
    if not transfer_bond_events:
        print("No transfer bond events found for the specified orchestrator.")
    else:
        print(f"Found {len(transfer_bond_events)} transfer bond events.")
    print("Processing transfer bond events...")
    transfer_bond_data = process_transfer_bond_events(transfer_bond_events, currency)

    print("Fetch all wallet transactions...")
    transactions_df = fetch_all_transactions(
        orchestrator, start_timestamp, end_timestamp
    )

    print("Filter transactions by sending address...")
    filtered_transactions_df = filter_transactions_by_sender(
        transactions_df, orchestrator
    )

    print("Add gas cost information to transactions")
    filtered_transactions_df = add_gas_cost_information(
        filtered_transactions_df, currency
    )

    print("Calculate total gas fees paid by orchestrator...")
    total_gas_cost = filtered_transactions_df["gas cost (ETH)"].sum()
    total_gas_cost_eur = filtered_transactions_df[f"gas cost ({currency})"].sum()

    combined_data = pd.concat(
        [
            reward_data,
            fee_data,
            transfer_bond_data,
        ]
    )

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

    print("\nStoring data in CSV format...")
    csv_file = "orchestrator_income.csv"
    combined_data.to_csv(csv_file, index=False)
    print(f"CSV file '{csv_file}' created successfully!")
