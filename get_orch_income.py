"""Retrieve and export orchestrator income data for tax reporting as a CSV file.

Upstream Improvements:
- Fix gasUsed on graph (see https://github.com/livepeer/subgraph/pull/164).
- PendingStake on graph for orchs (see https://github.com/livepeer/subgraph/pull/165).
- Have PendingStake respect round parameter (see https://github.com/livepeer/protocol/blob/e8b6243c48d9db33852310d2aefedd5b1c77b8b6/contracts/bonding/BondingManager.sol#L932).
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
import json


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

BONDING_MANAGER_CONTRACT_ADDRESS = "0x35Bcf3c30594191d53231E4FF333E8A770453e40"
ROUNDS_MANAGER_CONTRACT_ADDRESS = "0xdd6f56DcC28D3F5f27084381fE8Df634985cc39f"
LPT_TOKEN_CONTRACT_ADDRESS = "0x289ba1701C2F088cf0faf8B3705246331cB8A839"

with open("ABI/BondingManager.json", "r") as bonding_manager_abi_file:
    BONDING_MANAGER_ABI = json.load(bonding_manager_abi_file)
with open("ABI/RoundsManager.json", "r") as rounds_manager_abi_file:
    ROUNDS_MANAGER_ABI = json.load(rounds_manager_abi_file)
with open("ABI/LivepeerToken.json", "r") as lpt_token_abi_file:
    LPT_TOKEN_ABI = json.load(lpt_token_abi_file)

BONDING_MANAGER_CONTRACT = ARB_CLIENT.eth.contract(
    address=BONDING_MANAGER_CONTRACT_ADDRESS, abi=BONDING_MANAGER_ABI
)
ROUNDS_MANAGER_CONTRACT = ARB_CLIENT.eth.contract(
    address=ROUNDS_MANAGER_CONTRACT_ADDRESS, abi=ROUNDS_MANAGER_ABI
)
LPT_TOKEN_CONTRACT = ARB_CLIENT.eth.contract(
    address=LPT_TOKEN_CONTRACT_ADDRESS, abi=LPT_TOKEN_ABI
)

REWARD_EVENTS_QUERY_BASE = """
query RewardEvents($first: Int!, $skip: Int!) {{
  rewardEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
    orderBy: round__startBlock
    orderDirection: asc
  ) {{
    id
    timestamp
    transaction {{
      id
    }}
    rewardTokens
    round {{
      id
      pools(where: {{ delegate: "{delegate}" }}) {{
        rewardCut
      }}
    }}
  }}
}}
"""
WINNING_TICKET_REDEEMED_EVENTS_QUERY_BASE = """
query WinningTicketRedeemedEvents($first: Int!, $skip: Int!) {{
  winningTicketRedeemedEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
  ) {{
    id
    timestamp
    transaction {{
      id
    }}
    sender {{
      id
    }}
    faceValue
    round {{
      id
      pools(where: {{ delegate: "{recipient}" }}) {{
        feeShare
      }}
    }}
  }}
}}
"""
BOND_EVENTS_QUERY_BASE = """
query BondEvents($first: Int!, $skip: Int!) {{
  bondEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
  ) {{
    id
    timestamp
    additionalAmount
    round {{
      id
    }}
    newDelegate {{
      id
    }}
    oldDelegate {{
      id
    }}
    delegator {{
      id
    }}
    transaction {{
      id
    }}
  }}
}}
"""
UNBOND_EVENTS_QUERY_BASE = """
query UnbondEvents($first: Int!, $skip: Int!) {{
  unbondEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
  ) {{
    timestamp
    amount
    withdrawRound
    round {{
      id
    }}
    transaction {{
      id
    }}
  }}
}}
"""
TRANSFER_BOND_EVENTS_QUERY_BASE = """
query TransferBondEvents($first: Int!, $skip: Int!) {{
  transferBondEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
  ) {{
    timestamp
    amount
    round {{
      id
    }}
    oldDelegator {{
      id
    }}
    newDelegator {{
      id
    }}
    transaction {{
      id
    }}
  }}
}}
"""
WITHDRAW_STAKE_EVENTS_QUERY_BASE = """
query WithdrawStakeEvents($first: Int!, $skip: Int!) {{
  withdrawStakeEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
  ) {{
    id
    timestamp
    transaction {{
      id
    }}
    delegator {{
      id
    }}
    amount
    unbondingLockId
    round {{
      id
    }}
  }}
}}
"""
REBOND_EVENTS_QUERY_BASE = """
query RebondEvents($first: Int!, $skip: Int!) {{
  rebondEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
  ) {{
    id
    timestamp
    transaction {{
      id
    }}
    delegator {{
      id
    }}
    delegate {{
      id
    }}
    amount
    unbondingLockId
    round {{
      id
    }}
  }}
}}
"""
WITHDRAW_FEES_EVENTS_QUERY_BASE = """
query WithdrawFeesEvents($first: Int!, $skip: Int!) {{
  withdrawFeesEvents(
    where: {{ {where_clause} }}
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
  ) {{
    id
    timestamp
    transaction {{
      id
    }}
    delegator {{
      id
    }}
    amount
    round {{
      id
    }}
  }}
}}
"""
TRANSCODER_QUERY = """
query Transcoder($id: ID!) {
  transcoder(id: $id) {
    activationTimestamp
  }
}
"""
ROUND_QUERY = """
query Round($id: ID!) {
  round(id: $id) {
    id
    startBlock
    startTimestamp
  }
}
"""

def get_csv_column_order(currency: str) -> list:
    """Generate the CSV column order with dynamic currency names.

    Args:
        currency: The target currency (e.g., "EUR", "USD").

    Returns:
        A list of column names with the correct currency.
    """
    return [
        "timestamp",
        "transaction hash",
        "transaction url",
        "direction",
        "transaction type",
        "currency",
        "amount",
        f"value ({currency})",
        f"price ({currency})",
        f"gas cost ({currency})",
        "gas cost (ETH)",
        "round",
        "withdraw round",
        "release date",
        f"release price ({currency})",
        f"release value ({currency})",
        "released LPT amount",
        "release note",
        "pending stake",
        "pending fees",
        "compounding rewards",
        "reward cut",
        "fee share",
        "pool reward",
        "face value",
        "source function",
        "cumulative balance (ETH)",
        "cumulative balance (LPT)",
    ]


RPC_HISTORY_ERROR_DISPLAYED = False


def build_where_clause(filters: dict) -> str:
    """Convert a dictionary of filters into a GraphQL-compatible where clause string.

    Args:
        filters: A dictionary of filters where keys are field names and values are
            filter values.

    Returns:
        A GraphQL-compatible string representation of the where clause.
    """
    return ", ".join(
        f'{key}: "{value}"' if isinstance(value, str) else f"{key}: {value}"
        for key, value in filters.items()
        if value is not None
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
    if df.empty:
        print("No transactions found for the specified wallet address.")
        return pd.DataFrame()
    return df[df["from"].str.lower() == wallet_address.lower()]


def add_gas_cost_information(df: pd.DataFrame, currency: str = "EUR") -> pd.DataFrame:
    """Add gas cost information (in ETH and specified currency) to a DataFrame.

    Args:
        df: A Pandas DataFrame containing transaction data with columns 'gasPrice',
            'gasUsed', and 'timestamp'.
        currency: The target currency for conversion (default: "EUR").

    Returns:
        The updated DataFrame with gas cost information added.
    """
    df = df.copy()
    if df.empty:
        print("No transactions found to calculate gas costs.")
        return df

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
def fetch_activation_timestamp(orchestrator: str) -> int:
    """Fetch the activation timestamp for a given orchestrator.

    Args:
        orchestrator: The address of the orchestrator.

    Returns:
        The activation timestamp for the orchestrator, or None if not found.
    """
    try:
        query = gql(TRANSCODER_QUERY)
        variables = {"id": orchestrator.lower()}
        response = GRAPHQL_CLIENT.execute(query, variable_values=variables)

        transcoder = response.get("transcoder")
        if transcoder and transcoder.get("activationTimestamp"):
            return int(transcoder["activationTimestamp"])
        return None
    except Exception as e:
        print(f"Error fetching activation timestamp for {orchestrator}: {e}")
        return None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_round_info(round_id: int | str) -> dict | None:
    """Fetch basic round information (id, startBlock, startTimestamp) from the subgraph.

    Args:
        round_id: The round identifier (number or string).

    Returns:
        A dict with keys {"id", "startBlock", "startTimestamp"} or None if not
        found/error.
    """
    try:
        query = gql(ROUND_QUERY)
        variables = {"id": str(round_id)}
        response = GRAPHQL_CLIENT.execute(query, variable_values=variables)
        rnd = response.get("round")
        if not rnd:
            return None
        return {
            "id": int(rnd["id"]),
            "startBlock": int(rnd["startBlock"]),
            "startTimestamp": int(rnd["startTimestamp"]),
        }
    except Exception as e:
        print(f"Error fetching round info for {round_id}: {e}")
        return None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_starting_eth_balance(wallet_address: str, block_hash: str) -> float:
    """Fetch the ETH balance of a wallet at a specific block.

    Args:
        wallet_address: The wallet address to check.
        block_hash: The block hash to fetch the balance at.

    Returns:
        The ETH balance of the wallet at the specified block, in ETH units.
        Returns 0.0 if an error occurs.
    """
    try:
        checksum_address = Web3.to_checksum_address(wallet_address)
        balance_wei = ARB_CLIENT.eth.get_balance(
            checksum_address, block_identifier=block_hash
        )
        return balance_wei / 10**18
    except Exception as e:
        print(
            f"Error fetching ETH balance for {wallet_address} at block {block_hash}: "
            f"{e}"
        )
        return 0.0


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_starting_lpt_balance(wallet_address: str, block_hash: str) -> float:
    """Fetch the starting unbonded LPT balance of a wallet at a specific block.

    Args:
        wallet_address: The wallet address to check.
        block_hash: The block hash to fetch the balance at.

    Returns:
        The LPT balance of the wallet at the specified block, in LPT units.
        Returns 0.0 if an error occurs.
    """
    try:
        checksum_address = Web3.to_checksum_address(wallet_address)
        balance = LPT_TOKEN_CONTRACT.functions.balanceOf(checksum_address).call(
            block_identifier=block_hash
        )
        return balance / 10**18
    except Exception as e:
        print(
            f"Error fetching LPT balance for {wallet_address} at block {block_hash}: "
            f"{e}"
        )
        return 0.0


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_block_number_by_timestamp(timestamp: int, closest: str = "before") -> int:
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
def fetch_block_hash_for_round(round_number: str | int) -> str:
    """Fetch the block hash for a specific round using the RoundsManager contract.

    Args:
        round_number: The round number.

    Returns:
        The block hash as a hexadecimal string.
    """
    try:
        block_hash = ROUNDS_MANAGER_CONTRACT.functions.blockHashForRound(
            int(round_number)
        ).call()
        return Web3.to_hex(block_hash)
    except Exception as e:
        print(f"Error fetching block hash for round {round_number}: {e}")
        return None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_round_timestamp(round_number: int) -> int:
    """Fetch the timestamp for a specific round.

    Args:
        round_number: The round number to get the timestamp for.

    Returns:
        The timestamp when the round started, or None if error.
    """
    try:
        block_hash = fetch_block_hash_for_round(round_number)
        if not block_hash:
            return None
        block = ARB_CLIENT.eth.get_block(block_hash)
        return int(block["timestamp"])
    except Exception as e:
        print(f"Error fetching timestamp for round {round_number}: {e}")
        return None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_pending_stake(address: str, block_hash: str) -> int:
    """Fetch the pending stake for a given delegator at a specific block hash.

    Args:
        address: The address of the delegator.
        block_hash: The block hash to fetch the pending stake at.

    Returns:
        The pending stake for the delegator at the specified block hash.
    """
    try:
        checksum_address = Web3.to_checksum_address(address)
        pending_stake = BONDING_MANAGER_CONTRACT.functions.pendingStake(
            checksum_address, 0
        ).call(block_identifier=block_hash)
        return pending_stake / 10**18
    except Exception as e:
        if "missing trie node" in str(e) and not RPC_HISTORY_ERROR_DISPLAYED:
            print(
                "\033[93mWarning: RPC node lacks historical data for pendingStake. "
                "Switch to an archive node provider like Infura or Alchemy.\033[0m"
            )
            RPC_HISTORY_ERROR_DISPLAYED = True
        else:
            print(f"Error fetching pending stake for block hash {block_hash}: {e}")
        return None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_pending_fees(address: str, block_hash: str) -> float:
    """Fetch the pending fees for a given delegator at a specific block hash.

    Args:
        address: The address of the delegator.
        block_hash: The block hash to fetch the pending fees at.

    Returns:
        The pending fees for the delegator at the specified block hash.
        Returns None if an error occurs.
    """
    try:
        checksum_address = Web3.to_checksum_address(address)
        pending_fees = BONDING_MANAGER_CONTRACT.functions.pendingFees(
            checksum_address, 0
        ).call(block_identifier=block_hash)
        return pending_fees / 10**18
    except Exception as e:
        global RPC_HISTORY_ERROR_DISPLAYED
        if "missing trie node" in str(e) and not RPC_HISTORY_ERROR_DISPLAYED:
            print(
                "\033[93mWarning: RPC node lacks historical data for pendingFees. "
                "Switch to an archive node provider like Infura or Alchemy.\033[0m"
            )
            RPC_HISTORY_ERROR_DISPLAYED = True
        else:
            print(f"Error fetching pending fees for block hash {block_hash}: {e}")
        return None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type(Exception),
)
def fetch_graphql_events(query: str, variables: dict, event_key: str) -> list:
    """Fetch events from the GraphQL API based on the provided query and event key.

    Args:
        query: The GraphQL query string.
        variables: A dictionary of variables to be used in the query.
        event_key: The key in the response that contains the list of events.
    Returns:
        A list of events fetched from the GraphQL API.
    """
    all_events = []
    page_size = variables.get("first", 100)
    skip = variables.get("skip", 0)

    while True:
        variables.update({"first": page_size, "skip": skip})
        try:
            response = GRAPHQL_CLIENT.execute(query, variable_values=variables)
            events = response.get(event_key, [])
            all_events.extend(events)

            if len(events) < page_size:
                break
            skip += page_size
        except Exception as e:
            print(f"Error while fetching {event_key}: {e}")
            break
    return all_events


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

    Raises:
        ValueError: If the API response indicates an error or rate limit exceeded.
    """
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {
        "fsym": crypto_symbol,
        "tsym": target_currency,
        "limit": 1,
        "toTs": unix_timestamp,
        "api_key": CRYPTO_COMPARE_API_KEY,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise ValueError(f"Error fetching crypto price: {e}")

    if data.get("Response") == "Error":
        raise ValueError(f"CryptoCompare API Error: {data.get('Message')}")
    if not data.get("Data") or not data["Data"].get("Data"):
        raise ValueError("CryptoCompare API returned empty or invalid data.")
    return data["Data"]["Data"][-1]["close"]


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
    """Fetch transactions for a given address on the Arbitrum (Layer 2) chain using the
    Arbiscan API.

    Args:
        address: The wallet address to fetch transactions for.
        start_block: The starting block number.
        end_block: The ending block number.
        sort: The sorting order, either 'asc' or 'desc' (default: 'asc').
        action: The type of transaction to fetch (e.g., 'txlist', 'tokentx',
            'txlistinternal').

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

                # If limit hit, set next start_block to last block - 1.
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
    return fetch_transactions(
        address=address,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
        action="txlist",
    )


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
    return fetch_transactions(
        address=address,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
        action="tokentx",
    )


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
    return fetch_transactions(
        address=address,
        start_block=start_block,
        end_block=end_block,
        sort=sort,
        action="txlistinternal",
    )


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
    start_block = fetch_block_number_by_timestamp(timestamp=start_timestamp)
    end_block = fetch_block_number_by_timestamp(timestamp=end_timestamp)
    return fetch_arb_transactions(
        address=address, start_block=start_block, end_block=end_block, sort=sort
    )


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
    start_block = fetch_block_number_by_timestamp(timestamp=start_timestamp)
    end_block = fetch_block_number_by_timestamp(timestamp=end_timestamp)
    return fetch_arb_token_transactions(
        address=address, start_block=start_block, end_block=end_block, sort=sort
    )


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
    start_block = fetch_block_number_by_timestamp(timestamp=start_timestamp)
    end_block = fetch_block_number_by_timestamp(timestamp=end_timestamp)
    return fetch_arb_internal_transactions(
        address=address, start_block=start_block, end_block=end_block, sort=sort
    )


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
    transaction_types = [
        ("normal", fetch_arb_transactions_with_timestamps),
        ("token", fetch_arb_token_transactions_with_timestamps),
        ("internal", fetch_arb_internal_transactions_with_timestamps),
    ]

    combined_df = pd.DataFrame()

    for transaction_type, fetch_func in transaction_types:
        print(f"Fetching {transaction_type} transactions...")
        transactions = fetch_func(address, start_timestamp, end_timestamp, sort="asc")
        df = pd.DataFrame(transactions)
        print(f"{transaction_type.capitalize()} transactions fetched: {len(df)}")
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    print(f"Total transactions fetched: {len(combined_df)}")
    return combined_df


def fetch_reward_events(
    orchestrator: str,
    start_timestamp: int,
    end_timestamp: int,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch reward events for a given orchestrator within a specified time range.

    Args:
        orchestrator: The address of the orchestrator.
        start_timestamp: The start timestamp for the time range.
        end_timestamp: The end timestamp for the time range.
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of reward events.
    """
    where_clause = build_where_clause(
        {
            "delegate": orchestrator,
            "timestamp_gte": start_timestamp,
            "timestamp_lte": end_timestamp,
            "round": str(round) if round is not None else None,
        }
    )
    variables = {"first": page_size, "skip": 0}
    query = REWARD_EVENTS_QUERY_BASE.format(
        where_clause=where_clause, delegate=orchestrator
    )
    return fetch_graphql_events(
        query=gql(query),
        variables=variables,
        event_key="rewardEvents",
    )


def fetch_fee_events(
    recipient: str,
    start_timestamp: int,
    end_timestamp: int,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch fee events for a given recipient within a specified time range.

    Args:
        recipient: The address of the recipient.
        start_timestamp: The start timestamp for the time range.
        end_timestamp: The end timestamp for the time range.
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of fee events.
    """
    where_clause = build_where_clause(
        {
            "recipient": recipient,
            "timestamp_gte": start_timestamp,
            "timestamp_lte": end_timestamp,
            "round": str(round) if round is not None else None,
        }
    )
    variables = {"first": page_size, "skip": 0}
    query = WINNING_TICKET_REDEEMED_EVENTS_QUERY_BASE.format(
        where_clause=where_clause, recipient=recipient
    )
    return fetch_graphql_events(
        query=gql(query),
        variables=variables,
        event_key="winningTicketRedeemedEvents",
    )


def fetch_bond_events(
    delegator: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch bond events for a given delegator within a specified time range or
    round.

    Args:
        delegator: The address of the delegator.
        start_timestamp: The start timestamp for the time range (optional).
        end_timestamp: The end timestamp for the time range (optional).
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of bond events.
    """
    where_clause = build_where_clause(
        {
            "delegator": delegator,
            "timestamp_gte": start_timestamp,
            "timestamp_lte": end_timestamp,
            "round": str(round) if round is not None else None,
        }
    )
    variables = {"first": page_size, "skip": 0}
    query = BOND_EVENTS_QUERY_BASE.format(where_clause=where_clause)
    return fetch_graphql_events(
        query=gql(query),
        variables=variables,
        event_key="bondEvents",
    )


def fetch_unbond_events(
    delegator: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch unbond events for a given delegator within a specified time range or
    round.

    Args:
        delegator: The address of the delegator.
        start_timestamp: The start timestamp for the time range (optional).
        end_timestamp: The end timestamp for the time range (optional).
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of unbond events.
    """
    where_clause = build_where_clause(
        {
            "delegator": delegator,
            "timestamp_gte": start_timestamp,
            "timestamp_lte": end_timestamp,
            "round": str(round) if round is not None else None,
        }
    )
    variables = {"first": page_size, "skip": 0}
    query = UNBOND_EVENTS_QUERY_BASE.format(where_clause=where_clause)
    return fetch_graphql_events(
        query=gql(query),
        variables=variables,
        event_key="unbondEvents",
    )


def fetch_transfer_bond_events(
    delegator: str,
    start_timestamp: int,
    end_timestamp: int,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch transfer bond events for a given delegator within a specified time range.

    Args:
        delegator: The address of the delegator.
        start_timestamp: The start timestamp for the time range.
        end_timestamp: The end timestamp for the time range.
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of transfer bond events.
    """
    base_filters = {
        "timestamp_gte": start_timestamp,
        "timestamp_lte": end_timestamp,
        "round": str(round) if round is not None else None,
    }
    or_conditions = [
        f'{{oldDelegator: "{delegator}", {build_where_clause(base_filters)}}}',
        f'{{newDelegator: "{delegator}", {build_where_clause(base_filters)}}}',
    ]
    where_clause = f"or: [{', '.join(or_conditions)}]"
    variables = {"first": page_size, "skip": 0}
    query = TRANSFER_BOND_EVENTS_QUERY_BASE.format(where_clause=where_clause)
    return fetch_graphql_events(
        query=gql(query), variables=variables, event_key="transferBondEvents"
    )


def fetch_withdraw_stake_events(
    delegator: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch withdraw stake events for a given delegator within a specified time range.

    Args:
        delegator: The address of the delegator.
        start_timestamp: The start timestamp for the time range (optional).
        end_timestamp: The end timestamp for the time range (optional).
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of withdraw stake events.
    """
    where_clause = build_where_clause(
        {
            "delegator": delegator,
            "timestamp_gte": start_timestamp,
            "timestamp_lte": end_timestamp,
            "round": str(round) if round is not None else None,
        }
    )
    variables = {"first": page_size, "skip": 0}
    query = WITHDRAW_STAKE_EVENTS_QUERY_BASE.format(where_clause=where_clause)
    return fetch_graphql_events(
        query=gql(query),
        variables=variables,
        event_key="withdrawStakeEvents",
    )


def fetch_withdraw_fees_events(
    delegator: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch withdraw fees events for a given delegator within a specified time range.

    Args:
        delegator: The address of the delegator.
        start_timestamp: The start timestamp for the time range (optional).
        end_timestamp: The end timestamp for the time range (optional).
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of withdraw fees events.
    """
    where_clause = build_where_clause(
        {
            "delegator": delegator,
            "timestamp_gte": start_timestamp,
            "timestamp_lte": end_timestamp,
            "round": str(round) if round is not None else None,
        }
    )
    variables = {"first": page_size, "skip": 0}
    query = WITHDRAW_FEES_EVENTS_QUERY_BASE.format(where_clause=where_clause)
    return fetch_graphql_events(
        query=gql(query),
        variables=variables,
        event_key="withdrawFeesEvents",
    )


def fetch_rebond_events(
    delegator: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
    round: str | int = None,
    page_size: int = 100,
) -> list[object]:
    """Fetch rebond events for a given delegator within a specified time range.

    Args:
        delegator: The address of the delegator.
        start_timestamp: The start timestamp for the time range (optional).
        end_timestamp: The end timestamp for the time range (optional).
        round: The round to filter events by (optional).
        page_size: The number of events to fetch per page (default: 100).

    Returns:
        A list of rebond events.
    """
    where_clause = build_where_clause(
        {
            "delegator": delegator,
            "timestamp_gte": start_timestamp,
            "timestamp_lte": end_timestamp,
            "round": str(round) if round is not None else None,
        }
    )
    variables = {"first": page_size, "skip": 0}
    query = REBOND_EVENTS_QUERY_BASE.format(where_clause=where_clause)
    return fetch_graphql_events(
        query=gql(query),
        variables=variables,
        event_key="rebondEvents",
    )


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
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        pool_reward = float(event["rewardTokens"])
        reward_cut = int(event["round"]["pools"][0]["rewardCut"]) / 10**6
        orchestrator_reward = reward_cut * pool_reward
        transaction_type = "reward cut"

        lpt_price = fetch_crypto_price(
            crypto_symbol="LPT",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = orchestrator_reward * lpt_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
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
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        face_value = float(event["faceValue"])
        fee_share = int(event["round"]["pools"][0]["feeShare"]) / 10**6
        orch_fee = (1 - fee_share) * face_value
        transaction_type = "fee cut"

        eth_price = fetch_crypto_price(
            crypto_symbol="ETH",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = orch_fee * eth_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
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
                "sender": event["sender"]["id"],
            }
        )
    return pd.DataFrame(rows)


def process_bond_events(bond_events: list, currency: str) -> pd.DataFrame:
    """Process bond events and integrate transferBond events.

    Args:
        bond_events: A list of bond events.
        currency: The currency for the bond values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the bond data.
    """
    rows = []
    for event in tqdm(bond_events, desc="Processing bond events", unit="event"):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        amount = float(event["additionalAmount"])
        transaction_type = "bond"

        lpt_price = fetch_crypto_price(
            crypto_symbol="LPT",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = amount * lpt_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": transaction_type,
                "direction": "incoming",
                "currency": "LPT",
                "amount": amount,
                f"price ({currency})": lpt_price,
                f"value ({currency})": value_currency,
                "source function": "bond",
            }
        )
    return pd.DataFrame(rows)


def process_unbond_events(unbond_events: list, currency: str) -> pd.DataFrame:
    """Process unbond events and create a DataFrame with relevant information.

    Args:
        unbond_events: A list of unbond events.
        currency: The currency for the unbond values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the unbond data.
    """
    rows = []
    for event in tqdm(unbond_events, desc="Processing unbond events", unit="event"):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        amount = float(event["amount"])
        transaction_type = "unbond"
        withdraw_round = event.get("withdrawRound")

        # Get LPT price at unbond time.
        lpt_price = fetch_crypto_price(
            crypto_symbol="LPT",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = amount * lpt_price

        # Get withdrawal availability information.
        available_timestamp = None
        available_date = None
        available_lpt_price = None

        if withdraw_round:
            available_timestamp = fetch_round_timestamp(int(withdraw_round))
            if available_timestamp:
                available_date = datetime.fromtimestamp(
                    available_timestamp, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                available_lpt_price = fetch_crypto_price(
                    crypto_symbol="LPT",
                    target_currency=currency,
                    unix_timestamp=available_timestamp,
                )

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
                "withdraw round": withdraw_round or "N/A",
                "release date": available_date or "N/A",
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": transaction_type,
                "direction": "outgoing",
                "currency": "LPT",
                "amount": amount,
                f"price ({currency})": lpt_price,
                f"value ({currency})": value_currency,
                f"release price ({currency})": available_lpt_price or "N/A",
                f"release value ({currency})": "TBD",  # Will be calculated later
                "source function": "unbond",
            }
        )
    return pd.DataFrame(rows)


def calculate_actual_release_values(
    unbond_data: pd.DataFrame,
    transfer_bond_data: pd.DataFrame,
    bond_data: pd.DataFrame,
    currency: str,
) -> pd.DataFrame:
    """Calculate actual release values after accounting for transfer bonds and rebonds.

    Args:
        unbond_data: DataFrame containing unbond events.
        transfer_bond_data: DataFrame containing transfer bond events.
        bond_data: DataFrame containing bond events (rebonds).
        currency: The currency for the values (e.g., "EUR", "USD").

    Returns:
        Updated unbond_data DataFrame with corrected release values.
    """
    if unbond_data.empty:
        return unbond_data

    # Loop through each unbond event and adjust release values.
    unbond_data = unbond_data.copy()
    for idx, row in unbond_data.iterrows():
        release_price = row[f"release price ({currency})"]
        if release_price == "N/A":
            unbond_data.at[idx, f"release value ({currency})"] = "N/A"
            unbond_data.at[idx, "released LPT amount"] = "N/A"
            continue  # Skip if no release price available
        unbond_tx = row["transaction hash"]
        unbond_amount = row["amount"]
        unbond_round = int(row["round"])
        withdraw_round = row["withdraw round"]
        if withdraw_round == "N/A":
            unbond_data.at[idx, f"release value ({currency})"] = "N/A"
            unbond_data.at[idx, "released LPT amount"] = "N/A"
            continue  # Skip if no withdraw round available
        withdraw_round = int(withdraw_round)

        # Check for direct transferBond in same transaction (0 taxable value).
        if not transfer_bond_data.empty:
            direct_transfer_bond = transfer_bond_data[
                (transfer_bond_data["transaction hash"] == unbond_tx)
                & (transfer_bond_data["direction"] == "outgoing")
                & (transfer_bond_data["transaction type"] == "reward transfer")
            ]
            if not direct_transfer_bond.empty:
                unbond_data.at[idx, f"release value ({currency})"] = 0
                unbond_data.at[idx, "released LPT amount"] = 0
                unbond_data.at[idx, "release note"] = "direct transfer"
                continue

        # Calculate released amount after re-bonds.
        total_rebonded = 0
        if not bond_data.empty:
            rebonds_during_unbonding = bond_data[
                (bond_data["round"].astype(int) > unbond_round)
                & (bond_data["round"].astype(int) < withdraw_round)
            ]
            total_rebonded = (
                rebonds_during_unbonding["amount"].sum()
                if not rebonds_during_unbonding.empty
                else 0
            )
        
        remaining_amount = max(0, unbond_amount - total_rebonded)
        final_release_value = remaining_amount * release_price
        unbond_data.at[idx, f"release value ({currency})"] = final_release_value
        unbond_data.at[idx, "released LPT amount"] = remaining_amount
        if total_rebonded > 0:
            unbond_data.at[idx, "release note"] = f"rebonded {total_rebonded:.4f} LPT"
    return unbond_data


def process_transfer_bond_events(
    transfer_bond_events: list, currency: str, delegator: str
) -> pd.DataFrame:
    """Process transfer bond events and create a DataFrame with relevant information.

    Args:
        transfer_bond_events: A list of transfer bond events.
        currency: The currency for the reward values (e.g., "EUR", "USD").
        delegator: The address of the delegator/orchestrator to determine direction.

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
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        amount = float(event["amount"])
        transaction_type = "reward transfer"
        direction = (
            "incoming" if event["newDelegator"]["id"] == delegator else "outgoing"
        )

        lpt_price = fetch_crypto_price(
            crypto_symbol="LPT",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = amount * lpt_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": transaction_type,
                "direction": direction,
                "currency": "LPT",
                "amount": amount,
                f"price ({currency})": lpt_price,
                f"value ({currency})": value_currency,
                "source function": "transferBond",
            }
        )
    return pd.DataFrame(rows)


def process_withdraw_stake_events(
    withdraw_stake_events: list, currency: str
) -> pd.DataFrame:
    """Process withdraw stake events and create a DataFrame with relevant information.

    Args:
        withdraw_stake_events: A list of withdraw stake events.
        currency: The currency for the values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the withdraw stake data.
    """
    rows = []
    for event in tqdm(
        withdraw_stake_events, desc="Processing withdraw stake events", unit="event"
    ):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        amount = float(event["amount"])

        lpt_price = fetch_crypto_price(
            crypto_symbol="LPT",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = amount * lpt_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": "withdraw stake",
                "direction": "incoming",
                "currency": "LPT",
                "amount": amount,
                f"price ({currency})": lpt_price,
                f"value ({currency})": value_currency,
                "source function": "withdrawStake",
            }
        )
    return pd.DataFrame(rows)


def process_rebond_events(rebond_events: list, currency: str) -> pd.DataFrame:
    """Process rebond events and create a DataFrame with relevant information.

    Args:
        rebond_events: A list of rebond events.
        currency: The currency for the values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the rebond data.
    """
    rows = []
    for event in tqdm(rebond_events, desc="Processing rebond events", unit="event"):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        amount = float(event["amount"])

        lpt_price = fetch_crypto_price(
            crypto_symbol="LPT",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = amount * lpt_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": "rebond",
                "direction": "incoming",
                "currency": "LPT",
                "amount": amount,
                f"price ({currency})": lpt_price,
                f"value ({currency})": value_currency,
                "source function": "rebond",
            }
        )
    return pd.DataFrame(rows)


def process_withdraw_fees_events(
    withdraw_fees_events: list, currency: str
) -> pd.DataFrame:
    """Process withdraw fees events and create a DataFrame with relevant information.

    Args:
        withdraw_fees_events: A list of withdraw fees events.
        currency: The currency for the fee values (e.g., "EUR", "USD").

    Returns:
        A Pandas DataFrame representing the withdraw fees data.
    """
    rows = []
    for event in tqdm(
        withdraw_fees_events, desc="Processing withdraw fees events", unit="event"
    ):
        timestamp = datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        round_id = event["round"]["id"]
        transaction = event["transaction"]["id"]
        transaction_url = create_arbiscan_url(transaction)
        amount = float(event["amount"])

        eth_price = fetch_crypto_price(
            crypto_symbol="ETH",
            target_currency=currency,
            unix_timestamp=event["timestamp"],
        )
        value_currency = amount * eth_price

        rows.append(
            {
                "timestamp": timestamp,
                "round": round_id,
                "transaction hash": transaction,
                "transaction url": transaction_url,
                "transaction type": "withdraw fees",
                "direction": "outgoing",
                "currency": "ETH",
                "amount": amount,
                f"price ({currency})": eth_price,
                f"value ({currency})": value_currency,
                "source function": "withdrawFees",
            }
        )
    return pd.DataFrame(rows)


def fetch_and_process_events(
    address: str,
    start_timestamp: int,
    end_timestamp: int,
    currency: str,
    fetch_func: Callable[[str, int, int], list],
    process_func: Callable,
    event_name: str,
) -> pd.DataFrame:
    """Fetch and process blockchain events for a given address and time range.

    Args:
        address: The address of the orchestrator or delegator.
        start_timestamp: The start timestamp for the time range.
        end_timestamp: The end timestamp for the time range.
        currency: The currency for the event values (e.g., "EUR", "USD").
        fetch_func: A function to fetch events, taking address, start and
            end timestamps, and returning a list of events.
        process_func: A function to process fetched events. Should take
            (events, currency) as arguments and return a DataFrame.
        event_name: A string representing the name of the event being processed
            (e.g., "reward events").

    Returns:
        A Pandas DataFrame containing the processed event data. If no events are found,
            returns an empty DataFrame.
    """
    print(f"\nFetching {event_name}...")
    events = fetch_func(address, start_timestamp, end_timestamp)
    if events:
        print(f"Found {len(events)} {event_name}.")
        return process_func(events, currency)
    else:
        print(f"No {event_name} found for the specified address and time range.")
        return pd.DataFrame()


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
    if transactions_df.empty:
        print("No transactions available to process.")
        return pd.DataFrame()

    if "tokenSymbol" not in transactions_df.columns:
        transactions_df["tokenSymbol"] = None

    wallet_address = wallet_address.lower()
    processed_rows = []

    # Process LPT and ETH transactions.
    for token_symbol, token_filter in [
        ("LPT", transactions_df["tokenSymbol"] == "LPT"),
        (
            "ETH",
            transactions_df["tokenSymbol"].isna()
            & (transactions_df["value"].astype(float) > 0),
        ),
    ]:
        for direction, column in [("incoming", "to"), ("outgoing", "from")]:
            filtered_transactions = transactions_df[
                token_filter & (transactions_df[column].str.lower() == wallet_address)
            ]
            for _, row in filtered_transactions.iterrows():
                timestamp = datetime.fromtimestamp(
                    int(row["timeStamp"]), tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                amount = int(row["value"]) / 10**18
                price = fetch_crypto_price(
                    crypto_symbol=token_symbol,
                    target_currency=currency,
                    unix_timestamp=int(row["timeStamp"]),
                )
                amount = float(row["value"]) / 10**18
                function_name = infer_function_name(
                    row=row, transactions_df=transactions_df
                )
                processed_rows.append(
                    {
                        "timestamp": timestamp,
                        "transaction hash": row["hash"],
                        "transaction url": create_arbiscan_url(row["hash"]),
                        "direction": direction,
                        "transaction type": "transfer",
                        "currency": token_symbol,
                        "amount": amount,
                        f"price ({currency})": price,
                        f"value ({currency})": amount * price,
                        "source function": function_name,
                    }
                )
    return pd.DataFrame(processed_rows)


def add_pending_stake(
    address: str,
    reward_data: pd.DataFrame,
) -> pd.DataFrame:
    """Add pending stake information for each round in which a reward was received.

    Args:
        address: Delegator address to fetch pending stake for.
        reward_data: A DataFrame containing reward event data.

    Returns:
        A DataFrame with an additional column for pending stake.
    """
    if reward_data.empty:
        print("No reward data available to process.")
        return reward_data
    reward_data = reward_data.copy()

    print("Fetching block hashes and pending stakes...")
    reward_data["blockHash"] = reward_data["round"].progress_apply(
        fetch_block_hash_for_round
    )
    reward_data["pending stake"] = reward_data.progress_apply(
        lambda row: fetch_pending_stake(address=address, block_hash=row["blockHash"]),
        axis=1,
    )

    return reward_data


def add_pending_fees(
    address: str,
    fee_data: pd.DataFrame,
) -> pd.DataFrame:
    """Add pending fees information for each round in which a reward was received.

    Args:
        address: Delegator address to fetch pending fees for.
        reward_data: A DataFrame containing reward event data.

    Returns:
        A DataFrame with an additional column for pending fees.
    """
    if fee_data.empty:
        print("No reward data available to process.")
        return fee_data
    fee_data = fee_data.copy()

    print("Fetching block hashes and pending fees...")
    fee_data["blockHash"] = fee_data["round"].progress_apply(fetch_block_hash_for_round)
    fee_data["pending fees"] = fee_data.progress_apply(
        lambda row: fetch_pending_fees(address=address, block_hash=row["blockHash"]),
        axis=1,
    )

    return fee_data


def add_compounding_rewards(
    orchestrator: str,
    reward_data: pd.DataFrame,
    bond_events: pd.DataFrame,
    unbond_events: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate compounding rewards for each round in which a reward was received.

    Args:
        orchestrator: The address of the orchestrator.
        reward_data: A DataFrame containing reward event data with pending stake.
        bond_events: A DataFrame of bond events (can be empty).
        unbond_events: A DataFrame of unbond events (can be empty).

    Returns:
        A DataFrame with an additional column for compounding rewards.
    """
    if reward_data.empty:
        print("No reward data available to process.")
        return reward_data
    reward_data = reward_data.copy()

    # Fetch data for the round before the first reward round.
    first_reward_round = int(reward_data["round"].iloc[0])
    previous_round = first_reward_round - 1
    previous_pending_stake = fetch_pending_stake(
        address=orchestrator, block_hash=fetch_block_hash_for_round(previous_round)
    )
    prev_bond = sum(
        float(event["additionalAmount"])
        for event in fetch_bond_events(delegator=orchestrator, round=previous_round)
    )
    prev_unbond = sum(
        float(event["amount"])
        for event in fetch_unbond_events(delegator=orchestrator, round=previous_round)
    )

    print("Calculating expected pending stake and compounding rewards...")
    reward_data["compounding rewards"] = 0.0
    for i, row in tqdm(
        reward_data.iterrows(), total=len(reward_data), desc="Processing rewards"
    ):
        current_round = row["round"]
        reward = row["amount"]

        # Get bond and unbond totals for the current round.
        current_bond = (
            bond_events[bond_events["round"] == current_round]["amount"].sum()
            if not bond_events.empty
            else 0
        )
        current_unbond = (
            unbond_events[unbond_events["round"] == current_round]["amount"].sum()
            if not unbond_events.empty
            else 0
        )

        # Retrieve the previous stake.
        previous_stake = (
            previous_pending_stake
            if i == 0
            else reward_data.loc[i - 1, "pending stake"]
        )

        # Calculate the expected pending stake and compounding rewards.
        expected_pending_stake = previous_stake + reward - prev_unbond + prev_bond
        reward_data.at[i, "compounding rewards"] = (
            row["pending stake"] - expected_pending_stake
        )

        prev_bond, prev_unbond = current_bond, current_unbond
    return reward_data


def add_cumulative_balances(
    combined_df: pd.DataFrame,
    starting_eth_balance: float,
    starting_lpt_balance: float,
) -> pd.DataFrame:
    """Add total controlled balances (wallet + pending amounts).

    Args:
        combined_df: The DataFrame containing transaction data.
        starting_eth_balance: Starting ETH wallet balance.
        starting_lpt_balance: Starting LPT wallet balance.

    Returns:
        DataFrame with total controlled balances.
    """
    # Total ETH = Starting ETH + Pending Fees + Net Transfers.
    eth_transfers = combined_df.apply(
        lambda row: (
            row["amount"]
            if row["currency"] == "ETH"
            and row["direction"] == "incoming"
            and row["transaction type"] == "transfer"
            else (
                -row["amount"]
                if row["currency"] == "ETH"
                and row["direction"] == "outgoing"
                and row["transaction type"] == "transfer"
                else 0
            )
        ),
        axis=1,
    ).cumsum()

    combined_df["cumulative balance (ETH)"] = (
        starting_eth_balance
        + eth_transfers
        + (
            combined_df["pending fees"].fillna(0)
            if "pending fees" in combined_df.columns
            else 0
        )
    )

    # Total LPT = Starting LPT + Pending Stake + Net Transfers.
    lpt_transfers = combined_df.apply(
        lambda row: (
            row["amount"]
            if row["currency"] == "LPT"
            and row["direction"] == "incoming"
            and row["transaction type"] == "transfer"
            else (
                -row["amount"]
                if row["currency"] == "LPT"
                and row["direction"] == "outgoing"
                and row["transaction type"] == "transfer"
                else 0
            )
        ),
        axis=1,
    ).cumsum()
    combined_df["cumulative balance (LPT)"] = (
        starting_lpt_balance
        + lpt_transfers
        + (
            combined_df["pending stake"].fillna(0)
            if "pending stake" in combined_df.columns
            else 0
        )
    )

    return combined_df


def generate_overview_table(
    orchestrator: str,
    start_time: str,
    end_time: str,
    activation_timestamp: int,
    reward_data: pd.DataFrame,
    fee_data: pd.DataFrame,
    unbond_data: pd.DataFrame,
    withdraw_fees_data: pd.DataFrame,
    total_gas_cost: float,
    total_gas_cost_eur: float,
    currency: str,
    starting_eth_balance: float,
    starting_eth_value: float,
    starting_lpt_balance: float,
    starting_lpt_value: float,
    end_eth_balance: float,
    end_eth_value: float,
    end_lpt_balance: float,
    end_lpt_value: float,
    gateways: int = 0,
) -> list:
    """Generate an overview table with key metrics.

    Args:
        orchestrator: The address of the orchestrator.
        start_time: The start time of the data range.
        end_time: The end time of the data range.
        activation_timestamp: The activation timestamp of the orchestrator.
        reward_data: DataFrame containing reward data.
        fee_data: DataFrame containing fee data.
        unbond_data: DataFrame containing unbond data.
        withdraw_fees_data: DataFrame containing withdraw fees data.
        total_gas_cost: Total gas cost in ETH.
        total_gas_cost_eur: Total gas cost in the specified currency.
        currency: The currency for the overview table.
        starting_eth_balance: The starting ETH balance.
        starting_eth_value: The starting ETH value in the specified currency.
        starting_lpt_balance: The starting LPT balance.
        starting_lpt_value: The starting LPT value in the specified currency.
        end_eth_balance: The ending ETH balance.
        end_eth_value: The ending ETH value in the specified currency.
        end_lpt_balance: The ending LPT balance.
        end_lpt_value: The ending LPT value in the specified currency.
        gateways: The number of gateway sending tickets (default: 0).

    Returns:
        A list of lists representing the overview table.
    """
    reward_data = reward_data.copy()
    fee_data = fee_data.copy()

    total_orchestrator_reward = reward_data.get("amount", pd.Series(0)).sum()
    total_orchestrator_reward_value = reward_data.get(
        f"value ({currency})", pd.Series(0)
    ).sum()
    total_orchestrator_fees = fee_data.get("amount", pd.Series(0)).sum()
    total_orchestrator_fees_value = fee_data.get(
        f"value ({currency})", pd.Series(0)
    ).sum()
    total_compounding_rewards = reward_data.get(
        "compounding rewards", pd.Series(0)
    ).sum()
    reward_data["compounding rewards (currency)"] = reward_data.get(
        "compounding rewards", pd.Series(0)
    ) * reward_data.get(f"price ({currency})", pd.Series(0))
    total_compounding_rewards_value = reward_data.get(
        "compounding rewards (currency)", pd.Series(0)
    ).sum()
    total_value_accumulated = (
        total_orchestrator_reward_value
        + total_compounding_rewards_value
        - total_gas_cost_eur
    )

    # Calculate total withdrawn fees.
    withdraw_fees_data = withdraw_fees_data.copy()
    total_withdrawn_fees = 0
    total_withdrawn_fees_value = 0
    if not withdraw_fees_data.empty:
        total_withdrawn_fees = withdraw_fees_data.get("amount", pd.Series(0)).sum()
        total_withdrawn_fees_value = withdraw_fees_data.get(
            f"value ({currency})", pd.Series(0)
        ).sum()

    # Calculate total release value and amount from unbond events.
    unbond_data = unbond_data.copy()
    total_release_value = 0
    total_released_lpt = 0
    if not unbond_data.empty:
        # Only sum non-zero and non-"N/A" release values.
        valid_release_values = unbond_data[
            (unbond_data[f"release value ({currency})"] != "N/A")
            & (unbond_data[f"release value ({currency})"] != 0)
        ]
        if not valid_release_values.empty:
            total_release_value = valid_release_values[
                f"release value ({currency})"
            ].sum()
            valid_released_amounts = unbond_data[
                (unbond_data["released LPT amount"] != "N/A")
                & (unbond_data["released LPT amount"] != 0)
            ]
            if not valid_released_amounts.empty:
                total_released_lpt = valid_released_amounts["released LPT amount"].sum()

    activation_time = (
        datetime.fromtimestamp(activation_timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if activation_timestamp
        else "N/A"
    )

    overview_table = [
        ["Network", "Arbitrum"],
        ["Wallet Address", orchestrator],
        ["Activation Time", activation_time],
        ["Start Time", start_time],
        ["End Time", end_time],
        [
            "Starting ETH Balance",
            f"{starting_eth_balance:.4f} ETH ({starting_eth_value:.2f} {currency})",
        ],
        [
            "Starting LPT Balance",
            f"{starting_lpt_balance:.4f} LPT ({starting_lpt_value:.2f} {currency})",
        ],
        [
            "Ending ETH Balance",
            f"{end_eth_balance:.4f} ETH ({end_eth_value:.2f} {currency})",
        ],
        [
            "Ending LPT Balance",
            f"{end_lpt_balance:.4f} LPT ({end_lpt_value:.2f} {currency})",
        ],
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
        ["Total Withdrawn Fees (ETH)", f"{total_withdrawn_fees:.4f} ETH"],
        [
            f"Total Withdrawn Fees ({currency})",
            f"{total_withdrawn_fees_value:.4f} {currency}",
        ],
        ["Total Gas Cost (ETH)", f"{total_gas_cost:.4f} ETH"],
        [f"Total Gas Cost ({currency})", f"{total_gas_cost_eur:.4f} {currency}"],
        ["Total Compounding Rewards (LPT)", f"{total_compounding_rewards:.4f} LPT"],
        [
            f"Total Compounding Rewards ({currency})",
            f"{total_compounding_rewards_value:.4f} {currency}",
        ],
        [
            f"Total Value Accumulated ({currency})",
            f"{total_value_accumulated:.4f} {currency}",
        ],
        ["Total Released LPT", f"{total_released_lpt:.4f} LPT"],
        [
            f"Total Released LPT Value ({currency})",
            f"{total_release_value:.4f} {currency}",
        ],
        [
            "Gateways Sending Tickets",
            f"{gateways}" if gateways > 0 else "N/A",
        ],
    ]
    return overview_table


if __name__ == "__main__":
    print("== Orchestrator Income Data Exporter ==")

    start_time = input("Enter data range start (YYYY-MM-DD HH:MM:SS): ").strip()
    start_timestamp = human_to_unix_time(human_time=start_time)
    end_time = input("Enter data range end (YYYY-MM-DD HH:MM:SS): ").strip()
    end_timestamp = human_to_unix_time(human_time=end_time)
    orchestrator = input("Enter orchestrator address: ").strip().lower()
    if not orchestrator:
        print("Orchestrator address is required.")
        sys.exit(1)
    currency = input("Enter currency (default: EUR): ").strip().upper() or "EUR"

    print("\nFetching orchestrator activation timestamp...")
    activation_timestamp = fetch_activation_timestamp(orchestrator)
    if activation_timestamp:
        activation_time = datetime.fromtimestamp(
            activation_timestamp, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Orchestrator activated at: {activation_time}")
    else:
        print("Could not fetch activation timestamp")

    print("\nFetching start and end balances...")
    start_block_hash = fetch_block_number_by_timestamp(timestamp=start_timestamp)
    end_block_hash = fetch_block_number_by_timestamp(timestamp=end_timestamp)
    starting_eth_balance = fetch_starting_eth_balance(
        wallet_address=orchestrator, block_hash=start_block_hash
    )
    starting_lpt_balance = fetch_starting_lpt_balance(
        wallet_address=orchestrator, block_hash=start_block_hash
    )
    end_eth_balance = fetch_starting_eth_balance(
        wallet_address=orchestrator, block_hash=end_block_hash
    )
    end_lpt_balance = fetch_starting_lpt_balance(
        wallet_address=orchestrator, block_hash=end_block_hash
    )
    start_eth_price = fetch_crypto_price(
        crypto_symbol="ETH", target_currency=currency, unix_timestamp=start_timestamp
    )
    start_lpt_price = fetch_crypto_price(
        crypto_symbol="LPT", target_currency=currency, unix_timestamp=start_timestamp
    )
    end_eth_price = fetch_crypto_price(
        crypto_symbol="ETH", target_currency=currency, unix_timestamp=end_timestamp
    )
    end_lpt_price = fetch_crypto_price(
        crypto_symbol="LPT", target_currency=currency, unix_timestamp=end_timestamp
    )
    starting_eth_value = starting_eth_balance * start_eth_price
    starting_lpt_value = starting_lpt_balance * start_lpt_price
    end_eth_value = end_eth_balance * end_eth_price
    end_lpt_value = end_lpt_balance * end_lpt_price
    print(
        f"Starting ETH Balance: {starting_eth_balance:.4f} ETH "
        f"({starting_eth_value:.2f} {currency})"
    )
    print(
        f"Ending ETH Balance: {end_eth_balance:.4f} ETH "
        f"({end_eth_value:.2f} {currency})"
    )
    print(
        f"Starting LPT Balance: {starting_lpt_balance:.4f} LPT "
        f"({starting_lpt_value:.2f} {currency})"
    )
    print(
        f"Ending LPT Balance: {end_lpt_balance:.4f} LPT "
        f"({end_lpt_value:.2f} {currency})"
    )

    reward_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_reward_events,
        process_func=process_reward_events,
        event_name="reward events",
    )
    fee_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_fee_events,
        process_func=process_fee_events,
        event_name="fee events",
    )
    bond_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_bond_events,
        process_func=process_bond_events,
        event_name="bond events",
    )
    unbond_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_unbond_events,
        process_func=process_unbond_events,
        event_name="unbond events",
    )
    transfer_bond_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_transfer_bond_events,
        process_func=lambda events, currency: process_transfer_bond_events(
            transfer_bond_events=events, currency=currency, delegator=orchestrator
        ),
        event_name="transfer bond events",
    )
    withdraw_stake_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_withdraw_stake_events,
        process_func=process_withdraw_stake_events,
        event_name="withdraw stake events",
    )
    withdraw_fees_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_withdraw_fees_events,
        process_func=process_withdraw_fees_events,
        event_name="withdraw fees events",
    )
    rebond_data = fetch_and_process_events(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        currency=currency,
        fetch_func=fetch_rebond_events,
        process_func=process_rebond_events,
        event_name="rebond events",
    )

    print("\nFetching all wallet transactions...")
    transactions_df = fetch_all_transactions(
        address=orchestrator,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )

    print("Filtering transactions by sending address...")
    transactions_with_gas_info_df = filter_transactions_by_sender(
        df=transactions_df, wallet_address=orchestrator
    )
    print(f"Total transactions found: {len(transactions_with_gas_info_df)}")

    print("\nFetching gas cost information...")
    transactions_with_gas_info_df = add_gas_cost_information(
        df=transactions_with_gas_info_df, currency=currency
    )

    transactions_with_gas_info_df.rename(
        columns={"hash": "transaction hash"}, inplace=True
    )

    print("Calculating total gas fees paid by orchestrator...")
    total_gas_cost = transactions_with_gas_info_df.get(
        "gas cost (ETH)", pd.Series(0)
    ).sum()
    total_gas_cost_eur = transactions_with_gas_info_df.get(
        f"gas cost ({currency})", pd.Series(0)
    ).sum()

    print("Merging gas information into processed data...")
    reward_data = merge_gas_info(
        data=reward_data, gas_info_df=transactions_with_gas_info_df, currency=currency
    )
    fee_data = merge_gas_info(
        data=fee_data, gas_info_df=transactions_with_gas_info_df, currency=currency
    )
    bond_data = merge_gas_info(
        data=bond_data, gas_info_df=transactions_with_gas_info_df, currency=currency
    )
    unbond_data = merge_gas_info(
        data=unbond_data, gas_info_df=transactions_with_gas_info_df, currency=currency
    )
    transfer_bond_data = merge_gas_info(
        data=transfer_bond_data,
        gas_info_df=transactions_with_gas_info_df,
        currency=currency,
    )

    print("\nAdding pending stake to data...")
    reward_data = add_pending_stake(
        address=orchestrator,
        reward_data=reward_data,
    )
    print("\nAdding pending fees to data...")
    fee_data = add_pending_fees(
        address=orchestrator,
        fee_data=fee_data,
    )

    print("\nCalculating actual release values for unbond events...")
    unbond_data = calculate_actual_release_values(
        unbond_data=unbond_data,
        transfer_bond_data=transfer_bond_data,
        bond_data=bond_data,
        currency=currency,
    )

    print("\nCalculating compounding rewards...")
    reward_data = add_compounding_rewards(
        orchestrator=orchestrator,
        reward_data=reward_data,
        bond_events=bond_data,
        unbond_events=unbond_data,
    )

    print("\nGet number of gateways sending tickets to the orchestrator...")
    gateways = fee_data["sender"].nunique() if not fee_data.empty else 0
    print(f"Total gateways sending tickets: {gateways}")

    print(f"\nOverview ({start_time} - {end_time}):")
    overview_table = generate_overview_table(
        orchestrator=orchestrator,
        start_time=start_time,
        end_time=end_time,
        activation_timestamp=activation_timestamp,
        reward_data=reward_data,
        fee_data=fee_data,
        unbond_data=unbond_data,
        withdraw_fees_data=withdraw_fees_data,
        total_gas_cost=total_gas_cost,
        total_gas_cost_eur=total_gas_cost_eur,
        currency=currency,
        starting_eth_balance=starting_eth_balance,
        starting_eth_value=starting_eth_value,
        starting_lpt_balance=starting_lpt_balance,
        starting_lpt_value=starting_lpt_value,
        end_eth_balance=end_eth_balance,
        end_eth_value=end_eth_value,
        end_lpt_balance=end_lpt_balance,
        end_lpt_value=end_lpt_value,
        gateways=gateways,
    )
    print(tabulate(overview_table, headers=["Metric", "Value"], tablefmt="grid"))

    print("\nFetching token and ETH transfers...")
    token_and_eth_transfers = retrieve_token_and_eth_transfers(
        transactions_df=transactions_df, wallet_address=orchestrator, currency=currency
    )

    print("Add missing gas cost information to token and ETH transfers...")
    token_and_eth_transfers = merge_gas_info(
        data=token_and_eth_transfers,
        gas_info_df=transactions_with_gas_info_df,
        currency=currency,
    )

    # Exit early if no data was found.
    all_data = [
        reward_data,
        fee_data,
        bond_data,
        unbond_data,
        transfer_bond_data,
        token_and_eth_transfers,
        withdraw_stake_data,
        withdraw_fees_data,
        rebond_data,
    ]
    if all(
        df.empty
        for df in all_data
    ):
        print("\033[93mNo income data found, exiting.\033[0m")  # Yellow text
        sys.exit(0)

    print("Merging token and ETH transfers with reward, fee, and transfer bond data...")
    reindexed_data = [
        df.reindex(columns=get_csv_column_order(currency)) for df in all_data
    ]
    combined_df = pd.concat(
        reindexed_data,
        ignore_index=True,
    ).sort_values(by="timestamp")

    print("Adding cumulative balances to the combined DataFrame...")
    combined_df = add_cumulative_balances(
        combined_df=combined_df,
        starting_eth_balance=starting_eth_balance,
        starting_lpt_balance=starting_lpt_balance,
    )

    print("\nExporting data to Excel...")
    combined_df = combined_df[get_csv_column_order(currency)]
    overview_df = pd.DataFrame(overview_table, columns=["Metric", "Value"])
    with ExcelWriter("orchestrator_income.xlsx") as writer:
        overview_df.to_excel(writer, sheet_name="overview", index=False)

        lpt_transactions = combined_df[combined_df["currency"] == "LPT"]
        lpt_transactions.to_excel(writer, sheet_name="LPT transactions", index=False)

        eth_transactions = combined_df[combined_df["currency"] == "ETH"]
        eth_transactions.to_excel(writer, sheet_name="ETH transactions", index=False)

        combined_df.to_excel(writer, sheet_name="all transactions", index=False)

    print("Excel export completed.")
