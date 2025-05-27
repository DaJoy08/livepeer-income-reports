from web3 import Web3
import os
import json
from datetime import datetime
import requests

ARBITRUM_RPC_URL = os.getenv("ARBITRUM_RPC_URL", "https://arb1.arbitrum.io/rpc")
BONDING_VOTES_MANAGER_ADDRESS = "0x0B9C254837E72Ebe9Fe04960C43B69782E68169A"
ROUNDS_MANAGER_ADDRESS = "0xdd6f56DcC28D3F5f27084381fE8Df634985cc39f"
WALLET_ADDRESS = os.getenv(
    "WALLET_ADDRESS", "0x455A304A1D3342844f4Dd36731f8da066eFdd30B"
)

with open("ABI/BondingVotes.json", "r") as bonding_votes_abi_file:
    BONDING_VOTES_MANAGER_ABI = json.load(bonding_votes_abi_file)

with open("ABI/RoundsManager.json", "r") as rounds_abi_file:
    ROUNDS_MANAGER_ABI = json.load(rounds_abi_file)

web3 = Web3(Web3.HTTPProvider(ARBITRUM_RPC_URL))

if not web3.is_connected():
    raise ConnectionError("Failed to connect to the Arbitrum network.")

bonding_votes_manager = web3.eth.contract(
    address=web3.to_checksum_address(BONDING_VOTES_MANAGER_ADDRESS), abi=BONDING_VOTES_MANAGER_ABI
)

rounds_manager = web3.eth.contract(
    address=web3.to_checksum_address(ROUNDS_MANAGER_ADDRESS), abi=ROUNDS_MANAGER_ABI
)

SECONDS_PER_ROUND = 76524  # 6,377 blocks × 12 seconds per block


def fetch_current_round():
    return rounds_manager.functions.currentRound().call()


def estimate_round_at_time(target_time):
    current_round = fetch_current_round()
    current_time = int(datetime.now().timestamp())

    time_difference = target_time - current_time
    round_difference = time_difference // SECONDS_PER_ROUND

    estimated_round = current_round + round_difference
    return int(estimated_round)


def fetch_votes_and_delegate(delegator_address, round_number):
    delegator_address = web3.to_checksum_address(delegator_address)
    votes, delegate_address = bonding_votes_manager.functions.getVotesAndDelegateAtRoundStart(
        delegator_address, round_number
    ).call()
    return web3.from_wei(votes, "ether"), delegate_address


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


if __name__ == "__main__":
    print("=" * 75)
    print(f"Fetching Delegation for Wallet: {WALLET_ADDRESS}")
    print("=" * 75)

    try:
        TARGET_DATE = "2024-01-01 00:00:00"
        target_timestamp = int(datetime.strptime(TARGET_DATE, "%Y-%m-%d %H:%M:%S").timestamp())
        estimated_round = estimate_round_at_time(target_timestamp)

        votes, delegate_address = fetch_votes_and_delegate(WALLET_ADDRESS, estimated_round)
        lpt_price_eur = fetch_lpt_price_cryptocompare(target_timestamp)
        total_value_eur = float(votes) * lpt_price_eur

        print(f"Estimated Round for {TARGET_DATE}: {estimated_round}")
        print(f"LPT for Round {estimated_round}: {votes} LPT")
        print(f"LPT Price on {TARGET_DATE} in EUR: {lpt_price_eur}")
        print(f"Total Delegation Value in EUR: {total_value_eur}")
    except Exception as e:
        print(f"Error: {e}")
