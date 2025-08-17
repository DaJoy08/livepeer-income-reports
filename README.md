# Livepeer Income Reports

This repository provides Python scripts to retrieve and generate detailed reports on orchestrator and delegator wallet activity using the [Livepeer subgraph](https://thegraph.com/explorer/subgraphs/FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC?view=Query&chain=arbitrum-one) and Livepeer [Contracts](https://docs.livepeer.org/references/contract-addresses) via RPC calls. These tools are designed to help orchestrators and delegators analyze their income and balances for tax reporting or general insights.

## Scripts

> [!WARNING]
> A bug in the `get_orch_income.py` script's compounding rewards calculation may slightly understate actual rewards. In some cases, especially with daily reward transfers, it can even result in negative recomputed compounding rewards.
> [!IMPORTANT]
> The `get_delegator_income.py` script has not been fully validated yet. Currently, it retrieves `pendingRewards` and fees at the end of the round, rather than at the exact moment they become available.

- `add_crypto_values.py`: Enriches CSV or Excel files containing cryptocurrency portfolios by adding historical price data and calculated values.
- `get_delegator_balance.py`: Retrieves the ETH and LPT balances (both bonded and unbonded) of one or more delegator wallets on Arbitrum at a specific timestamp, with support for aggregated multi-wallet reporting.
- `get_delegator_income.py`: Tracks delegator rewards (LPT) and fees (ETH) over time, factoring in wallet activity and historical pricing for accurate income and tax reporting.
- `get_orchestrator_info.py`: Retrieves information about your orchestrator's income into a CSV file.

## Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/rickstaa/livepeer-income-scripts.git
   ```

2. Set the following API tokens in your environment variables or replace `your_token_here` with your actual tokens:

   ```bash
   export GRAPH_TOKEN=your_token_here
   export ARBISCAN_API_KEY_TOKEN=your_token_here
   export CRYPTO_COMPARE_API_KEY=your_token_here
   export ARB_RPC_URL=https://arb1.arbitrum.io/rpc
   ```

   Where to get these tokens:
   - GRAPH_TOKEN (The Graph gateway API key): [The Graph Studio – API Keys](https://thegraph.com/studio)
   - ARBISCAN_API_KEY_TOKEN (Etherscan developer API key): [Etherscan API Keys](https://etherscan.io/myapikey)
   - CRYPTO_COMPARE_API_KEY: [CryptoCompare API Keys](https://www.cryptocompare.com/cryptopian/api-keys)
   - ARB_RPC_URL (Arbitrum One RPC endpoint): use a provider like [Alchemy](https://www.alchemy.com/), [Infura](https://www.infura.io/), [QuickNode](https://www.quicknode.com/), or [Ankr](https://www.ankr.com/); use an archive-capable RPC.

3. Create a python virtual environment and install the required packages:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   pip install -r requirements.txt
   ```

4. Run the python script:

   ```bash
   python get_orchestrator_info.py
   ```

   Or for delegator balance reports:

   ```bash
   python get_delegator_balance.py
   ```

   Or to add crypto prices and values to a portfolio file:

   ```bash
   python add_crypto_values.py
   ```

5. The script will generate an Excel file named `orchestrator_income.xlsx` with two tabs: `overview` and `transactions`. The `overview` tab contains a summary of your orchestrator's income, while the `transactions` tab contains detailed transaction data.
