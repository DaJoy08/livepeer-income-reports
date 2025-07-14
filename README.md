# Livepeer Income

This repository contains some simple python scripts that can be used to gather information about your Livepeer orchestrator income. It retrieves this data using the [Livepeer subgraph](https://thegraph.com/explorer/subgraphs/FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC?view=Query&chain=arbitrum-one) and [Contracts](https://docs.livepeer.org/references/contract-addresses) or RPC calls. It currently contains the following scripts:

- `get_orchestrator_info.py`: Retrieves information about your orchestrator's income into a CSV file.
- `get_delegator_balance.py`: Retrieves the ETH and LPT balances (both bonded and unbonded) of a delegator wallet on Arbitrum at a specific timestamp.

## Usage

1. Clone the repository:

   ```bash
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

5. The script will generate an Excel file named `orchestrator_income.xlsx` with two tabs: `overview` and `transactions`. The `overview` tab contains a summary of your orchestrator's income, while the `transactions` tab contains detailed transaction data.
