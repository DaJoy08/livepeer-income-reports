# Livepeer Income Reports

This repository provides Python scripts to retrieve and generate detailed reports on orchestrator and delegator wallet activity using the [Livepeer subgraph](https://thegraph.com/explorer/subgraphs/FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC?view=Query&chain=arbitrum-one) and Livepeer [Contracts](https://docs.livepeer.org/references/contract-addresses) via RPC calls. These tools are designed to help orchestrators and delegators analyze their income and balances for tax reporting or general insights.

## Scripts

- `add_crypto_price_to_csv.py`: Adds historical crypto prices and values to a CSV file containing asset symbols and amounts.
- `get_delegator_balance.py`: Retrieves the ETH and LPT balances (both bonded and unbonded) of a delegator wallet on Arbitrum at a specific timestamp.
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

   Or to add crypto prices to a CSV file:

   ```bash
   python add_crypto_price_to_csv.py
   ```

5. The script will generate an Excel file named `orchestrator_income.xlsx` with two tabs: `overview` and `transactions`. The `overview` tab contains a summary of your orchestrator's income, while the `transactions` tab contains detailed transaction data.
