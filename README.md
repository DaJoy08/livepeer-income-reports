# Livepeer Income

This repository contains some simple python scripts that can be used to gather information about your Livepeer orchestrator or delegator income. It retrieves this data using the [Livepeer subgraph](https://thegraph.com/explorer/subgraphs/FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC?view=Query&chain=arbitrum-one) and [Contracts](https://docs.livepeer.org/references/contract-addresses) or RPC calls. It currently contains the following scripts:
- `get_orchestrator_info.py`: Retrieves information about your orchestrator's income into a CSV file.
- `get_delegator_fees.py`: Retrieves information about your delegator's fees.
- `get_delegator_stake.py`: Retrieves information about your delegator's stake.

## Usage

1. Clone the repository:

    ```bash
    git clone https://github.com/rickstaa/livepeer-income-scripts.git
    ```

2. Set the `GRAPH_AUTH_TOKEN` environment variable with your Graph authentication token:

   ```bash
    export GRAPH_AUTH_TOKEN=your_token_here
    ```

3. Run one of the scripts:

   ```bash
   python get_orchestrator_info.py
   ```
