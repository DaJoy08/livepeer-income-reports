# Delegator Info Scripts

This repository contains some simple scripts that can be used to gather information about your orchestrator and delegator wallets using the [Livepeer subgraph](https://thegraph.com/explorer/subgraphs/FE63YgkzcpVocxdCEyEYbvjYqEf2kb1A6daMYRxmejYC?view=Query&chain=arbitrum-one) and [Contracts](https://docs.livepeer.org/references/contract-addresses).

## Usage

1. Clone the repository:

    ```bash
    git clone https://github.com/rickstaa/get-delegator-info.git
    ```

2. Set the `GRAPH_AUTH_TOKEN` environment variable with your Graph authentication token:

   ```bash
    export GRAPH_AUTH_TOKEN=your_token_here
    ```

3. Run one of the scripts:

   ```bash
   python get_orchestrator_info.py
   ```
