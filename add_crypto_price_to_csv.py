"""Add crypto prices and values to a CSV file with Asset and Amount columns."""

import pandas as pd
import sys
from datetime import datetime
from get_orch_income import fetch_crypto_price, human_to_unix_time


def normalize_asset_symbol(asset: str) -> str:
    """Normalize asset symbol by removing spaces and converting to uppercase.

    Args:
        asset: The asset symbol to normalize.

    Returns:
        The normalized asset symbol.
    """
    return asset.strip().replace(" ", "").upper()


def find_column_case_insensitive(df: pd.DataFrame, column_name: str) -> str:
    """Find a column name in the DataFrame regardless of case and spaces.

    Args:
        df: The DataFrame to search in.
        column_name: The column name to find (case insensitive).

    Returns:
        The actual column name found in the DataFrame.

    Raises:
        ValueError: If the column is not found.
    """
    for col in df.columns:
        if col.strip().lower() == column_name.lower():
            return col
    raise ValueError(f"Column '{column_name}' not found in CSV")


def add_crypto_prices_to_csv(
    input_file: str,
    output_file: str = None,
    currency: str = "EUR",
    timestamp: int = None,
) -> None:
    """Add crypto prices and values to a CSV file.

    Args:
        input_file: Path to the input CSV file with Asset and Amount columns
        output_file: Path to the output CSV file (optional, defaults to input_file)
        currency: Target currency for prices (default: EUR)
        timestamp: Unix timestamp for historical prices (optional, uses current time)
    """
    try:
        df = pd.read_csv(input_file)
        try:
            asset_col = find_column_case_insensitive(df, "Asset")
            amount_col = find_column_case_insensitive(df, "Amount")
        except ValueError as e:
            raise ValueError(
                "CSV must contain 'Asset' and 'Amount' columns (case insensitive)"
            )

        if timestamp is None:
            timestamp = int(datetime.now().timestamp())
        print(f"Processing {len(df)} rows...")
        print(f"Using columns: '{asset_col}' and '{amount_col}'")

        prices = []
        values = []
        for _, row in df.iterrows():
            original_asset = row[asset_col]
            asset = normalize_asset_symbol(original_asset)
            amount = float(row[amount_col])
            try:
                print(f"Fetching price for {asset} (from '{original_asset}')...")
                price = fetch_crypto_price(
                    crypto_symbol=asset,
                    target_currency=currency,
                    unix_timestamp=timestamp,
                )
                value = amount * price
                prices.append(price)
                values.append(value)
                print(
                    f"{asset}: {price:.4f} {currency} per token, {amount:.4f} tokens = {value:.2f} {currency}"
                )
            except Exception as e:
                print(
                    f"Error fetching price for {asset} (from '{original_asset}'): {e}"
                )
                prices.append(0.0)
                values.append(0.0)

        # Add columns and save to CSV.
        df[f"Price ({currency})"] = prices
        df[f"Value ({currency})"] = values
        if output_file is None:
            output_file = input_file.replace(
                ".csv", f"_with_prices_{currency.lower()}.csv"
            )
        df.to_csv(output_file, index=False)

        total_value = sum(values)
        print(f"\nSummary:")
        print(f"Total portfolio value: {total_value:.2f} {currency}")
        print(f"Output saved to: {output_file}")
    except Exception as e:
        print(f"Error processing CSV: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("== Crypto Portfolio Price Calculator ==")

    input_file = input("Enter path to CSV file: ").strip()
    if not input_file:
        print("CSV file path is required.")
        sys.exit(1)
    currency = input("Enter currency (default: EUR): ").strip().upper() or "EUR"
    date_time = input("Enter date and time (YYYY-MM-DD HH:MM:SS): ").strip()
    timestamp = human_to_unix_time(human_time=date_time)

    add_crypto_prices_to_csv(
        input_file=input_file, currency=currency, timestamp=timestamp
    )
