import pandas as pd
from pathlib import Path
from typing import Optional


def should_download_year(year: int, base_dir: str = "data/raw/fars_data") -> bool:
    year_folder = Path(base_dir) / str(year)
    return not year_folder.exists()


def get_latest_downloaded_year(base_dir: str = "data/raw/fars_data") -> Optional[int]:
    path = Path(base_dir)
    years = [int(p.name)
             for p in path.iterdir() if p.is_dir() and p.name.isdigit()]
    return max(years) if years else None


def summarize_accident_data(df: pd.DataFrame):
    """
    Print quick summary statistics and missing value counts for the standardized data.
    """
    print("\n--- Column Summary ---")
    print(df.dtypes)
    print("\n--- Missing Values ---")
    print(df.isna().sum().sort_values(ascending=False).head(20))

    # Display the top 10 manufacturers if applicable (adjust based on your dataset)
    if 'county' in df.columns:
        print("\n--- Top Counties ---")
        print(df['county'].value_counts().head(10))

    # Display the top 10 states if applicable (adjust based on your dataset)
    if 'state' in df.columns:
        print("\n--- Top States ---")
        print(df['state'].value_counts().head(10))

    # Display a few basic statistics
    print(f"\n--- Accident Summary Statistics ---")
    print(df.describe(include='all').transpose().head(10))


def summarize_vehicle_data(df: pd.DataFrame):
    """
    Print quick summary statistics and missing value counts for the standardized data.
    """
    print("\n--- Column Summary ---")
    print(df.dtypes)
    print("\n--- Missing Values ---")
    print(df.isna().sum().sort_values(ascending=False).head(20))

    # Display the top 10 manufacturers if applicable (adjust based on your dataset)
    if 'make' in df.columns:
        print("\n--- Top Vehicle Makes ---")
        print(df['make'].value_counts().head(10))

    # Display the top 10 states if applicable (adjust based on your dataset)
    if 'state' in df.columns:
        print("\n--- Top States ---")
        print(df['state'].value_counts().head(10))

    # Display a few basic statistics
    print(f"\n--- Vehicle Summary Statistics ---")
    print(df.describe(include='all').transpose().head(10))
