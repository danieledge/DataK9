#!/usr/bin/env python3
"""
Create simple baseline Parquet files for temporal validation testing.

Generates synthetic historical baseline data for:
- BaselineComparisonCheck: 30-day history of row counts
- TrendDetectionCheck: Daily metrics for trend analysis

Author: Daniel Edge
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

def create_baseline_files():
    """Create baseline Parquet files with simple synthetic data."""

    # Output directory
    output_dir = Path("/home/daniel/www/test-data")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create 30 days of historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    dates = []
    values = []

    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        # Simulate row counts around 179 million with some variation
        base_count = 179_000_000
        variation = base_count * 0.05  # 5% variation
        import random
        random.seed(int(current_date.timestamp()))
        value = base_count + random.uniform(-variation, variation)
        values.append(int(value))
        current_date += timedelta(days=1)

    # Create baseline DataFrame
    baseline_df = pd.DataFrame({
        'date': dates,
        'value': values
    })

    # Save as Parquet for BaselineComparisonCheck
    baseline_path = output_dir / "HI-Large_Trans_baseline.parquet"
    baseline_df.to_parquet(baseline_path, index=False)
    print(f"✓ Created baseline file: {baseline_path}")
    print(f"  Rows: {len(baseline_df)}")
    print(f"  Date range: {baseline_df['date'].min()} to {baseline_df['date'].max()}")
    print(f"  Value range: {baseline_df['value'].min():,} to {baseline_df['value'].max():,}")

    # Save same data for TrendDetectionCheck
    daily_path = output_dir / "HI-Large_Trans_daily.parquet"
    baseline_df.to_parquet(daily_path, index=False)
    print(f"\n✓ Created daily trends file: {daily_path}")
    print(f"  Rows: {len(baseline_df)}")

    print("\n✓ Baseline data files created successfully!")
    print(f"  Location: {output_dir}")
    print(f"  Files: HI-Large_Trans_baseline.parquet, HI-Large_Trans_daily.parquet")

if __name__ == "__main__":
    create_baseline_files()
