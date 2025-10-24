import pandas as pd

# Define price buckets as percentage floats (shell boundaries)
BUCKETS = [
    0.0001,  # 0.01%
    0.0002,  # 0.02%
    0.0005,  # 0.05%
    0.001,   # 0.1%
    0.0015,  # 0.15%
    0.002,   # 0.2%
    0.003,   # 0.3%
    0.005,   # 0.5%
    0.007,   # 0.7%
    0.01,    # 1.0%
    0.015,   # 1.5%
    0.02,    # 2.0%
    0.03,    # 3.0%
    0.05,    # 5.0%
    0.10     # 10.0%
]

def calculate_mid_price(dfb: pd.DataFrame, dfa: pd.DataFrame) -> float:
    """Calculate mid price from best bid and ask."""
    best_bid = dfb.iloc[0]['price']
    best_ask = dfa.iloc[0]['price']
    return (best_bid + best_ask) / 2


def assign_to_shells(df: pd.DataFrame, mid_price: float, buckets: list) -> pd.DataFrame:
    """
    Assign price levels to non-overlapping shells.
    
    Shell i contains prices where: buckets[i-1] < distance <= buckets[i]
    Shell 0 is special: 0 < distance <= buckets[0]
    
    Args:
        df: DataFrame with 'price' and 'volume' columns
        mid_price: Reference mid price
        buckets: List of cumulative percentage thresholds
    
    Returns:
        DataFrame with added 'shell_id' and 'distance_from_mid' columns
    """
    df = df.copy()
    df['distance_from_mid'] = abs(df['price'] - mid_price) / mid_price
    
    # Initialize all as unassigned (-1)
    df['shell_id'] = -1
    
    # Assign to shells (non-overlapping rings)
    for i in range(len(buckets)):
        lower = 0.0 if i == 0 else buckets[i - 1]
        upper = buckets[i]
        
        # Assign to shell i if: lower < distance <= upper
        mask = (df['distance_from_mid'] > lower) & (df['distance_from_mid'] <= upper)
        df.loc[mask, 'shell_id'] = i
    
    # Anything beyond the last bucket gets assigned to last shell
    mask = df['distance_from_mid'] > buckets[-1]
    df.loc[mask, 'shell_id'] = len(buckets) - 1
    
    return df


def calculate_shell_metrics(df_shelled: pd.DataFrame, n_shells: int) -> dict:
    """
    Calculate minimal sufficient statistics for each shell.
    
    Returns dict with keys: shell_{i}_{metric} for i in 0..n_shells-1
    """
    metrics = {}
    
    for shell_id in range(n_shells):
        shell_data = df_shelled[df_shelled['shell_id'] == shell_id]
        
        if len(shell_data) == 0:
            # Empty shell - use 0.0 for missing data (JSON-serializable)
            metrics[f'shell_{shell_id}_total_volume'] = 0.0
            metrics[f'shell_{shell_id}_vwap'] = 0.0  # Changed from None
            metrics[f'shell_{shell_id}_volume_concentration'] = 0.0  # Changed from None
            metrics[f'shell_{shell_id}_price_levels_count'] = 0
        else:
            total_volume = shell_data['volume'].sum()
            
            # VWAP: volume-weighted average price
            vwap = (shell_data['price'] * shell_data['volume']).sum() / total_volume
            
            # Volume concentration: largest single order / total shell volume
            # Measures if liquidity is from one big order (1.0) or many small (→0)
            volume_concentration = shell_data['volume'].max() / total_volume
            
            # Price levels count: number of distinct price levels in shell
            price_levels_count = len(shell_data)
            
            metrics[f'shell_{shell_id}_total_volume'] = float(total_volume)
            metrics[f'shell_{shell_id}_vwap'] = float(vwap)
            metrics[f'shell_{shell_id}_volume_concentration'] = float(volume_concentration)
            metrics[f'shell_{shell_id}_price_levels_count'] = int(price_levels_count)
    
    return metrics


def process_orderbook(df: pd.DataFrame, buckets: list = BUCKETS) -> dict:
    """
    Process orderbook snapshot into minimal sufficient statistics.
    
    Args:
        df: DataFrame with columns ['symbol', 'timestamp', 'record_time', 'bids', 'asks']
            where 'bids' and 'asks' are lists of [price, volume] pairs
        buckets: List of percentage thresholds for shell boundaries
    
    Returns:
        Flat dictionary with 121 keys (all JSON-serializable):
        - Metadata: symbol, timestamp, record_time, mid_price
        - Bid features: bid_0_total_volume, bid_0_vwap, bid_0_volume_concentration, 
                       bid_0_price_levels_count, ... (15 shells × 4 metrics = 60)
        - Ask features: ask_0_total_volume, ... (15 shells × 4 metrics = 60)
    """
    # Extract bids and asks
    dfb = pd.DataFrame(df['bids'][0], columns=['price', 'volume'])
    dfb = dfb.sort_values('price', ascending=False).reset_index(drop=True)
    
    dfa = pd.DataFrame(df['asks'][0], columns=['price', 'volume'])
    dfa = dfa.sort_values('price', ascending=True).reset_index(drop=True)
    
    # Extract metadata - ensure Python native types
    symbol = str(df['symbol'][0])
    timestamp = int(df['timestamp'][0]) if pd.notna(df['timestamp'][0]) else 0
    record_time = int(df['record_time'][0]) if pd.notna(df['record_time'][0]) else 0
    
    # Calculate mid price
    mid_price = calculate_mid_price(dfb, dfa)
    
    # Assign to shells
    dfb_shelled = assign_to_shells(dfb, mid_price, buckets)
    dfa_shelled = assign_to_shells(dfa, mid_price, buckets)
    
    # Calculate metrics for each side
    n_shells = len(buckets)
    bid_metrics = calculate_shell_metrics(dfb_shelled, n_shells)
    ask_metrics = calculate_shell_metrics(dfa_shelled, n_shells)
    
    # Build flat output dictionary (all JSON-serializable types)
    result = {
        'symbol': symbol,
        'timestamp': timestamp,
        'record_time': record_time,
        'mid_price': float(mid_price),
    }
    
    # Add bid metrics with 'bid_' prefix
    for key, value in bid_metrics.items():
        result[f"bid_{key}"] = value
    
    # Add ask metrics with 'ask_' prefix
    for key, value in ask_metrics.items():
        result[f"ask_{key}"] = value
    
    return result


# Example usage and batch processing helper
def process_orderbook_stream(df_stream: pd.DataFrame, buckets: list = BUCKETS) -> pd.DataFrame:
    """
    Process multiple orderbook snapshots into a time series DataFrame.
    
    Args:
        df_stream: DataFrame where each row is an orderbook snapshot
        buckets: Shell boundary thresholds
    
    Returns:
        DataFrame where each row is a flattened feature vector (121 columns)
    """
    results = []
    
    for idx in range(len(df_stream)):
        snapshot = df_stream.iloc[[idx]]  # Keep as DataFrame for compatibility
        try:
            features = process_orderbook(snapshot, buckets)
            results.append(features)
        except Exception as e:
            print(f"Error processing snapshot at index {idx}: {e}")
            continue
    
    return pd.DataFrame(results)


# Example of how to use:
"""
# Single snapshot processing:
snapshot = df.iloc[[0]]  # One row DataFrame
features = process_orderbook(snapshot)
print(f"Features extracted: {len(features)} keys")
print(f"Mid price: {features['mid_price']}")
print(f"Bid shell 0 volume: {features['bid_shell_0_total_volume']}")

# Batch processing (e.g., after downloading from VPS):
df_features = process_orderbook_stream(df_all_snapshots)
df_features.to_parquet('orderbook_features.parquet')  # Efficient storage

# Later analysis on desktop:
df = pd.read_parquet('orderbook_features.parquet')

# Calculate derived features as needed:
df['bucket_0_bid_ask_ratio'] = df['bid_shell_0_total_volume'] / df['ask_shell_0_total_volume']
df['bucket_0_volume_imbalance'] = df['bid_shell_0_total_volume'] - df['ask_shell_0_total_volume']
df['bucket_0_cumulative_bid_volume'] = df[[f'bid_shell_{i}_total_volume' for i in range(15)]].sum(axis=1)

# Bot detection example:
# Detect spoofing: high concentration appearing suddenly in far shells
df['spoof_signal'] = (
    (df['bid_shell_10_volume_concentration'] > 0.8) & 
    (df['bid_shell_10_total_volume'] > df['bid_shell_10_total_volume'].rolling(60).mean() * 3)
)
"""