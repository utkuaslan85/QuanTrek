import pandas as pd

# Define price buckets as percentage floats (relative, e.g. 0.02% = 0.0002)
buckets = [
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

def calculate_mid_price(dfb:pd.DataFrame, dfa:pd.DataFrame):
    """Calculate mid price from best bid and ask"""
    best_bid = dfb.iloc[0]['price']
    best_ask = dfa.iloc[0]['price']
    return (best_bid + best_ask) / 2

def create_price_buckets(mid_price:float, buckets:list[float]) -> list[dict]:
    """Create price bucket boundaries"""
    bucket_boundaries = []
    for i, pct in enumerate(buckets):
        lower_bound = mid_price * (1 - pct)
        upper_bound = mid_price * (1 + pct)
        
        bucket_boundaries.append({
            'bucket_id': i,
            'percentage': pct * 100,  # Convert to percentage for display
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'range_label': f"±{pct*100:.3f}%"
        })
    
    return bucket_boundaries

def assign_to_buckets(df:pd.DataFrame, mid_price:float, buckets:list[float], side='bid'):
    """Assign price levels to buckets"""
    df = df.copy()
    df['distance_from_mid'] = abs(df['price'] - mid_price) / mid_price
    
    # Initialize all as unassigned (will go to largest bucket)
    df['bucket_id'] = len(buckets) - 1  # Default to largest bucket
    
    # Vectorized assignment - much faster than loops
    for i, bucket_pct in enumerate(buckets):
        # Assign to current bucket if within range and not already assigned to smaller bucket
        mask = (df['distance_from_mid'] <= bucket_pct) & (df['bucket_id'] >= i)
        df.loc[mask, 'bucket_id'] = i
    
    return df

def calculate_bucket_metrics(df_bucketed:pd.DataFrame, bucket_boundaries:list[dict], side='bid'):
    """Calculate comprehensive metrics for each bucket"""
    bucket_metrics = []
    
    for bucket in bucket_boundaries:
        bucket_id = bucket['bucket_id']
        bucket_data = df_bucketed[df_bucketed['bucket_id'] == bucket_id]
        
        if len(bucket_data) == 0:
            # Empty bucket
            metrics = {
                'bucket_id': bucket_id,
                'range_label': bucket['range_label'],
                'side': side,
                'price_levels_count': 0,
                'total_volume': 0,
                'vwap': None,
                'price_min': None,
                'price_max': None,
                'price_std': None,
                'volume_concentration': None,
                'volume_std': None,
                'largest_volume': None,
                'smallest_volume': None,
                'volume_distribution_pct': 0,
                'cumulative_volume': 0,
                'cumulative_levels': 0
            }
        else:
            total_volume = bucket_data['volume'].sum()
            total_volume_all = df_bucketed['volume'].sum()
            
            # Calculate VWAP
            vwap = (bucket_data['price'] * bucket_data['volume']).sum() / total_volume if total_volume > 0 else None
            
            # Volume concentration (largest single volume / total bucket volume)
            volume_concentration = bucket_data['volume'].max() / total_volume if total_volume > 0 else None
            
            metrics = {
                'bucket_id': bucket_id,
                'range_label': bucket['range_label'],
                'side': side,
                'price_levels_count': len(bucket_data),
                'total_volume': total_volume,
                'vwap': vwap,
                'price_min': bucket_data['price'].min(),
                'price_max': bucket_data['price'].max(),
                'price_std': bucket_data['price'].std(),
                'volume_concentration': volume_concentration,
                'volume_std': bucket_data['volume'].std(),
                'largest_volume': bucket_data['volume'].max(),
                'smallest_volume': bucket_data['volume'].min(),
                'volume_distribution_pct': (total_volume / total_volume_all * 100) if total_volume_all > 0 else 0,
                'cumulative_volume': 0,  # Will be calculated later
                'cumulative_levels': 0   # Will be calculated later
            }
        
        bucket_metrics.append(metrics)
    
    # Calculate cumulative metrics
    cumulative_volume = 0
    cumulative_levels = 0
    
    for i, metrics in enumerate(bucket_metrics):
        cumulative_volume += metrics['total_volume']
        cumulative_levels += metrics['price_levels_count']
        bucket_metrics[i]['cumulative_volume'] = cumulative_volume
        bucket_metrics[i]['cumulative_levels'] = cumulative_levels
    
    return pd.DataFrame(bucket_metrics)

def analyze_round_number_bias(df_bucketed):
    """Analyze tendency for prices to cluster at round numbers"""
    round_number_counts = {}
    
    for _, row in df_bucketed.iterrows():
        price = row['price']
        # Check various round number patterns
        if price == int(price):  # Whole numbers
            round_number_counts['whole'] = round_number_counts.get('whole', 0) + 1
        elif price * 10 == int(price * 10):  # One decimal place
            round_number_counts['one_decimal'] = round_number_counts.get('one_decimal', 0) + 1
        elif price * 100 == int(price * 100):  # Two decimal places
            round_number_counts['two_decimal'] = round_number_counts.get('two_decimal', 0) + 1
    
    total_levels = len(df_bucketed)
    round_number_bias = {k: v/total_levels for k, v in round_number_counts.items()}
    
    return round_number_bias

def process_orderbook(df, buckets):
    """Main function to process order book data"""
    # Extract bids and asks
    dfb = pd.DataFrame(df['bids'][0], columns=['price', 'volume'])
    dfb.sort_values('price', ascending=False, inplace=True)
    dfb = dfb.reset_index(drop=True)
    symbol = df['symbol'][0]
    ts = df['timestamp'][0]
    rt = df['record_time'][0]
    dfa = pd.DataFrame(df['asks'][0], columns=['price', 'volume'])
    dfa.sort_values('price', ascending=True, inplace=True)
    dfa = dfa.reset_index(drop=True)
    
    # Calculate mid price
    mid_price = calculate_mid_price(dfb, dfa)
    
    # Assign to buckets
    dfb_bucketed = assign_to_buckets(dfb, mid_price, buckets, side='bid')
    dfa_bucketed = assign_to_buckets(dfa, mid_price, buckets, side='ask')
    
    # Create bucket boundaries for reference
    bucket_boundaries = create_price_buckets(mid_price, buckets)
    
    # Calculate metrics
    bid_metrics = calculate_bucket_metrics(dfb_bucketed, bucket_boundaries, side='bid')
    ask_metrics = calculate_bucket_metrics(dfa_bucketed, bucket_boundaries, side='ask')
    
    # Calculate bid/ask ratios
    bid_ask_ratios = []
    for i in range(len(bucket_boundaries)):
        bid_vol = bid_metrics.iloc[i]['total_volume']
        ask_vol = ask_metrics.iloc[i]['total_volume']
        ratio = bid_vol / ask_vol if ask_vol > 0 else float('inf') if bid_vol > 0 else None
        bid_ask_ratios.append(ratio)
    
    # Analyze round number bias
    bid_round_bias = analyze_round_number_bias(dfb_bucketed)
    ask_round_bias = analyze_round_number_bias(dfa_bucketed)
    
    return {
        'symbol': symbol,
        'timestamp': ts,
        'record_time':rt,
        'mid_price': mid_price,
        'bucket_boundaries': bucket_boundaries,
        'bid_metrics': bid_metrics,
        'ask_metrics': ask_metrics,
        'bid_ask_ratios': bid_ask_ratios,
        'bid_round_bias': bid_round_bias,
        'ask_round_bias': ask_round_bias,
        # 'dfb_bucketed': dfb_bucketed,
        # 'dfa_bucketed': dfa_bucketed
    }

# Example usage:
"""
# Assuming you have your df with 'bids' and 'asks' data
result = process_orderbook(df, buckets)

print(f"Mid Price: {result['mid_price']}")
print("\nBid Metrics:")
print(result['bid_metrics'])
print("\nAsk Metrics:")
print(result['ask_metrics'])
print(f"\nBid/Ask Ratios: {result['bid_ask_ratios']}")
"""