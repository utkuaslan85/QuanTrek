from dumper_v2 import StreamParquetConsumer
import asyncio

# This version (v1) implements the logic required to decode and unpack data serialized by bucket_v1.py.

symbol = 'ethbtc'

NATS_URL = "nats://localhost:4222"
stream = ["binance_depth"]
subject = [f"binance.depth.{symbol}"]
base_path = "/mnt/vol1/dummy/testing"
durable = f"depth_{symbol}"
streamer = StreamParquetConsumer(base_path,
                                 streams=stream, 
                                 subject_pattern=subject, 
                                 nats_url=NATS_URL,
                                #  durable_name=durable
                                 )

config = {
    "binance_depth": {
        f"binance.depth.{symbol}": [
            {"mid_price": {"no_change": "mid_price"}},
            {"bid_ask_ratios":{"bid_ask_ratios": "bid_ask_ratios"}},
            {"bid_round_bias": {"round_bias":"bid_round_bias"}},
            {"ask_round_bias": {"round_bias":"ask_round_bias"}},
            {"bucket_boundaries": {"bucket_boundaries":"bucket_boundaries"}},
            {"ask_metrics": {"metrics_transformer": "ask_metrics"}},
            {"bid_metrics": {"metrics_transformer": "bid_metrics"}}
        ]
    }
}

def no_change(msg):
    return msg

def metrics_transformer(msg):
    """
    msg is a dict of lists, e.g. 
    {'bucket_id': [...], 'side': [...], 'price_levels_count': [...], ...}
    """
    out = {}

    # side is constant across all buckets, just take first
    out["side"] = msg["side"][0]

    # bucket ids are explicit
    bucket_ids = msg["bucket_id"]

    # metrics we want to flatten
    metrics = [
        'price_levels_count', 'total_volume', 'vwap',
        'price_min', 'price_max', 'price_std',
        'volume_concentration', 'volume_std',
        'largest_volume', 'smallest_volume',
        'volume_distribution_pct',
        'cumulative_volume', 'cumulative_levels'
    ]

    for metric in metrics:
        values = msg[metric]
        for b, val in zip(bucket_ids, values):
            out[f"{metric}_bucket{b}"] = val

    return out

def bid_ask_ratios(msg):
    out = {}
    for i, ratio in enumerate(msg):
        out[f"bid_ask_ratios_bucket{i}"] = ratio
    return out
    
def bucket_boundaries(msg):
    out = {}
    for i in msg:
        out[f"percentage_bucket{i['bucket_id']}"] = i['percentage']
        out[f"lower_bound_bucket{i['bucket_id']}"] = i['lower_bound']
        out[f"upper_bound_bucket{i['bucket_id']}"] = i['upper_bound']
    return out

def round_bias(msg):
    out = {}
    out['two_decimal'] = msg.get('two_decimal', 0)
    out['one_decimal'] = msg.get('one_decimal', 0)
    out['whole'] = msg.get('whole', 0)
    return out

async def main():
    streamer.add_transformer(metrics_transformer)
    streamer.add_transformer(bid_ask_ratios)
    streamer.add_transformer(bucket_boundaries)
    streamer.add_transformer(round_bias)
    streamer.add_transformer(no_change)
    streamer.add_config(config)
    await streamer.run()
    await streamer.shutdown()
asyncio.run(main())
