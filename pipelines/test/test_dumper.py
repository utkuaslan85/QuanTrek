from dumper_v2 import StreamParquetConsumer
import asyncio
NATS_URL = "nats://localhost:4222"
stream = ["binance_test"]
subject = ["binance.test.test5"]
base_path = "/mnt/vol1/dummy/testing"
streamer = StreamParquetConsumer(base_path,
                                 streams=stream, 
                                 subject_pattern=subject, 
                                 nats_url=NATS_URL)

config = {
    "binance_test": {
        "binance.test.test5": [
            {"counter1": {"no_change": "counter1"}},
            {"counter2": {"no_change": "counter2"}}
            # {"bid_metrics": {"metrics_transformer": "bid_metrics"}}
        ]
    }
}

def no_change(msg):
    return msg

async def main():
    streamer.add_transformer(no_change)
    streamer.add_config(config)
    await streamer.run()
    await streamer.shutdown()
asyncio.run(main())
