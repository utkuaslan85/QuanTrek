from dumper_v2 import StreamParquetConsumer
import asyncio

# This version (v2) aligns with bucket_v2.py, where previously nested (packed) structures 
# are now flattened and interdependent metrics have been excluded.

# if subject_pattern is used, all subject in given streams are dumped
# in other case change symbol to select related subject

NATS_URL = "nats://localhost:4222"

symbol = 'btcusdt'
stream = ["binance_depth"]
subject_pattern = ["binance.depth.*"]
base_path = "/mnt/vol1/dummy/testing/prefect_test"

subject = [f"{subject_pattern[0][:-2]}.{symbol}"]

streamer = StreamParquetConsumer(base_path,
                                 streams=stream, 
                                #  subject_pattern=subject, # for single subject
                                 subject_pattern=subject_pattern, #for all subjects 
                                 nats_url=NATS_URL,
                                 )

async def main():
    await streamer.run()
    await streamer.shutdown()
asyncio.run(main())
