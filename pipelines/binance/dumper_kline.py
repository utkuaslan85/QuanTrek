from dumper import StreamParquetConsumer
import asyncio

# if subject_pattern is used, all subject in given streams are dumped
# in other case change symbol to select related subject

NATS_URL = "nats://localhost:4222"

symbol = 'btcusdt'
stream = ["binance_kline"]
subject_pattern = ["binance.kline.*"]
base_path = "/mnt/vol1/data"

subject = [f"{subject_pattern[:-2]}.{symbol}"]

streamer = StreamParquetConsumer(base_path,
                                 streams=stream, 
                                #  subject_pattern=subject, 
                                 subject_pattern=subject_pattern, 
                                 nats_url=NATS_URL,
                                 )

async def main():
    try:
        await streamer.run()
    except KeyboardInterrupt:
        print("KeyboardInterrupt received, shutting down gracefully...")
    finally:
        await streamer.shutdown()

asyncio.run(main())