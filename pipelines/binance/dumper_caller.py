from dumper import StreamParquetConsumer
import asyncio

# This version (v2) aligns with bucket_v2.py, where previously nested (packed) structures 
# are now flattened and interdependent metrics have been excluded.

# if subject_pattern is used, all subject in given streams are dumped
# in other case change symbol to select related subject

NATS_URL = "nats://localhost:4222"
base_path = "/mnt/vol1/dummy/testing"
streams: list[list[list[str]]] = []


streams.append([[["binance_depth"], ["binance.depth.*"]]])
streams.append([[["binance_kline"], ["binance.kline.*"]]])


async def main():
    for stream in streams:
        for stream_info in stream:
            
            stream = stream_info[0]
            subject_pattern = stream_info[1]
            
            streamer = StreamParquetConsumer(base_path,
                                            streams=stream, 
                                            subject_pattern=subject_pattern,
                                            nats_url=NATS_URL,
                                            )
            try:
                await streamer.run()
            except KeyboardInterrupt:
                print("KeyboardInterrupt received, shutting down gracefully...")
            finally:
                await streamer.shutdown()

asyncio.run(main())