import asyncio
import json
from typing import Dict, Any
import threading
import time
from helper import get_now
from binance import ThreadedDepthCacheManager
from faststream import FastStream, Logger
from faststream.nats import NatsBroker, JStream
import logging

# Configure logging
# logging.basicConfig(level=logging.INFO)
logging.basicConfig(
    filename="/mnt/vol1/logs/len_test.log",  # Ensure this directory exists
    level=logging.INFO,
    format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# NATS configuration
NATS_URL = "nats://localhost:4222"

# Initialize NATS broker and FastStream app
broker = NatsBroker(NATS_URL)
app = FastStream(broker)

# Define JetStream
depth_stream = JStream(name="binance_depth")

class SimpleBinanceRecorder:
    def __init__(self):
        self.dcm = None
        self.running = False
        
    def format_depth_data(self, depth_cache) -> Dict[str, Any]:
        """Format depth cache data for storage"""
        return {
            "symbol": depth_cache.symbol,
            "record_time": get_now().isoformat(),
            "timestamp": depth_cache.update_time,
            "bids": depth_cache.get_bids(),
            "asks": depth_cache.get_asks(),
        }
    
    def handle_depth_cache(self, depth_cache):
        """Handle depth cache updates from Binance"""
        if not self.running:
            return
            
        try:
            # Format the data
            depth_data = self.format_depth_data(depth_cache)
            subject = f"binance.depth.{depth_data['symbol'].lower()}"
            
            # Schedule async publish to JetStream
            if hasattr(self, 'loop') and self.loop and not self.loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    broker.publish(
                        json.dumps(depth_data),
                        # max_age=60*60*2, 
                        subject=subject, 
                        stream="binance_depth"  # This ensures JetStream storage
                    ),
                    self.loop
                )
                
            # logger.info(f"Published depth data for {depth_cache.symbol}")
            bid_count = len(depth_cache.get_bids())
            ask_count = len(depth_cache.get_asks())
            logger.info(f"bid count: {bid_count}")
            logger.info(f"ask count: {ask_count}")
            
        except Exception as e:
            logger.error(f"Error handling depth cache: {e}")
    
    def start_binance_streams(self, symbols: list, loop):
        """Start Binance depth cache streams"""
        self.loop = loop
        self.running = True
        
        def run_streams():
            try:
                self.dcm = ThreadedDepthCacheManager()
                self.dcm.start()
                
                for symbol in symbols:
                    self.dcm.start_depth_cache(self.handle_depth_cache,
                                               symbol=symbol, 
                                               refresh_interval=60*60,
                                            #    ws_interval=100,
                                               limit=5000)
                    logger.info(f"Started depth cache for {symbol}")
                
                # Keep running until stopped
                while self.running:
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in Binance streams: {e}")
            finally:
                if self.dcm:
                    self.dcm.close()
        
        # Start in background thread
        thread = threading.Thread(target=run_streams, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop the recorder"""
        self.running = False
        if self.dcm:
            self.dcm.close()

# Global recorder
recorder = SimpleBinanceRecorder()

# JetStream subscriber to monitor all depth data
@broker.subscriber(
    "binance.depth.*",
    stream=depth_stream,
    deliver_policy="new",
)
async def monitor_depth_data(msg: str, logger: Logger):
    """Monitor depth data stored in JetStream"""
    try:
        data = json.loads(msg)
        logger.info(f"JetStream received: {data['symbol']} at {data['timestamp']} - "
                   f"{len(data['bids'])} bids, {len(data['asks'])} asks")
    except Exception as e:
        logger.error(f"Monitor error: {e}")

@app.on_startup
async def setup():
    """Setup on application startup"""
    logger.info("Starting Binance to NATS JetStream recorder...")
    
    # Wait for broker to be ready
    await asyncio.sleep(1)
    
    # Start Binance streams
    symbols = [
                # 'BNBBTC', 
                # 'ETHBTC', 
                'BTCUSDT']

    loop = asyncio.get_running_loop()
    recorder.start_binance_streams(symbols, loop)

@app.on_shutdown
async def cleanup():
    """Cleanup on shutdown"""
    logger.info("Shutting down...")
    recorder.stop()

if __name__ == "__main__":
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        recorder.stop()
        
        
# nohup python /mnt/vol1/jetstream/orderbook.py > /mnt/vol1/logs/nohup2.out 2>&1 &