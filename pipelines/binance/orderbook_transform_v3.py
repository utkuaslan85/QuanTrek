import asyncio
import json
from typing import Dict, Any
import threading
import time
from binance import ThreadedDepthCacheManager
from faststream import FastStream, Logger
from faststream.nats import NatsBroker
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from bucket_v2 import process_orderbook
from helper import get_now
import gc  # Add garbage collection

# Configure logging with rotation
log_file = "/mnt/vol1/logs/orderbook_transform_v3.log"
handler = RotatingFileHandler(
    log_file,
    maxBytes=50*1024*1024,  # 50MB per file
    backupCount=5,  # Keep 5 backup files
    encoding='utf-8'
)
handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# NATS configuration
NATS_URL = "nats://localhost:4222"

# Initialize NATS broker and FastStream app
broker = NatsBroker(NATS_URL)
app = FastStream(broker)

# Define JetStream
subject_pattern = "binance.depth.*"
stream = "binance_depth"

class SimpleBinanceRecorder:
    def __init__(self):
        self.dcm = None
        self.running = False
        self.update_count = 0  # Track updates for periodic GC
        self.max_bid_count = 0  # Track maximum bid count seen
        self.max_ask_count = 0  # Track maximum ask count seen
        self.anomaly_count = 0  # Track how many anomalies detected
    
    def rotate_log(self):
        """Manually rotate the log file - closes and reopens file handle"""
        for handler in logger.handlers[:]:
            if isinstance(handler, RotatingFileHandler):
                handler.doRollover()
                logger.info("Log file rotated manually")
                break
        
    def format_depth_data(self, depth_cache) -> Dict[str, Any]:
        """Format depth cache data for storage"""
        return {
            "symbol": depth_cache.symbol,
            "record_time": get_now(),
            "timestamp": depth_cache.update_time,
            "bids": depth_cache.get_bids(),  # Get all levels
            "asks": depth_cache.get_asks(),  # Get all levels
        }
    
    def handle_depth_cache(self, depth_cache):
        """Handle depth cache updates from Binance"""
        if not self.running:
            return
        
        # FIX 1: Check if this is an error dict instead of depth_cache object
        if isinstance(depth_cache, dict):
            error_type = depth_cache.get('type', 'Unknown')
            error_msg = depth_cache.get('m', 'No message')
            logger.error(f"Error in depth event restarting cache: {error_type} - {error_msg}")
            return
            
        try:
            # FIX 2: Format the data WITHOUT creating intermediate DataFrame
            depth_data = self.format_depth_data(depth_cache)
            subject = f"{subject_pattern[:-2]}.{depth_data['symbol'].lower()}"
            
            # FIX 3: Use dict directly instead of pd.json_normalize
            # Create minimal dict for process_orderbook
            minimal_data = {
                'symbol': [depth_data['symbol']],
                'timestamp': [depth_data['timestamp']],
                'record_time': [depth_data['record_time']],
                'bids': [depth_data['bids']],
                'asks': [depth_data['asks']]
            }
            df = pd.DataFrame(minimal_data)
            
            # Process orderbook
            result = process_orderbook(df)
            
            # FIX 4: Explicitly delete large objects
            del df
            del depth_data
            del minimal_data
            
            # FIX 5: Schedule async publish with error handling
            if hasattr(self, 'loop') and self.loop and not self.loop.is_closed():
                future = asyncio.run_coroutine_threadsafe(
                    broker.publish(
                        json.dumps(result),
                        subject=subject, 
                        stream=stream
                    ),
                    self.loop
                )
                # Don't wait for result to avoid blocking, but handle errors
                future.add_done_callback(self._handle_publish_result)
            
            # Monitor for data quality issues
            bid_count = len(depth_cache.get_bids())
            ask_count = len(depth_cache.get_asks())
            
            # Track maximums
            self.max_bid_count = max(self.max_bid_count, bid_count)
            self.max_ask_count = max(self.max_ask_count, ask_count)
            
            # Warn if counts are ridiculously high (beyond what we requested)
            if bid_count > 5500 or ask_count > 5500:
                self.anomaly_count += 1
                logger.warning(f"⚠️ ABNORMAL ORDERBOOK #{self.anomaly_count} - {depth_cache.symbol}: bids={bid_count}, asks={ask_count} (exceeds limit=5000)")
            elif self.update_count % 50 == 0:  # Log every 50th update for normal monitoring
                logger.info(f"{depth_cache.symbol}: bids={bid_count}, asks={ask_count}, mid={result.get('mid_price', 0):.8f}")
            
            # Log stats every 1000 updates
            if self.update_count % 1000 == 0:
                logger.info(f"📊 Stats after {self.update_count} updates - Max bids: {self.max_bid_count}, Max asks: {self.max_ask_count}, Anomalies: {self.anomaly_count}")
            
            # FIX 6: Periodic garbage collection
            self.update_count += 1
            if self.update_count % 100 == 0:
                gc.collect()
                logger.info(f"Processed {self.update_count} updates, GC triggered")
            
        except Exception as e:
            logger.error(f"Error handling depth cache: {e}", exc_info=True)
    
    def _handle_publish_result(self, future):
        """Handle publish future completion"""
        try:
            future.result()
        except Exception as e:
            logger.error(f"Error publishing to NATS: {e}")
    
    def start_binance_streams(self, symbols: list, loop):
        """Start Binance depth cache streams"""
        self.loop = loop
        self.running = True
        
        def run_streams():
            try:
                self.dcm = ThreadedDepthCacheManager()
                self.dcm.start()
                
                for symbol in symbols:
                    # Use full limit for comprehensive statistics
                    self.dcm.start_depth_cache(
                        self.handle_depth_cache,
                        symbol=symbol, 
                        refresh_interval=30*60,  # 30 min refresh
                        limit=5000,  # Full depth for complete statistics
                    )
                    logger.info(f"Started depth cache for {symbol} with limit=5000")
                
                # Keep running until stopped
                while self.running:
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in Binance streams: {e}", exc_info=True)
            finally:
                if self.dcm:
                    try:
                        self.dcm.close()
                    except Exception as e:
                        logger.error(f"Error closing DCM: {e}")
        
        # Start in background thread
        thread = threading.Thread(target=run_streams, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop the recorder"""
        logger.info("Stopping recorder...")
        self.running = False
        if self.dcm:
            try:
                self.dcm.close()
            except Exception as e:
                logger.error(f"Error during stop: {e}")

# Global recorder
recorder = SimpleBinanceRecorder()

# JetStream subscriber to monitor all depth data
@broker.subscriber(
    subject=subject_pattern,
    stream=stream,
    deliver_policy="new",
)
async def monitor_depth_data(msg: str, logger: Logger):
    """Monitor depth data stored in JetStream"""
    try:
        data = json.loads(msg)
        logger.info(f"JetStream received: {data['symbol']} at {data['timestamp']}")
    except Exception as e:
        logger.error(f"Monitor error: {e}")

@app.on_startup
async def setup():
    """Setup on application startup"""
    logger.info("Starting Binance to NATS JetStream recorder...")
    
    # Wait for broker to be ready
    await asyncio.sleep(1)
    
    # Start Binance streams
    symbols = ['ETHBTC', 'BTCUSDT']

    loop = asyncio.get_running_loop()
    recorder.start_binance_streams(symbols, loop)

@app.on_shutdown
async def cleanup():
    """Cleanup on shutdown"""
    logger.info("Shutting down...")
    recorder.stop()
    # Force garbage collection on shutdown
    gc.collect()

if __name__ == "__main__":
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        recorder.stop()
        gc.collect()