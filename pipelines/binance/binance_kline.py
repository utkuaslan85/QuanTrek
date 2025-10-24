import asyncio
from datetime import datetime
import json
from logging.handlers import RotatingFileHandler
from typing import Dict, Any
import threading
import time
from binance import ThreadedWebsocketManager
from faststream import FastStream, Logger
from faststream.nats import NatsBroker
import logging

# Configure logging with rotation
log_file = "/mnt/vol1/logs/binance_kline_app.log"
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

stream = "binance_kline"
subject_pattern = "binance.kline.*"

class SimpleBinanceKlineRecorder:
    def __init__(self):
        self.twm = None
        self.running = False
        self.field_map = {
            'e': 'event_type',
            'E': 'timestamp',
            's': 'symbol',
            'k_t': 'kline_start_time',
            'k_T': 'kline_end_time',
            'k_s': 'kline_symbol',
            'k_i': 'kline_interval',
            'k_f': 'first_trade_id',
            'k_L': 'last_trade_id',
            'k_o': 'open_price',
            'k_c': 'close_price',
            'k_h': 'high_price',
            'k_l': 'low_price',
            'k_v': 'volume',
            'k_n': 'number_of_trades',
            'k_x': 'check_if_final_bar',
            'k_q': 'quote_volume',
            'k_V': 'active_buy_volume',
            'k_Q': 'active_buy_quote_volume'
        }
    
    def flatten_json(self, nested_json):
        """Flatten nested JSON structure"""
        out = {}
        
        def flatten(x, name=''):
            if isinstance(x, dict):
                for a in x:
                    flatten(x[a], name + a + '_')
            elif isinstance(x, list):
                for i, a in enumerate(x):
                    flatten(a, name + str(i) + '_')
            else:
                out[name[:-1]] = x
        
        flatten(nested_json)
        return out
    
    def format_kline_data(self, msg) -> Dict[str, Any]:
        """Format kline message data for storage"""
        flattened_data = self.flatten_json(msg)
        
        # Rename fields using field map
        for old_key, new_key in self.field_map.items():
            if old_key in flattened_data:
                flattened_data[new_key] = flattened_data.pop(old_key)
        
        # Add timestamp
        flattened_data['record_time'] = datetime.now().timestamp()
        
        return flattened_data
    
    def handle_kline_message(self, msg):
        """Handle kline updates from Binance"""
        if not self.running:
            return
            
        try:
            # Handle error messages from Binance websocket
            if msg.get('e') == 'error':
                error_type = msg.get('type', 'Unknown')
                error_msg = msg.get('m', 'No message')
                logger.warning(f"Binance WebSocket error: {error_type} - {error_msg}")
                return
            
            # Only process actual kline messages
            if msg.get('e') != 'kline':
                logger.debug(f"Skipping non-kline message type: {msg.get('e')}")
                return
            
            # Format the data
            kline_data = self.format_kline_data(msg)
            symbol = kline_data.get('kline_symbol', kline_data.get('symbol', 'UNKNOWN'))
            
            # This should never be UNKNOWN now, but keep as safety net
            if symbol == 'UNKNOWN':
                logger.error(f"Could not determine symbol from kline data: {kline_data}")
                return
            
            subject = f"{subject_pattern[:-2]}.{symbol.lower()}"
            
            # Schedule async publish to JetStream
            if hasattr(self, 'loop') and self.loop and not self.loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    broker.publish(
                        json.dumps(kline_data), 
                        subject=subject, 
                        stream=stream
                    ),
                    self.loop
                )
                
        except Exception as e:
            logger.error(f"Error handling kline message: {e}")
    
    def start_binance_streams(self, symbols: list, loop):
        """Start Binance kline websocket streams"""
        self.loop = loop
        self.running = True
        
        def run_streams():
            try:
                self.twm = ThreadedWebsocketManager()
                self.twm.start()
                
                for symbol in symbols:
                    self.twm.start_kline_socket(
                        callback=self.handle_kline_message,
                        symbol=symbol, 
                        interval='1s'
                    )
                    logger.info(f"Started kline stream for {symbol}")
                
                # Keep running until stopped
                while self.running:
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in Binance kline streams: {e}")
            finally:
                if self.twm:
                    self.twm.stop()
        
        # Start in background thread
        thread = threading.Thread(target=run_streams, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop the recorder"""
        self.running = False
        if self.twm:
            self.twm.stop()

# Global recorder
recorder = SimpleBinanceKlineRecorder()

# JetStream subscriber to monitor all kline data
@broker.subscriber(
    subject=subject_pattern,
    stream=stream,
    deliver_policy="new",
)
async def monitor_kline_data(msg: str, logger: Logger):
    """Monitor kline data stored in JetStream"""
    try:
        data = json.loads(msg)
        symbol = data.get('kline_symbol', data.get('symbol', 'UNKNOWN'))
        interval = data.get('kline_interval', '1s')
        timestamp = data.get('timestamp', '')
        
        logger.info(f"JetStream received: {symbol} {interval} at {timestamp}")
                    
    except Exception as e:
        logger.error(f"Monitor error: {e}")

@app.on_startup
async def setup():
    """Setup on application startup"""
    logger.info("Starting Binance Kline to NATS JetStream recorder...")
    
    # Wait for broker to be ready
    await asyncio.sleep(1)
    
    # Start Binance kline streams
    symbols = ['BTCUSDT', 'ETHBTC']

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
        
# nohup python /mnt/vol1/quantrek_sandbox/pipelines/binance/kline.py > /mnt/vol1/logs/kline.out 2>&1 &