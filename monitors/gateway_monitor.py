import time
import logging
from datetime import datetime
import requests
from ib_insync import IB
from threading import Thread

# Telegram config
TELEGRAM_BOT_TOKEN = "8368353420:AAFwR5eXV8TkEFExBeqqjgkSdTZYdXzva_Q"
TELEGRAM_CHAT_ID = "7004807409"

# IB Gateway config
GATEWAY_HOST = "192.168.193.243"
GATEWAY_PORT = 7497
CLIENT_ID = 2
CHECK_INTERVAL = 60  # Check every minute

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Track connection state to avoid spam
last_alert_status = None


def send_telegram_alert(message):
    """Send alert message to Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Telegram alert sent successfully")
            return True
        else:
            logger.error(f"Failed to send Telegram alert: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error sending Telegram alert: {e}")
        return False


def check_gateway():
    """Check IB Gateway connection"""
    ib = IB()
    
    try:
        ib.connect(GATEWAY_HOST, GATEWAY_PORT, clientId=CLIENT_ID)
        
        if ib.isConnected():
            status = "✅ CONNECTED"
            logger.info("Gateway is connected")
            return True, status, None
        else:
            status = "❌ CONNECTION FAILED"
            error = "Connection object created but not connected"
            logger.warning(status)
            return False, status, error
            
    except ConnectionRefusedError as e:
        status = "❌ CONNECTION REFUSED"
        error = f"Connection refused - Gateway may be down"
        logger.error(f"{status}: {e}")
        return False, status, error
        
    except TimeoutError as e:
        status = "❌ TIMEOUT"
        error = f"Connection timeout - Host unreachable"
        logger.error(f"{status}: {e}")
        return False, status, error
        
    except OSError as e:
        status = "❌ NETWORK ERROR"
        error = f"OS Error: {e}"
        logger.error(f"{status}: {e}")
        return False, status, error
        
    except Exception as e:
        status = "❌ UNKNOWN ERROR"
        error = f"Exception: {type(e).__name__}: {str(e)}"
        logger.error(f"{status}: {e}")
        return False, status, error
        
    finally:
        try:
            ib.disconnect()
        except:
            pass


def monitor_loop():
    """Main monitoring loop"""
    global last_alert_status
    
    logger.info(f"Starting IB Gateway monitor - checking every {CHECK_INTERVAL} seconds")
    logger.info(f"Connecting to {GATEWAY_HOST}:{GATEWAY_PORT}")
    
    while True:
        try:
            is_connected, status, error = check_gateway()
            
            # Only send alert if status changed
            if last_alert_status != is_connected:
                last_alert_status = is_connected
                
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                if is_connected:
                    message = f"🟢 IB Gateway is now ONLINE\n\nTime: {timestamp}"
                else:
                    message = f"🔴 IB Gateway is DOWN\n\nStatus: {status}\nError: {error}\nTime: {timestamp}"
                
                send_telegram_alert(message)
                logger.info(f"Alert sent: {message}")
            
            time.sleep(CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Monitor stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in monitor loop: {e}")
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        monitor_loop()
    except KeyboardInterrupt:
        logger.info("Shutting down...")