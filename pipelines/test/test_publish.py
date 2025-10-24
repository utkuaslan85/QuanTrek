import asyncio
import json
import datetime
from datetime import timedelta, timezone
import nats
from nats.js import JetStreamContext


async def publish_messages(days_back: int = 1, symbol: str = "test"):
    """
    Publish 5 messages to NATS JetStream with timestamps going back from now.
    
    Args:
        days_back (int): How many days back from now to start
        symbol (str): Symbol to use in messages
    """
    # Calculate start timestamp
    tz = timezone.utc
    now = datetime.datetime.now(tz=tz)
    start_time = now - timedelta(days=days_back)
    start_timestamp = int(start_time.timestamp())
    
    print(f"Starting from: {start_time.isoformat()}")
    print(f"Start timestamp: {start_timestamp}")
    
    # Connect to NATS
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()
    
    try:
        # Create stream if it doesn't exist
        try:
            await js.add_stream(
                name="binance_test",
                subjects=["binance.test.*"]
            )
            print("Stream 'binance_depth' created or already exists")
        except Exception as e:
            print(f"Stream might already exist: {e}")
        
        # Publish 5 messages
        for i in range(5):
            counter = i + 1
            _timestamp = start_timestamp + i  # Increment by 1 second each time
            timestamp = _timestamp * 1000
            
            message = {
                'symbol': symbol,
                'timestamp': timestamp,
                'counter1': counter,
                'counter2': counter + 50
            }
            
            # Convert to JSON
            message_json = json.dumps(message)
            
            # Publish to subject
            subject = "binance.test.test5"
            ack = await js.publish(subject, message_json.encode())
            
            print(f"Published message {counter}: {message}")
            print(f"  ACK: stream={ack.stream}, seq={ack.seq}")
            
            # Optional: small delay between messages
            await asyncio.sleep(0.1)
            
    except Exception as e:
        print(f"Error publishing messages: {e}")
    finally:
        await nc.close()


async def main():
    """Main function to run the publisher"""
    # Example usage: publish messages starting 2 days back
    await publish_messages(days_back=2, symbol="test5")


if __name__ == "__main__":
    # Run the async function
    asyncio.run(main())


# Alternative synchronous version using nats-py sync client
def publish_messages_sync(days_back: int = 1, symbol: str = "test"):
    """
    Synchronous version - if you prefer not to use async
    Note: Requires nats-py with sync support or pynats
    """
    import nats
    from datetime import datetime, timedelta, timezone
    
    # Calculate start timestamp
    tz = timezone.utc
    now = datetime.now(tz=tz)
    start_time = now - timedelta(days=days_back)
    start_timestamp = int(start_time.timestamp())
    
    print(f"Starting from: {start_time.isoformat()}")
    
    # Note: This is pseudocode for sync version
    # You'll need to adjust based on your NATS client library
    
    for i in range(5):
        counter = i + 1
        timestamp = start_timestamp + i
        
        message = {
            'symbol': symbol,
            'timestamp': timestamp,
            'counter': counter
        }
        
        print(f"Message {counter}: {message}")


# Usage examples:
# asyncio.run(publish_messages(days_back=1, symbol="test"))
# asyncio.run(publish_messages(days_back=7, symbol="ETHUSDT"))