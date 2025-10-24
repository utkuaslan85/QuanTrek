#!/usr/bin/env python3
"""
Example: Per-Subject Message Tracking for JetStream
Monitors each subject independently to detect stale publishing
"""
import asyncio
import nats
from datetime import datetime
from typing import Dict
import json

class PerSubjectMonitor:
    def __init__(self, stale_threshold_seconds: int = 120):
        self.stale_threshold_seconds = stale_threshold_seconds
        # Track last seen time per subject
        self.subject_last_seen: Dict[str, datetime] = {}
        self.subject_message_count: Dict[str, int] = {}
        # Track which subjects are currently in stale state
        self.stale_subjects: set = set()
        
    async def message_handler(self, msg):
        """Handle each message and track per-subject activity"""
        subject = msg.subject
        current_time = datetime.now()
        
        # Check if this subject was previously stale
        was_stale = subject in self.stale_subjects
        
        # Update tracking
        self.subject_last_seen[subject] = current_time
        self.subject_message_count[subject] = self.subject_message_count.get(subject, 0) + 1
        
        # If it was stale, it's now recovered!
        if was_stale:
            self.stale_subjects.remove(subject)
            # Calculate how long it was stale
            recovery_msg = (f"\n{'='*80}\n"
                          f"✅ RECOVERY DETECTED!\n"
                          f"{'='*80}\n"
                          f"📡 Subject: {subject}\n"
                          f"🕐 Recovered at: {current_time.strftime('%H:%M:%S')}\n"
                          f"📊 Total messages: {self.subject_message_count[subject]}\n"
                          f"{'='*80}\n")
            print(recovery_msg)
        
        # Parse message to show what we're tracking
        try:
            data = json.loads(msg.data.decode())
            symbol = data.get('symbol', 'unknown')
            timestamp = data.get('timestamp', 'unknown')
            
            status = "🔄" if was_stale else "✅"
            print(f"{status} [{current_time.strftime('%H:%M:%S')}] {subject} | "
                  f"Symbol: {symbol} | TS: {timestamp} | "
                  f"Count: {self.subject_message_count[subject]}")
        except:
            status = "🔄" if was_stale else "✅"
            print(f"{status} [{current_time.strftime('%H:%M:%S')}] {subject} | "
                  f"Count: {self.subject_message_count[subject]}")
    
    async def check_stale_subjects(self):
        """Check if any subject has gone stale and detect newly stale subjects"""
        current_time = datetime.now()
        newly_stale_subjects = []
        
        for subject, last_seen in self.subject_last_seen.items():
            time_since_last = (current_time - last_seen).total_seconds()
            
            if time_since_last > self.stale_threshold_seconds:
                # Check if this is newly stale (wasn't stale before)
                if subject not in self.stale_subjects:
                    self.stale_subjects.add(subject)
                    newly_stale_subjects.append({
                        'subject': subject,
                        'last_seen': last_seen,
                        'seconds_ago': int(time_since_last),
                        'message_count': self.subject_message_count.get(subject, 0)
                    })
        
        return newly_stale_subjects
    
    def print_status(self):
        """Print current status of all subjects"""
        current_time = datetime.now()
        print("\n" + "="*80)
        print(f"📊 SUBJECT STATUS REPORT - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        if not self.subject_last_seen:
            print("⚠️  No subjects seen yet")
            return
        
        for subject in sorted(self.subject_last_seen.keys()):
            last_seen = self.subject_last_seen[subject]
            seconds_ago = int((current_time - last_seen).total_seconds())
            msg_count = self.subject_message_count.get(subject, 0)
            
            # Status indicator
            is_currently_stale = subject in self.stale_subjects
            
            if is_currently_stale:
                status = "🔴 STALE"
            elif seconds_ago > self.stale_threshold_seconds / 2:
                status = "🟡 WARNING"
            else:
                status = "🟢 ACTIVE"
            
            print(f"{status} | {subject:40s} | "
                  f"Last: {seconds_ago:4d}s ago | "
                  f"Count: {msg_count:6d} | "
                  f"Time: {last_seen.strftime('%H:%M:%S')}")
        
        print("="*80 + "\n")


async def main():
    # Configuration
    NATS_URL = "nats://localhost:4222"
    STREAM_NAME = "binance_depth"
    SUBJECT_PATTERN = "binance.depth.*"
    STALE_THRESHOLD = 120  # Alert if no message for 2 minutes
    
    print(f"🌊 Per-Subject Monitor Starting")
    print(f"📡 NATS: {NATS_URL}")
    print(f"🎯 Stream: {STREAM_NAME}")
    print(f"🔍 Pattern: {SUBJECT_PATTERN}")
    print(f"⏱️  Stale Threshold: {STALE_THRESHOLD}s")
    print("="*80 + "\n")
    
    # Create monitor
    monitor = PerSubjectMonitor(stale_threshold_seconds=STALE_THRESHOLD)
    
    # Connect to NATS
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    
    print("✅ Connected to NATS JetStream\n")
    
    # Subscribe to all subjects in the pattern
    # Using deliver_policy="all" to see historical messages
    # In production, you'd use "last_per_subject" or "new"
    sub = await js.subscribe(
        subject=SUBJECT_PATTERN,
        stream=STREAM_NAME,
        durable="subject_monitor",  # Durable consumer for tracking
        config=nats.js.api.ConsumerConfig(
            deliver_policy=nats.js.api.DeliverPolicy.NEW,
            ack_policy=nats.js.api.AckPolicy.EXPLICIT,
        )
    )
    
    print(f"📬 Subscribed to {SUBJECT_PATTERN}\n")
    print("Starting to monitor messages...\n")
    
    # Background task to check for stale subjects
    async def stale_checker():
        status_print_interval = 30  # Print status every 30s
        check_interval = 10  # Check for stale every 10s
        last_status_print = datetime.now()
        
        while True:
            await asyncio.sleep(check_interval)
            
            # Check for stale subjects
            stale = await monitor.check_stale_subjects()
            
            # Only print alert for NEWLY stale subjects
            if stale:
                print("\n" + "!"*80)
                print(f"🚨 ALERT: {len(stale)} NEWLY STALE SUBJECT(S) DETECTED")
                print("!"*80)
                for item in stale:
                    print(f"🔴 {item['subject']}")
                    print(f"   └─ Last seen: {item['seconds_ago']}s ago "
                          f"({item['last_seen'].strftime('%H:%M:%S')})")
                    print(f"   └─ Total messages: {item['message_count']}")
                print("!"*80 + "\n")
            
            # Print status report periodically
            current_time = datetime.now()
            if (current_time - last_status_print).total_seconds() >= status_print_interval:
                monitor.print_status()
                last_status_print = current_time
    
    # Start stale checker in background
    checker_task = asyncio.create_task(stale_checker())
    
    try:
        # Main message processing loop
        async for msg in sub.messages:
            await monitor.message_handler(msg)
            await msg.ack()
            
    except KeyboardInterrupt:
        print("\n⏹️  Stopping monitor...")
    finally:
        checker_task.cancel()
        await nc.close()
        print("🔌 Disconnected from NATS")
        
        # Final status
        monitor.print_status()


if __name__ == "__main__":
    """
    This example shows the core logic for per-subject monitoring:
    
    1. Subscribe to wildcard pattern (binance.depth.*)
    2. Track last_seen timestamp for EACH subject independently
    3. Periodically check if any subject has gone stale
    4. Alert when a specific subject stops publishing
    
    Key Advantage:
    - If binance.depth.ethbtc stops publishing, you get an alert for THAT subject
    - Even if binance.depth.btcusdt keeps publishing fine
    - Stream-level metrics (total message count) are irrelevant
    
    To use this logic in your talert_stream.py:
    - Replace the stream-level message count tracking
    - Add subject-level tracking like this example
    - Send Telegram alerts when stale subjects are detected
    """
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")