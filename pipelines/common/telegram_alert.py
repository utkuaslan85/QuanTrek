#!/usr/bin/env python3
"""
Per-Subject JetStream Monitor with Telegram Alerts
Monitors each subject independently across multiple streams
"""
import asyncio
import nats
from datetime import datetime
from typing import Dict
from dataclasses import dataclass, field
import json
import requests

@dataclass
class StreamMonitorConfig:
    stream_name: str
    subject_pattern: str
    stale_threshold_seconds: int
    # Track state per stream
    subject_last_seen: Dict[str, datetime] = field(default_factory=dict)
    subject_message_count: Dict[str, int] = field(default_factory=dict)
    stale_subjects: set = field(default_factory=set)


class PerSubjectJetStreamMonitor:
    def __init__(self, nats_url: str, telegram_bot_token: str, telegram_chat_id: str):
        self.nats_url = nats_url
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.telegram_url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
        
        self.monitors: Dict[str, StreamMonitorConfig] = {}
        self.nc = None
        self.js = None
        self.running = False
    
    def add_stream_monitor(self, stream_name: str, subject_pattern: str, 
                          stale_threshold_seconds: int = 120):
        """Add a stream to monitor with per-subject tracking"""
        self.monitors[stream_name] = StreamMonitorConfig(
            stream_name=stream_name,
            subject_pattern=subject_pattern,
            stale_threshold_seconds=stale_threshold_seconds
        )
        print(f"📊 Added monitor for stream '{stream_name}' | "
              f"Pattern: {subject_pattern} | "
              f"Threshold: {stale_threshold_seconds}s")
    
    def send_telegram_alert(self, message: str) -> bool:
        """Send alert message to Telegram"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"🌊 <b>JETSTREAM SUBJECT MONITOR</b>\n⏰ {timestamp}\n\n{message}"
        
        payload = {
            'chat_id': self.telegram_chat_id,
            'text': full_message,
            'parse_mode': 'HTML'
        }
        
        try:
            response = requests.post(self.telegram_url, data=payload, timeout=10)
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"❌ Failed to send Telegram alert: {e}")
            return False
    
    async def message_handler(self, msg, monitor_config: StreamMonitorConfig):
        """Handle each message and track per-subject activity"""
        subject = msg.subject
        current_time = datetime.now()
        
        # Check if this subject was previously stale
        was_stale = subject in monitor_config.stale_subjects
        
        # Update tracking
        monitor_config.subject_last_seen[subject] = current_time
        monitor_config.subject_message_count[subject] = \
            monitor_config.subject_message_count.get(subject, 0) + 1
        
        # If it was stale, it's now recovered!
        if was_stale:
            monitor_config.stale_subjects.remove(subject)
            
            recovery_msg = f"""✅ <b>SUBJECT RECOVERY</b>

🌊 <b>Stream:</b> <code>{monitor_config.stream_name}</code>
📡 <b>Subject:</b> <code>{subject}</code>
📊 <b>Total Messages:</b> {monitor_config.subject_message_count[subject]:,}

Subject is publishing again!"""
            
            if self.send_telegram_alert(recovery_msg):
                print(f"✅ Recovery alert sent for {subject} in {monitor_config.stream_name}")
        
        # Log to console
        try:
            data = json.loads(msg.data.decode())
            symbol = data.get('symbol', 'unknown')
            status = "🔄" if was_stale else "✅"
            print(f"{status} [{current_time.strftime('%H:%M:%S')}] "
                  f"[{monitor_config.stream_name}] {subject} | "
                  f"Symbol: {symbol} | Count: {monitor_config.subject_message_count[subject]}")
        except:
            status = "🔄" if was_stale else "✅"
            print(f"{status} [{current_time.strftime('%H:%M:%S')}] "
                  f"[{monitor_config.stream_name}] {subject} | "
                  f"Count: {monitor_config.subject_message_count[subject]}")
    
    async def check_stale_subjects(self, monitor_config: StreamMonitorConfig):
        """Check if any subject has gone stale for a specific stream"""
        current_time = datetime.now()
        newly_stale_subjects = []
        
        for subject, last_seen in monitor_config.subject_last_seen.items():
            time_since_last = (current_time - last_seen).total_seconds()
            
            if time_since_last > monitor_config.stale_threshold_seconds:
                # Check if this is newly stale (wasn't stale before)
                if subject not in monitor_config.stale_subjects:
                    monitor_config.stale_subjects.add(subject)
                    newly_stale_subjects.append({
                        'stream': monitor_config.stream_name,
                        'subject': subject,
                        'last_seen': last_seen,
                        'seconds_ago': int(time_since_last),
                        'message_count': monitor_config.subject_message_count.get(subject, 0)
                    })
        
        return newly_stale_subjects
    
    async def subscribe_to_stream(self, monitor_config: StreamMonitorConfig):
        """Subscribe to a stream and start monitoring its subjects"""
        try:
            # Create a durable consumer name based on stream
            durable_name = f"subject_monitor_{monitor_config.stream_name}"
            
            sub = await self.js.subscribe(
                subject=monitor_config.subject_pattern,
                stream=monitor_config.stream_name,
                durable=durable_name,
                config=nats.js.api.ConsumerConfig(
                    deliver_policy=nats.js.api.DeliverPolicy.NEW,
                    ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                )
            )
            
            print(f"📬 Subscribed to {monitor_config.stream_name} "
                  f"({monitor_config.subject_pattern})")
            
            # Process messages for this stream
            async for msg in sub.messages:
                if not self.running:
                    break
                await self.message_handler(msg, monitor_config)
                await msg.ack()
                
        except Exception as e:
            error_msg = f"""💥 <b>SUBSCRIPTION ERROR</b>

🌊 <b>Stream:</b> <code>{monitor_config.stream_name}</code>
❌ <b>Error:</b> {str(e)}

Failed to subscribe to stream!"""
            self.send_telegram_alert(error_msg)
            print(f"❌ Error subscribing to {monitor_config.stream_name}: {e}")
    
    async def stale_checker(self):
        """Background task to check for stale subjects across all streams"""
        check_interval = 10  # Check for stale every 10s
        
        while self.running:
            await asyncio.sleep(check_interval)
            
            # Check each monitored stream
            for stream_name, monitor_config in self.monitors.items():
                stale = await self.check_stale_subjects(monitor_config)
                
                # Send Telegram alert for newly stale subjects
                if stale:
                    subjects_list = []
                    for item in stale:
                        minutes_stale = item['seconds_ago'] // 60
                        subjects_list.append(
                            f"• <code>{item['subject']}</code>\n"
                            f"  └─ Stale for: {minutes_stale}m {item['seconds_ago'] % 60}s\n"
                            f"  └─ Last seen: {item['last_seen'].strftime('%H:%M:%S')}\n"
                            f"  └─ Messages: {item['message_count']:,}"
                        )
                    
                    subjects_text = "\n\n".join(subjects_list)
                    
                    alert_msg = f"""🚨 <b>STALE SUBJECT(S) DETECTED</b>

🌊 <b>Stream:</b> <code>{stream_name}</code>
⚠️ <b>Threshold:</b> {monitor_config.stale_threshold_seconds}s
🔴 <b>Count:</b> {len(stale)} subject(s)

<b>Stale Subjects:</b>
{subjects_text}

These subjects have stopped publishing!"""
                    
                    if self.send_telegram_alert(alert_msg):
                        print(f"🚨 Stale alert sent for {len(stale)} subject(s) in {stream_name}")
    
    async def status_reporter(self):
        """Background task to send periodic status reports"""
        report_interval = 60*60*5  # Report every 5 hours
        
        while self.running:
            await asyncio.sleep(report_interval)
            
            # Generate status report for all streams
            for stream_name, monitor_config in self.monitors.items():
                if not monitor_config.subject_last_seen:
                    continue
                
                current_time = datetime.now()
                active_count = 0
                warning_count = 0
                stale_count = len(monitor_config.stale_subjects)
                
                for subject, last_seen in monitor_config.subject_last_seen.items():
                    seconds_ago = (current_time - last_seen).total_seconds()
                    if subject not in monitor_config.stale_subjects:
                        if seconds_ago > monitor_config.stale_threshold_seconds / 2:
                            warning_count += 1
                        else:
                            active_count += 1
                
                status_msg = f"""📊 <b>STATUS REPORT</b>

🌊 <b>Stream:</b> <code>{stream_name}</code>
⏱️ <b>Threshold:</b> {monitor_config.stale_threshold_seconds}s

<b>Subject Status:</b>
🟢 Active: {active_count}
🟡 Warning: {warning_count}
🔴 Stale: {stale_count}

<b>Total Subjects:</b> {len(monitor_config.subject_last_seen)}"""
                
                self.send_telegram_alert(status_msg)
                print(f"📊 Status report sent for {stream_name}")
    
    async def start_monitoring(self):
        """Start the JetStream monitoring system"""
        try:
            # Connect to NATS
            print(f"🔌 Connecting to NATS at {self.nats_url}...")
            self.nc = await nats.connect(self.nats_url)
            self.js = self.nc.jetstream()
            print("✅ Connected to NATS JetStream\n")
            
            # Verify streams exist
            existing_streams = []
            try:
                streams_list = await self.js.streams_info()
                if hasattr(streams_list, '__aiter__'):
                    async for stream_info in streams_list:
                        existing_streams.append(stream_info.config.name)
                else:
                    for stream_info in streams_list:
                        existing_streams.append(stream_info.config.name)
            except Exception as e:
                print(f"⚠️ Warning: Could not list streams: {e}")
            
            # Check which monitored streams exist
            missing_streams = []
            for stream_name in self.monitors.keys():
                if stream_name not in existing_streams:
                    missing_streams.append(stream_name)
            
            # Send startup notification
            streams_status = []
            for stream_name, monitor_config in self.monitors.items():
                status = "✅" if stream_name not in missing_streams else "❌"
                streams_status.append(
                    f"{status} <code>{stream_name}</code>\n"
                    f"  └─ Pattern: {monitor_config.subject_pattern}\n"
                    f"  └─ Threshold: {monitor_config.stale_threshold_seconds}s"
                )
            
            startup_msg = f"""🚀 <b>PER-SUBJECT MONITOR STARTED</b>

🖥️ <b>NATS Server:</b> <code>{self.nats_url}</code>

<b>Monitoring Streams:</b>
{chr(10).join(streams_status)}

📊 <b>Available Streams:</b> {len(existing_streams)}
{f"⚠️ <b>Missing Streams:</b> {', '.join(missing_streams)}" if missing_streams else ""}

🎯 <b>Mode:</b> Per-subject tracking
Monitoring individual subjects within each stream!"""
            
            self.send_telegram_alert(startup_msg)
            
            if missing_streams:
                print(f"⚠️ Warning: These streams don't exist: {', '.join(missing_streams)}")
            
            self.running = True
            
            # Start background tasks
            checker_task = asyncio.create_task(self.stale_checker())
            reporter_task = asyncio.create_task(self.status_reporter())
            
            # Create subscription tasks for each stream
            subscription_tasks = []
            for monitor_config in self.monitors.values():
                task = asyncio.create_task(
                    self.subscribe_to_stream(monitor_config)
                )
                subscription_tasks.append(task)
            
            print("🎯 Multi-stream per-subject monitoring started! Press Ctrl+C to stop.\n")
            
            # Wait for all tasks
            await asyncio.gather(*subscription_tasks, checker_task, reporter_task)
            
        except KeyboardInterrupt:
            print("\n⏹️ Stopping monitor...")
            self.running = False
        except Exception as e:
            error_msg = f"""💥 <b>MONITOR ERROR</b>

❌ <b>Error:</b> {str(e)}

Monitoring system has stopped!"""
            self.send_telegram_alert(error_msg)
            print(f"❌ Error: {e}")
        finally:
            self.running = False
            if self.nc and not self.nc.is_closed:
                await self.nc.close()
                print("🔌 NATS connection closed")


async def main():
    # Configuration
    NATS_URL = "nats://localhost:4222"
    TELEGRAM_BOT_TOKEN = "8431905462:AAFdZ0idlGzNzHbMxZkMYBCm_LHAmOTAAHA"
    TELEGRAM_CHAT_ID = "7004807409"
    
    print("🌊 Per-Subject JetStream Monitor with Telegram Alerts")
    print("="*80)
    
    # Create monitor instance
    monitor = PerSubjectJetStreamMonitor(
        nats_url=NATS_URL,
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID
    )
    
    # Add streams to monitor with per-subject tracking
    monitor.add_stream_monitor(
        stream_name="binance_depth",
        subject_pattern="binance.depth.*",
        stale_threshold_seconds=120  # Alert if subject stale for 2 minutes
    )
    
    monitor.add_stream_monitor(
        stream_name="binance_kline",
        subject_pattern="binance.kline.*",
        stale_threshold_seconds=120  # Alert if subject stale for 2 minutes
    )
    
    print("="*80 + "\n")
    
    # Start monitoring
    await monitor.start_monitoring()


if __name__ == "__main__":
    """
    Per-Subject JetStream Monitor with Telegram Alerts
    
    Features:
    - Monitors individual subjects within each stream
    - Detects when specific subjects go stale (not just stream-level)
    - Sends recovery alerts when stale subjects resume
    - Periodic status reports showing active/warning/stale subjects
    - Independent thresholds per stream
    - Telegram notifications for all events
    
    Usage:
    python talert_stream_v2.py
    
    Or run in background:
    nohup python talert_stream_v2.py > /mnt/vol1/logs/talert_stream_v2.out 2>&1 &
    """
    print("Monitoring capabilities:")
    print("• Per-subject activity tracking")
    print("• Individual subject stale detection")
    print("• Subject recovery notifications")
    print("• Periodic status reports")
    print("• Real-time Telegram alerts")
    print("="*80 + "\n")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")