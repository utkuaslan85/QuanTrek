#!/usr/bin/env python3
"""
Per-Subject JetStream Monitor with Telegram Alerts and Auto-Restart
Monitors each subject independently and restarts associated processes when stale
"""
import asyncio
import time
import nats
from datetime import datetime
from typing import Dict, Optional
from dataclasses import dataclass, field
import json
import requests
import xmlrpc.client
from enum import Enum

class RestartAction(Enum):
    """Actions to take when a subject goes stale"""
    NONE = "none"
    ALERT_ONLY = "alert_only"
    RESTART_PROCESS = "restart_process"

@dataclass
class StreamMonitorConfig:
    stream_name: str
    subject_pattern: str
    stale_threshold_seconds: int
    # NEW: Process restart configuration
    supervisor_process_name: Optional[str] = None
    restart_action: RestartAction = RestartAction.ALERT_ONLY
    max_restarts_per_hour: int = 3
    
    # Track state per stream
    subject_last_seen: Dict[str, datetime] = field(default_factory=dict)
    subject_message_count: Dict[str, int] = field(default_factory=dict)
    stale_subjects: set = field(default_factory=set)
    restart_history: list = field(default_factory=list)


class SupervisorManager:
    """Manages Supervisor process control via XML-RPC"""
    
    def __init__(self, supervisor_url: str = "http://localhost:9001/RPC2",
                 username: str = "admin", password: str = "admin123"):
        self.supervisor_url = supervisor_url
        self.username = username
        self.password = password
        self.server = None
    
    def connect(self):
        """Connect to Supervisor"""
        try:
            # Add authentication to URL
            if self.username and self.password:
                # Parse URL and add credentials
                import re
                match = re.match(r'(https?://)(.+)', self.supervisor_url)
                if match:
                    protocol, rest = match.groups()
                    auth_url = f"{protocol}{self.username}:{self.password}@{rest}"
                    self.server = xmlrpc.client.ServerProxy(auth_url)
                else:
                    self.server = xmlrpc.client.ServerProxy(self.supervisor_url)
            else:
                self.server = xmlrpc.client.ServerProxy(self.supervisor_url)
            
            # Test connection
            self.server.supervisor.getState()
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Supervisor: {e}")
            return False
    
    def restart_process(self, process_name: str) -> bool:
        """Restart a specific process via Supervisor XML-RPC."""
        if not self.server:
            if not self.connect():
                print("❌ Failed to connect to Supervisor.")
                return False
        
        try:
            # Attempt stop
            print(f"🔄 Stopping process: {process_name}")
            try:
                self.server.supervisor.stopProcess(process_name)
            except xmlrpc.client.Fault as e:
                # If already stopped, ignore NOT_RUNNING
                if "NOT_RUNNING" not in e.faultString:
                    raise
                print(f"⚠️  Process already stopped: {process_name}")

            # Small pause to allow supervisor state sync
            time.sleep(2)

            # Start the process
            print(f"▶️ Starting process: {process_name}")
            self.server.supervisor.startProcess(process_name)
            print(f"✅ Restarted successfully: {process_name}")
            return True

        except xmlrpc.client.Fault as e:
            print(f"❌ Supervisor error restarting {process_name}: {e.faultString}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error restarting {process_name}: {e}")
    
    
    def get_process_info(self, process_name: str) -> Optional[dict]:
        """Get process information"""
        if not self.server:
            if not self.connect():
                return None
        
        try:
            info = self.server.supervisor.getProcessInfo(process_name)
            return info
        except Exception as e:
            print(f"❌ Error getting info for {process_name}: {e}")
            return None


class PerSubjectJetStreamMonitor:
    def __init__(self, nats_url: str, telegram_bot_token: str, telegram_chat_id: str,
                 supervisor_url: str = "http://localhost:9001/RPC2",
                 supervisor_username: str = "admin",
                 supervisor_password: str = "admin123"):
        self.nats_url = nats_url
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.telegram_url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
        
        self.supervisor = SupervisorManager(supervisor_url, supervisor_username, supervisor_password)
        self.monitors: Dict[str, StreamMonitorConfig] = {}
        self.nc = None
        self.js = None
        self.running = False
    
    def add_stream_monitor(self, stream_name: str, subject_pattern: str, 
                          stale_threshold_seconds: int = 120,
                          supervisor_process_name: Optional[str] = None,
                          restart_action: RestartAction = RestartAction.ALERT_ONLY,
                          max_restarts_per_hour: int = 3):
        """Add a stream to monitor with per-subject tracking and restart config"""
        self.monitors[stream_name] = StreamMonitorConfig(
            stream_name=stream_name,
            subject_pattern=subject_pattern,
            stale_threshold_seconds=stale_threshold_seconds,
            supervisor_process_name=supervisor_process_name,
            restart_action=restart_action,
            max_restarts_per_hour=max_restarts_per_hour
        )
        
        restart_info = ""
        if restart_action == RestartAction.RESTART_PROCESS and supervisor_process_name:
            restart_info = f" | Auto-restart: {supervisor_process_name}"
        
        print(f"📊 Added monitor for stream '{stream_name}' | "
              f"Pattern: {subject_pattern} | "
              f"Threshold: {stale_threshold_seconds}s{restart_info}")
    
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
    
    def should_restart(self, monitor_config: StreamMonitorConfig) -> bool:
        """Check if we should restart based on restart history"""
        if not monitor_config.restart_history:
            return True
        
        # Count restarts in the last hour
        current_time = datetime.now()
        recent_restarts = [
            r for r in monitor_config.restart_history
            if (current_time - r).total_seconds() < 3600
        ]
        
        return len(recent_restarts) < monitor_config.max_restarts_per_hour
    
    async def handle_stale_subject(self, monitor_config: StreamMonitorConfig, 
                                   stale_info: dict):
        """Handle a stale subject - send alert and optionally restart process"""
        subject = stale_info['subject']
        stream_name = stale_info['stream']
        
        # Build alert message
        minutes_stale = stale_info['seconds_ago'] // 60
        alert_msg = f"""🚨 <b>STALE SUBJECT DETECTED</b>

🌊 <b>Stream:</b> <code>{stream_name}</code>
📡 <b>Subject:</b> <code>{subject}</code>
⏱️ <b>Stale Duration:</b> {minutes_stale}m {stale_info['seconds_ago'] % 60}s
📊 <b>Messages:</b> {stale_info['message_count']:,}
🕐 <b>Last Seen:</b> {stale_info['last_seen'].strftime('%H:%M:%S')}"""
        
        # Check if we should restart
        if (monitor_config.restart_action == RestartAction.RESTART_PROCESS and 
            monitor_config.supervisor_process_name):
            
            if self.should_restart(monitor_config):
                alert_msg += f"\n\n🔄 <b>Action:</b> Restarting process <code>{monitor_config.supervisor_process_name}</code>"
                
                # Send alert first
                self.send_telegram_alert(alert_msg)
                
                # Attempt restart
                print(f"🔄 Attempting to restart {monitor_config.supervisor_process_name}...")
                success = self.supervisor.restart_process(monitor_config.supervisor_process_name)
                
                if success:
                    monitor_config.restart_history.append(datetime.now())
                    restart_msg = f"""✅ <b>PROCESS RESTARTED</b>

🌊 <b>Stream:</b> <code>{stream_name}</code>
📡 <b>Subject:</b> <code>{subject}</code>
🔄 <b>Process:</b> <code>{monitor_config.supervisor_process_name}</code>

Process has been restarted successfully!"""
                    self.send_telegram_alert(restart_msg)
                    print(f"✅ Successfully restarted {monitor_config.supervisor_process_name}")
                else:
                    error_msg = f"""❌ <b>RESTART FAILED</b>

🌊 <b>Stream:</b> <code>{stream_name}</code>
📡 <b>Subject:</b> <code>{subject}</code>
🔄 <b>Process:</b> <code>{monitor_config.supervisor_process_name}</code>

Failed to restart process! Manual intervention required."""
                    self.send_telegram_alert(error_msg)
            else:
                alert_msg += f"\n\n⚠️ <b>Restart Limit Reached:</b> {monitor_config.max_restarts_per_hour}/hour"
                alert_msg += "\n<b>Manual intervention required!</b>"
                self.send_telegram_alert(alert_msg)
                print(f"⚠️ Restart limit reached for {monitor_config.supervisor_process_name}")
        else:
            # Just send alert
            self.send_telegram_alert(alert_msg)
    
    async def message_handler(self, msg, monitor_config: StreamMonitorConfig):
        """Handle each message and track per-subject activity"""
        subject = msg.subject
        current_time = datetime.now()

        # Check if this subject was previously stale
        was_stale = subject in monitor_config.stale_subjects

        # Update tracking
        monitor_config.subject_last_seen[subject] = current_time
        monitor_config.subject_message_count[subject] = monitor_config.subject_message_count.get(subject, 0) + 1

        # If it was stale, it's now recovered!
        if was_stale:
            monitor_config.stale_subjects.remove(subject)
            recovery_msg = (
                f"✅ <b>SUBJECT RECOVERY</b>\n\n"
                f"🌊 <b>Stream:</b> <code>{monitor_config.stream_name}</code>\n"
                f"📡 <b>Subject:</b> <code>{subject}</code>\n"
                f"📊 <b>Total Messages:</b> {monitor_config.subject_message_count[subject]:,}\n\n"
                "Subject is publishing again!"
            )
            await asyncio.to_thread(self.send_telegram_alert, recovery_msg)
            print(f"✅ Recovery alert sent for {subject} in {monitor_config.stream_name}")

        # Log to console
        try:
            data = json.loads(msg.data.decode())
            symbol = data.get("symbol", "unknown")
            status = "🔄" if was_stale else "✅"
            print(
                f"{status} [{current_time.strftime('%H:%M:%S')}] "
                f"[{monitor_config.stream_name}] {subject} | "
                f"Symbol: {symbol} | Count: {monitor_config.subject_message_count[subject]}"
            )
        except Exception:
            status = "🔄" if was_stale else "✅"
            print(
                f"{status} [{current_time.strftime('%H:%M:%S')}] "
                f"[{monitor_config.stream_name}] {subject} | "
                f"Count: {monitor_config.subject_message_count[subject]}"
            )

    async def check_stale_subjects(self, monitor_config: StreamMonitorConfig):
        """Check if any subject has gone stale for a specific stream"""
        current_time = datetime.now()
        newly_stale_subjects = []

        for subject, last_seen in monitor_config.subject_last_seen.items():
            time_since_last = (current_time - last_seen).total_seconds()

            if time_since_last > monitor_config.stale_threshold_seconds:
                # Check if this is newly stale
                if subject not in monitor_config.stale_subjects:
                    monitor_config.stale_subjects.add(subject)
                    stale_info = {
                        "stream": monitor_config.stream_name,
                        "subject": subject,
                        "last_seen": last_seen,
                        "seconds_ago": int(time_since_last),
                        "message_count": monitor_config.subject_message_count.get(subject, 0),
                    }
                    newly_stale_subjects.append(stale_info)

                    # Handle stale subject (async-safe)
                    await self.handle_stale_subject(monitor_config, stale_info)

        return newly_stale_subjects


    async def subscribe_to_stream(self, monitor_config: StreamMonitorConfig):
        """Subscribe to a stream and start monitoring its subjects"""
        try:
            # Create a unique durable consumer name based on stream and timestamp
            import time
            durable_name = f"subject_monitor_{monitor_config.stream_name}_{int(time.time())}"
            
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
                await self.check_stale_subjects(monitor_config)
    
    async def status_reporter(self):
        """Background task to send periodic status reports"""
        report_interval = 60*60*24  # Report every 24 hours
        
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
                
                # Count restarts in last hour
                recent_restarts = [
                    r for r in monitor_config.restart_history
                    if (current_time - r).total_seconds() < 3600
                ]
                
                status_msg = f"""📊 <b>STATUS REPORT</b>

🌊 <b>Stream:</b> <code>{stream_name}</code>
⏱️ <b>Threshold:</b> {monitor_config.stale_threshold_seconds}s

<b>Subject Status:</b>
🟢 Active: {active_count}
🟡 Warning: {warning_count}
🔴 Stale: {stale_count}

<b>Total Subjects:</b> {len(monitor_config.subject_last_seen)}"""
                
                if monitor_config.restart_action == RestartAction.RESTART_PROCESS:
                    status_msg += f"\n\n🔄 <b>Restarts (last hour):</b> {len(recent_restarts)}/{monitor_config.max_restarts_per_hour}"
                
                self.send_telegram_alert(status_msg)
                print(f"📊 Status report sent for {stream_name}")
    
    async def start_monitoring(self):
        """Start the JetStream monitoring system"""
        try:
            # Connect to Supervisor
            print("🔌 Connecting to Supervisor...")
            if self.supervisor.connect():
                print("✅ Connected to Supervisor\n")
            else:
                print("⚠️ Could not connect to Supervisor (auto-restart disabled)\n")
            
            # Connect to NATS
            print(f"🔌 Connecting to NATS at {self.nats_url}...")
            self.nc = await nats.connect(self.nats_url)
            self.js = self.nc.jetstream()
            print("✅ Connected to NATS JetStream\n")
            
            # Send startup notification
            streams_status = []
            for stream_name, monitor_config in self.monitors.items():
                restart_info = ""
                if monitor_config.restart_action == RestartAction.RESTART_PROCESS:
                    restart_info = f"\n  └─ Auto-restart: {monitor_config.supervisor_process_name}"
                
                streams_status.append(
                    f"📊 <code>{stream_name}</code>\n"
                    f"  └─ Pattern: {monitor_config.subject_pattern}\n"
                    f"  └─ Threshold: {monitor_config.stale_threshold_seconds}s"
                    f"{restart_info}"
                )
            
            startup_msg = f"""🚀 <b>PER-SUBJECT MONITOR STARTED</b>

🖥️ <b>NATS Server:</b> <code>{self.nats_url}</code>

<b>Monitoring Streams:</b>
{chr(10).join(streams_status)}

🎯 <b>Mode:</b> Per-subject tracking with auto-restart
Monitoring individual subjects and auto-restarting processes when needed!"""
            
            self.send_telegram_alert(startup_msg)
            
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
    # SUPERVISOR_URL = "http://localhost:9001/RPC2"
    SUPERVISOR_URL = "http://192.168.193.147:9001/RPC2" #zerotier ip address
    SUPERVISOR_USERNAME = "admin"
    SUPERVISOR_PASSWORD = "admin123"
    
    print("🌊 Per-Subject JetStream Monitor with Auto-Restart")
    print("="*80)
    
    # Create monitor instance
    monitor = PerSubjectJetStreamMonitor(
        nats_url=NATS_URL,
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID,
        supervisor_url=SUPERVISOR_URL,
        supervisor_username=SUPERVISOR_USERNAME,
        supervisor_password=SUPERVISOR_PASSWORD
    )
    
    # Add streams to monitor with auto-restart configuration
    monitor.add_stream_monitor(
        stream_name="binance_depth",
        subject_pattern="binance.depth.*",
        stale_threshold_seconds=60,
        supervisor_process_name="binance_pipelines:binance_depth",  # Name in supervisord
        restart_action=RestartAction.RESTART_PROCESS,
        max_restarts_per_hour=3
    )
    
    monitor.add_stream_monitor(
        stream_name="binance_kline",
        subject_pattern="binance.kline.*",
        stale_threshold_seconds=60,
        supervisor_process_name="binance_pipelines:binance_kline",  # Name in supervisord
        restart_action=RestartAction.RESTART_PROCESS,
        max_restarts_per_hour=3
    )
    
    print("="*80 + "\n")
    
    # Start monitoring
    await monitor.start_monitoring()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")