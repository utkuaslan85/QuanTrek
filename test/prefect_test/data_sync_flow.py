"""
Prefect Data Sync Flow with Failover
Reads counter file and syncs to remote targets (Ubuntu/WSL) with automatic failover
"""
import subprocess
import json
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from prefect import flow, task


# =====================
# CONFIGURATION
# =====================
TELEGRAM_BOT_TOKEN = "8368353420:AAFwR5eXV8TkEFExBeqqjgkSdTZYdXzva_Q"
TELEGRAM_CHAT_ID = "7004807409"

# Paths
COUNTER_FILE = "/mnt/vol1/rsync_test/counter.txt"
STATE_FILE = "/mnt/vol1/rsync_test/.sync_state.json"
SOURCE_DIR = "/mnt/vol1/rsync_test/"

# Targets
UBUNTU_IP = "192.168.193.40"
UBUNTU_TARGET = "ubuntu:/media/me/ssdsata/project/rsync_test/"
WSL_IP = "192.168.193.10"
WSL_TARGET = "wsl:/mnt/d/project/rsync_test/"

# Retry configuration
TASK1_MAX_RETRIES = 5
TASK1_RETRY_DELAY = 15  # seconds
TASK2_MAX_RETRIES = 5
TASK2_RETRY_DELAY = 15  # seconds

# Alert thresholds
WARNING_THRESHOLD = 1
CRITICAL_THRESHOLD = 3
EMERGENCY_THRESHOLD = 7

# =====================
# UTILITY FUNCTIONS
# =====================

def send_telegram_message(message: str) -> bool:
    """Send message via Telegram bot"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")
        return False

def load_state() -> Dict[str, Any]:
    """Load sync state from file"""
    default_state = {
        "pending_sync": False,
        "last_successful_sync": None,
        "last_failed_sync": None,
        "consecutive_failures": 0,
        "total_failures": 0,
        "first_failure_timestamp": None,
        "last_counter_value": None
    }
    
    try:
        if Path(STATE_FILE).exists():
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading state file: {e}")
    
    return default_state

def save_state(state: Dict[str, Any]) -> bool:
    """Save sync state to file"""
    try:
        Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving state file: {e}")
        return False

def ping_host(ip: str, count: int = 3, timeout: int = 2) -> tuple[bool, Optional[str]]:
    """Ping a host and return success status and latency"""
    try:
        result = subprocess.run(
            ["ping", "-c", str(count), "-W", str(timeout), ip],
            capture_output=True,
            text=True,
            timeout=timeout * count + 2
        )
        
        if result.returncode == 0:
            # Extract average latency from ping output
            for line in result.stdout.split('\n'):
                if 'avg' in line or 'min/avg/max' in line:
                    parts = line.split('/')
                    if len(parts) >= 5:
                        avg_latency = parts[4]
                        return True, f"{avg_latency}ms"
            return True, "success"
        return False, "timeout"
    except Exception as e:
        return False, str(e)

def execute_rsync(source: str, target: str, timeout: int = 60) -> tuple[bool, Dict[str, Any]]:
    """Execute rsync command and parse results"""
    cmd = [
        "rsync",
        "-avz",
        "--progress",
        "--stats",
        f"--timeout={timeout}",
        source,
        target
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 10
        )
        
        stats = {
            "exit_code": result.returncode,
            "command": ' '.join(cmd),
            "stdout": result.stdout,
            "stderr": result.stderr
        }
        
        # Parse rsync stats
        if result.returncode == 0:
            output = result.stdout + result.stderr
            stats["files_transferred"] = 0
            stats["total_size"] = "0"
            stats["speedup"] = "1.00"
            
            for line in output.split('\n'):
                if "files transferred" in line.lower():
                    try:
                        stats["files_transferred"] = int(line.split(':')[1].strip().split()[0])
                    except:
                        pass
                elif "total size" in line.lower():
                    try:
                        stats["total_size"] = line.split(':')[1].strip()
                    except:
                        pass
                elif "speedup is" in line.lower():
                    try:
                        stats["speedup"] = line.split()[-1]
                    except:
                        pass
        
        return result.returncode == 0, stats
    except subprocess.TimeoutExpired:
        return False, {"exit_code": -1, "error": "Timeout", "command": ' '.join(cmd)}
    except Exception as e:
        return False, {"exit_code": -1, "error": str(e), "command": ' '.join(cmd)}

def get_directory_info(path: str) -> Dict[str, Any]:
    """Get directory size and file count"""
    try:
        result = subprocess.run(
            ["du", "-sh", path],
            capture_output=True,
            text=True,
            timeout=5
        )
        size = result.stdout.split()[0] if result.returncode == 0 else "unknown"
        
        result = subprocess.run(
            ["find", path, "-type", "f"],
            capture_output=True,
            text=True,
            timeout=5
        )
        file_count = len(result.stdout.strip().split('\n')) if result.returncode == 0 else 0
        
        return {"size": size, "file_count": file_count}
    except Exception as e:
        return {"size": "unknown", "file_count": 0, "error": str(e)}

def calculate_data_age(last_success: Optional[str]) -> Optional[float]:
    """Calculate hours since last successful sync"""
    if not last_success:
        return None
    try:
        last_dt = datetime.fromisoformat(last_success.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        return (now - last_dt).total_seconds() / 3600
    except:
        return None

# =====================
# PREFECT TASKS
# =====================

@task(retries=TASK1_MAX_RETRIES, retry_delay_seconds=TASK1_RETRY_DELAY)
def read_counter_file() -> Optional[Dict[str, Any]]:
    """
    Task 1: Read counter file and validate contents
    Returns file info if successful, None if failed
    """
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task 1: read_counter_file]"
    
    print(f"{log_prefix} START")
    print(f"{log_prefix} Reading file: {COUNTER_FILE}")
    
    try:
        # Check if file exists
        if not Path(COUNTER_FILE).exists():
            print(f"{log_prefix} File does not exist yet (emulator may be starting)")
            raise FileNotFoundError(f"Counter file not found: {COUNTER_FILE}")
        
        # Read file
        with open(COUNTER_FILE, 'r') as f:
            content = f.read()
        
        # Validate content
        if not content.strip():
            print(f"{log_prefix} File is empty")
            raise ValueError("Counter file is empty")
        
        # Parse content
        lines = content.strip().split('\n')
        line_count = len(lines)
        last_line = lines[-1].strip()
        
        # Extract counter value
        counter_value = None
        if last_line.startswith("Counter_"):
            try:
                counter_value = int(last_line.split('_')[1])
            except:
                pass
        
        # Get file info
        file_size = Path(COUNTER_FILE).stat().st_size
        
        result = {
            "file_path": COUNTER_FILE,
            "file_size": file_size,
            "line_count": line_count,
            "last_line": last_line,
            "counter_value": counter_value,
            "content_preview": content[:200] if len(content) > 200 else content
        }
        
        print(f"{log_prefix} File size: {file_size} bytes")
        print(f"{log_prefix} Lines in file: {line_count}")
        print(f"{log_prefix} Latest counter: {counter_value}")
        print(f"{log_prefix} SUCCESS (duration: -)")
        
        return result
        
    except Exception as e:
        print(f"{log_prefix} ERROR: {e}")
        raise

@task
def check_pending_sync() -> Dict[str, Any]:
    """Check if there's a pending sync from previous failure"""
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task: check_pending_sync]"
    
    print(f"{log_prefix} START")
    state = load_state()
    
    pending = state.get("pending_sync", False)
    consecutive = state.get("consecutive_failures", 0)
    
    print(f"{log_prefix} Pending sync: {pending}")
    print(f"{log_prefix} Consecutive failures: {consecutive}")
    
    if pending:
        data_age = calculate_data_age(state.get("last_successful_sync"))
        if data_age:
            print(f"{log_prefix} Data age: {data_age:.1f} hours")
    
    print(f"{log_prefix} SUCCESS")
    return state

@task(retries=TASK2_MAX_RETRIES, retry_delay_seconds=TASK2_RETRY_DELAY)
def sync_to_ubuntu() -> tuple[bool, Dict[str, Any]]:
    """Attempt to sync to Ubuntu target"""
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task 2A: sync_to_ubuntu]"
    
    print(f"{log_prefix} START")
    print(f"{log_prefix} Target: {UBUNTU_TARGET}")
    
    # Get source info
    source_info = get_directory_info(SOURCE_DIR)
    print(f"{log_prefix} Source size: {source_info['size']} ({source_info['file_count']} files)")
    
    # Ping check
    print(f"{log_prefix} Pinging Ubuntu ({UBUNTU_IP})...")
    is_alive, latency = ping_host(UBUNTU_IP)
    
    if not is_alive:
        print(f"{log_prefix} Ubuntu ping failed: {latency}")
        return False, {"error": f"Ping failed: {latency}", "target": "ubuntu"}
    
    print(f"{log_prefix} Ubuntu ping: {latency} (alive)")
    
    # Execute rsync
    print(f"{log_prefix} Executing rsync to Ubuntu...")
    success, stats = execute_rsync(SOURCE_DIR, UBUNTU_TARGET)
    
    if success:
        print(f"{log_prefix} Rsync stats: {stats.get('files_transferred', 0)} files, exit code: {stats['exit_code']}")
        print(f"{log_prefix} SUCCESS - Ubuntu target")
        return True, {"target": "ubuntu", "stats": stats, "source_info": source_info}
    else:
        print(f"{log_prefix} Rsync failed: exit code {stats.get('exit_code', -1)}")
        print(f"{log_prefix} Error: {stats.get('stderr', 'Unknown error')}")
        raise Exception(f"Rsync to Ubuntu failed: {stats.get('stderr', 'Unknown error')}")

@task(retries=TASK2_MAX_RETRIES, retry_delay_seconds=TASK2_RETRY_DELAY)
def sync_to_wsl() -> tuple[bool, Dict[str, Any]]:
    """Attempt to sync to WSL target (fallback)"""
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task 2B: sync_to_wsl]"
    
    print(f"{log_prefix} START (FALLBACK)")
    print(f"{log_prefix} Target: {WSL_TARGET}")
    
    # Get source info
    source_info = get_directory_info(SOURCE_DIR)
    print(f"{log_prefix} Source size: {source_info['size']} ({source_info['file_count']} files)")
    
    # Ping check
    print(f"{log_prefix} Pinging WSL ({WSL_IP})...")
    is_alive, latency = ping_host(WSL_IP)
    
    if not is_alive:
        print(f"{log_prefix} WSL ping failed: {latency}")
        return False, {"error": f"Ping failed: {latency}", "target": "wsl"}
    
    print(f"{log_prefix} WSL ping: {latency} (alive)")
    
    # Execute rsync
    print(f"{log_prefix} Executing rsync to WSL...")
    success, stats = execute_rsync(SOURCE_DIR, WSL_TARGET)
    
    if success:
        print(f"{log_prefix} Rsync stats: {stats.get('files_transferred', 0)} files, exit code: {stats['exit_code']}")
        print(f"{log_prefix} SUCCESS - WSL target (fallback)")
        return True, {"target": "wsl", "stats": stats, "source_info": source_info}
    else:
        print(f"{log_prefix} Rsync failed: exit code {stats.get('exit_code', -1)}")
        print(f"{log_prefix} Error: {stats.get('stderr', 'Unknown error')}")
        raise Exception(f"Rsync to WSL failed: {stats.get('stderr', 'Unknown error')}")

@task
def sync_data_with_failover() -> bool:
    """
    Task 2: Sync data with automatic failover
    Tries Ubuntu first, falls back to WSL if Ubuntu fails
    """
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task 2: sync_data_with_failover]"
    
    print(f"{log_prefix} START")
    
    ubuntu_error = None
    wsl_error = None
    
    # Try Ubuntu first
    try:
        success, result = sync_to_ubuntu()
        if success:
            # Send success alert
            message = (
                f"✅ <b>SUCCESS: Data synced to Ubuntu</b>\n\n"
                f"Target: {UBUNTU_TARGET}\n"
                f"Files: {result['source_info']['file_count']} files\n"
                f"Size: {result['source_info']['size']}\n"
                f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            send_telegram_message(message)
            print(f"{log_prefix} Telegram alert sent (Ubuntu success)")
            return True
    except Exception as e:
        ubuntu_error = str(e)
        print(f"{log_prefix} Ubuntu failed after retries: {ubuntu_error}")
        print(f"{log_prefix} Attempting WSL failover...")
    
    # Try WSL fallback
    try:
        success, result = sync_to_wsl()
        if success:
            # Send fallback success alert
            message = (
                f"⚠️ <b>SUCCESS (Fallback): Data synced to WSL</b>\n\n"
                f"Target: {WSL_TARGET}\n"
                f"Reason: Ubuntu ({UBUNTU_IP}) was unreachable/failed\n"
                f"Files: {result['source_info']['file_count']} files\n"
                f"Size: {result['source_info']['size']}\n"
                f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"Ubuntu error: {ubuntu_error}"
            )
            send_telegram_message(message)
            print(f"{log_prefix} Telegram alert sent (WSL fallback success)")
            return True
    except Exception as e:
        wsl_error = str(e)
        print(f"{log_prefix} WSL failed after retries: {wsl_error}")
    
    # Both failed
    print(f"{log_prefix} FAILED - Both targets failed")
    
    # Get source info for alert
    source_info = get_directory_info(SOURCE_DIR)
    
    # Send failure alert
    message = (
        f"🚨 <b>CRITICAL FAILURE: Data sync failed to all targets</b>\n\n"
        f"<b>Ubuntu ({UBUNTU_IP}):</b>\n"
        f"Target: {UBUNTU_TARGET}\n"
        f"Status: Failed\n"
        f"Error: {ubuntu_error}\n\n"
        f"<b>WSL ({WSL_IP}):</b>\n"
        f"Target: {WSL_TARGET}\n"
        f"Status: Failed\n"
        f"Error: {wsl_error}\n\n"
        f"Source: {SOURCE_DIR}\n"
        f"Pending data size: {source_info['size']}\n"
        f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
        f"⚠️ Will retry on next scheduled run"
    )
    send_telegram_message(message)
    print(f"{log_prefix} Telegram alert sent (failure)")
    
    return False

@task
def update_sync_state(task1_success: bool, task2_success: Optional[bool], counter_value: Optional[int]):
    """Update state file based on task results"""
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task: update_sync_state]"
    
    print(f"{log_prefix} START")
    
    state = load_state()
    now = datetime.now(timezone.utc).isoformat()
    
    if not task1_success:
        # Task 1 failed - don't update state
        print(f"{log_prefix} Task 1 failed - no state update")
        return
    
    if task2_success is None:
        # Task 2 was not attempted
        print(f"{log_prefix} Task 2 not attempted - no state update")
        return
    
    if task2_success:
        # Success - clear failure state
        old_failures = state.get("consecutive_failures", 0)
        
        state["pending_sync"] = False
        state["last_successful_sync"] = now
        state["consecutive_failures"] = 0
        state["first_failure_timestamp"] = None
        state["last_counter_value"] = counter_value
        
        save_state(state)
        print(f"{log_prefix} State updated: SUCCESS (cleared {old_failures} failures)")
        
        # Send recovery alert if there were previous failures
        if old_failures > 0:
            data_age = calculate_data_age(state.get("last_failed_sync"))
            source_info = get_directory_info(SOURCE_DIR)
            
            message = (
                f"✅ <b>SYNC RECOVERED - Backlog Cleared</b>\n\n"
                f"Task 2 successfully synced after {old_failures} previous failure(s).\n\n"
                f"Files synced: {source_info['file_count']} files\n"
                f"Data size: {source_info['size']}\n"
                f"Data age: {data_age:.1f} hours\n"
                f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"✓ All data now backed up"
            )
            send_telegram_message(message)
            print(f"{log_prefix} Recovery alert sent")
    else:
        # Failure - update failure state
        state["pending_sync"] = True
        state["last_failed_sync"] = now
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
        state["total_failures"] = state.get("total_failures", 0) + 1
        state["last_counter_value"] = counter_value
        
        if state["consecutive_failures"] == 1:
            state["first_failure_timestamp"] = now
        
        save_state(state)
        print(f"{log_prefix} State updated: FAILED (consecutive: {state['consecutive_failures']})")
        
        # Send escalated alert for consecutive failures
        if state["consecutive_failures"] >= CRITICAL_THRESHOLD:
            data_age = calculate_data_age(state.get("last_successful_sync"))
            source_info = get_directory_info(SOURCE_DIR)
            
            emoji = "🚨" if state["consecutive_failures"] < EMERGENCY_THRESHOLD else "🆘"
            level = "CRITICAL" if state["consecutive_failures"] < EMERGENCY_THRESHOLD else "EMERGENCY"
            
            message = (
                f"{emoji} <b>{level}: Sync Failed for {state['consecutive_failures']} Runs</b>\n\n"
                f"Task 2 has failed {state['consecutive_failures']} consecutive times.\n\n"
                f"First failure: {state['first_failure_timestamp']}\n"
                f"Latest failure: {now}\n"
                f"Data age: {data_age:.1f} hours ⚠️\n\n"
                f"Unsynced data size: {source_info['size']}\n\n"
                f"<b>Action required: Check backup targets!</b>\n"
                f"- Ubuntu ({UBUNTU_IP}): Down for {state['consecutive_failures']} runs\n"
                f"- WSL ({WSL_IP}): Down for {state['consecutive_failures']} runs"
            )
            send_telegram_message(message)
            print(f"{log_prefix} Escalated alert sent (consecutive failures: {state['consecutive_failures']})")

# =====================
# MAIN FLOW
# =====================

@flow(name="data-sync-flow", log_prints=True)
def data_sync_flow():
    """
    Main flow: Read counter file and sync to remote targets with failover
    Automatically retries failed syncs on next scheduled run
    """
    print(f"\n{'='*80}")
    print("DATA SYNC FLOW - START")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*80}\n")
    
    # Task 1: Read counter file
    try:
        file_info = read_counter_file()
        task1_success = file_info is not None
        counter_value = file_info.get("counter_value") if file_info else None
    except Exception as e:
        print(f"\nTask 1 failed after all retries: {e}")
        
        # Send alert for Task 1 failure
        message = (
            f"❌ <b>FAILED: Cannot read counter file after {TASK1_MAX_RETRIES} attempts</b>\n\n"
            f"File: {COUNTER_FILE}\n"
            f"Last error: {str(e)}\n"
            f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            f"Flow terminated. Will retry on next scheduled run."
        )
        send_telegram_message(message)
        
        print(f"\n{'='*80}")
        print("DATA SYNC FLOW - FAILED (Task 1)")
        print(f"{'='*80}\n")
        return
    
    # Task 2: Sync data with failover
    task2_success = None
    if task1_success:
        try:
            task2_success = sync_data_with_failover()
        except Exception as e:
            print(f"\nTask 2 encountered unexpected error: {e}")
            task2_success = False
    
    # Update state
    update_sync_state(task1_success, task2_success, counter_value)
    
    # Final status
    print(f"\n{'='*80}")
    if task2_success:
        print("DATA SYNC FLOW - SUCCESS")
    elif task2_success is False:
        print("DATA SYNC FLOW - FAILED (Task 2) - Will retry on next run")
    else:
        print("DATA SYNC FLOW - PARTIAL (Task 1 only)")
    print(f"{'='*80}\n")

# =====================
# DEPLOYMENT
# =====================

if __name__ == "__main__":
    # Run the flow
    data_sync_flow()
    
    
# if __name__ == "__main__":
#     data_sync_flow.serve(name="rsync_test", cron="* * * * *")