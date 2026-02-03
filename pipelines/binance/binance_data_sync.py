"""
Binance Data Sync Flow - Production
Runs NATS dumper and syncs to remote targets (Ubuntu/WSL) with automatic failover
"""
import subprocess
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple
from prefect import flow, task

# =====================
# CONFIGURATION
# =====================
TELEGRAM_BOT_TOKEN = "8368353420:AAFwR5eXV8TkEFExBeqqjgkSdTZYdXzva_Q"
TELEGRAM_CHAT_ID = "7004807409"

# Dumper configuration
DUMPER_SCRIPT = "/mnt/vol1/quantrek/pipelines/binance/dumper_caller.py"
PYTHON_PATH = "/mnt/vol1/.venv/bin/python"
DUMPER_MAX_RETRIES = 5
DUMPER_BASE_DELAY = 240  # 4 minutes (in seconds)
DUMPER_TIMEOUT = 1200  # 20 minutes per attempt

# Paths
SOURCE_DIR = "/mnt/vol1/data"
STATE_FILE = "/mnt/vol1/data/.sync_state.json"

# Targets
UBUNTU_IP = "192.168.193.40"
UBUNTU_TARGET = "ubuntu:/media/me/ssdsata/project/data/"
WSL_IP = "192.168.193.10"
WSL_TARGET = "wsl:/mnt/d/project/data/"

# Rsync configuration
RSYNC_MAX_RETRIES = 5
RSYNC_RETRY_DELAY = 15  # seconds
RSYNC_TIMEOUT = 900  # 15 minutes

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
        "last_dumper_success": None,
        "total_runs": 0
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

def execute_rsync(source: str, target: str, timeout: int = 900) -> tuple[bool, Dict[str, Any]]:
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
            timeout=timeout + 60
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
            timeout=10
        )
        size = result.stdout.split()[0] if result.returncode == 0 else "unknown"
        
        result = subprocess.run(
            ["find", path, "-type", "f"],
            capture_output=True,
            text=True,
            timeout=10
        )
        file_count = len(result.stdout.strip().split('\n')) if result.returncode == 0 and result.stdout.strip() else 0
        
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

@task
def run_dumper_with_retry() -> Tuple[bool, Dict[str, Any]]:
    """
    Task 1: Run NATS dumper with exponential backoff retry
    
    Retry schedule:
    - Attempt 1: Immediate
    - Attempt 2: Wait 4 minutes (240s)
    - Attempt 3: Wait 8 minutes (480s)
    - Attempt 4: Wait 16 minutes (960s)
    - Attempt 5: Wait 32 minutes (1920s)
    
    Total max time: ~60 minutes if all retries fail
    """
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task 1: run_dumper_with_retry]"
    
    print(f"{log_prefix} START")
    print(f"{log_prefix} Dumper script: {DUMPER_SCRIPT}")
    print(f"{log_prefix} Max retries: {DUMPER_MAX_RETRIES}")
    print(f"{log_prefix} Base delay: {DUMPER_BASE_DELAY}s (exponential backoff)")
    print(f"{log_prefix} Timeout per attempt: {DUMPER_TIMEOUT}s (20 min)")
    
    result_info = {
        "attempts": 0,
        "total_duration": 0,
        "last_error": None,
        "output": None
    }
    
    start_time = time.time()
    
    for attempt in range(1, DUMPER_MAX_RETRIES + 1):
        result_info["attempts"] = attempt
        attempt_start = time.time()
        
        print(f"\n{log_prefix} ===== ATTEMPT {attempt}/{DUMPER_MAX_RETRIES} =====")
        
        try:
            # Run dumper_caller.py
            print(f"{log_prefix} Executing: {PYTHON_PATH} {DUMPER_SCRIPT}")
            result = subprocess.run(
                [PYTHON_PATH, DUMPER_SCRIPT],
                capture_output=True,
                text=True,
                timeout=DUMPER_TIMEOUT,
                cwd=Path(DUMPER_SCRIPT).parent
            )
            
            attempt_duration = time.time() - attempt_start
            print(f"{log_prefix} Attempt duration: {attempt_duration:.1f}s")
            print(f"{log_prefix} Exit code: {result.returncode}")
            
            # Log stdout (truncated)
            if result.stdout:
                stdout_preview = result.stdout[-500:] if len(result.stdout) > 500 else result.stdout
                print(f"{log_prefix} STDOUT (last 500 chars):\n{stdout_preview}")
            
            # Check for success
            if result.returncode == 0:
                result_info["total_duration"] = time.time() - start_time
                result_info["output"] = result.stdout
                print(f"{log_prefix} SUCCESS on attempt {attempt}")
                print(f"{log_prefix} Total time: {result_info['total_duration']:.1f}s")
                return True, result_info
            
            # Failure - check error type
            error_output = result.stderr + result.stdout
            result_info["last_error"] = error_output[-1000:] if len(error_output) > 1000 else error_output
            
            print(f"{log_prefix} STDERR:\n{result.stderr}")
            
            # Check if it's a NATS timeout (retryable)
            is_nats_timeout = (
                "nats: timeout" in error_output or 
                "TimeoutError" in error_output or
                "nats.errors.TimeoutError" in error_output
            )
            
            if is_nats_timeout:
                print(f"{log_prefix} Detected NATS timeout error (retryable)")
                
                if attempt < DUMPER_MAX_RETRIES:
                    # Calculate exponential backoff delay
                    delay = DUMPER_BASE_DELAY * (2 ** (attempt - 1))
                    print(f"{log_prefix} Will retry in {delay}s ({delay/60:.1f} minutes)")
                    print(f"{log_prefix} Waiting...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"{log_prefix} Max retries exhausted after NATS timeout")
                    result_info["total_duration"] = time.time() - start_time
                    return False, result_info
            else:
                # Non-retryable error
                print(f"{log_prefix} Non-retryable error detected")
                result_info["total_duration"] = time.time() - start_time
                return False, result_info
            
        except subprocess.TimeoutExpired:
            attempt_duration = time.time() - attempt_start
            print(f"{log_prefix} Dumper timeout after {attempt_duration:.1f}s (limit: {DUMPER_TIMEOUT}s)")
            result_info["last_error"] = f"Process timeout after {DUMPER_TIMEOUT}s"
            
            if attempt < DUMPER_MAX_RETRIES:
                delay = DUMPER_BASE_DELAY * (2 ** (attempt - 1))
                print(f"{log_prefix} Will retry in {delay}s ({delay/60:.1f} minutes)")
                time.sleep(delay)
                continue
            else:
                print(f"{log_prefix} Max retries exhausted after timeout")
                result_info["total_duration"] = time.time() - start_time
                return False, result_info
                
        except Exception as e:
            print(f"{log_prefix} Unexpected error: {e}")
            result_info["last_error"] = str(e)
            result_info["total_duration"] = time.time() - start_time
            return False, result_info
    
    # All retries exhausted
    result_info["total_duration"] = time.time() - start_time
    print(f"{log_prefix} FAILED after {DUMPER_MAX_RETRIES} attempts")
    print(f"{log_prefix} Total time: {result_info['total_duration']:.1f}s")
    return False, result_info

@task(retries=RSYNC_MAX_RETRIES, retry_delay_seconds=RSYNC_RETRY_DELAY)
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
    print(f"{log_prefix} Executing rsync to Ubuntu (timeout: {RSYNC_TIMEOUT}s = 15 min)...")
    success, stats = execute_rsync(SOURCE_DIR, UBUNTU_TARGET, RSYNC_TIMEOUT)
    
    if success:
        print(f"{log_prefix} Rsync stats: {stats.get('files_transferred', 0)} files, exit code: {stats['exit_code']}")
        print(f"{log_prefix} SUCCESS - Ubuntu target")
        return True, {"target": "ubuntu", "stats": stats, "source_info": source_info}
    else:
        print(f"{log_prefix} Rsync failed: exit code {stats.get('exit_code', -1)}")
        print(f"{log_prefix} Error: {stats.get('stderr', 'Unknown error')}")
        raise Exception(f"Rsync to Ubuntu failed: {stats.get('stderr', 'Unknown error')}")

@task(retries=RSYNC_MAX_RETRIES, retry_delay_seconds=RSYNC_RETRY_DELAY)
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
    print(f"{log_prefix} Executing rsync to WSL (timeout: {RSYNC_TIMEOUT}s = 15 min)...")
    success, stats = execute_rsync(SOURCE_DIR, WSL_TARGET, RSYNC_TIMEOUT)
    
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
        f"⚠️ Will retry on next scheduled run (tomorrow)"
    )
    send_telegram_message(message)
    print(f"{log_prefix} Telegram alert sent (failure)")
    
    return False

@task
def update_sync_state(dumper_success: bool, sync_success: Optional[bool], dumper_info: Dict[str, Any]):
    """Update state file based on task results"""
    log_prefix = f"[{datetime.now(timezone.utc).isoformat()}] [Task: update_sync_state]"
    
    print(f"{log_prefix} START")
    
    state = load_state()
    now = datetime.now(timezone.utc).isoformat()
    state["total_runs"] = state.get("total_runs", 0) + 1
    
    if not dumper_success:
        # Dumper failed - no sync attempted
        print(f"{log_prefix} Dumper failed - no sync attempted")
        state["pending_sync"] = True
        state["last_failed_sync"] = now
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
        state["total_failures"] = state.get("total_failures", 0) + 1
        
        if state["consecutive_failures"] == 1:
            state["first_failure_timestamp"] = now
        
        save_state(state)
        print(f"{log_prefix} State updated: DUMPER FAILED (consecutive: {state['consecutive_failures']})")
        
        # Send escalated alert for consecutive dumper failures
        if state["consecutive_failures"] >= CRITICAL_THRESHOLD:
            emoji = "🚨" if state["consecutive_failures"] < EMERGENCY_THRESHOLD else "🆘"
            level = "CRITICAL" if state["consecutive_failures"] < EMERGENCY_THRESHOLD else "EMERGENCY"
            
            message = (
                f"{emoji} <b>{level}: Dumper Failed for {state['consecutive_failures']} Days</b>\n\n"
                f"NATS dumper has failed {state['consecutive_failures']} consecutive times.\n\n"
                f"First failure: {state['first_failure_timestamp']}\n"
                f"Latest failure: {now}\n"
                f"Attempts in last run: {dumper_info.get('attempts', 0)}\n"
                f"Total duration: {dumper_info.get('total_duration', 0):.1f}s\n\n"
                f"Last error:\n{dumper_info.get('last_error', 'Unknown')[:500]}\n\n"
                f"<b>Action required: Check NATS server and dumper script!</b>"
            )
            send_telegram_message(message)
            print(f"{log_prefix} Escalated alert sent (consecutive dumper failures: {state['consecutive_failures']})")
        
        return
    
    # Dumper succeeded
    state["last_dumper_success"] = now
    print(f"{log_prefix} Dumper succeeded in {dumper_info.get('attempts', 0)} attempt(s)")
    
    if sync_success is None:
        # Sync was not attempted (shouldn't happen)
        print(f"{log_prefix} Sync not attempted - no state update")
        return
    
    if sync_success:
        # Success - clear failure state
        old_failures = state.get("consecutive_failures", 0)
        
        state["pending_sync"] = False
        state["last_successful_sync"] = now
        state["consecutive_failures"] = 0
        state["first_failure_timestamp"] = None
        
        save_state(state)
        print(f"{log_prefix} State updated: SUCCESS (cleared {old_failures} failures)")
        
        # Send recovery alert if there were previous failures
        if old_failures > 0:
            data_age = calculate_data_age(state.get("last_failed_sync"))
            source_info = get_directory_info(SOURCE_DIR)
            
            message = (
                f"✅ <b>SYNC RECOVERED - Backlog Cleared</b>\n\n"
                f"Successfully synced after {old_failures} previous failure(s).\n\n"
                f"Files synced: {source_info['file_count']} files\n"
                f"Data size: {source_info['size']}\n"
                f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"✓ All data now backed up"
            )
            send_telegram_message(message)
            print(f"{log_prefix} Recovery alert sent")
    else:
        # Sync failure (dumper succeeded but sync failed)
        state["pending_sync"] = True
        state["last_failed_sync"] = now
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
        state["total_failures"] = state.get("total_failures", 0) + 1
        
        if state["consecutive_failures"] == 1:
            state["first_failure_timestamp"] = now
        
        save_state(state)
        print(f"{log_prefix} State updated: SYNC FAILED (consecutive: {state['consecutive_failures']})")
        
        # Send escalated alert for consecutive sync failures
        if state["consecutive_failures"] >= CRITICAL_THRESHOLD:
            data_age = calculate_data_age(state.get("last_successful_sync"))
            source_info = get_directory_info(SOURCE_DIR)
            
            emoji = "🚨" if state["consecutive_failures"] < EMERGENCY_THRESHOLD else "🆘"
            level = "CRITICAL" if state["consecutive_failures"] < EMERGENCY_THRESHOLD else "EMERGENCY"
            
            message = (
                f"{emoji} <b>{level}: Sync Failed for {state['consecutive_failures']} Days</b>\n\n"
                f"Sync has failed {state['consecutive_failures']} consecutive times.\n\n"
                f"First failure: {state['first_failure_timestamp']}\n"
                f"Latest failure: {now}\n"
                f"Data age: {data_age:.1f} hours ⚠️\n\n"
                f"Unsynced data size: {source_info['size']}\n\n"
                f"<b>Action required: Check backup targets!</b>\n"
                f"- Ubuntu ({UBUNTU_IP}): Check connectivity\n"
                f"- WSL ({WSL_IP}): Check connectivity"
            )
            send_telegram_message(message)
            print(f"{log_prefix} Escalated alert sent (consecutive sync failures: {state['consecutive_failures']})")

# =====================
# MAIN FLOW
# =====================

@flow(name="binance-data-sync", log_prints=True)
def binance_data_sync_flow():
    """
    Main flow: Run NATS dumper and sync to remote targets with failover
    Schedule: Daily at 12:00 UTC or 13:00 UTC
    """
    print(f"\n{'='*80}")
    print("BINANCE DATA SYNC FLOW - START")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*80}\n")
    
    # Task 1: Run NATS dumper with exponential backoff retry
    dumper_success, dumper_info = run_dumper_with_retry()
    
    if not dumper_success:
        print(f"\nDumper failed after {dumper_info['attempts']} attempts")
        print(f"Total time spent: {dumper_info.get('total_duration', 0):.1f}s")
        
        # Send alert for dumper failure
        message = (
            f"❌ <b>FAILED: NATS Dumper failed after {dumper_info['attempts']} attempts</b>\n\n"
            f"Script: {DUMPER_SCRIPT}\n"
            f"Total duration: {dumper_info.get('total_duration', 0):.1f}s\n"
            f"Attempts: {dumper_info['attempts']}\n"
            f"Last error:\n{dumper_info.get('last_error', 'Unknown')[:500]}\n\n"
            f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            f"⚠️ Will retry tomorrow at scheduled time"
        )
        send_telegram_message(message)
        
        # Update state
        update_sync_state(False, None, dumper_info)
        
        print(f"\n{'='*80}")
        print("BINANCE DATA SYNC FLOW - FAILED (Dumper)")
        print(f"{'='*80}\n")
        return
    
    print(f"\nDumper succeeded in {dumper_info['attempts']} attempt(s)")
    print(f"Total time: {dumper_info.get('total_duration', 0):.1f}s")
    
    # Task 2: Sync data with failover
    sync_success = None
    try:
        sync_success = sync_data_with_failover()
    except Exception as e:
        print(f"\nTask 2 encountered unexpected error: {e}")
        sync_success = False
    
    # Update state
    update_sync_state(dumper_success, sync_success, dumper_info)
    
    # Final status
    print(f"\n{'='*80}")
    if sync_success:
        print("BINANCE DATA SYNC FLOW - SUCCESS")
    elif sync_success is False:
        print("BINANCE DATA SYNC FLOW - FAILED (Sync) - Will retry tomorrow")
    else:
        print("BINANCE DATA SYNC FLOW - PARTIAL (Dumper only)")
    print(f"{'='*80}\n")

# =====================
# DEPLOYMENT
# =====================

if __name__ == "__main__":
    # Run the flow
    binance_data_sync_flow()