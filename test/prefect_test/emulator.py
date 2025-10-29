"""
Jet Stream Emulator - Simulates continuous data generation
Writes counter to file every 5 seconds (append mode)
Run this in a separate terminal/screen session
"""
import time
from pathlib import Path
from datetime import datetime

# Configuration
OUTPUT_FILE = "/mnt/vol1/rsync_test/counter.txt"
WRITE_INTERVAL = 5  # seconds

def write_counter(counter: int):
    """Write counter to file in append mode"""
    try:
        with open(OUTPUT_FILE, 'a') as f:
            f.write(f"Counter_{counter}\n")
            f.flush()  # Ensure data is written immediately
        print(f"[{datetime.now().isoformat()}] Written: Counter_{counter}")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ERROR writing counter: {e}")

def main():
    # Ensure directory exists
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    
    print("Starting Jet Stream Emulator")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Write interval: {WRITE_INTERVAL} seconds")
    print("Press Ctrl+C to stop\n")
    
    counter = 1
    
    try:
        while True:
            write_counter(counter)
            counter += 1
            time.sleep(WRITE_INTERVAL)
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().isoformat()}] Emulator stopped gracefully")
        print(f"Total counters written: {counter - 1}")

if __name__ == "__main__":
    main()