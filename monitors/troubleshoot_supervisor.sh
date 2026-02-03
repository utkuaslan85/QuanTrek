#!/bin/bash
# Troubleshoot and fix Supervisor setup

echo "🔍 Troubleshooting Supervisor Setup"
echo "===================================="

# 1. Check all process status
echo ""
echo "📊 Current process status:"
sudo supervisorctl status

# 2. Check if jetstream_monitor file exists
echo ""
echo "📁 Checking if monitor file exists:"
MONITOR_FILE="/mnt/vol1/quantrek/monitors/jetstream_monitor_test.py"
if [ -f "$MONITOR_FILE" ]; then
    echo "✅ Found: $MONITOR_FILE"
else
    echo "❌ NOT FOUND: $MONITOR_FILE"
    echo "Please create the monitor file first or update the path in config"
fi

# 3. Check recent logs for errors
echo ""
echo "📋 Recent error logs for jetstream_monitor:"
if [ -f "/mnt/vol1/logs/jetstream_monitor.err.log" ]; then
    echo "--- Last 20 lines of error log ---"
    tail -20 /mnt/vol1/logs/jetstream_monitor.err.log
else
    echo "No error log found yet"
fi

# 4. Check configuration
echo ""
echo "📋 Current binance_pipelines.ini config:"
cat /etc/supervisord.d/binance_pipelines.ini

# 5. Try to start the monitor manually
echo ""
echo "🔄 Attempting to start jetstream_monitor..."
sudo supervisorctl start jetstream_monitor

# 6. Wait a moment and check status again
sleep 2
echo ""
echo "📊 Updated process status:"
sudo supervisorctl status

# 7. Provide helpful commands
echo ""
echo "======================================"
echo "Helpful troubleshooting commands:"
echo "======================================"
echo ""
echo "View live logs:"
echo "  sudo supervisorctl tail -f jetstream_monitor"
echo "  sudo supervisorctl tail -f jetstream_monitor stderr"
echo ""
echo "Restart specific process:"
echo "  sudo supervisorctl restart jetstream_monitor"
echo "  sudo supervisorctl restart binance_kline"
echo ""
echo "Restart all:"
echo "  sudo supervisorctl restart all"
echo ""
echo "Check config validity:"
echo "  sudo supervisorctl reread"
echo "  sudo supervisorctl update"
echo ""
echo "Manual test (to see errors directly):"
echo "  /mnt/vol1/.venv/bin/python /mnt/vol1/quantrek/monitors/jetstream_monitor_test.py"