#!/bin/bash
# Restart processes after configuration changes

echo "🔄 Restarting JetStream Monitor and Binance Processes"
echo "====================================================="
echo ""

# 1. Show current status
echo "1️⃣  Current Status:"
echo "   ----------------"
sudo supervisorctl status binance_pipelines:*
echo ""

# 2. Stop all processes cleanly
echo "2️⃣  Stopping all processes..."
echo "   --------------------------"
sudo supervisorctl stop binance_pipelines:jetstream_monitor
sleep 2
sudo supervisorctl stop binance_pipelines:binance_kline
sleep 1
sudo supervisorctl stop binance_pipelines:binance_depth
echo "   ✅ All processes stopped"
echo ""

# 3. Check they're all stopped
echo "3️⃣  Verifying processes are stopped:"
echo "   ----------------------------------"
sudo supervisorctl status binance_pipelines:*
echo ""

# 4. Start processes in order
echo "4️⃣  Starting processes in order..."
echo "   -------------------------------"
echo "   Starting binance_kline..."
sudo supervisorctl start binance_pipelines:binance_kline
sleep 2

echo "   Starting binance_depth..."
sudo supervisorctl start binance_pipelines:binance_depth
sleep 2

echo "   Starting jetstream_monitor..."
sudo supervisorctl start binance_pipelines:jetstream_monitor
sleep 3
echo ""

# 5. Check final status
echo "5️⃣  Final Status:"
echo "   -------------"
sudo supervisorctl status binance_pipelines:*
echo ""

# 6. Check monitor startup
echo "6️⃣  Monitor Startup Logs (last 30 lines):"
echo "   ---------------------------------------"
tail -30 /mnt/vol1/logs/jetstream_monitor.out.log | grep -A 1 "Connecting\|Connected\|Subscribed\|Added monitor" | sed 's/^/   /'
echo ""

# 7. Check for errors
echo "7️⃣  Checking for startup errors:"
echo "   -----------------------------"
if [ -f "/mnt/vol1/logs/jetstream_monitor.err.log" ]; then
    error_count=$(tail -20 /mnt/vol1/logs/jetstream_monitor.err.log | wc -l)
    if [ "$error_count" -gt 0 ]; then
        echo "   ⚠️  Recent errors found:"
        tail -20 /mnt/vol1/logs/jetstream_monitor.err.log | sed 's/^/   | /'
    else
        echo "   ✅ No recent errors"
    fi
else
    echo "   ✅ No error log"
fi
echo ""

# 8. Verify configuration
echo "8️⃣  Monitoring Configuration:"
echo "   --------------------------"
echo "   Checking which streams are being monitored..."
grep "Added monitor for stream" /mnt/vol1/logs/jetstream_monitor.out.log | tail -5 | sed 's/^/   /'
echo ""

# 9. Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Restart Complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Your configuration monitors:"
echo "  • binance_depth stream → restarts 'binance_depth' process"
echo "  • binance_kline stream → restarts 'binance_kline' process"
echo "  • binance_test stream → restarts 'binance_kline' process (30s threshold)"
echo ""
echo "To test with binance_test stream (faster - 30s):"
echo "  1. Watch logs: tail -f /mnt/vol1/logs/jetstream_monitor.out.log"
echo "  2. Stop process: sudo supervisorctl stop binance_pipelines:binance_kline"
echo "  3. Wait 30 seconds (binance_test threshold)"
echo "  4. Should auto-restart!"
echo ""
echo "To watch everything live:"
echo "  tail -f /mnt/vol1/logs/jetstream_monitor.out.log"