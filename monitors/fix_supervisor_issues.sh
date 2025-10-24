#!/bin/bash
# Fix Supervisor and Monitor Issues

echo "🔧 Fixing Supervisor Issues"
echo "============================"

# 1. Create the monitors directory if it doesn't exist
echo ""
echo "📁 Creating monitors directory..."
mkdir -p /mnt/vol1/quantrek_sandbox/monitors
chmod 755 /mnt/vol1/quantrek_sandbox/monitors
echo "✅ Directory created: /mnt/vol1/quantrek_sandbox/monitors"

# 2. Update the Supervisor config to remove directory directive
echo ""
echo "📋 Updating Supervisor config..."
sudo tee /etc/supervisord.d/binance_pipelines.ini > /dev/null <<EOF
# Binance data pipelines configuration

[program:binance_kline]
command=/mnt/vol1/.venv/bin/python /mnt/vol1/quantrek_sandbox/pipelines/binance/binance_kline_test.py
directory=/mnt/vol1/quantrek_sandbox/pipelines/binance
user=opc
autostart=true
autorestart=true
startretries=3
stderr_logfile=/mnt/vol1/logs/binance_kline_test.err.log
stdout_logfile=/mnt/vol1/logs/binance_kline_test.out.log
environment=PYTHONUNBUFFERED="1"

[program:binance_depth]
command=/mnt/vol1/.venv/bin/python /mnt/vol1/quantrek_sandbox/pipelines/binance/binance_depth_test.py
directory=/mnt/vol1/quantrek_sandbox/pipelines/binance
user=opc
autostart=true
autorestart=true
startretries=3
stderr_logfile=/mnt/vol1/logs/binance_depth_test.err.log
stdout_logfile=/mnt/vol1/logs/binance_depth_test.out.log
environment=PYTHONUNBUFFERED="1"

[program:jetstream_monitor]
command=/mnt/vol1/.venv/bin/python /mnt/vol1/quantrek_sandbox/monitors/jetstream_monitor_test.py
directory=/mnt/vol1
user=opc
autostart=true
autorestart=true
startretries=3
stderr_logfile=/mnt/vol1/logs/jetstream_monitor.err.log
stdout_logfile=/mnt/vol1/logs/jetstream_monitor.out.log
environment=PYTHONUNBUFFERED="1"

[group:binance_pipelines]
programs=binance_kline,binance_depth,jetstream_monitor
EOF

# 3. Stop all processes first to avoid consumer conflicts
echo ""
echo "⏹️ Stopping all processes..."
sudo supervisorctl stop binance_pipelines:*

# 4. Reload supervisor config
echo ""
echo "🔄 Reloading Supervisor configuration..."
sudo supervisorctl reread
sudo supervisorctl update

# 5. Start processes one by one
echo ""
echo "▶️ Starting processes..."
sudo supervisorctl start binance_pipelines:binance_kline
sleep 2
sudo supervisorctl start binance_pipelines:binance_depth
sleep 2
sudo supervisorctl start binance_pipelines:jetstream_monitor

# 6. Check status
echo ""
echo "📊 Process status:"
sudo supervisorctl status

# 7. Show recent monitor logs
echo ""
echo "📋 Recent monitor logs (last 15 lines):"
echo "--- STDOUT ---"
tail -15 /mnt/vol1/logs/jetstream_monitor.out.log 2>/dev/null || echo "No stdout log yet"
echo ""
echo "--- STDERR ---"
tail -15 /mnt/vol1/logs/jetstream_monitor.err.log 2>/dev/null || echo "No stderr log yet"

echo ""
echo "✅ Fix applied!"
echo ""
echo "To follow logs in real-time:"
echo "  sudo supervisorctl tail -f jetstream_monitor"