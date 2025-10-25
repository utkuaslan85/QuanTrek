#!/bin/bash
# Setup script for Supervisor on RHEL/CentOS/Rocky Linux

echo "🚀 Setting up Supervisor for Binance Pipelines (RHEL/CentOS)"
echo "============================================================="

# 1. Install Supervisor if not installed
echo "📦 Checking Supervisor installation..."
if ! command -v supervisord &> /dev/null; then
    echo "Installing Supervisor..."
    # sudo yum install -y supervisor
    # Or if using dnf:
    sudo dnf install -y supervisor
else
    echo "✅ Supervisor already installed"
fi

# 2. Check main config file location
MAIN_CONFIG="/etc/supervisord.conf"
if [ -f "$MAIN_CONFIG" ]; then
    echo "✅ Found main config at $MAIN_CONFIG"
else
    echo "❌ Main config not found at $MAIN_CONFIG"
    exit 1
fi

# 3. Enable Supervisor XML-RPC interface
echo ""
echo "🔧 Configuring Supervisor XML-RPC interface..."
sudo sed -i '/^\[inet_http_server\]/,/^$/d' $MAIN_CONFIG
sudo tee -a $MAIN_CONFIG > /dev/null <<'EOF'

# given ip:port is ip from zerotier vpn
# change to 'localhost:9001' if zerotier network is abandoned
[inet_http_server]
port=192.168.193.147:9001
username=admin
password=admin123
EOF

# 4. Ensure include directive exists
echo ""
echo "🔧 Checking include directive..."
if ! grep -q "files = /etc/supervisord.d/\*.ini" $MAIN_CONFIG; then
    echo "Adding include directive..."
    sudo tee -a $MAIN_CONFIG > /dev/null <<'EOF'

[include]
files = /etc/supervisord.d/*.ini
EOF
fi

# 5. Create log directory
echo ""
echo "📁 Creating log directories..."
sudo mkdir -p /mnt/vol1/logs
sudo chown -R $USER:$USER /mnt/vol1/logs

# 6. Create the configuration file for binance pipelines
echo ""
echo "📋 Creating Binance pipelines configuration..."
sudo tee /etc/supervisord.d/binance_pipelines.ini > /dev/null <<EOF
# Binance data pipelines configuration

[program:binance_kline]
command=/mnt/vol1/.venv/bin/python /mnt/vol1/quantrek_sandbox/pipelines/binance/binance_kline.py
directory=/mnt/vol1/quantrek_sandbox/pipelines/binance
user=$USER
autostart=true
autorestart=true
startretries=3
stderr_logfile=/mnt/vol1/logs/binance_kline_sup.err.log
stdout_logfile=/mnt/vol1/logs/binance_kline_sup.out.log
environment=PYTHONUNBUFFERED="1"

[program:binance_depth]
command=/mnt/vol1/.venv/bin/python /mnt/vol1/quantrek_sandbox/pipelines/binance/binance_depth.py
directory=/mnt/vol1/quantrek_sandbox/pipelines/binance
user=$USER
autostart=true
autorestart=true
startretries=3
stderr_logfile=/mnt/vol1/logs/binance_depth_sup.err.log
stdout_logfile=/mnt/vol1/logs/binance_depth_sup.out.log
environment=PYTHONUNBUFFERED="1"

[program:jetstream_monitor]
command=/mnt/vol1/.venv/bin/python /mnt/vol1/quantrek_sandbox/monitors/jetstream_monitor.py
directory=/mnt/vol1/quantrek_sandbox/monitors
user=$USER
autostart=true
autorestart=true
startretries=3
stderr_logfile=/mnt/vol1/logs/jetstream_monitor_sup.err.log
stdout_logfile=/mnt/vol1/logs/jetstream_monitor_sup.out.log
environment=PYTHONUNBUFFERED="1"

[group:binance_pipelines]
programs=binance_kline,binance_depth,jetstream_monitor
EOF

# 7. Enable and start supervisord service
echo ""
echo "🔄 Enabling and starting Supervisor service..."
sudo systemctl enable supervisord
sudo systemctl restart supervisord

# Give it a moment to start
sleep 2

# 8. Reload configuration
echo ""
echo "🔄 Reloading Supervisor configuration..."
sudo supervisorctl reread
sudo supervisorctl update

# 9. Check status
echo ""
echo "📊 Current process status:"
sudo supervisorctl status

echo ""
echo "✅ Setup complete!"
echo ""
echo "Configuration file: /etc/supervisord.d/binance_pipelines.ini"
echo "Main config: /etc/supervisord.conf"
echo ""
echo "Useful commands:"
echo "  sudo supervisorctl status                    # View all processes"
echo "  sudo supervisorctl restart binance_kline     # Restart specific process"
echo "  sudo supervisorctl restart binance_pipelines:*  # Restart all in group"
echo "  sudo supervisorctl tail -f binance_kline     # Follow logs"
echo "  sudo supervisorctl stop binance_kline        # Stop process"
echo "  sudo supervisorctl start binance_kline       # Start process"
echo ""
echo "Web interface: http://localhost:9001"
echo "  Username: admin"
echo "  Password: admin123"
echo ""
echo "Check service status:"
echo "  sudo systemctl status supervisord"