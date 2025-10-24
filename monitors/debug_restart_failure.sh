#!/bin/bash
# Debug why Supervisor restart is failing

echo "🔍 Debugging Supervisor Restart Failure"
echo "========================================"
echo ""

# 1. Check Supervisor XML-RPC is accessible
echo "1️⃣  Testing Supervisor XML-RPC Connection:"
echo "   ----------------------------------------"
curl -u admin:admin123 http://localhost:9001/RPC2 2>&1 | head -5
echo ""

# 2. Check if web interface is accessible
echo "2️⃣  Testing Supervisor Web Interface:"
echo "   -----------------------------------"
response=$(curl -s -u admin:admin123 -o /dev/null -w "%{http_code}" http://localhost:9001/ 2>&1)
if [ "$response" == "200" ]; then
    echo "   ✅ Web interface accessible (HTTP $response)"
else
    echo "   ❌ Web interface not accessible (HTTP $response)"
fi
echo ""

# 3. Check supervisor main config
echo "3️⃣  Checking Supervisor Configuration:"
echo "   ------------------------------------"
echo "   Looking for inet_http_server section..."
if grep -q "\[inet_http_server\]" /etc/supervisord.conf; then
    echo "   ✅ Found [inet_http_server] section:"
    grep -A 5 "\[inet_http_server\]" /etc/supervisord.conf | grep -v "^;" | sed 's/^/      /'
else
    echo "   ❌ [inet_http_server] section not found!"
fi
echo ""

# 4. Test XML-RPC with Python
echo "4️⃣  Testing XML-RPC with Python:"
echo "   ------------------------------"
/mnt/vol1/.venv/bin/python3 << 'EOF'
import xmlrpc.client
import sys

# Test without auth first
print("   Testing without auth:")
try:
    server = xmlrpc.client.ServerProxy('http://localhost:9001/RPC2')
    state = server.supervisor.getState()
    print(f"   ✅ Connected without auth: {state}")
except Exception as e:
    print(f"   ❌ Failed without auth: {e}")

print()

# Test with auth in URL
print("   Testing with auth in URL:")
try:
    server = xmlrpc.client.ServerProxy('http://admin:admin123@localhost:9001/RPC2')
    state = server.supervisor.getState()
    print(f"   ✅ Connected with auth: {state}")
    
    # Try to get process info
    print()
    print("   Getting process info for binance_kline:")
    info = server.supervisor.getProcessInfo('binance_pipelines:binance_kline')
    print(f"      Name: {info['name']}")
    print(f"      State: {info['statename']}")
    print(f"      PID: {info['pid']}")
    
    # Try a test restart
    print()
    print("   Testing restart capability:")
    try:
        # Just check if we can stop and start
        current_state = info['statename']
        print(f"      Current state: {current_state}")
        
        if current_state == 'RUNNING':
            print("      ✅ Process is running - restart should work")
        else:
            print(f"      ⚠️  Process is {current_state} - may affect restart")
            
    except Exception as e:
        print(f"      ❌ Error checking restart capability: {e}")
        
except Exception as e:
    print(f"   ❌ Failed with auth: {e}")
    import traceback
    traceback.print_exc()
EOF
echo ""

# 5. Check monitor error logs
echo "5️⃣  Monitor Error Logs (last 20 lines):"
echo "   -------------------------------------"
if [ -f "/mnt/vol1/logs/jetstream_monitor.err.log" ]; then
    tail -20 /mnt/vol1/logs/jetstream_monitor.err.log | sed 's/^/   /'
else
    echo "   No error log found"
fi
echo ""

# 6. Check monitor output for restart attempts
echo "6️⃣  Monitor Restart Attempts:"
echo "   --------------------------"
grep -A 5 "Attempting to restart\|Failed to restart\|Successfully restarted" /mnt/vol1/logs/jetstream_monitor.out.log | tail -30 | sed 's/^/   /'
echo ""

# 7. Check if supervisorctl works manually
echo "7️⃣  Testing Manual Restart:"
echo "   ------------------------"
echo "   Current status:"
sudo supervisorctl status binance_pipelines:binance_kline | sed 's/^/      /'
echo ""
echo "   Attempting manual restart via supervisorctl..."
sudo supervisorctl restart binance_pipelines:binance_kline
sleep 2
echo ""
echo "   Status after restart:"
sudo supervisorctl status binance_pipelines:binance_kline | sed 's/^/      /'
echo ""

# 8. Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Common issues and fixes:"
echo ""
echo "Issue 1: Wrong process name"
echo "  Check: Should be 'binance_pipelines:binance_kline' not 'binance_kline'"
echo "  Fix: Update monitor code to use full name with group prefix"
echo ""
echo "Issue 2: Authentication failing"
echo "  Check: Supervisor XML-RPC requires correct username/password"
echo "  Fix: Verify credentials in monitor match supervisord.conf"
echo ""
echo "Issue 3: Permissions"
echo "  Check: Monitor running as user 'opc' needs permission to restart"
echo "  Fix: May need to add opc to supervisor group or adjust permissions"
echo ""
echo "Next steps:"
echo "  1. Check the output above for specific errors"
echo "  2. If you see 'no such process', the process name is wrong"
echo "  3. If you see '401 Unauthorized', auth credentials are wrong"
echo "  4. If you see 'permission denied', it's a permissions issue"