#!/bin/bash
# Wrapper script for systemd service — resolves dynamic Xwayland auth path
# Also ensures TWS is running before starting the monitor.
export DISPLAY=:0
export XDG_RUNTIME_DIR=/run/user/$(id -u)
export WAYLAND_DISPLAY=wayland-0

# Wait for graphical session to be fully ready (up to 90s)
echo "$(date): Waiting for graphical session..."
for i in $(seq 1 30); do
    XAUTH_FILE=$(find /run/user/$(id -u) -name '.mutter-Xwaylandauth.*' -type f 2>/dev/null | head -1)
    if [ -n "$XAUTH_FILE" ]; then
        echo "$(date): Xwayland auth found after $((i*3))s"
        break
    fi
    sleep 3
done

# Find the current Xwayland auth file (path changes on each login)
if [ -z "$XAUTH_FILE" ]; then
    XAUTH_FILE=$(find /run/user/$(id -u) -name '.mutter-Xwaylandauth.*' -type f 2>/dev/null | head -1)
fi
if [ -n "$XAUTH_FILE" ]; then
    export XAUTHORITY="$XAUTH_FILE"
else
    export XAUTHORITY="$HOME/.Xauthority"
fi

# ── Ensure TWS is running (via IBC auto-login) ──
IBC_SCRIPT="$HOME/ibc/twsstart.sh"
if ! pgrep -f "java.*jts" > /dev/null 2>&1; then
    echo "$(date): TWS not running — launching via IBC..."
    if [ -x "$IBC_SCRIPT" ]; then
        "$IBC_SCRIPT" -inline &
        # Wait up to 120s for API port
        for i in $(seq 1 40); do
            if ss -tlnp | grep -q 7497; then
                echo "$(date): TWS API port 7497 is ready"
                break
            fi
            sleep 3
        done
    else
        echo "$(date): WARNING: IBC script not found at $IBC_SCRIPT"
    fi
fi

cd /home/matan-shaar/trading-bot
exec /usr/bin/python -u monitor/screen_monitor.py
