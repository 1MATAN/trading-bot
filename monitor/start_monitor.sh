#!/bin/bash
# Wrapper script for systemd service — resolves dynamic Xwayland auth path
export DISPLAY=:0
export XDG_RUNTIME_DIR=/run/user/$(id -u)
export WAYLAND_DISPLAY=wayland-0

# Find the current Xwayland auth file (path changes on each login)
XAUTH_FILE=$(find /run/user/$(id -u) -name '.mutter-Xwaylandauth.*' -type f 2>/dev/null | head -1)
if [ -n "$XAUTH_FILE" ]; then
    export XAUTHORITY="$XAUTH_FILE"
else
    export XAUTHORITY="$HOME/.Xauthority"
fi

cd /home/matan-shaar/trading-bot
exec /usr/bin/python monitor/screen_monitor.py
