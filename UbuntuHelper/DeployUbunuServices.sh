#!/bin/bash
# Deploy Ubuntu systemd services and timers

set -e  # Exit on error

LOG_DIR="$HOME/NBBCService_logs"

if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    echo "✅ Created folder: $LOG_DIR"
else
    echo "ℹ️ Folder already exists: $LOG_DIR"
fi


# Copy service and timer files to systemd directory
sudo cp *.service /etc/systemd/system/
sudo cp *.timer /etc/systemd/system/
echo "✅ All services and timers files deployed successfully."
# Reload systemd to recognize new units
sudo systemctl daemon-reload
echo "✅ Reloaded the Daemon Services."

# Enable and start all timers
for timer in ./*.timer; do
    unit=$(basename "$timer")
    sudo systemctl enable --now "$unit"
done

echo "✅ All services and timers deployed successfully."
# to Check if the Services have been deployed and listed correctly use follows.
# systemctl list-timers | grep mfbc
# systemctl list-timers | grep nbbc
# systemctl list-timers | grep vsparse
