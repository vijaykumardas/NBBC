#!/bin/bash
# Deploy Ubuntu systemd services and timers

set -e  # Exit on error

# Copy service and timer files to systemd directory
sudo cp *.service /etc/systemd/system/
sudo cp *.timer /etc/systemd/system/
echo "✅ All services and timers files deployed successfully."
# Reload systemd to recognize new units
sudo systemctl daemon-reload
echo "✅ Reloaded the Daemon Services."

# Enable and start all timers
for timer in ./*.timer; do
    sudo systemctl enable "$(basename "$timer")"
    sudo systemctl start "$(basename "$timer")"
done

echo "✅ All services and timers deployed successfully."
