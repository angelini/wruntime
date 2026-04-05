#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <hostname>"
    echo "Example: $0 wrt-01"
    exit 1
fi

HOSTNAME="$1"

echo "==> Setting hostname to $HOSTNAME"
sudo hostnamectl set-hostname "$HOSTNAME"
sudo sed -i "s/127.0.1.1.*/127.0.1.1\t$HOSTNAME/" /etc/hosts

echo "==> Regenerating machine-id"
sudo rm -f /etc/machine-id /var/lib/dbus/machine-id
sudo systemd-machine-id-setup
sudo ln -sf /etc/machine-id /var/lib/dbus/machine-id

echo "==> Configuring passwordless sudo for $(whoami)"
echo "$(whoami) ALL=(ALL) NOPASSWD: ALL" | sudo tee /etc/sudoers.d/$(whoami) > /dev/null

echo "==> Regenerating SSH host keys"
sudo rm -f /etc/ssh/ssh_host_*
sudo dpkg-reconfigure openssh-server
sudo systemctl restart ssh

echo "Done. Verify with:"
echo "  hostnamectl"
echo "  cat /etc/machine-id"
