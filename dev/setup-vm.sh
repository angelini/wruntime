#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <hostname> <static-ip>"
    echo "Example: $0 vm1 192.168.64.11"
    exit 1
fi

HOSTNAME="$1"
IP="$2"
GATEWAY="${3:-192.168.64.1}"
IFACE="${4:-enp0s1}"

echo "==> Setting hostname to $HOSTNAME"
sudo hostnamectl set-hostname "$HOSTNAME"
sudo sed -i "s/127.0.1.1.*/127.0.1.1\t$HOSTNAME/" /etc/hosts

echo "==> Regenerating machine-id"
sudo rm -f /etc/machine-id /var/lib/dbus/machine-id
sudo systemd-machine-id-setup
sudo ln -sf /etc/machine-id /var/lib/dbus/machine-id

echo "==> Regenerating SSH host keys"
sudo rm -f /etc/ssh/ssh_host_*
sudo dpkg-reconfigure openssh-server
sudo systemctl restart ssh

echo "==> Configuring static IP $IP on $IFACE"
sudo tee /etc/network/interfaces > /dev/null <<EOF
auto lo
iface lo inet loopback

auto $IFACE
iface $IFACE inet static
    address $IP/24
    gateway $GATEWAY
EOF

echo "==> Restarting networking"
sudo systemctl restart networking

echo "Done. Verify with:"
echo "  hostnamectl"
echo "  ip addr show $IFACE"
echo "  cat /etc/machine-id"
