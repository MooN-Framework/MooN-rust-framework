#!/bin/bash

# -----------------------------
# Usage:
# ./deploy_pi.sh user host password config_file static_ip gateway dns update_flag
# Example:
# ./deploy_pi.sh pi 192.168.178.20 raspberry config.yaml 192.168.178.50 192.168.178.1 "1.1.1.1 8.8.8.8" true
# -----------------------------

USER=$1
HOST=$2
PASSWORD=$3
CONFIG_FILE=$4
STATIC_IP=$5
GATEWAY=$6
DNS=$7
UPDATE_FLAG=$8  # "true" oder "false"

if [ $# -lt 8 ]; then
  echo "Usage:"
  echo "./deploy_pi.sh user host password config_file static_ip gateway dns update_flag"
  exit 1
fi

# -----------------------------
# System Update (optional)
# -----------------------------
if [ "$UPDATE_FLAG" = "true" ]; then
    echo "==> Updating system on $HOST..."
    sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no $USER@$HOST "
    sudo apt update &&
    sudo apt -y upgrade
    "
else
    echo "==> Skipping system update on $HOST..."
fi

# -----------------------------
# Set static IP
# -----------------------------
echo "==> Setting static IP on eth0..."
sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no $USER@$HOST "
# Find the active eth0 connection
CONN=\$(nmcli -t -f NAME,DEVICE connection show | grep eth0 | cut -d: -f1)

# Set static IP
sudo nmcli connection modify \$CONN \
ipv4.addresses $STATIC_IP/24 \
ipv4.gateway $GATEWAY \
ipv4.dns '$DNS' \
ipv4.method manual

# Bring connection up
sudo nmcli connection up \$CONN
"

# -----------------------------
# Copy config file and set ENV
# -----------------------------
echo "==> Copying config file to home directory..."
sshpass -p "$PASSWORD" scp -o StrictHostKeyChecking=no "$CONFIG_FILE" "$USER@$HOST:/tmp/"

sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no "$USER@$HOST" bash -c "'
# Use the remote $HOME
HOME_DIR=\$HOME

# Move and rename config file
mv /tmp/$(basename "$CONFIG_FILE") \$HOME_DIR/config.json

# Set environment variable for the user
ENV_FILE=\$HOME_DIR/.bashrc
if ! grep -q \"CONFIG_PATH=\" \$ENV_FILE; then
    echo \"export CONFIG_PATH=\$HOME_DIR/config.json\" >> \$ENV_FILE
else
    # Replace existing entry
    sed -i \"s|^export CONFIG_PATH=.*|export CONFIG_PATH=\$HOME_DIR/config.json|\" \$ENV_FILE
fi
'"

echo "==> Deployment finished on $HOST"