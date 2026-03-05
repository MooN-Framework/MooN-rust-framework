#!/bin/bash
set -e

# -----------------------------
# Usage:
# ./run_remote.sh <user> <password> <host>
# Example:
# ./run_remote.sh generic 123 192.168.178.26
# -----------------------------

USER=$1
PASSWORD=$2
HOST=$3

# Input validation
if [[ -z "$USER" || -z "$PASSWORD" || -z "$HOST" ]]; then
    echo "[Error] You must provide <user> <password> <host> as arguments"
    echo "Usage: ./run_remote.sh <user> <password> <host>"
    exit 1
fi

echo "Starting remote execution on $HOST with user $USER..."

# Execute remote script
sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no "$USER@$HOST" "/home/$USER/main"

echo "Remote execution finished."