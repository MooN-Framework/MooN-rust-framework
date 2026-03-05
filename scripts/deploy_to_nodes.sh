#!/bin/bash
# == SCRIPT FOR DEPLOYING ON A LIST OF NODES WITH CONFIG FILES ==
# - Requires `cross` and `sshpass`
# - Execute from the root project directory
# ---------------------------------------------

set -e

# --- Input Arguments ---
# $1 = user
# $2 = password
# $3 = path to config file (optional, can be empty "")
# $4+ = list of hosts
if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <user> <password> <config_file or empty> <host1> [host2 ... hostN]"
    exit 1
fi

TARGET_USER="$1"
TARGET_PW="$2"
CONFIG_FILE="$3"
shift 3
TARGETS=("$@")  # remaining arguments are hosts

REMOTE_PATH="/home/$TARGET_USER"
BINARY_NAME="main"
LOCAL_BINARY="target/aarch64-unknown-linux-gnu/release/$BINARY_NAME"

# --- Check project root ---
if [ ! -e "./Cargo.toml" ]; then
    echo "[ERROR] Execute this script from the project root (Cargo.toml not found)."
    exit 1
fi

# --- Build binary ---
echo "==> Building binary..."
cross build --release --target aarch64-unknown-linux-gnu

# --- Deploy to all hosts ---
echo "==> Deploying to nodes =="
for HOST in "${TARGETS[@]}"; do
    echo "- Deploying binary to $HOST"
    sshpass -p "$TARGET_PW" scp -o StrictHostKeyChecking=no "$LOCAL_BINARY" "$TARGET_USER@$HOST:$REMOTE_PATH/"

    if [[ -n "$CONFIG_FILE" && -f "$CONFIG_FILE" ]]; then
        echo "- Deploying config $CONFIG_FILE to $HOST"
        sshpass -p "$TARGET_PW" scp -o StrictHostKeyChecking=no "$CONFIG_FILE" "$TARGET_USER@$HOST:$REMOTE_PATH/config.json"
    else
        echo "[WARNING] Config file $CONFIG_FILE not found or not provided, skipping for $HOST"
    fi

    echo "- Setting executable permissions on $HOST"
    sshpass -p "$TARGET_PW" ssh -o StrictHostKeyChecking=no "$TARGET_USER@$HOST" "chmod +x $REMOTE_PATH/$BINARY_NAME"

    echo "==> $HOST done."
done

echo "Deployment complete."