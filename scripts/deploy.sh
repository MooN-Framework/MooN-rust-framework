#!/bin/bash
# == SCRIPT FOR DEPLOYING ON A LIST OF NODES WITH CONFIG FILES ==
# - REQUIRES CROSS & SSHPASS
# - Execute from the root project directory
# ---------------------------------------------

TARGET_USER="generic"
TARGET_PW="123"
TARGETS=("node0" "node1" "node2")
REMOTE_PATH="/home/$TARGET_USER"
# ==== CROSS =====
BINARY_NAME="main"
LOCAL_BINARY="target/aarch64-unknown-linux-gnu/release/main"

set -e

if [ ! -e "./Cargo.toml" ]; then
    echo "[ERROR] you have to execute this script from the project root."
    exit 1
fi

cross build --release --target aarch64-unknown-linux-gnu

echo "== Deploying to nodes =="
for HOST in "${TARGETS[@]}"; do
  echo "-Deploying binary to $HOST"
  sshpass -p "$TARGET_PW" scp -o StrictHostKeyChecking=no \
    "$LOCAL_BINARY" "$TARGET_USER@$HOST:$REMOTE_PATH"

  # Extract node number from hostname, e.g., node0 -> 0
  NODE_NUM="${HOST//[^0-9]/}"
  CONFIG_FILE="config/config_$NODE_NUM.json"

  if [ -f "$CONFIG_FILE" ]; then
      echo "-Deploying config $CONFIG_FILE to $HOST"
      sshpass -p "$TARGET_PW" scp -o StrictHostKeyChecking=no \
        "$CONFIG_FILE" "$TARGET_USER@$HOST:$REMOTE_PATH/config.json"
  else
      echo "[WARNING] Config file $CONFIG_FILE not found for $HOST"
  fi

  echo "-Changing access rights @ $HOST"
  sshpass -p "$TARGET_PW" ssh -o StrictHostKeyChecking=no \
    "$TARGET_USER@$HOST" "chmod +x $REMOTE_PATH/$BINARY_NAME"

  echo "==> $HOST done."
done
echo "Deployment complete."