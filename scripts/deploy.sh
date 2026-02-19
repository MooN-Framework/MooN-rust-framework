#!/bin/bash
# == SCRIPT FOR DEPLOYING ON A LIST OF NODES ==
# - REQUIRES CROSS & SSHPASS
# - Execute from the root project directory
# ---------------------------------------------

# Example, dont save important passwords in scripts
TARGET_USER="generic"
TARGET_PW="123"
TARGETS=("node0" "node1" "node2")
REMOTE_PATH="/home/$TARGET_USER/"
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
  echo "---- Deploying binary to $HOST ----"
  sshpass -p "$TARGET_PW" scp -o StrictHostKeyChecking=no \
    "$LOCAL_BINARY" "$TARGET_USER@$HOST:$REMOTE_PATH"
  echo "---- Changing access rights @ $HOST ----"
  sshpass -p "$TARGET_PW" ssh -o StrictHostKeyChecking=no \
    "$TARGET_USER@$HOST" "chmod +x $REMOTE_PATH"
  echo "==> $HOST done."
done
echo "Deployment complete."
