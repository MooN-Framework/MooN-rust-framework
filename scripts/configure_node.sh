#!/bin/bash
set -e

# --- CONFIG -------
TARGET_USER="generic"
TARGET_PW="123"
IP_OFFSET=5
TARGETS=("node0" "node1" "node2")
REMOTE_FILE="/run/NetworkManager/system-connections/netplan-eth0.nmconnection"
GATEWAY="192.168.1.1"
DNS="192.168.1.1;1.1.1.1"
# ----------------

for HOST in "${TARGETS[@]}"; do
  echo "---- Configuring $HOST ----"

  NODE_NUM=$(echo $HOST | grep -oE '[0-9]+')
  IP_LAST=$((NODE_NUM + IP_OFFSET))
  IP_ADDR="192.168.1.$IP_LAST/24"

  echo "Setting static IP: $IP_ADDR"

  # Erstelle die neue Config temporär lokal
  TMPFILE=$(mktemp)
  cat > "$TMPFILE" <<EOF
[connection]
id=netplan-eth0
type=ethernet
uuid=$(uuidgen)

[ethernet]
wake-on-lan=0

[ipv4]
method=manual
address1=$IP_ADDR
gateway=$GATEWAY
dns=$DNS

[ipv6]
method=auto
ip6-privacy=0

[proxy]
EOF

  # Upload in /tmp
  sshpass -p "$TARGET_PW" scp "$TMPFILE" $TARGET_USER@$HOST:/tmp/netplan-eth0.nmconnection

  # Move + Rechte + NM reload + Connection down/up
  sshpass -p "$TARGET_PW" ssh $TARGET_USER@$HOST "
      sudo mv /tmp/netplan-eth0.nmconnection $REMOTE_FILE
      sudo chmod 600 $REMOTE_FILE
      sudo nmcli connection reload
      # Finde die aktive Connection für eth0
      CON_NAME=\$(nmcli -t -f NAME,DEVICE connection show --active | grep eth0 | cut -d: -f1)
      sudo nmcli connection down \"\$CON_NAME\"
      sudo nmcli connection up \"\$CON_NAME\"
  "

  # CONFIG_PATH nur einfügen, falls nicht existiert
  sshpass -p "$TARGET_PW" ssh -o StrictHostKeyChecking=no $TARGET_USER@$HOST "
      if ! grep -q '^export CONFIG_PATH=/home/$TARGET_USER/config.json' ~/.bashrc; then
          echo 'export CONFIG_PATH=/home/$TARGET_USER/config.json' >> ~/.bashrc
      fi
  "

  rm "$TMPFILE"
  echo "✅ $HOST configured with IP $IP_ADDR"
done
