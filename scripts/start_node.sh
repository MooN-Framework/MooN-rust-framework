TARGET_USER="generic"
TARGET_PW="123"
set -e

if [[ ! -n "$1" ]]; then
    echo "[Error] You have to set the hostname as an argument"
    exit -1
else
    echo "Starting TARGET_HOST: $1"
    TARGET_HOST="$1"
fi 

sshpass -p "123" ssh $TARGET_USER@$TARGET_HOST "/home/$TARGET_USER/main"