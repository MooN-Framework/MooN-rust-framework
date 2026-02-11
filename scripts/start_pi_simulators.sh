#!/bin/bash

IMAGE_NAME="rust-pie-image"
HOST_DIR="/home/kevmatz/repos/master-projekt/"
CONTAINER_DIR="/root/app/"
NETWORK_NAME="pie-net"
CONTAINER_NAME="pie$1"

if !(docker network ls --format '{{.Name}}' | grep -wq "$NETWORK_NAME";) then
    docker network create "$NETWORK_NAME"
fi

# Start container (create if not exists, restart if stopped)
if [ "$(docker ps -a -q -f name="^${CONTAINER_NAME}$")" ]; then
    docker start "$CONTAINER_NAME"
else
    docker run -dit --name "$CONTAINER_NAME" --network "$NETWORK_NAME" -e CONFIG_PATH=/root/app/config/config_$1.json -v "${HOST_DIR}:${CONTAINER_DIR}" "$IMAGE_NAME"
fi

docker exec -it $CONTAINER_NAME /bin/bash

docker kill $CONTAINER_NAME