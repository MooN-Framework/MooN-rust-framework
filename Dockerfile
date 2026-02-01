# Builder für ARM (Raspberry Pi 4 ist aarch64)
FROM debian:bookworm

WORKDIR /root/app/

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    iproute2 \
    iputils-ping \
    netcat-openbsd

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
RUN . "$HOME/.cargo/env"

CMD ["/bin/bash"]