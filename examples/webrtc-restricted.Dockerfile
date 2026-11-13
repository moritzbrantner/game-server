FROM ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852
RUN apt-get update -qq \
    && apt-get install -y -qq nftables iproute2 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
