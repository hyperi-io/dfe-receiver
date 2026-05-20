# Project:   dfe-receiver
# File:      Dockerfile
# Purpose:   Production container image with dynamic librdkafka
#
# License:   FSL-1.1-ALv2
# Copyright: (c) 2026 HYPERI PTY LIMITED

FROM ubuntu:24.04@sha256:186072bba1b2f436cbb91ef2567abca677337cfc786c86e107d25b7072feef0c

LABEL io.hyperi.profile="production"

# Runtime shared libraries for dynamically-linked Rust crates.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd iputils-ping gnupg \
    && curl -fsSL https://packages.confluent.io/clients/deb/archive.key \
       | gpg --dearmor -o /usr/share/keyrings/confluent-clients.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent-clients.gpg] \
       https://packages.confluent.io/clients/deb noble main" \
       > /etc/apt/sources.list.d/confluent-clients.list \
    && apt-get update && apt-get install -y --no-install-recommends \
       librdkafka1 libssl3 zlib1g libzstd1 \
    && rm -rf /var/lib/apt/lists/*

COPY dfe-receiver /usr/local/bin/dfe-receiver
RUN chmod +x /usr/local/bin/dfe-receiver

# Ubuntu 24.04 ships with ubuntu user at UID 1000 — remove before creating appuser
RUN userdel -r ubuntu && useradd --create-home --uid 1000 appuser
USER appuser

EXPOSE 9090 8080 6000 4317 4318 5044 8088 9091 514 6514 24224 12201 2055/udp 4739/udp 6343/udp

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/health/live > /dev/null || exit 1

ENTRYPOINT ["dfe-receiver"]
CMD ["--config", "/etc/dfe-receiver/config.yaml"]
