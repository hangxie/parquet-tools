FROM debian:13-slim
ARG DEBIAN_FRONTEND=noninteractive

COPY ./build/release/*-linux*.gz /tmp/

RUN adduser --home /app --disabled-password --system app \
 && apt-get update -qq \
 && apt-get -y -qq install ca-certificates \
 && case $(uname -m) in \
        aarch64) \
            TARBALL=$(ls /tmp/*-linux-arm64.gz) \
            ;; \
        x86_64) \
            TARBALL=$(ls /tmp/*-linux-amd64.gz) \
            ;; \
        armv7l) \
            TARBALL=$(ls /tmp/*-linux-arm.gz) \
            ;; \
        *) \
            echo Unsupported arch $(uname -m); \
            exit 1 \
            ;; \
    esac \
 && gzip -dc ${TARBALL} > /app/parquet-tools \
 && chmod +x /app/parquet-tools \
 && chown app /app/parquet-tools \
 && rm -rf /var/lib/apt/lists/* /tmp/*-linux*.gz

USER app
WORKDIR /app
ENTRYPOINT ["/app/parquet-tools"]
