FROM golang:1.22-bullseye as builder

ADD . /src
WORKDIR /src

RUN apt-get update -qq \
    && apt-get install -y -qq bash make git \
    && make build

FROM debian:12-slim
ARG DEBIAN_FRONTEND=noninteractive

COPY --from=builder /src/build/parquet-tools /tmp/

RUN adduser --home /app --disabled-password --system app \
 && echo This is to fix linux/arm build \
 && for U in dpkg-split dpkg-deb tar gzip chmod chown rm; do \
        ln -fs `which $U` /usr/sbin/; \
        ln -fs `which $U` /usr/local/sbin/; \
    done \
 && apt-get update -qq \
 && apt-get -y -qq install ca-certificates \
 && mv /tmp/parquet-tools /app/ \
 && chmod +x /app/parquet-tools \
 && chown app /app/parquet-tools \
 && rm -rf /var/lib/apt/lists/* \
 && for U in dpkg-split dpkg-deb tar gzip chmod chown rm; do \
        rm /usr/sbin/$U /usr/local/sbin/$U; \
    done

USER app
WORKDIR /app
ENTRYPOINT ["/app/parquet-tools"]
