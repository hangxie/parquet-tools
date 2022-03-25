FROM golang:1.18-stretch as builder

ADD . /src
WORKDIR /src

RUN apt-get update -qq \
    && apt-get install -y -qq bash make git \
    && make build

FROM debian:stretch-slim
ARG DEBIAN_FRONTEND=noninteractive
RUN adduser --home /app --disabled-password --system app \
 && echo This is to fix linux/arm build \
 && for U in dpkg-split dpkg-deb tar rm; do \
        ln -fs `which $U` /usr/sbin/; \
        ln -fs `which $U` /usr/local/sbin/; \
    done \
 && apt-get update -qq \
 && apt-get -y -qq install ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && for U in dpkg-split dpkg-deb tar rm; do \
        rm /usr/sbin/$U /usr/local/sbin/$U; \
    done

USER app
COPY --from=builder /src/build/parquet-tools /app/
WORKDIR /app
ENTRYPOINT ["/app/parquet-tools"]
