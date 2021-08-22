FROM golang:1.17-stretch as builder

ADD . /src
WORKDIR /src

RUN apt update \
    && apt install -y bash make git \
    && make build

FROM debian:stretch-slim
RUN adduser --home /app --disabled-password --system app \
 && echo This is to fix linux/arm build \
 && for U in dpkg-split dpkg-deb tar rm; do \
        ln -fs `which $U` /usr/sbin/; \
        ln -fs `which $U` /usr/local/sbin/; \
    done \
 && apt update \
 && apt install ca-certificates -y \
 && rm -rf /var/lib/apt/lists/* \
 && for U in dpkg-split dpkg-deb tar rm; do \
        rm /usr/sbin/$U /usr/local/sbin/$U; \
    done

USER app
COPY --from=builder /src/build/parquet-tools /app/
WORKDIR /app
ENTRYPOINT ["/app/parquet-tools"]
