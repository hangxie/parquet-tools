FROM golang:1.16-stretch as builder

ADD . /src
WORKDIR /src

RUN apt update \
    && apt install -y bash make git \
    && make build

FROM debian:stretch-slim
RUN adduser --home /app --disabled-password --system app \
 && apt update \
 && apt install ca-certificates -y \
 && rm -rf /var/lib/apt/lists/*

USER app
COPY --from=builder /src/build/parquet-tools /app/
WORKDIR /app
ENTRYPOINT ["/app/parquet-tools"]
