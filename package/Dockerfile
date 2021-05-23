FROM golang:1.16-alpine as builder

ADD . /src
WORKDIR /src

RUN apk update \
    && apk add make bash git \
    && make build

FROM alpine
RUN adduser -S -D -H -h /app app
USER app
COPY --from=builder /src/build/parquet-tools /app/
WORKDIR /app
ENTRYPOINT ["/app/parquet-tools"]
