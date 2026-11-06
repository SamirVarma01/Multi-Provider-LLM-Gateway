# syntax=docker/dockerfile:1
# Builds one binary from ./cmd/${CMD} (gateway, mockprovider, or reconcile).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG CMD
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM alpine:3.22
# /data is owned by the runtime user so named volumes mounted there are writable.
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 app \
 && mkdir /data && chown app /data
USER app
COPY --from=build /out/app /usr/local/bin/app
# Route tables are baked in (bind mounts are not available on every Docker
# host); rebuild after editing config/.
COPY config/ /etc/gateway/
ENTRYPOINT ["/usr/local/bin/app"]
