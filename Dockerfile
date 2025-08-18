FROM ghcr.io/blinklabs-io/go:1.24.5-1 AS build

WORKDIR /code
RUN go env -w GOCACHE=/go-cache
RUN go env -w GOMODCACHE=/gomod-cache
COPY go.* .
RUN --mount=type=cache,target=/gomod-cache go mod download
COPY . .
RUN --mount=type=cache,target=/gomod-cache --mount=type=cache,target=/go-cache make build

FROM ghcr.io/blinklabs-io/cardano-configs:20250812-1 AS cardano-configs
FROM ghcr.io/blinklabs-io/txtop:0.13.0 AS txtop

FROM debian:bookworm-slim AS dingo
COPY --from=build /code/dingo /bin/
COPY --from=cardano-configs /config/ /opt/cardano/config/
COPY --from=txtop /bin/txtop /usr/local/bin/
ENV CARDANO_CONFIG=/opt/cardano/config/preview/config.json
ENV CARDANO_NETWORK=preview
# Create database dir owned by container user
VOLUME /data/db
ENV CARDANO_DATABASE_PATH=/data/db
# Create socket dir owned by container user
VOLUME /ipc
ENV CARDANO_NODE_SOCKET_PATH=/ipc/dingo.socket
ENV CARDANO_SOCKET_PATH=/ipc/dingo.socket
ENTRYPOINT ["dingo"]
