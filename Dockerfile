FROM ghcr.io/blinklabs-io/go:1.24.7-1 AS build

WORKDIR /code
RUN go env -w GOCACHE=/go-cache
RUN go env -w GOMODCACHE=/gomod-cache
COPY go.* .
RUN --mount=type=cache,target=/gomod-cache go mod download
COPY . .
RUN --mount=type=cache,target=/gomod-cache --mount=type=cache,target=/go-cache make build

FROM ghcr.io/blinklabs-io/cardano-cli:10.11.1.0-1 AS cardano-cli
FROM ghcr.io/blinklabs-io/cardano-configs:20250917-1 AS cardano-configs
FROM ghcr.io/blinklabs-io/mithril-client:0.12.30-1 AS mithril-client
FROM ghcr.io/blinklabs-io/txtop:0.13.0 AS txtop

FROM debian:bookworm-slim AS dingo
RUN apt-get update -y && \
  apt-get install -y \
    ca-certificates \
    liblmdb0 \
    libssl3 \
    sqlite3 \
    wget && \
  rm -rf /var/lib/apt/lists/*
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
ENV PKG_CONFIG_PATH="/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH"
COPY --from=build /code/dingo /bin/
COPY --from=cardano-cli /usr/local/bin/cardano-cli /usr/local/bin/
COPY --from=cardano-cli /usr/local/include/ /usr/local/include/
COPY --from=cardano-cli /usr/local/lib/ /usr/local/lib/
COPY --from=cardano-configs /config/ /opt/cardano/config/
COPY --from=mithril-client /bin/mithril-client /usr/local/bin/
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
