FROM ghcr.io/blinklabs-io/go:1.23.4-1 AS build

WORKDIR /code
COPY go.* .
RUN go mod download
COPY . .
RUN make build

FROM ghcr.io/blinklabs-io/cardano-configs:20241028-1 AS cardano-configs
FROM ghcr.io/blinklabs-io/txtop:0.12.2 AS txtop

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
