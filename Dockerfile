FROM ghcr.io/blinklabs-io/go:1.26.1-1 AS build

ARG VERSION
ARG COMMIT_HASH
ENV VERSION=${VERSION}
ENV COMMIT_HASH=${COMMIT_HASH}

WORKDIR /code
RUN go env -w GOCACHE=/go-cache
RUN go env -w GOMODCACHE=/gomod-cache
COPY go.* .
RUN go mod download
COPY . .
RUN make build

FROM build AS antithesis-build
RUN go get github.com/antithesishq/antithesis-sdk-go@latest
RUN go install github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@latest
RUN make mod-tidy
RUN mkdir -p /antithesis
# Create instrumented code in /antithesis
RUN `go env GOPATH`/bin/antithesis-go-instrumentor /code /antithesis
WORKDIR /antithesis/customer
RUN make build

FROM ghcr.io/blinklabs-io/cardano-cli:10.15.1.0-1 AS cardano-cli
FROM ghcr.io/blinklabs-io/cardano-configs:20251128-1 AS cardano-configs
FROM ghcr.io/blinklabs-io/nview:0.13.0 AS nview
FROM ghcr.io/blinklabs-io/txtop:0.15.0 AS txtop

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
COPY --from=nview /bin/nview /usr/local/bin/
COPY --from=txtop /bin/txtop /usr/local/bin/
COPY --chmod=0755 bin/entrypoint.sh /bin/entrypoint.sh
ENV CARDANO_NODE_BINARY=dingo
ENV CARDANO_NETWORK=preview
# Create database dir owned by container user
VOLUME /data/db
ENV CARDANO_DATABASE_PATH=/data/db
# Create socket dir owned by container user
VOLUME /ipc
ENV DINGO_SOCKET_PATH=/ipc/dingo.socket
ENV CARDANO_NODE_SOCKET_PATH=/ipc/dingo.socket
ENV CARDANO_SOCKET_PATH=/ipc/dingo.socket
EXPOSE 3001 3002 9090 12798
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD wget -qO/dev/null http://127.0.0.1:12798/metrics || exit 1
RUN addgroup --system dingo && adduser --system --no-create-home --ingroup dingo dingo
RUN mkdir -p /data/db /ipc && chown -R dingo:dingo /data/db /ipc
USER dingo
ENTRYPOINT ["/bin/entrypoint.sh"]
CMD ["serve"]

FROM dingo AS antithesis
USER root
RUN apt-get update -y && \
  apt-get install -y \
    curl \
    lsof \
    netcat-openbsd \
    socat
COPY --from=antithesis-build /antithesis/customer/dingo /bin/
COPY --from=antithesis-build /antithesis/symbols/*.sym.tsv /symbols/

FROM dingo AS final
