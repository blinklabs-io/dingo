# 1.26.3-1 is the newest published tag of this image, and it is behind the Go
# patch releases that fix the standard-library advisories govulncheck finds
# reachable from this module (the last of them fixed in 1.26.6). What actually
# compiles the release binary is go.mod's `toolchain` floor, not this tag:
# GOTOOLCHAIN is `auto` in this image, so the build fetches that toolchain and
# uses it in place of the image's own go1.26.3. Advance this tag when
# blinklabs-io/docker-go publishes a newer one; never lower the go.mod floor to
# match it.
FROM ghcr.io/blinklabs-io/go:1.26.3-1 AS build

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

FROM ghcr.io/blinklabs-io/cardano-cli:11.0.0.0-1 AS cardano-cli
FROM ghcr.io/blinklabs-io/cardano-configs:20260817-1 AS cardano-configs
FROM ghcr.io/blinklabs-io/nview:0.15.0 AS nview
FROM ghcr.io/blinklabs-io/txtop:0.15.0 AS txtop

FROM debian:bookworm-slim AS dingo
# pg_dump/pg_restore version compatibility is asymmetric and narrower than
# it first appears, confirmed by actually running both directions against
# real Postgres 16/17 servers while building this image:
#   - pg_dump refuses outright to dump from a server NEWER than itself (a
#     hard safety check) -- Debian bookworm's own postgresql-client is
#     stuck on v15, which can't dump from the v16/v17 servers common today.
#   - pg_restore's failure mode going the other way is subtler: each major
#     version's pg_restore emits its own standard restore-preamble SET
#     statements for session GUCs introduced in or before its own version
#     (e.g. v17 added "transaction_timeout"), and that preamble runs
#     against whatever server it's pointed at regardless of the archive's
#     origin. A v17 pg_restore therefore fails outright against a v16 (or
#     older) server with "unrecognized configuration parameter
#     transaction_timeout" -- confirmed live -- even though v17 client
#     against v16 server looks like it should be the "safe," backward
#     compatible direction pg_dump allows.
# Pinning to v16 here (rather than always tracking latest) is a deliberate,
# verified choice: it dumps from/restores into any currently-supported
# Postgres server at v16 or older cleanly, matching this repo's own
# CI service (postgres:16, .github/workflows/go-test.yml). It cannot dump
# FROM a v17+ server (pg_dump's own version-mismatch guard); bump this pin
# (and re-verify pg_restore against every currently-supported server
# version, not just the newest) if that becomes a real requirement.
RUN apt-get update -y && \
  apt-get install -y --no-install-recommends ca-certificates gnupg wget && \
  install -d /usr/share/postgresql-common/pgdg && \
  wget -qO /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
  echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list && \
  apt-get update -y && \
  apt-get install -y \
    default-mysql-client \
    liblmdb0 \
    libssl3 \
    postgresql-client-16 \
    sqlite3 && \
  rm -rf /var/lib/apt/lists/*
ENV LD_LIBRARY_PATH="/usr/local/lib"
ENV PKG_CONFIG_PATH="/usr/local/lib/pkgconfig"
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
# UID/GID are pinned (not left to adduser's dynamic system-UID allocation)
# so they're stable and documentable across image rebuilds: this container
# never runs as root, so a custom --db-snapshot-dir (or any other data path)
# bind-mounted from outside /data/db must be pre-chowned by the operator to
# this UID:GID on the host before mounting -- see dingo.yaml.example's
# snapshotDir entry.
RUN addgroup --system --gid 1000 dingo && \
  adduser --system --uid 1000 --no-create-home --ingroup dingo dingo
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
