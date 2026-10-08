# 1.26.3-1 is the newest published tag of this image, and it is behind the Go
# patch releases that fix the standard-library advisories govulncheck finds
# reachable from this module (the last of them fixed in 1.26.6). What actually
# compiles the release binary is go.mod's `toolchain` floor, not this tag:
# GOTOOLCHAIN is `auto` in this image, so the build fetches that toolchain and
# uses it in place of the image's own go1.26.3. Advance this tag when
# blinklabs-io/docker-go publishes a newer one; never lower the go.mod floor to
# match it.
FROM ghcr.io/blinklabs-io/go:1.26.7-1@sha256:33d5a9fc16157790277b088625c1030a9c80afc2dc86542e0fb6ec0bdfeceba5 AS build

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
RUN apk add --no-cache bash
RUN version=v0.8.0 && \
  expected=h1:bVV5ZAuwaREn4klRSJusYaA+A7NXq8Qi9C/knCaQ2N4= && \
  actual="$(go mod download -json "github.com/antithesishq/antithesis-sdk-go@${version}" \
      | sed -n 's/^[[:space:]]*"Sum": "\(.*\)",/\1/p')" && \
  test "$actual" = "$expected" && \
  go get "github.com/antithesishq/antithesis-sdk-go@${version}" && \
  go install "github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@${version}"
RUN make mod-tidy
RUN mkdir -p /antithesis
# Create instrumented code in /antithesis
RUN `go env GOPATH`/bin/antithesis-go-instrumentor /code /antithesis
WORKDIR /antithesis/customer
RUN make CGO_ENABLED=1 build

FROM ghcr.io/blinklabs-io/cardano-cli:11.2.3.1-1@sha256:bc943590a8f0685a34a63697df18527b5e35d06dc0ce5fb931d7bc78ed5c4866 AS cardano-cli
FROM ghcr.io/blinklabs-io/cardano-configs:20260915-1@sha256:d13289f8c094b8ec9e0f0c64888254c94183239400f9564fb933e323ee2fa5f7 AS cardano-configs
FROM ghcr.io/blinklabs-io/nview:0.15.2@sha256:2c00f8542bc17073e4f8f75c403ba01226fb0949de4bb8db8f5170b29f85676d AS nview
FROM ghcr.io/blinklabs-io/txtop:0.16.0@sha256:7386a04aefdeef17e84f1f80790374c8da74142fadb6b558f093496bb1a427b1 AS txtop

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS dingo
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
RUN sed -i \
    -e 's|http://deb.debian.org/debian-security|http://snapshot.debian.org/archive/debian-security/20261005T000000Z|g' \
    -e 's|http://deb.debian.org/debian|http://snapshot.debian.org/archive/debian/20261005T000000Z|g' \
    /etc/apt/sources.list.d/debian.sources && \
  apt-get -o Acquire::Check-Valid-Until=false update -y && \
  apt-get install -y --no-install-recommends \
    ca-certificates=20250419~deb12u1 \
    wget=1.21.3-1+deb12u1 && \
  arch="$(dpkg --print-architecture)" && \
  case "$arch" in \
    amd64) \
      postgresql_sha=e4c00577ff40b59ccdf115a585a9a4e766539669473d272885325b674731dc53; \
      libpq_sha=9dc15f3f41090e632e0449693323f18c96bfd25794c439d8bcde06af6c5012f6 ;; \
    arm64) \
      postgresql_sha=42a82b0a547c94e8b172a482961efa1d64069a9d97efbc80a6dc0bd70337e656; \
      libpq_sha=1e2b364b93ff16293d6f4a409e49b0d8c0521b9f7ae3423007f443c31c0f6766 ;; \
    *) exit 1 ;; \
  esac && \
  postgresql_deb="postgresql-client-16_16.15-1.pgdg12+2_${arch}.deb" && \
  libpq_deb="libpq5_18.6-1.pgdg12+2_${arch}.deb" && \
  common_deb=postgresql-client-common_293.pgdg12+1_all.deb && \
  pgdg=https://apt-archive.postgresql.org/pub/repos/apt/pool/main && \
  wget -qO "/tmp/${postgresql_deb}" \
    "${pgdg}/p/postgresql-16/${postgresql_deb}" && \
  wget -qO "/tmp/${libpq_deb}" \
    "${pgdg}/p/postgresql-18/${libpq_deb}" && \
  wget -qO "/tmp/${common_deb}" \
    "${pgdg}/p/postgresql-common/${common_deb}" && \
  printf '%s  %s\n' \
    "$postgresql_sha" "/tmp/${postgresql_deb}" \
    "$libpq_sha" "/tmp/${libpq_deb}" \
    a4e2461975abffae23688fc95c4fdc97fea4c4c9cc096c2c4a7a4e334dfc3353 "/tmp/${common_deb}" \
    | sha256sum --check --strict - && \
  apt-get install -y \
    default-mysql-client=1.1.0 \
    liblmdb0=0.9.24-1 \
    libssl3=3.0.22-1~deb12u1 \
    sqlite3=3.40.1-2+deb12u2 \
    "/tmp/${libpq_deb}" \
    "/tmp/${common_deb}" \
    "/tmp/${postgresql_deb}" && \
  rm -f "/tmp/${libpq_deb}" "/tmp/${common_deb}" "/tmp/${postgresql_deb}" && \
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
EXPOSE 3001 3002 9090 12798 12799
# Probes the dedicated health listener's LIVENESS path, not /readyz, and not
# /metrics as this previously did.
#
#   - /metrics only proved the metrics listener had bound; it says nothing
#     about the node, and it disappears if metricsPort is repurposed.
#   - /readyz would be wrong here: Docker, Swarm and ECS respond to an
#     unhealthy container by replacing it, and a node doing an initial sync
#     is legitimately not ready for hours or days, so it would never survive
#     long enough to finish. Readiness belongs in a Kubernetes
#     readinessProbe or a load-balancer target check, where failing it
#     drains traffic instead of killing the node.
#
# The liveness body still carries the readiness verdict and the observed tip
# gap, so `docker inspect` shows why a live node is not yet serving.
#
# The port follows DINGO_HEALTH_PORT, which is the only one of the three
# healthPort sources (flag, YAML, environment) a HEALTHCHECK can read. An
# explicit 0 disables the listener, so the check reports healthy rather than
# probing a port nothing is bound to and driving the container into a
# replacement loop.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD health_port="${DINGO_HEALTH_PORT:-12799}"; \
  [ "$health_port" = "0" ] && exit 0; \
  wget -qO/dev/null "http://127.0.0.1:$health_port/health" || exit 1
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
RUN apt-get -o Acquire::Check-Valid-Until=false update -y && \
  apt-get install -y \
    curl=7.88.1-10+deb12u15 \
    lsof=4.95.0-1 \
    netcat-openbsd=1.219-1 \
    socat=1.7.4.4-2
COPY --from=antithesis-build /antithesis/customer/dingo /bin/
COPY --from=antithesis-build /antithesis/symbols/*.sym.tsv /symbols/

FROM dingo AS final
