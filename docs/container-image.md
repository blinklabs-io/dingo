# Container image

## Runtime policy

The primary Dingo container is an operator-tooling image based on Debian slim,
not a distroless application image. The runtime includes behavior that cannot
be reduced to the `dingo` binary without removing supported operation:

- Bash runs the entrypoint that resolves configuration, bootstraps a new node,
  and forwards shutdown signals.
- `sqlite3` lets the entrypoint distinguish a complete database from an
  interrupted bootstrap before deciding whether to restore a snapshot.
- `pg_dump`, `pg_restore`, `mysqldump`, and `mysql` implement backup and restore
  for the supported PostgreSQL and MySQL metadata providers.
- `wget` implements the image health check. CA certificates provide trust roots
  for Dingo's HTTPS integrations.
- `liblmdb0` and `libssl3` are runtime libraries used by the bundled Cardano
  tooling.
- `cardano-cli`, `nview`, and `txtop` are deliberately shipped with the node so
  an operator can inspect and administer it using the matching release image.
  The matching Cardano network configurations are bundled for the same reason.

Packages used only while assembling the image are not runtime tools. In
particular, GnuPG verifies the PostgreSQL repository key while the image is
built and is removed after the database clients are installed.

The image runs Dingo as the unprivileged `dingo` user with stable UID and GID
1000. Only the database and socket volume paths are prepared as writable by
that user. The Antithesis stage is a separate test image and intentionally runs
as root; it is not the published `final` stage.

Retain a package in the final stage only when it supports the entrypoint or
health check, satisfies a linked-library dependency, implements a supported
backup or restore path, or is one of the bundled operator commands above.
Build-only tools should be removed in the same layer that installs them.
