FROM ghcr.io/blinklabs-io/go:1.22.7-1 AS build

WORKDIR /code
COPY go.* .
RUN go mod download
COPY . .
RUN make build

FROM debian:bookworm-slim AS node
COPY --from=build /code/node /bin/
ENTRYPOINT ["node"]
