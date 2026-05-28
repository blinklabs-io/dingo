FROM ghcr.io/blinklabs-io/go:1.26.1-1 AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/dingo-gov-lens .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/dingo-gov-lens /dingo-gov-lens
EXPOSE 8088
USER nonroot:nonroot
ENTRYPOINT ["/dingo-gov-lens"]
