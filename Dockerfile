# --- build: statically link the custom Bento distribution ---------------------
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# No cgo required: pgx and the whole schema-to-table stack are pure Go.
RUN CGO_ENABLED=0 go build -trimpath -o /out/bento ./cmd/bento

# --- certs: CA roots the schema fetcher needs for https:// schema URLs --------
FROM debian:bookworm-slim AS certs
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --- run: minimal image --------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bento /bento
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# /config/bento.yaml is expected to be mounted as read-only (see bento/).
VOLUME ["/config"]
EXPOSE 4195
ENTRYPOINT ["/bento"]
CMD ["-c", "/config/bento.yaml"]