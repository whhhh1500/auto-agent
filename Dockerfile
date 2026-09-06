FROM golang:1.25.13-bookworm AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
# Build-only network overrides. Keep the default on the public Go proxy while
# allowing restricted networks to provide a reviewed mirror at build time.
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOSUMDB=sum.golang.org

WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY="${GOPROXY}" GOSUMDB="${GOSUMDB}" go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X github.com/cc-auto-agent/harness-core/pkg/buildinfo.Version=${VERSION} -X github.com/cc-auto-agent/harness-core/pkg/buildinfo.Commit=${COMMIT} -X github.com/cc-auto-agent/harness-core/pkg/buildinfo.BuildDate=${BUILD_DATE}" \
    -o /out/harness-server ./cmd/server

FROM alpine:3.21.3

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN addgroup -S harness && adduser -S -G harness harness && \
    mkdir -p /data && chown harness:harness /data
COPY --from=build /out/harness-server /usr/local/bin/harness-server

LABEL org.opencontainers.image.title="Harness Core server" \
      org.opencontainers.image.description="SaaS/container runtime for Harness Core" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$COMMIT" \
      org.opencontainers.image.created="$BUILD_DATE" \
      org.opencontainers.image.source="https://github.com/cc-auto-agent/harness-core"

ENV HARNESS_DATA_DIR=/data
ENV HARNESS_SERVER_PORT=8080
ENV HARNESS_MODE=production
ENV HARNESS_DATABASE_TYPE=postgres
WORKDIR /data
USER harness:harness
EXPOSE 8080
# Liveness is intentionally independent of PostgreSQL readiness. The server
# only binds after migrations and bootstrap have completed; /readyz is the
# deployment gate for load balancers and rolling upgrades.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O - http://127.0.0.1:8080/healthz >/dev/null || exit 1
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/harness-server"]
