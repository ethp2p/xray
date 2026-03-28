# ── Go backend ───────────────────────────────────────────────────────
FROM golang:1.25-bookworm AS backend
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /introspector ./cmd/introspector

# ── Dashboard ────────────────────────────────────────────────────────
FROM oven/bun:1 AS dashboard
WORKDIR /src
COPY dashboard/package.json dashboard/bun.lock ./
RUN bun install --frozen-lockfile
COPY dashboard/ .
RUN bun run build

# ── Final image ──────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12
COPY --from=backend /introspector /usr/local/bin/introspector
COPY --from=dashboard /src/dist /srv/dashboard
EXPOSE 9100
ENTRYPOINT ["introspector"]
CMD [ \
  "--ingest", "/tmp/wiretap.sock", \
  "--listen", "0.0.0.0:9100", \
  "--data-dir", "/data", \
  "--static-dir", "/srv/dashboard" \
]
