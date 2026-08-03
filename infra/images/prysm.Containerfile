ARG GO_IMAGE=docker.io/library/golang:1.25-bookworm
FROM ${GO_IMAGE} AS build

WORKDIR /src
COPY go.mod go.sum ./
COPY third_party/go-bip39 ./third_party/go-bip39
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -trimpath -o /out/beacon-chain ./cmd/beacon-chain

FROM docker.io/library/debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 ethp2p \
    && useradd --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin ethp2p

COPY --from=build /out/beacon-chain /usr/local/bin/beacon-chain

ENV HOME=/tmp
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/beacon-chain"]
