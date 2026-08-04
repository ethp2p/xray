# Raptor deployment

This directory defines the production Xray stack as Podman 4.9 Quadlets managed by systemd.

## Ownership

- `quadlet/nethermind.container` owns the execution client process and `/data/nethermind`.
- `quadlet/prysm.container` owns the consensus client process and `/data/.eth2`.
- `quadlet/xray.container` owns the Xray process, `/home/ubuntu/.xray/data`, and the loopback dashboard port.
- `tmpfiles/xray.conf` owns the shared runtime socket directory.
- `images/prysm.Containerfile` builds the instrumented Prysm fork.

The services share no container lifecycle. Prysm starts after Nethermind and Xray, but systemd can restart or upgrade each service on its own.

Xray uses a read-only root filesystem. Prysm and Nethermind use writable disposable image overlays because Podman 4.9 cannot create their JWT secret mountpoint after applying a read-only root. Their persistent state remains limited to the explicit host bind mounts.

## Image tags

| Component | Quadlet tag | Notes |
|---|---|---|
| Xray | `ghcr.io/ethp2p/xray:0.1.0` | Semver from git tag `v0.1.0`. Also published: `latest` (tip of `main`), short SHA |
| Prysm fork | `ghcr.io/ethp2p/xray-prysm:stable` | Floating supported pin. Also published: `latest`, short SHA, full SHA |
| Nethermind | digest pin in `nethermind.container` | `1.36.0` linux/amd64 `sha256:d915b29966286ec9ceee400c889e0b18fd4d84e7895402f3f4fa5750209c0a25` |

Quadlets use `Pull=missing`. Bump the Xray semver in `xray.container` when
cutting a release; retag Prysm `stable` by re-running the publish workflow.

## Publish images (GHCR)

Workflows in this repo push to the GitHub Container Registry:

| Workflow | Image | Trigger |
| --- | --- | --- |
| `.github/workflows/publish-xray.yml` | `ghcr.io/ethp2p/xray` | push to `main`, `v*` tags, or manual |
| `.github/workflows/publish-xray-prysm.yml` | `ghcr.io/ethp2p/xray-prysm` | manual (`prysm_ref` input; default `1fcc706ce…`) |

```bash
# Cut an Xray release (publishes :0.1.0, :0.1, short SHA; :latest stays tip of main)
git tag v0.1.0
git push origin v0.1.0

# Refresh Prysm :stable / :latest from a prysm commit
gh workflow run publish-xray-prysm.yml --repo ethp2p/xray \
  -f prysm_ref=1fcc706ce44eacd253ae3f5078995c5b3437e5fd
```

## Build images locally

Build Prysm from a clean archive so local Git objects, binaries, databases, and keys cannot enter the image context:

```bash
build_dir=$(mktemp -d)
git -C /home/ubuntu/prysm archive 1fcc706ce44eacd253ae3f5078995c5b3437e5fd | tar -x -C "$build_dir"
sudo podman build \
  --file /home/ubuntu/xray/infra/images/prysm.Containerfile \
  --tag ghcr.io/ethp2p/xray-prysm:stable \
  "$build_dir"
```

Build Xray from this tree (or pull the published tag):

```bash
sudo podman pull ghcr.io/ethp2p/xray:0.1.0
# or:
sudo podman build -t ghcr.io/ethp2p/xray:0.1.0 -f Dockerfile .
```

Do not build with unrelated untracked files in the context.
## Install

Install Podman, create the shared runtime directory, and import the existing JWT as a Podman secret:

```bash
sudo apt-get update
sudo apt-get install --yes podman
sudo install -m 0644 infra/tmpfiles/xray.conf /etc/tmpfiles.d/xray.conf
sudo systemd-tmpfiles --create /etc/tmpfiles.d/xray.conf
sudo podman secret create eth-jwt /home/ubuntu/jwt.hex
```

Install and validate the Quadlets:

```bash
sudo install -d -m 0755 /etc/containers/systemd
sudo install -m 0644 infra/quadlet/*.container /etc/containers/systemd/
sudo env QUADLET_UNIT_DIRS=/etc/containers/systemd \
  /usr/lib/systemd/system-generators/podman-system-generator --dryrun
sudo systemctl daemon-reload
```

Generated services are named `nethermind.service`, `xray.service`, and `prysm.service`.

## Rollout

Record the current API state first. Stop each legacy process cleanly before starting the matching Quadlet because both versions use the same database and ports.

```bash
sudo systemctl start nethermind.service
sudo systemctl start xray.service
sudo systemctl start prysm.service
```

Verify:

```bash
systemctl --no-pager --full status nethermind.service xray.service prysm.service
curl -fsS -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  http://127.0.0.1:8545
curl -fsS http://127.0.0.1:3500/eth/v1/node/syncing
curl -fsS http://127.0.0.1:9100/api/sources
curl -fsS https://xray.ethp2p.dev/api/sources
```

## Rollback

Stop the Quadlets before restarting any legacy process:

```bash
sudo systemctl stop prysm.service xray.service nethermind.service
```

The rollout does not delete the native binaries, tmux sessions, Docker image, Docker container, or existing data directories. They remain rollback inputs until the new services pass a reboot test and an operator removes them.

## Upgrade policy

1. For Xray: cut a new `v*` tag, bump `Image=` in `xray.container`, pull, restart.
2. For Prysm: re-publish so `:stable` moves, pull `ghcr.io/ethp2p/xray-prysm:stable`, restart.
3. For Nethermind: bump the digest pin in `nethermind.container` in a reviewed change.
4. Restart only the service you changed.
5. Check sync distance, peer count, Xray source connection, and logs.
6. Keep the prior image until the next successful upgrade.

Do not enable registry auto-update for these stateful clients (`Pull=missing`
is intentional).

## Rollout checks recorded on raptor

- Podman `4.9.3`, cgroup v2, overlay storage, and runc.
- The Podman generator produced all three services without errors.
- Nethermind `1.36.0` started and stopped cleanly with UID/GID 1000, the JWT secret, dropped capabilities, and a temporary data directory.
- Xray started with its read-only root, loopback port, bind-mounted data, and Unix socket.
- Prysm commit `1fcc706ce4` connected to an isolated Xray ingest socket and reported `P2P instrumentation enabled`.
- The production services were active after cutover. Nethermind had 100 peers, Prysm had zero sync distance and 84 connected peers, and local and public Xray APIs showed the same connected Prysm source.
- Intentional Xray and Prysm service restarts recovered cleanly. The host still needs a reboot test after pending kernel and libc updates.
