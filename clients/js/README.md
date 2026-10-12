# Node.js probe

`@xray/probe` captures negotiated libp2p application streams and sends Xray v3
envelopes to a local collector. It requires Node.js 22 or later and the accompanying
js-libp2p middleware extension: global `use(middleware)` and `unuse(middleware)`
overloads, with isolated middleware arrays for each stream. Existing
`use(protocol, middleware)` calls still replace that protocol's middleware;
protocol strings, including `"*"`, are matched literally.

```ts
import { createLibp2p } from "libp2p";
import { XrayProbe } from "@xray/probe";

const node = await createLibp2p({ start: false /* other libp2p options */ });
const probe = new XrayProbe(node, {
  address: "/tmp/xray.sock", // or "127.0.0.1:9091"
  clientName: "lodestar/local",
  onError: (error) => console.warn(error.code, error.message),
});
await probe.waitForAttach(AbortSignal.timeout(30_000)); // optional startup barrier
await node.start();

// Keep capture attached while libp2p emits its final lifecycle events.
try {
  await node.stop();
} finally {
  await probe.stop();
}
```

Capture runs in the same thread as libp2p, including Lodestar's network worker.
The probe observes inbound message dispatch without adding a consuming listener,
and wraps `send` without changing its return value or errors. Read-ahead returned
through `unshift` is counted once. `close()` only closes the write half; a stream
closes in Xray after libp2p closes it and any buffered reads drain. Buffered bytes
remain observable even after a remote reset or connection close. The probe does
not keep abandoned streams alive. Connections, streams, and binary peer IDs
receive independent aliases. Multiaddr data supplies transport
and remote address metadata; unavailable local addresses remain unset.

The captured bytes exclude transport encryption, multiplexing, and protocol
negotiation overhead. They include both gossip and request/response streams.
Ethereum decoding stays in the collector.

## Native and evented transports

`XrayStreamProbe` accepts already-negotiated application streams without a libp2p
host. The producer supplies Unix nanosecond timestamps at the read/write boundary,
unique connection/stream IDs, binary peer IDs, and ordered lifecycle events:

```ts
import { XrayStreamProbe } from "@xray/probe";

const probe = new XrayStreamProbe({
  address: "/tmp/xray.sock",
  peerId: localPeerIdBytes,
  clientName: "lodestar-z/local",
  transport: "quic-v1",
  security: "tls",
  muxer: "quic-v1",
});
await probe.waitForAttach(AbortSignal.timeout(30_000));
// Start the native owner only after the optional attachment barrier.
probe.capture(nativeNetwork.drainStreamTrace());
```

Each batch contains `events` and `reset`. Events are `connection`, `stream`,
`data`, `stream_closed`, or `connection_closed`. Announce the connection before
its streams and the negotiated stream before its bytes. Report only bytes
accepted by a write or returned by a read, including negotiation read-ahead once.
A write-half close does not close the stream: its response may still be read.
`capture()` copies data synchronously, so producers may reuse their buffers.
The configured transport/security/muxer names apply to every connection in this
probe; use separate probes for different transport stacks.

If a bounded producer buffer loses events, discard the pending batch and set
`reset` on the next batch. That batch must contain a complete snapshot of live
connections and negotiated streams, without data events. It invalidates SDK
replay and marks those streams incomplete in the collector. Resume ordinary
events afterward. The producer owns its live-table and protocol-name bounds;
the SDK retains only live metadata and interned protocol names, plus replay.

Drain final native events after the owner stops, then call `probe.stop()`. The
probe closes any remaining live streams/connections and attempts a bounded flush.

## Buffering and reconnects

The probe owns a 16 MiB replay buffer by default (`maxBufferBytes`, minimum
128 KiB), with chunks capped at 64 KiB. Network writes run asynchronously and
never wait in the application stream. Reconnect uses the collector's
`last_acked_seq`; the writer reads directly from the replay buffer, without a
second queue that could silently lose events. Nanosecond timestamps and sequence
numbers use protobuf `bigint` values.

When retention is exhausted, the probe sends a metadata snapshot through
`SnapshotEnd.last_included_seq`. Existing streams are marked
`capture_started_midstream`; the collector reports their subsequent traffic as
`capture_incomplete` instead of attempting to decode an arbitrary framing suffix.
New streams decode normally. A collector built from this revision is required
for that gap behavior. The SDK checks `ServerHello.supports_capture_gaps` and
reports `XRAY_UNSUPPORTED_COLLECTOR` when the collector needs an upgrade. Older
v3 Go probes remain supported by the collector.

`onError` reports collector failures and retention gaps. Capture is opt-in and
continues retrying when the collector is unavailable. `stop()` allows one second
for a final flush, then cancels pending I/O. This is best-effort tracing, not a
durable audit log.

## Build and test

```sh
npm ci
npm run generate
npm run build
npm run check-types
npm test
npm run lint
npm pack
```

The TypeScript bindings are generated from `../../proto/xray/xray.proto` with the
pinned Buf and protoc-gen-es versions in `package-lock.json`. Regenerate Go
bindings from the repository root with `buf generate` and protoc-gen-go v1.36.9.
Generated files are checked in so consumers do not need code generators.

The package is currently consumed by Lodestar as a locally packed archive.
Publish the SDK and merge/release the libp2p changes before replacing that archive
with registry dependencies in an upstream Lodestar PR.

## Ethereum forks

The collector accepts Lodestar's larger gossip RPCs and Electra
`SingleAttestation` layout. For Gloas, pass every active Gloas fork digest to
`xray --gloas-fork-digests=11223344,55667788`. This selects the changed column and
block layouts and the new bid, payload attestation, preference, and execution
envelope topics. Execution envelopes carry a payload timestamp rather than a
slot; configure the collector's genesis time and slot duration for the network.
Request/response streams remain attributed as raw protocol bytes.
