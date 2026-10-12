import assert from "node:assert/strict";
import { test } from "node:test";
import { XrayStreamProbe, type StreamEvent } from "../src/stream.js";
import { collector, until } from "./helpers.js";

const observedAtNs = 1791380000123456789n;
const connection: StreamEvent = {
  type: "connection",
  id: 1n,
  peerId: Uint8Array.of(1),
  remoteAddr: "/ip4/127.0.0.1/udp/9000/quic-v1",
  localAddr: "/ip4/127.0.0.1/udp/9001/quic-v1",
  direction: "outbound",
  observedAtNs,
};
const stream: StreamEvent = {
  type: "stream",
  id: 2n,
  connection: 1n,
  protocol: "/meshsub/1.2.0",
  direction: "outbound",
  observedAtNs,
};

test("evented stream capture preserves producer timestamps, bytes, and close ordering", async () => {
  const sink = await collector();
  const probe = new XrayStreamProbe({
    address: sink.address,
    peerId: Uint8Array.of(2),
    clientName: "native/test",
    transport: "quic-v1",
    security: "tls",
    muxer: "quic-v1",
  });
  try {
    await probe.waitForAttach(AbortSignal.timeout(5000));
    const bytes = Uint8Array.of(1, 2, 3);
    probe.capture({
      reset: false,
      events: [
        connection,
        stream,
        {
          type: "data",
          stream: 2n,
          direction: "outbound",
          data: bytes,
          observedAtNs,
        },
        { type: "connection_closed", connection: 1n, observedAtNs },
      ],
    });
    bytes.fill(9);
    await until(() =>
      sink.events.some((e) => e.payload.case === "connectionClosed"),
    );
    const chunk = sink.events.find((e) => e.payload.case === "streamChunk")!;
    assert.equal(chunk.observedAtNs, observedAtNs);
    assert.equal(chunk.payload.case, "streamChunk");
    if (chunk.payload.case === "streamChunk")
      assert.deepEqual(Array.from(chunk.payload.value.data), [1, 2, 3]);
    const closes = sink.events.filter(
      (e) =>
        e.payload.case === "streamClosed" ||
        e.payload.case === "connectionClosed",
    );
    assert.deepEqual(
      closes.map((e) => e.payload.case),
      ["streamClosed", "connectionClosed"],
    );
  } finally {
    await probe.stop();
    await sink.close();
  }
});

test("producer loss forces a snapshot even when the collector caught up", async () => {
  const sink = await collector();
  const errors: string[] = [];
  const probe = new XrayStreamProbe({
    address: sink.address,
    peerId: Uint8Array.of(2),
    clientName: "native/test",
    transport: "quic-v1",
    security: "tls",
    muxer: "quic-v1",
    onError: (e) => errors.push(e.code),
  });
  try {
    await probe.waitForAttach(AbortSignal.timeout(5000));
    probe.capture({ reset: false, events: [connection, stream] });
    await until(() =>
      sink.events.some((e) => e.payload.case === "streamUpsert"),
    );
    probe.capture({ reset: true, events: [connection, stream] });
    await until(() =>
      sink.events.some(
        (e) =>
          e.payload.case === "streamUpsert" &&
          e.payload.value.captureStartedMidstream,
      ),
    );
    probe.capture({
      reset: false,
      events: [
        {
          type: "data",
          stream: 2n,
          direction: "inbound",
          data: Uint8Array.of(9),
          observedAtNs,
        },
      ],
    });
    await until(() =>
      sink.events.some((e) => e.payload.case === "streamChunk"),
    );
    assert.ok(errors.includes("XRAY_CAPTURE_GAP"));
    const snapshots = sink.events.filter(
      (e) => e.payload.case === "snapshotEnd",
    );
    assert.equal(snapshots.length, 2);
    assert.ok(
      snapshots[1]!.payload.case === "snapshotEnd" &&
        snapshots[1]!.payload.value.lastIncludedSeq > 0n,
    );
  } finally {
    await probe.stop();
    await sink.close();
  }
});

test("connection updates preserve one peer alias across overlapping connections", async () => {
  const sink = await collector();
  const probe = new XrayStreamProbe({
    address: sink.address,
    peerId: Uint8Array.of(2),
    clientName: "native/test",
    transport: "tcp",
    security: "noise",
    muxer: "yamux",
  });
  try {
    await probe.waitForAttach(AbortSignal.timeout(5000));
    probe.capture({
      reset: false,
      events: [
        connection,
        { ...connection, id: 3n },
        { ...connection, remoteAddr: "/ip4/127.0.0.2/tcp/9000" },
        { type: "connection_closed", connection: 1n, observedAtNs },
        { ...connection, id: 4n },
        { type: "connection_closed", connection: 3n, observedAtNs },
        { type: "connection_closed", connection: 4n, observedAtNs },
      ],
    });
    await until(
      () =>
        sink.events.filter((e) => e.payload.case === "connectionClosed")
          .length === 3,
    );
    assert.equal(
      sink.events.filter((e) => e.payload.case === "peerUpsert").length,
      1,
    );
    const aliases = sink.events.flatMap((e) =>
      e.payload.case === "connectionUpsert" ? [e.payload.value.peerAlias] : [],
    );
    assert.equal(aliases.length, 4);
    assert.equal(new Set(aliases).size, 1);
    const strings = sink.events.flatMap((e) =>
      e.payload.case === "stringDef" ? [e.payload.value.value] : [],
    );
    assert.deepEqual(strings, ["tcp", "noise", "yamux"]);
  } finally {
    await probe.stop();
    await sink.close();
  }
});
