import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { create, equals, fromBinary } from "@bufbuild/protobuf";
import { EnvelopeSchema } from "../src/gen/proto/xray/xray_pb.js";
import { ReplayBuffer } from "../src/buffer.js";
import { IngestSession } from "../src/session.js";
import {
  encodeEnvelope,
  frame,
  FrameReader,
  MAX_MESSAGE_SIZE,
} from "../src/wire.js";

import { collector, until } from "./helpers.js";

test("preserves bigint fields across fragmented and coalesced frames", () => {
  const envelope = create(EnvelopeSchema, {
    seq: 9007199254740993n,
    observedAtNs: 1791380000123456789n,
    payload: {
      case: "streamChunk",
      value: {
        streamAlias: 9007199254740995n,
        direction: 1,
        data: Uint8Array.of(4, 5, 6),
      },
    },
  });
  const encoded = Buffer.from(encodeEnvelope(envelope));
  const reader = new FrameReader();
  const messages = [];
  for (const byte of encoded)
    messages.push(...reader.push(Uint8Array.of(byte)));
  assert.equal(messages.length, 1);
  assert.ok(
    equals(
      EnvelopeSchema,
      fromBinary(EnvelopeSchema, messages[0]!.body),
      envelope,
    ),
  );
  assert.equal(reader.push(Buffer.concat([encoded, encoded])).length, 2);
  assert.throws(() => frame(3, new Uint8Array(MAX_MESSAGE_SIZE + 1)), {
    code: "XRAY_FRAME_TOO_LARGE",
  });
  assert.throws(
    () => new FrameReader().push(Uint8Array.of(3, 255, 255, 255, 255, 255)),
    { code: "XRAY_FRAME_TOO_LARGE" },
  );
});

test("replays every retained event once after collector disconnect", async () => {
  const sink = await collector();
  const session = new IngestSession(
    {
      address: sink.address,
      peerId: Uint8Array.of(1),
      clientName: "test",
      retryMs: 20,
    },
    () => [],
  );
  session.start();
  try {
    await session.waitForAttach(AbortSignal.timeout(5000));
    for (let id = 0; id < 10; id++)
      session.emit({ case: "stringDef", value: { id, value: String(id) } });
    await until(() => sink.events.filter((e) => e.seq).length === 10);
    sink.disconnect();
    await delay(10);
    for (let id = 10; id < 1000; id++)
      session.emit({ case: "stringDef", value: { id, value: String(id) } });
    await until(
      () =>
        sink.attaches >= 2 && sink.events.filter((e) => e.seq).length === 1000,
    );
    assert.deepEqual(
      sink.events.filter((e) => e.seq).map((e) => e.seq),
      Array.from({ length: 1000 }, (_, i) => BigInt(i + 1)),
    );
  } finally {
    await session.stop();
    await sink.close();
  }
});

test("bounds replay memory and snapshots past unrecoverable gaps", async () => {
  const sink = await collector();
  const errors: string[] = [];
  const session = new IngestSession(
    {
      address: sink.address,
      peerId: Uint8Array.of(1),
      clientName: "test",
      maxBufferBytes: 128 * 1024,
      onError: (error) => errors.push(error.code),
    },
    () => [
      {
        case: "streamUpsert",
        value: { streamAlias: 1n, captureStartedMidstream: true },
      },
    ],
  );
  for (let i = 0; i < 10; i++)
    session.emit({
      case: "streamChunk",
      value: { streamAlias: 1n, direction: 1, data: new Uint8Array(64 * 1024) },
    });
  assert.ok(session.buffer.byteLength <= 128 * 1024);
  session.start();
  try {
    await session.waitForAttach(AbortSignal.timeout(5000));
    session.emit({
      case: "streamChunk",
      value: { streamAlias: 1n, direction: 1, data: Uint8Array.of(9) },
    });
    await until(() => sink.events.some((e) => e.seq === 11n));
    assert.ok(errors.includes("XRAY_CAPTURE_GAP"));
    const end = sink.events.find((e) => e.payload.case === "snapshotEnd")!;
    assert.equal(
      end.payload.case === "snapshotEnd" && end.payload.value.lastIncludedSeq,
      10n,
    );
    assert.deepEqual(
      sink.events.filter((e) => e.seq).map((e) => e.seq),
      [11n],
    );
  } finally {
    await session.stop();
    await sink.close();
  }
});

test("preserves event bytes, order and timestamps across write batches", async () => {
  const sink = await collector();
  const session = new IngestSession(
    {
      address: sink.address,
      peerId: Uint8Array.of(1),
      clientName: "batch-test",
    },
    () => [],
  );
  const data = Array.from({ length: 600 }, (_, i) =>
    new Uint8Array(i < 512 ? 1 : 64 * 1024).fill(i % 256),
  );
  session.start();
  try {
    await session.waitForAttach(AbortSignal.timeout(5000));
    for (const [i, bytes] of data.entries())
      session.emit(
        { case: "streamChunk", value: { streamAlias: 1n, data: bytes } },
        BigInt(i + 100),
      );
    await until(
      () => sink.events.filter((event) => event.seq).length === data.length,
    );
    const events = sink.events.filter((event) => event.seq);
    for (const [i, event] of events.entries()) {
      assert.equal(event.seq, BigInt(i + 1));
      assert.equal(event.observedAtNs, BigInt(i + 100));
      assert.equal(event.payload.case, "streamChunk");
      if (event.payload.case === "streamChunk")
        assert.deepEqual(new Uint8Array(event.payload.value.data), data[i]);
    }
  } finally {
    await session.stop();
    await sink.close();
  }
});

test("stop cancels attachment and reconnect work when collector is unavailable", async () => {
  const session = new IngestSession(
    {
      address: "/tmp/xray-nonexistent-test.sock",
      peerId: Uint8Array.of(1),
      clientName: "test",
    },
    () => [],
  );
  session.start();
  const attached = assert.rejects(session.waitForAttach());
  await session.stop(20);
  await attached;
});

test("replay range detects an evicted prefix", () => {
  const buffer = new ReplayBuffer(128 * 1024);
  for (let i = 0; i < 3; i++)
    buffer.append(
      { case: "streamChunk", value: { data: new Uint8Array(64 * 1024) } },
      1n,
    );
  assert.equal(buffer.covers(1n), false);
  assert.equal(buffer.covers(3n), true);
  assert.equal(buffer.covers(4n), true);
});

test("rejects malformed addresses before starting background work", () => {
  for (const address of [
    "",
    "localhost",
    "localhost:0",
    "localhost:65536",
    "tcp://localhost:1234",
    "user@localhost:1234",
    "localhost:1234?query",
  ]) {
    assert.throws(
      () =>
        new IngestSession(
          { address, peerId: new Uint8Array(), clientName: "test" },
          () => [],
        ),
      { code: "XRAY_INVALID_ADDRESS" },
      address,
    );
  }
});

test("resnapshots when a running producer outruns the bounded writer", async () => {
  const sink = await collector();
  const session = new IngestSession(
    {
      address: sink.address,
      peerId: Uint8Array.of(1),
      clientName: "test",
      maxBufferBytes: 128 * 1024,
    },
    () => [],
  );
  session.start();
  try {
    await session.waitForAttach(AbortSignal.timeout(5000));
    for (let i = 0; i < 20; i++)
      session.emit({
        case: "streamChunk",
        value: { data: new Uint8Array(64 * 1024) },
      });
    await until(() =>
      sink.events.some(
        (e) =>
          e.payload.case === "snapshotEnd" &&
          e.payload.value.lastIncludedSeq === 20n,
      ),
    );
    session.emit({ case: "stringDef", value: { id: 1, value: "after gap" } });
    await until(() => sink.events.some((e) => e.seq === 21n));
    assert.ok(session.buffer.byteLength <= 128 * 1024);
  } finally {
    await session.stop();
    await sink.close();
  }
});

test("rejects collectors that cannot represent capture gaps", async () => {
  const sink = await collector(false);
  const errors: string[] = [];
  const session = new IngestSession(
    {
      address: sink.address,
      peerId: Uint8Array.of(1),
      clientName: "test",
      onError: (error) => errors.push(error.code),
    },
    () => [],
  );
  session.start();
  try {
    await until(() => errors.includes("XRAY_UNSUPPORTED_COLLECTOR"));
    await assert.rejects(session.waitForAttach(AbortSignal.timeout(20)));
    assert.equal(sink.events.length, 0);
  } finally {
    await session.stop(20);
    await sink.close();
  }
});

test("reconnects after an idle collector disconnect without new trace events", async () => {
  const sink = await collector();
  const session = new IngestSession(
    {
      address: sink.address,
      peerId: Uint8Array.of(1),
      clientName: "idle-test",
      retryMs: 10,
    },
    () => [],
  );
  session.start();
  try {
    await session.waitForAttach(AbortSignal.timeout(5000));
    sink.disconnect();
    await until(() => sink.attaches === 2);
    await session.waitForAttach(AbortSignal.timeout(5000));
  } finally {
    await session.stop();
    await sink.close();
  }
});
