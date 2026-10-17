import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { generateKeyPair } from "@libp2p/crypto/keys";
import type { Connection, StreamMiddleware } from "@libp2p/interface";
import { peerIdFromPrivateKey } from "@libp2p/peer-id";
import { streamPair } from "@libp2p/utils";
import { multiaddr } from "@multiformats/multiaddr";
import { stubInterface } from "sinon-ts";
import { XrayProbe } from "../src/index.js";
import { collector, until } from "./helpers.js";

test("captures buffered reads, backpressure, read-ahead, half-close and reset without altering streams", async () => {
  const sink = await collector();
  const node = stubInterface<ConstructorParameters<typeof XrayProbe>[0]>();
  node.peerId = peerIdFromPrivateKey(await generateKeyPair("Ed25519"));
  node.getConnections.returns([]);
  let observe: StreamMiddleware | undefined;
  node.use.callsFake((middleware: StreamMiddleware) => {
    observe = middleware;
  });
  const probe = new XrayProbe(node, {
    address: sink.address,
    clientName: "adapter-test",
  });
  const [local, remote] = await streamPair({
    protocol: "/test/1",
    capacity: 2,
  });
  const connection = stubInterface<Connection>();
  connection.id = "connection";
  connection.remotePeer = peerIdFromPrivateKey(
    await generateKeyPair("Ed25519"),
  );
  connection.remoteAddr = multiaddr("/ip4/127.0.0.1/tcp/1234");
  connection.direction = "outbound";
  connection.status = "open";
  connection.timeline = { open: Date.now() };
  connection.streams = [local];
  const send = local.send;
  const dispatch = local.dispatchEvent;
  try {
    await probe.waitForAttach(AbortSignal.timeout(5000));
    remote.send(Uint8Array.of(1, 2, 3));
    await delay(20);
    let nextCalls = 0;
    assert.ok(observe);
    observe(local, connection, () => {
      nextCalls++;
    });
    observe(local, connection, () => {
      nextCalls++;
    });
    assert.equal(nextCalls, 2);
    const received: number[][] = [];
    let pushedBack = false;
    local.addEventListener("message", (event) => {
      received.push([...event.data.subarray()]);
      if (!pushedBack) {
        pushedBack = true;
        local.unshift(event.data.subarray(1));
      }
    });
    await until(() => received.length === 2);
    assert.deepEqual(received, [
      [1, 2, 3],
      [2, 3],
    ]);
    const sent: number[] = [];
    remote.addEventListener("message", (event) =>
      sent.push(...event.data.subarray()),
    );
    assert.equal(local.send(Uint8Array.of(4)), true);
    assert.equal(local.send(Uint8Array.of(5)), false);
    await until(() => sent.length === 2);
    await local.close();
    assert.throws(() => local.send(Uint8Array.of(99)));
    await delay(20);
    assert.equal(
      sink.events.filter((e) => e.payload.case === "streamClosed").length,
      0,
      "write half-close must keep the readable stream alive",
    );
    remote.send(Uint8Array.of(6));
    await until(() => received.length === 3);
    await remote.close();
    await until(() =>
      sink.events.some((e) => e.payload.case === "streamClosed"),
    );
    assert.equal(
      sink.events.filter((e) => e.payload.case === "streamUpsert").length,
      1,
      "middleware must be idempotent for the same stream",
    );
    const chunks = sink.events.flatMap((e) =>
      e.payload.case === "streamChunk" ? [e.payload.value] : [],
    );
    assert.deepEqual(
      chunks.filter((c) => c.direction === 1).flatMap((c) => [...c.data]),
      [1, 2, 3, 6],
    );
    assert.deepEqual(
      chunks.filter((c) => c.direction === 2).flatMap((c) => [...c.data]),
      [4, 5],
    );
    assert.equal(local.send, send);
    assert.equal(local.dispatchEvent, dispatch);

    const [resetLocal, resetRemote] = await streamPair({
      protocol: "/test/reset",
    });
    observe(resetLocal, connection, () => {});
    resetRemote.abort(new Error("test reset"));
    await until(
      () =>
        sink.events.filter((e) => e.payload.case === "streamClosed").length ===
        2,
    );
    const reasons = sink.events.flatMap((e) =>
      e.payload.case === "streamClosed" ? [e.payload.value.reason] : [],
    );
    assert.deepEqual(reasons, [1, 2]);
  } finally {
    local.abort(new Error("test cleanup"));
    remote.abort(new Error("test cleanup"));
    await probe.stop();
    assert.deepEqual(node.unuse.firstCall.args, [observe]);
    await sink.close();
  }
});

test("captures buffered reads after stream and connection closure", async () => {
  const sink = await collector();
  const node = stubInterface<ConstructorParameters<typeof XrayProbe>[0]>();
  node.peerId = peerIdFromPrivateKey(await generateKeyPair("Ed25519"));
  node.getConnections.returns([]);
  let observe: StreamMiddleware | undefined;
  node.use.callsFake((middleware: StreamMiddleware) => {
    observe = middleware;
  });
  let onConnectionClose: ((event: CustomEvent<Connection>) => void) | undefined;
  node.addEventListener.callsFake((type: string, listener: unknown) => {
    if (type === "connection:close" && typeof listener === "function") {
      onConnectionClose = (event) => listener(event);
    }
  });
  const probe = new XrayProbe(node, {
    address: sink.address,
    clientName: "buffered-close-test",
  });
  try {
    await probe.waitForAttach(AbortSignal.timeout(5000));
    for (const mode of ["close", "reset", "connection"] as const) {
      const [local, remote] = await streamPair({ protocol: "/test/buffered" });
      const connection = stubInterface<Connection>();
      connection.id = mode;
      connection.remotePeer = node.peerId;
      connection.remoteAddr = multiaddr("/ip4/127.0.0.1/tcp/1234");
      connection.direction = "outbound";
      connection.status = "open";
      connection.timeline = { open: Date.now() };
      assert.ok(observe);
      observe(local, connection, () => {});
      remote.send(Uint8Array.of(1, 2, 3));
      await until(() => local.readBufferLength === 3);
      if (mode === "reset") {
        remote.abort(new Error("remote reset with buffered data"));
        await until(() => local.status === "reset");
      } else {
        await local.close();
        await remote.close();
        await until(() => local.status === "closed");
      }
      if (mode === "connection") {
        connection.status = "closed";
        assert.ok(onConnectionClose);
        onConnectionClose(
          new CustomEvent("connection:close", { detail: connection }),
        );
      }
      const received: number[] = [];
      local.addEventListener("message", (event) =>
        received.push(...event.data.subarray()),
      );
      await until(() => received.length === 3);
      assert.deepEqual(received, [1, 2, 3], mode);
    }
    await probe.stop();
    await until(
      () =>
        sink.events.filter((e) => e.payload.case === "streamClosed").length ===
        3,
    );
    for (let alias = 0n; alias < 3n; alias++) {
      const chunks = sink.events.filter(
        (e) =>
          e.payload.case === "streamChunk" &&
          e.payload.value.streamAlias === alias,
      );
      assert.equal(chunks.length, 1, `stream ${alias} lost buffered bytes`);
      const closeIndex = sink.events.findIndex(
        (e) =>
          e.payload.case === "streamClosed" &&
          e.payload.value.streamAlias === alias,
      );
      assert.ok(
        sink.events.indexOf(chunks[0]!) < closeIndex,
        `stream ${alias} closed before its final chunk`,
      );
    }
  } finally {
    await probe.stop();
    await sink.close();
  }
});
