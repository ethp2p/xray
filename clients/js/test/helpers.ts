import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type Socket } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ClientHelloSchema,
  EnvelopeSchema,
  ServerHelloSchema,
  type Envelope,
} from "../src/gen/proto/xray/xray_pb.js";
import { frame, FrameReader } from "../src/wire.js";

export async function until(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, "timed out waiting for ingest");
    await delay(5);
  }
}

export async function collector(supportsCaptureGaps = true) {
  let cursor = 0n;
  let attaches = 0;
  const events: Envelope[] = [];
  const connections = new Set<Socket>();
  const server = createServer((socket) => {
    connections.add(socket);
    socket.on("close", () => connections.delete(socket));
    socket.on("error", () => {});
    const reader = new FrameReader();
    socket.on("data", (data) => {
      for (const message of reader.push(data)) {
        if (message.type === 1) {
          const hello = fromBinary(ClientHelloSchema, message.body);
          assert.equal(hello.protocolVersion, 3);
          assert.equal(hello.bootId.length, 16);
          attaches++;
          socket.write(
            frame(
              2,
              toBinary(
                ServerHelloSchema,
                create(ServerHelloSchema, {
                  protocolVersion: 3,
                  sourceId: "test",
                  supportsCaptureGaps,
                  lastAckedSeq: cursor,
                }),
              ),
            ),
          );
        } else {
          assert.equal(message.type, 3);
          const envelope = fromBinary(EnvelopeSchema, message.body);
          if (envelope.payload.case === "snapshotStart") cursor = 0n;
          if (envelope.payload.case === "snapshotEnd")
            cursor = envelope.payload.value.lastIncludedSeq;
          if (envelope.seq && envelope.seq <= cursor) continue;
          events.push(envelope);
          if (envelope.seq) cursor = envelope.seq;
        }
      }
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return {
    events,
    get attaches() {
      return attaches;
    },
    address: `127.0.0.1:${address.port}`,
    disconnect() {
      for (const socket of connections) socket.destroy();
    },
    async close() {
      for (const socket of connections) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
