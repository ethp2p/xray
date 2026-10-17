import { fromBinary, toBinary } from "@bufbuild/protobuf";
import type { Socket } from "node:net";
import {
  ClientHelloSchema,
  EnvelopeSchema,
  ServerHelloSchema,
} from "./gen/proto/xray/xray_pb.js";
import type {
  ClientHello,
  Envelope,
  ServerHello,
} from "./gen/proto/xray/xray_pb.js";

export const PROTOCOL_VERSION = 3;
export const MAX_MESSAGE_SIZE = 4 * 1024 * 1024;

export class ProbeError extends Error {
  constructor(
    readonly code: string,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ProbeError";
  }
}

export function frame(type: number, body: Uint8Array): Uint8Array {
  if (body.length > MAX_MESSAGE_SIZE)
    throw new ProbeError("XRAY_FRAME_TOO_LARGE", "Ingest frame exceeds 4 MiB");
  const prefix = [type];
  let length = body.length;
  do {
    const byte = length & 127;
    length >>>= 7;
    prefix.push(byte | (length ? 128 : 0));
  } while (length);
  const data = new Uint8Array(prefix.length + body.length);
  data.set(prefix);
  data.set(body, prefix.length);
  return data;
}

export function encodeHello(hello: ClientHello): Uint8Array {
  return frame(1, toBinary(ClientHelloSchema, hello));
}

export function encodeEnvelope(envelope: Envelope): Uint8Array {
  return frame(3, toBinary(EnvelopeSchema, envelope));
}

export class FrameReader {
  private buffer = Buffer.alloc(0);

  push(chunk: Uint8Array): { type: number; body: Uint8Array }[] {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    const frames: { type: number; body: Uint8Array }[] = [];
    while (this.buffer.length >= 2) {
      let length = 0;
      let offset = 1;
      for (; offset < this.buffer.length && offset <= 4; offset++) {
        const byte = this.buffer[offset]!;
        length += (byte & 127) * 2 ** (7 * (offset - 1));
        if (length > MAX_MESSAGE_SIZE)
          throw new ProbeError(
            "XRAY_FRAME_TOO_LARGE",
            "Ingest frame exceeds 4 MiB",
          );
        if ((byte & 128) === 0) break;
      }
      if (offset > 4)
        throw new ProbeError(
          "XRAY_INVALID_LENGTH",
          "Invalid ingest frame length",
        );
      if (
        offset >= this.buffer.length ||
        this.buffer.length < offset + 1 + length
      )
        break;
      frames.push({
        type: this.buffer[0]!,
        body: this.buffer.subarray(offset + 1, offset + 1 + length),
      });
      this.buffer = this.buffer.subarray(offset + 1 + length);
    }
    return frames;
  }
}

export async function readServerHello(socket: Socket): Promise<ServerHello> {
  const reader = new FrameReader();
  for await (const chunk of socket.iterator({ destroyOnReturn: false })) {
    const frames = reader.push(chunk as Buffer);
    if (frames.length === 0) continue;
    if (frames.length !== 1 || frames[0]!.type !== 2) {
      throw new ProbeError(
        "XRAY_INVALID_HANDSHAKE",
        "Expected a single ServerHello",
      );
    }
    const hello = fromBinary(ServerHelloSchema, frames[0]!.body);
    if (hello.protocolVersion !== PROTOCOL_VERSION) {
      throw new ProbeError(
        "XRAY_PROTOCOL_VERSION",
        `Unsupported ingest version ${hello.protocolVersion}`,
      );
    }
    if (!hello.supportsCaptureGaps) {
      throw new ProbeError(
        "XRAY_UNSUPPORTED_COLLECTOR",
        "Collector does not support capture gap snapshots; update the collector",
      );
    }
    return hello;
  }
  throw new ProbeError(
    "XRAY_HANDSHAKE_CLOSED",
    "Collector disconnected during handshake",
  );
}
