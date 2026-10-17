import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { EnvelopeSchema } from "./gen/proto/xray/xray_pb.js";
import { encodeEnvelope, ProbeError } from "./wire.js";

export type Payload = NonNullable<
  MessageInitShape<typeof EnvelopeSchema>["payload"]
>;

export class ReplayBuffer {
  private readonly frames = new Map<bigint, Uint8Array>();
  private bytes = 0;
  lastSequence = 0n;

  constructor(readonly maxBytes: number) {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 128 * 1024) {
      throw new ProbeError(
        "XRAY_BUFFER_SIZE",
        "Replay buffer must be at least 128 KiB",
      );
    }
  }

  get byteLength(): number {
    return this.bytes;
  }

  append(payload: Payload, observedAtNs: bigint): void {
    const seq = this.lastSequence + 1n;
    const data = encodeEnvelope(
      create(EnvelopeSchema, { seq, observedAtNs, payload }),
    );
    if (data.byteLength > this.maxBytes)
      throw new ProbeError(
        "XRAY_BUFFER_SIZE",
        "Envelope exceeds replay buffer",
      );
    this.lastSequence = seq;
    this.frames.set(seq, data);
    this.bytes += data.byteLength;
    while (this.bytes > this.maxBytes) {
      const oldest = this.frames.entries().next().value!;
      this.frames.delete(oldest[0]);
      this.bytes -= oldest[1].byteLength;
    }
  }

  covers(sequence: bigint): boolean {
    return sequence === this.lastSequence + 1n || this.frames.has(sequence);
  }

  get(sequence: bigint): Uint8Array | undefined {
    return this.frames.get(sequence);
  }

  clear(): void {
    this.frames.clear();
    this.bytes = 0;
  }
}
