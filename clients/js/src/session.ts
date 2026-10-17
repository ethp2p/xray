import { create } from "@bufbuild/protobuf";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { Socket, type NetConnectOpts } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { ReplayBuffer } from "./buffer.js";
import type { Payload } from "./buffer.js";
import { ClientHelloSchema, EnvelopeSchema } from "./gen/proto/xray/xray_pb.js";
import {
  encodeEnvelope,
  encodeHello,
  PROTOCOL_VERSION,
  ProbeError,
  readServerHello,
} from "./wire.js";

export interface SessionOptions {
  address: string;
  peerId: Uint8Array;
  clientName: string;
  maxBufferBytes?: number;
  retryMs?: number;
  onError?: (error: ProbeError) => void;
}

export class IngestSession {
  readonly buffer: ReplayBuffer;
  private readonly address: NetConnectOpts;
  private readonly controller = new AbortController();
  private readonly hello;
  private task?: Promise<void>;
  private socket?: Socket;
  private wake?: () => void;
  private attached = false;
  private stopping = false;
  private readonly attachWaiters = new Set<() => void>();
  private lastErrorCode?: string;

  constructor(
    private readonly options: SessionOptions,
    private readonly snapshot: () => Payload[],
  ) {
    this.address = parseAddress(options.address);
    this.buffer = new ReplayBuffer(options.maxBufferBytes ?? 16 * 1024 * 1024);
    this.hello = create(ClientHelloSchema, {
      protocolVersion: PROTOCOL_VERSION,
      peerId: options.peerId,
      clientName: options.clientName,
      bootId: randomBytes(16),
      startedAtNs: now(),
    });
  }

  start(): void {
    this.task ??= this.run();
  }

  emit(payload: Payload): void {
    if (this.stopping) return;
    this.buffer.append(payload, now());
    this.wake?.();
  }

  async waitForAttach(signal?: AbortSignal): Promise<void> {
    if (this.attached) return;
    const combined = AbortSignal.any([
      this.controller.signal,
      ...(signal ? [signal] : []),
    ]);
    combined.throwIfAborted();
    await new Promise<void>((resolve, reject) => {
      const done = (): void => {
        combined.removeEventListener("abort", abort);
        this.attachWaiters.delete(done);
        resolve();
      };
      const abort = (): void => {
        this.attachWaiters.delete(done);
        reject(combined.reason);
      };
      this.attachWaiters.add(done);
      combined.addEventListener("abort", abort, { once: true });
    });
  }

  async stop(timeoutMs = 1000): Promise<void> {
    this.stopping = true;
    this.wake?.();
    const timer = setTimeout(() => {
      this.controller.abort();
      this.socket?.destroy();
      this.wake?.();
    }, timeoutMs);
    timer.unref();
    try {
      await this.task;
    } finally {
      clearTimeout(timer);
      this.controller.abort();
      this.socket?.destroy();
      this.buffer.clear();
    }
  }

  private async run(): Promise<void> {
    while (!this.controller.signal.aborted) {
      const socket = new Socket();
      this.socket = socket;
      socket.on("error", () => {});
      socket.on("close", () => this.wake?.());
      try {
        if (this.stopping) return;
        socket.setTimeout(5000, () =>
          socket.destroy(
            new ProbeError(
              "XRAY_CONNECT_TIMEOUT",
              "Collector handshake timed out",
            ),
          ),
        );
        const connected = once(socket, "connect", {
          signal: this.controller.signal,
        });
        socket.connect(this.address);
        await connected;
        await this.write(socket, encodeHello(this.hello));
        const hello = await readServerHello(socket);
        socket.setTimeout(0);
        let cursor = hello.lastAckedSeq + 1n;
        if (hello.lastAckedSeq === 0n && this.buffer.covers(1n)) {
          await this.writeSnapshot(socket, [], 0n);
        } else if (!this.buffer.covers(cursor)) {
          cursor = await this.resnapshot(socket);
        }
        this.lastErrorCode = undefined;
        this.attached = true;
        for (const done of this.attachWaiters) done();
        while (!socket.destroyed && !this.controller.signal.aborted) {
          if (!this.buffer.covers(cursor)) {
            cursor = await this.resnapshot(socket);
          }
          const frames: Uint8Array[] = [];
          let bytes = 0;
          let next = cursor;
          for (let count = 0; count < 256; count++) {
            const data = this.buffer.get(next);
            if (!data || (bytes > 0 && bytes + data.byteLength > 1024 * 1024))
              break;
            frames.push(data);
            bytes += data.byteLength;
            next++;
          }
          if (frames.length > 0) {
            await this.write(socket, Buffer.concat(frames, bytes));
            cursor = next;
          } else if (this.stopping) {
            await new Promise<void>((resolve) => socket.end(resolve));
            return;
          } else {
            await new Promise<void>((resolve) => {
              this.wake = resolve;
            });
            this.wake = undefined;
          }
        }
      } catch (cause) {
        if (!this.stopping) {
          this.report(
            cause instanceof ProbeError
              ? cause
              : new ProbeError(
                  "XRAY_COLLECTOR_UNAVAILABLE",
                  "Xray collector connection failed",
                  { cause },
                ),
          );
        }
      } finally {
        this.attached = false;
        socket.destroy();
        this.socket = undefined;
      }
      if (this.stopping) return;
      await delay(this.options.retryMs ?? 1000, undefined, {
        signal: this.controller.signal,
      }).catch(() => {});
    }
  }

  private async resnapshot(socket: Socket): Promise<bigint> {
    const payloads = this.snapshot();
    const sequence = this.buffer.lastSequence;
    this.report(
      new ProbeError(
        "XRAY_CAPTURE_GAP",
        "Capture gap; existing streams will be counted as incomplete",
      ),
    );
    await this.writeSnapshot(socket, payloads, sequence);
    return sequence + 1n;
  }

  private async writeSnapshot(
    socket: Socket,
    payloads: Payload[],
    sequence: bigint,
  ): Promise<void> {
    const snapshot: Payload[] = [
      { case: "snapshotStart", value: {} },
      ...payloads,
      { case: "snapshotEnd", value: { lastIncludedSeq: sequence } },
    ];
    for (const payload of snapshot) {
      await this.write(
        socket,
        encodeEnvelope(create(EnvelopeSchema, { payload })),
      );
    }
  }

  private async write(socket: Socket, data: Uint8Array): Promise<void> {
    this.controller.signal.throwIfAborted();
    await new Promise<void>((resolve, reject) => {
      socket.write(data, (error) => (error ? reject(error) : resolve()));
    });
  }

  private report(error: ProbeError): void {
    if (this.lastErrorCode === error.code) return;
    this.lastErrorCode = error.code;
    try {
      this.options.onError?.(error);
    } catch {}
  }
}

export function now(): bigint {
  return BigInt(Date.now()) * 1_000_000n;
}

function parseAddress(address: string): NetConnectOpts {
  if (
    address.includes("/") &&
    !address.includes("://") &&
    !address.includes("\0")
  ) {
    return { path: address };
  }
  try {
    const url = new URL(`tcp://${address}`);
    const port = Number(url.port);
    if (
      url.username ||
      url.password ||
      url.pathname ||
      url.search ||
      url.hash ||
      !url.hostname ||
      !Number.isInteger(port) ||
      port < 1 ||
      port > 65535
    ) {
      throw new ProbeError(
        "XRAY_INVALID_ADDRESS",
        "Expected a Unix socket path or host:port",
      );
    }
    return { host: url.hostname.replace(/^\[|\]$/g, ""), port };
  } catch (cause) {
    throw new ProbeError(
      "XRAY_INVALID_ADDRESS",
      "Expected a Unix socket path or host:port",
      { cause },
    );
  }
}
