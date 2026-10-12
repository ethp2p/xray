import type { Payload } from "./buffer.js";
import { Direction } from "./gen/proto/xray/xray_pb.js";
import type {
  ConnectionUpsert,
  PeerUpsert,
  StreamUpsert,
} from "./gen/proto/xray/xray_pb.js";
import { create } from "@bufbuild/protobuf";
import {
  ConnectionUpsertSchema,
  PeerUpsertSchema,
  StreamUpsertSchema,
} from "./gen/proto/xray/xray_pb.js";
import { IngestSession, now, type SessionOptions } from "./session.js";
import { ProbeError } from "./wire.js";

export type StreamEvent = { observedAtNs: bigint } & (
  | {
      type: "connection";
      id: bigint;
      peerId: Uint8Array;
      remoteAddr: string;
      localAddr: string;
      direction: "inbound" | "outbound";
    }
  | {
      type: "stream";
      id: bigint;
      connection: bigint;
      protocol: string;
      direction: "inbound" | "outbound";
    }
  | {
      type: "data";
      stream: bigint;
      direction: "inbound" | "outbound";
      data: Uint8Array;
    }
  | { type: "stream_closed"; stream: bigint; reset: boolean }
  | { type: "connection_closed"; connection: bigint }
);

/** A reset batch contains a complete snapshot of live connections and negotiated streams. */
export interface StreamBatch {
  reset: boolean;
  events: readonly StreamEvent[];
}

export interface StreamProbeOptions extends SessionOptions {
  transport: string;
  security: string;
  muxer: string;
}

/**
 * Ingest for native/evented transports. Call before starting networking. IDs must remain
 * unique for this probe's lifetime; report accepted application bytes, excluding negotiation.
 * capture() copies bytes synchronously. Only complete snapshots may cross a producer gap.
 */
export class XrayStreamProbe {
  private readonly session: IngestSession;
  private readonly connections = new Map<
    bigint,
    { upsert: ConnectionUpsert; peer: string }
  >();
  private readonly peers = new Map<
    string,
    { upsert: PeerUpsert; connections: number }
  >();
  private nextPeer = 1n;
  private readonly streams = new Map<bigint, StreamUpsert>();
  private readonly strings = new Map<string, number>();
  private stopped = false;

  constructor(private readonly options: StreamProbeOptions) {
    this.session = new IngestSession(options, () => this.snapshot());
    this.session.start();
  }

  waitForAttach(signal?: AbortSignal): Promise<void> {
    return this.session.waitForAttach(signal);
  }

  get bufferedBytes(): number {
    return this.session.buffer.byteLength;
  }

  capture(batch: StreamBatch): void {
    if (this.stopped) return;
    if (batch.reset) {
      this.connections.clear();
      this.peers.clear();
      this.streams.clear();
      this.strings.clear();
    }
    for (const event of batch.events) this.apply(event, batch.reset);
    if (batch.reset) this.session.invalidate();
  }

  async stop(timeoutMs = 1000): Promise<void> {
    if (this.stopped) return;
    for (const connection of this.connections.keys())
      this.apply(
        { type: "connection_closed", connection, observedAtNs: now() },
        false,
      );
    this.stopped = true;
    await this.session.stop(timeoutMs);
    this.connections.clear();
    this.peers.clear();
    this.streams.clear();
    this.strings.clear();
  }

  private apply(event: StreamEvent, snapshot: boolean): void {
    const emit = (payload: Payload): void => {
      if (!snapshot) this.session.emit(payload, event.observedAtNs);
    };
    switch (event.type) {
      case "connection": {
        const key = Buffer.from(event.peerId).toString("hex");
        let peer = this.peers.get(key);
        if (!peer) {
          peer = {
            upsert: create(PeerUpsertSchema, {
              peerAlias: this.nextPeer++,
              peerId: event.peerId.slice(),
            }),
            connections: 0,
          };
          this.peers.set(key, peer);
          emit({ case: "peerUpsert", value: peer.upsert });
        }
        if (!this.connections.has(event.id)) peer.connections++;
        const connection = create(ConnectionUpsertSchema, {
          connAlias: event.id,
          peerAlias: peer.upsert.peerAlias,
          remoteAddr: event.remoteAddr,
          localAddr: event.localAddr,
          direction: direction(event.direction),
          transportId: this.intern(this.options.transport, emit),
          securityId: this.intern(this.options.security, emit),
          muxerId: this.intern(this.options.muxer, emit),
          openedAtNs: event.observedAtNs,
        });
        this.connections.set(event.id, { upsert: connection, peer: key });
        emit({ case: "connectionUpsert", value: connection });
        break;
      }
      case "stream": {
        if (!this.connections.has(event.connection))
          throw new ProbeError(
            "XRAY_SOURCE_ORDER",
            "Stream preceded its connection",
          );
        const stream = create(StreamUpsertSchema, {
          streamAlias: event.id,
          connAlias: event.connection,
          protocolId: this.intern(event.protocol, emit),
          direction: direction(event.direction),
          openedAtNs: event.observedAtNs,
          captureStartedMidstream: snapshot,
        });
        this.streams.set(event.id, stream);
        emit({ case: "streamUpsert", value: stream });
        break;
      }
      case "data":
        if (snapshot || !this.streams.has(event.stream))
          throw new ProbeError(
            "XRAY_SOURCE_ORDER",
            "Bytes preceded their stream",
          );
        for (
          let offset = 0;
          offset < event.data.byteLength;
          offset += 64 * 1024
        )
          emit({
            case: "streamChunk",
            value: {
              streamAlias: event.stream,
              direction: direction(event.direction),
              data: event.data.subarray(offset, offset + 64 * 1024),
            },
          });
        break;
      case "stream_closed":
        if (!this.streams.delete(event.stream)) break;
        emit({
          case: "streamClosed",
          value: {
            streamAlias: event.stream,
            closedAtNs: event.observedAtNs,
            reason: event.reset ? 2 : 1,
          },
        });
        break;
      case "connection_closed": {
        const connection = this.connections.get(event.connection);
        if (!connection) break;
        for (const [alias, stream] of this.streams)
          if (stream.connAlias === event.connection) {
            emit({
              case: "streamClosed",
              value: {
                streamAlias: alias,
                closedAtNs: event.observedAtNs,
                reason: 3,
              },
            });
            this.streams.delete(alias);
          }
        this.connections.delete(event.connection);
        const peer = this.peers.get(connection.peer)!;
        if (--peer.connections === 0) this.peers.delete(connection.peer);
        emit({
          case: "connectionClosed",
          value: {
            connAlias: event.connection,
            closedAtNs: event.observedAtNs,
          },
        });
        break;
      }
    }
  }

  private intern(value: string, emit: (payload: Payload) => void): number {
    const existing = this.strings.get(value);
    if (existing !== undefined) return existing;
    const id = this.strings.size + 1;
    this.strings.set(value, id);
    emit({ case: "stringDef", value: { id, value } });
    return id;
  }

  private snapshot(): Payload[] {
    return [
      ...Array.from(
        this.strings,
        ([value, id]): Payload => ({ case: "stringDef", value: { id, value } }),
      ),
      ...Array.from(
        this.peers.values(),
        ({ upsert }): Payload => ({ case: "peerUpsert", value: upsert }),
      ),
      ...Array.from(
        this.connections.values(),
        ({ upsert }): Payload => ({ case: "connectionUpsert", value: upsert }),
      ),
      ...Array.from(
        this.streams.values(),
        (value): Payload => ({
          case: "streamUpsert",
          value: { ...value, captureStartedMidstream: true },
        }),
      ),
    ];
  }
}

function direction(value: "inbound" | "outbound"): Direction {
  return value === "inbound" ? Direction.IN : Direction.OUT;
}
