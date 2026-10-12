import { create } from "@bufbuild/protobuf";
import type {
  Connection,
  Libp2p,
  Stream,
  StreamMiddleware,
  StreamMessageEvent,
} from "@libp2p/interface";
import type { Payload } from "./buffer.js";
import {
  ConnectionUpsertSchema,
  Direction,
  PeerUpsertSchema,
  StreamUpsertSchema,
} from "./gen/proto/xray/xray_pb.js";
import type {
  ConnectionUpsert,
  PeerUpsert,
  StreamUpsert,
} from "./gen/proto/xray/xray_pb.js";
import { IngestSession, now } from "./session.js";
import { ProbeError } from "./wire.js";

export { ProbeError } from "./wire.js";

export interface XrayOptions {
  address: string;
  clientName: string;
  maxBufferBytes?: number;
  retryMs?: number;
  onError?: (error: ProbeError) => void;
}

type Node = Pick<
  Libp2p,
  "peerId" | "getConnections" | "addEventListener" | "removeEventListener"
> & {
  use(middleware: StreamMiddleware): void;
  unuse(middleware: StreamMiddleware): void;
};
type PeerState = { upsert: PeerUpsert; connections: number };
type ConnectionState = { upsert: ConnectionUpsert; peer: string };
type StreamState = {
  upsert: StreamUpsert;
  detach: () => void;
  close: (reason: number) => void;
};

/** Requires libp2p's global use(middleware) and unuse(middleware) overloads. */
export class XrayProbe {
  private readonly session: IngestSession;
  private readonly strings = new Map<string, number>();
  private readonly peers = new Map<string, PeerState>();
  private readonly connections = new Map<string, ConnectionState>();
  // Closed streams may still deliver buffered reads. Weak ownership lets an
  // abandoned reader be collected instead of retaining it until probe.stop().
  private readonly streams = new Map<bigint, WeakRef<StreamState>>();
  private readonly finalizedStreams = new FinalizationRegistry<bigint>(
    (alias) => this.closeStream(alias, 3),
  );
  private readonly seenStreams = new WeakSet<Stream>();
  private nextPeer = 0n;
  private nextConnection = 0n;
  private nextStream = 0n;
  private stopped = false;

  constructor(
    private readonly node: Node,
    private readonly options: XrayOptions,
  ) {
    this.session = new IngestSession(
      {
        ...options,
        peerId: node.peerId.toMultihash().bytes,
      },
      () => this.snapshot(),
    );
    node.use(this.middleware);
    node.addEventListener("connection:open", this.onConnectionOpen);
    node.addEventListener("connection:close", this.onConnectionClose);
    for (const connection of node.getConnections()) {
      this.trackConnection(connection);
      for (const stream of connection.streams)
        this.trackStream(stream, connection, true);
    }
    this.session.start();
  }

  waitForAttach(signal?: AbortSignal): Promise<void> {
    return this.session.waitForAttach(signal);
  }

  get bufferedBytes(): number {
    return this.session.buffer.byteLength;
  }

  async stop(timeoutMs = 1000): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.node.unuse(this.middleware);
    this.node.removeEventListener("connection:open", this.onConnectionOpen);
    this.node.removeEventListener("connection:close", this.onConnectionClose);
    for (const ref of this.streams.values()) {
      const state = ref.deref();
      if (state) {
        state.detach();
        this.finalizedStreams.unregister(state);
      }
    }
    await this.session.stop(timeoutMs);
    this.streams.clear();
    this.connections.clear();
    this.peers.clear();
    this.strings.clear();
  }

  private readonly middleware: StreamMiddleware = (
    stream,
    connection,
    next,
  ) => {
    this.observe(() => this.trackStream(stream, connection, false));
    next(stream, connection);
  };

  private readonly onConnectionOpen = (
    event: CustomEvent<Connection>,
  ): void => {
    this.observe(() => this.trackConnection(event.detail));
  };

  private readonly onConnectionClose = (
    event: CustomEvent<Connection>,
  ): void => {
    this.observe(() => {
      const state = this.connections.get(event.detail.id);
      if (!state) return;
      for (const ref of this.streams.values()) {
        const stream = ref.deref();
        if (stream?.upsert.connAlias === state.upsert.connAlias)
          stream.close(3);
      }
      this.session.emit({
        case: "connectionClosed",
        value: { connAlias: state.upsert.connAlias, closedAtNs: now() },
      });
      this.connections.delete(event.detail.id);
      const peer = this.peers.get(state.peer)!;
      if (--peer.connections === 0) this.peers.delete(state.peer);
    });
  };

  private trackConnection(connection: Connection): ConnectionState {
    const existing = this.connections.get(connection.id);
    if (existing) return existing;
    const key = connection.remotePeer.toString();
    let peer = this.peers.get(key);
    if (!peer) {
      peer = {
        upsert: create(PeerUpsertSchema, {
          peerAlias: this.nextPeer++,
          peerId: connection.remotePeer.toMultihash().bytes,
        }),
        connections: 0,
      };
      this.peers.set(key, peer);
      this.session.emit({ case: "peerUpsert", value: peer.upsert });
    }
    peer.connections++;
    const components = connection.remoteAddr.getComponents();
    const transport = components.some((c) => c.name === "quic-v1")
      ? "quic-v1"
      : components.some((c) => c.name === "tcp")
        ? "tcp"
        : "";
    const upsert = create(ConnectionUpsertSchema, {
      connAlias: this.nextConnection++,
      peerAlias: peer.upsert.peerAlias,
      remoteAddr: connection.remoteAddr.toString(),
      direction: direction(connection.direction),
      transportId: this.intern(transport),
      securityId: this.intern(connection.encryption ?? ""),
      muxerId: this.intern(connection.multiplexer ?? ""),
      openedAtNs: BigInt(Math.trunc(connection.timeline.open)) * 1_000_000n,
    });
    const state = { upsert, peer: key };
    this.connections.set(connection.id, state);
    this.session.emit({ case: "connectionUpsert", value: upsert });
    return state;
  }

  private trackStream(
    stream: Stream,
    connection: Connection,
    midstream: boolean,
  ): void {
    if (this.seenStreams.has(stream) || this.stopped) return;
    this.seenStreams.add(stream);
    const conn = this.trackConnection(connection);
    const upsert = create(StreamUpsertSchema, {
      streamAlias: this.nextStream++,
      connAlias: conn.upsert.connAlias,
      protocolId: this.intern(stream.protocol),
      direction: direction(stream.direction),
      openedAtNs: BigInt(Math.trunc(stream.timeline.open)) * 1_000_000n,
      captureStartedMidstream: midstream,
    });
    this.session.emit({ case: "streamUpsert", value: upsert });
    const originalSend = stream.send;
    const originalDispatch = stream.dispatchEvent;
    const originalUnshift = stream.unshift;
    let pushedBack = 0;
    const send: Stream["send"] = (data) => {
      let copy: Uint8Array | undefined;
      this.observe(() => {
        copy = data.subarray().slice();
      });
      const result = originalSend.call(stream, data);
      if (copy)
        this.observe(() =>
          this.chunk(upsert.streamAlias, Direction.OUT, copy!),
        );
      return result;
    };
    // A message listener would consume buffered data before the protocol reader attaches.
    const dispatch: Stream["dispatchEvent"] = (event) => {
      if (event.type === "message") {
        const data = (event as StreamMessageEvent).data;
        const skip = Math.min(pushedBack, data.byteLength);
        pushedBack -= skip;
        this.observe(() =>
          this.chunk(upsert.streamAlias, Direction.IN, data.subarray(skip)),
        );
      }
      return originalDispatch.call(stream, event);
    };
    const unshift: Stream["unshift"] = (data) => {
      originalUnshift.call(stream, data);
      pushedBack += data.byteLength;
    };
    let closeReason: number | undefined;
    const close = (): void =>
      state.close(
        connection.status === "closed" || connection.status === "aborted"
          ? 3
          : stream.status === "reset" || stream.status === "aborted"
            ? 2
            : 1,
      );
    const end = (): void => {
      if (closeReason !== undefined) state.close(closeReason);
    };
    const state: StreamState = {
      upsert,
      close: (reason) => {
        const finalReason = (closeReason ??= reason);
        if (stream.readBufferLength === 0) {
          this.observe(() => this.closeStream(upsert.streamAlias, finalReason));
        }
      },
      detach: () => {
        if (stream.send === send) stream.send = originalSend;
        if (stream.dispatchEvent === dispatch)
          stream.dispatchEvent = originalDispatch;
        if (stream.unshift === unshift) stream.unshift = originalUnshift;
        stream.removeEventListener("close", close);
        stream.removeEventListener("end", end);
      },
    };
    stream.send = send;
    stream.dispatchEvent = dispatch;
    stream.unshift = unshift;
    stream.addEventListener("close", close, { once: true });
    stream.addEventListener("end", end, { once: true });
    this.streams.set(upsert.streamAlias, new WeakRef(state));
    this.finalizedStreams.register(state, upsert.streamAlias, state);
    if (
      stream.status === "closed" ||
      stream.status === "reset" ||
      stream.status === "aborted"
    )
      close();
  }

  private chunk(streamAlias: bigint, flow: Direction, data: Uint8Array): void {
    for (let offset = 0; offset < data.byteLength; offset += 64 * 1024) {
      this.session.emit({
        case: "streamChunk",
        value: {
          streamAlias,
          direction: flow,
          data: data.subarray(offset, offset + 64 * 1024),
        },
      });
    }
  }

  private closeStream(alias: bigint, reason: number): void {
    const ref = this.streams.get(alias);
    if (!ref) return;
    const state = ref.deref();
    if (state) {
      state.detach();
      this.finalizedStreams.unregister(state);
    }
    this.streams.delete(alias);
    this.session.emit({
      case: "streamClosed",
      value: { streamAlias: alias, closedAtNs: now(), reason },
    });
  }

  private intern(value: string): number {
    const existing = this.strings.get(value);
    if (existing !== undefined) return existing;
    const id = this.strings.size;
    this.strings.set(value, id);
    this.session.emit({ case: "stringDef", value: { id, value } });
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
        (peer): Payload => ({ case: "peerUpsert", value: peer.upsert }),
      ),
      ...Array.from(
        this.connections.values(),
        (conn): Payload => ({ case: "connectionUpsert", value: conn.upsert }),
      ),
      ...Array.from(this.streams.values()).flatMap((ref): Payload[] => {
        const stream = ref.deref();
        if (!stream) return [];
        stream.upsert.captureStartedMidstream = true;
        return [{ case: "streamUpsert", value: stream.upsert }];
      }),
    ];
  }

  private observe(fn: () => void): void {
    if (this.stopped) return;
    try {
      fn();
    } catch (cause) {
      try {
        this.options.onError?.(
          new ProbeError(
            "XRAY_CAPTURE_FAILED",
            "Unable to capture libp2p stream event",
            { cause },
          ),
        );
      } catch {}
    }
  }
}

function direction(value: "inbound" | "outbound"): Direction {
  return value === "inbound" ? Direction.IN : Direction.OUT;
}
