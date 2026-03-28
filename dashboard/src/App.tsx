import {
  createSignal, createMemo, createEffect,
  onMount, onCleanup, Show, For,
  type JSX,
} from 'solid-js';

// ── Data model ──────────────────────────────────────────────────────────

type ThemeName = "dark" | "light";

const THEME_VARS: Record<ThemeName, Record<`--${string}`, string>> = {
  dark: {
    "--0": "#080808", "--1": "#141414", "--2": "#222", "--3": "#666",
    "--fg": "#c8cacc", "--hi": "#fff",
    "--sel-bg": "#141414", "--sel-border": "#c8cacc",
    "--hover": "#0c0c0c",
    "--proto-gsub": "#e8c476", "--proto-reqr": "#76b8c8",
    "--proto-dv5": "#8cb876", "--proto-eth": "#c496d0",
    "--stream-opacity": "0.65", "--stream-dim": "0.12",
    "--bar-strong": "0.7", "--bar-weak": "0.3",
  },
  light: {
    "--0": "#f4f0e8", "--1": "#e8e3d8", "--2": "#d0c9ba", "--3": "#9e9585",
    "--fg": "#2c2822", "--hi": "#1a1714",
    "--sel-bg": "#e8e3d8", "--sel-border": "#2c2822",
    "--hover": "#ede8df",
    "--proto-gsub": "#b8872e", "--proto-reqr": "#2e7f92",
    "--proto-dv5": "#4d8a32", "--proto-eth": "#8b4fa0",
    "--stream-opacity": "0.65", "--stream-dim": "0.08",
    "--bar-strong": "0.75", "--bar-weak": "0.3",
  },
};

const PROTO_COLOR_MAP: Record<string, `--${string}`> = {
  gossipsub: "--proto-gsub", "req-resp": "--proto-reqr",
  discv5: "--proto-dv5", "eth-wire": "--proto-eth",
};

const FALLBACK_PALETTE = ["#e8c476", "#76b8c8", "#8cb876", "#c496d0", "#d4a574", "#74bba8", "#b8a474", "#a8749c"];

function getProtoColor(key: string, t: ThemeName): string {
  const mapped = PROTO_COLOR_MAP[key];
  if (mapped) return THEME_VARS[t][mapped] ?? "#888";
  let h = 0;
  for (let i = 0; i < key.length; i++) h = ((h << 5) - h + key.charCodeAt(i)) | 0;
  return FALLBACK_PALETTE[Math.abs(h) % FALLBACK_PALETTE.length];
}

const TOPICS_BY_PROTO: Record<string, string[]> = {
  gossipsub: [
    "beacon_block", "beacon_agg_proof", "blob_sidecar_0", "blob_sidecar_1",
    "blob_sidecar_2", "blob_sidecar_3", "sync_committee", "voluntary_exit",
  ],
  "req-resp": [
    "blocks_by_range", "blocks_by_root", "blobs_by_range", "blobs_by_root",
    "status", "metadata", "ping",
  ],
  discv5: ["findnode", "nodes", "ping", "pong"],
  "eth-wire": ["NewPooledTxHashes", "GetPooledTx", "PooledTx", "Transactions"],
};

const ALL_TOPICS = Object.values(TOPICS_BY_PROTO).flat();

const TOPIC_HUES: Record<string, number> = {};
{
  const hues = [
    30, 50, 175, 195, 115, 140, 275, 305,
    15, 65, 155, 210, 95, 245, 335, 55,
    185, 225, 105, 265, 5, 75, 145, 220,
  ];
  ALL_TOPICS.forEach((t, i) => { TOPIC_HUES[t] = hues[i % hues.length]; });
}

function topicColor(t: string, theme: ThemeName): string {
  let h = TOPIC_HUES[t];
  if (h === undefined) {
    let hash = 0;
    for (let i = 0; i < t.length; i++) hash = ((hash << 5) - hash + t.charCodeAt(i)) | 0;
    h = Math.abs(hash) % 360;
  }
  return theme === "dark" ? `hsl(${h}, 45%, 62%)` : `hsl(${h}, 50%, 38%)`;
}

type TopicValues = { i: number; e: number };
type ProtoData = { i: number; e: number; topics: Record<string, TopicValues> };

type SlotMetaData = {
  proposer_index?: number;
  attestation_count?: number;
  blob_commitments?: number;
  tx_count?: number;
};

type SlotData = {
  slot: number;
  protos: Record<string, ProtoData>;
  totalIn: number;
  totalOut: number;
  total: number;
  meta?: SlotMetaData;
};

// ── Flow data model ─────────────────────────────────────────────────────

type BleedDistData = Record<string, { in: number; out: number }>;

type FlowTimePoint = {
  offsetMs: number;
  flows: Record<string, { in: number; out: number; bleedIn: number; bleedOut: number; bleedByDist: BleedDistData }>;
};

// Normalize raw topic paths from the API into human-readable names.
// e.g. "/eth2/8c9f62fe/beacon_attestation_15/ssz_snappy" -> "beacon_attestation_15"
//      "/eth2/beacon_chain/req/status/2/ssz_snappy" -> "status"
//      "/meshsub/1.2.0" -> "meshsub"
function normalizeTopic(raw: string): string {
  let t = raw;
  // Strip /eth2/{hash}/ prefix (gossipsub topics)
  t = t.replace(/^\/eth2\/[0-9a-f]+\//, "");
  // Strip /eth2/beacon_chain/req/ prefix (req/resp)
  t = t.replace(/^\/eth2\/beacon.chain\/req\//, "");
  // Strip version + encoding suffixes (/2/ssz_snappy, /1/ssz_snappy, /ssz_snappy, /ssz)
  t = t.replace(/\/\d+\/ssz[_ ]snappy$/, "");
  t = t.replace(/\/ssz[_ ]snappy$/, "");
  t = t.replace(/\/ssz$/, "");
  // Strip leading slash
  if (t.startsWith("/")) t = t.slice(1);
  // Spaces to underscores
  t = t.replace(/ /g, "_");
  return t;
}

const TOPIC_GROUP_RULES: [RegExp, string][] = [
  [/^beacon_attestation_\d+$/, "attestation"],
  [/^beacon_attn_subnet_\d+$/, "attestation"],
  [/^blob_sidecar_\d+$/, "blob_sidecar"],
  [/^data_column_sidecar_\d+$/, "data_column_sidecar"],
  [/^sync_committee_\d+$/, "sync_committee"],
  [/^sync_committee_contribution_\d+$/, "sync_committee_contribution"],
];

function groupTopic(raw: string): string {
  const t = normalizeTopic(raw);
  for (const [re, group] of TOPIC_GROUP_RULES) {
    if (re.test(t)) return group;
  }
  return t;
}

type FlowChild = {
  topic: string;
  protocol: string;
  dataIn: number;
  dataOut: number;
  bleedIn: number;
  bleedOut: number;
  bleedByDist: BleedDistData;
  control: Record<string, { in: number; out: number }>;
  totalIn: number;
  totalOut: number;
};

type FlowRow = {
  flow: string;
  protocol: string;
  dataIn: number;
  dataOut: number;
  bleedIn: number;
  bleedOut: number;
  bleedByDist: BleedDistData;
  control: Record<string, { in: number; out: number }>;
  totalIn: number;
  totalOut: number;
  children: FlowChild[];
};

const CONTROL_KINDS = ['IHAVE', 'IWANT', 'GRAFT', 'PRUNE', 'IDONTWANT', 'SUBSCRIBE', 'UNSUBSCRIBE'];

// Protocol-level overhead (framing, topic-less control messages)
type ProtocolRow = {
  protocol: string;
  framing: { in: number; out: number };
  control: Record<string, { in: number; out: number }>;
  totalIn: number;
  totalOut: number;
};

function isGossipProtocol(protocol: string): boolean {
  const p = protocol.toLowerCase();
  return p.includes("meshsub") || p.includes("floodsub") || p.includes("gossipsub");
}

function deriveProtocols(breakdown: ApiBucketBreakdown[]): ProtocolRow[] {
  const byProto = new Map<string, ProtocolRow>();
  for (const row of breakdown) {
    if (row.bytes_in === 0 && row.bytes_out === 0) continue;
    // Only capture gossipsub protocol overhead (topic-less framing + control)
    if (row.topic !== "" && row.topic !== undefined) continue;
    if (!isGossipProtocol(row.protocol)) continue;
    const pk = normalizeTopic(row.protocol);
    let entry = byProto.get(pk);
    if (!entry) {
      entry = { protocol: pk, framing: { in: 0, out: 0 }, control: {}, totalIn: 0, totalOut: 0 };
      byProto.set(pk, entry);
    }
    entry.totalIn += row.bytes_in;
    entry.totalOut += row.bytes_out;
    if (CONTROL_KINDS.includes(row.message_kind)) {
      if (!entry.control[row.message_kind]) entry.control[row.message_kind] = { in: 0, out: 0 };
      entry.control[row.message_kind].in += row.bytes_in;
      entry.control[row.message_kind].out += row.bytes_out;
    } else {
      // Framing or unknown message kinds
      entry.framing.in += row.bytes_in;
      entry.framing.out += row.bytes_out;
    }
  }
  return [...byProto.values()]
    .filter(p => p.totalIn + p.totalOut > 0)
    .sort((a, b) => (b.totalIn + b.totalOut) - (a.totalIn + a.totalOut));
}

function deriveFlows(breakdown: ApiBucketBreakdown[]): FlowRow[] {
  const byFlow = new Map<string, FlowRow>();
  // Track per-topic children within each flow
  const childMap = new Map<string, Map<string, FlowChild>>();

  for (const row of breakdown) {
    if (row.bytes_in === 0 && row.bytes_out === 0) continue;
    // Skip gossipsub overhead (goes to deriveProtocols)
    if (!row.topic && isGossipProtocol(row.protocol)) continue;
    // Use normalized protocol name as flow for topic-less req/resp RPCs
    const source = row.topic || row.protocol;
    const flow = groupTopic(source);
    const rawTopic = normalizeTopic(source);

    // Update flow aggregate
    let entry = byFlow.get(flow);
    if (!entry) {
      entry = { flow, protocol: row.protocol, dataIn: 0, dataOut: 0, bleedIn: 0, bleedOut: 0, bleedByDist: {}, control: {}, totalIn: 0, totalOut: 0, children: [] };
      byFlow.set(flow, entry);
      childMap.set(flow, new Map());
    }
    entry.totalIn += row.bytes_in;
    entry.totalOut += row.bytes_out;
    entry.bleedIn += row.bleed_bytes_in ?? 0;
    entry.bleedOut += row.bleed_bytes_out ?? 0;
    if (row.bleed_by_distance) {
      for (const [dist, val] of Object.entries(row.bleed_by_distance)) {
        if (!entry.bleedByDist[dist]) entry.bleedByDist[dist] = { in: 0, out: 0 };
        entry.bleedByDist[dist].in += val.bytes_in;
        entry.bleedByDist[dist].out += val.bytes_out;
      }
    }
    if (CONTROL_KINDS.includes(row.message_kind)) {
      if (!entry.control[row.message_kind]) entry.control[row.message_kind] = { in: 0, out: 0 };
      entry.control[row.message_kind].in += row.bytes_in;
      entry.control[row.message_kind].out += row.bytes_out;
    } else {
      entry.dataIn += row.bytes_in;
      entry.dataOut += row.bytes_out;
    }

    // Update child (only if topic was grouped, i.e. rawTopic differs from flow)
    if (rawTopic !== flow) {
      const cm = childMap.get(flow)!;
      let child = cm.get(rawTopic);
      if (!child) {
        child = { topic: rawTopic, protocol: row.protocol, dataIn: 0, dataOut: 0, bleedIn: 0, bleedOut: 0, bleedByDist: {}, control: {}, totalIn: 0, totalOut: 0 };
        cm.set(rawTopic, child);
      }
      child.totalIn += row.bytes_in;
      child.totalOut += row.bytes_out;
      child.bleedIn += row.bleed_bytes_in ?? 0;
      child.bleedOut += row.bleed_bytes_out ?? 0;
      if (row.bleed_by_distance) {
        for (const [dist, val] of Object.entries(row.bleed_by_distance)) {
          if (!child.bleedByDist[dist]) child.bleedByDist[dist] = { in: 0, out: 0 };
          child.bleedByDist[dist].in += val.bytes_in;
          child.bleedByDist[dist].out += val.bytes_out;
        }
      }
      if (CONTROL_KINDS.includes(row.message_kind)) {
        if (!child.control[row.message_kind]) child.control[row.message_kind] = { in: 0, out: 0 };
        child.control[row.message_kind].in += row.bytes_in;
        child.control[row.message_kind].out += row.bytes_out;
      } else {
        child.dataIn += row.bytes_in;
        child.dataOut += row.bytes_out;
      }
    }
  }

  // Attach sorted children to each flow
  for (const [flow, entry] of byFlow) {
    const cm = childMap.get(flow);
    if (cm && cm.size > 0) {
      entry.children = [...cm.values()].sort((a, b) => (b.totalIn + b.totalOut) - (a.totalIn + a.totalOut));
    }
  }

  return [...byFlow.values()]
    .filter(f => f.totalIn + f.totalOut > 0)
    .sort((a, b) => (b.totalIn + b.totalOut) - (a.totalIn + a.totalOut));
}

function deriveFlowTimeSeries(buckets: ApiSlotBucketPoint[]): FlowTimePoint[] {
  const byOffset = new Map<number, ApiSlotBucketPoint>();
  for (const b of buckets) {
    const snapped = Math.round(b.offset_ms / TICK_MS) * TICK_MS;
    const existing = byOffset.get(snapped);
    if (existing) {
      existing.bytes_in += b.bytes_in;
      existing.bytes_out += b.bytes_out;
      if (b.breakdown) {
        if (!existing.breakdown) existing.breakdown = [];
        existing.breakdown.push(...b.breakdown);
      }
    } else {
      byOffset.set(snapped, { ...b, offset_ms: snapped, breakdown: b.breakdown ? [...b.breakdown] : [] });
    }
  }

  const result: FlowTimePoint[] = [];
  for (let i = 0; i < POINTS_PER_SLOT; i++) {
    const ms = i * TICK_MS;
    const b = byOffset.get(ms);
    const flows: Record<string, { in: number; out: number; bleedIn: number; bleedOut: number; bleedByDist: BleedDistData }> = {};
    if (b) {
      for (const row of b.breakdown ?? []) {
        const flow = groupTopic(row.topic || row.protocol);
        if (!flows[flow]) flows[flow] = { in: 0, out: 0, bleedIn: 0, bleedOut: 0, bleedByDist: {} };
        flows[flow].in += row.bytes_in;
        flows[flow].out += row.bytes_out;
        flows[flow].bleedIn += row.bleed_bytes_in ?? 0;
        flows[flow].bleedOut += row.bleed_bytes_out ?? 0;
        if (row.bleed_by_distance) {
          for (const [dist, val] of Object.entries(row.bleed_by_distance)) {
            if (!flows[flow].bleedByDist[dist]) flows[flow].bleedByDist[dist] = { in: 0, out: 0 };
            flows[flow].bleedByDist[dist].in += val.bytes_in;
            flows[flow].bleedByDist[dist].out += val.bytes_out;
          }
        }
      }
    }
    result.push({ offsetMs: ms, flows });
  }
  return result;
}

// ── API types (match Go backend JSON) ───────────────────────────────────

type ApiSlotSummary = {
  slot: number; epoch: number;
  slot_start_ns: number; slot_end_ns: number;
  bytes_in: number; bytes_out: number;
  finalized: boolean; last_updated_ns: number;
  meta?: SlotMetaData;
};

type ApiBucketBreakdown = {
  protocol: string; topic: string; message_kind: string;
  bytes_in: number; bytes_out: number;
  bleed_bytes_in?: number; bleed_bytes_out?: number;
  bleed_by_distance?: Record<string, { bytes_in: number; bytes_out: number }>;
};

type ApiSlotBucketPoint = {
  offset_ms: number; bytes_in: number; bytes_out: number;
  breakdown?: ApiBucketBreakdown[];
};

type ApiSlotDetail = {
  summary: ApiSlotSummary;
  buckets: ApiSlotBucketPoint[];
  breakdown: ApiBucketBreakdown[];
};

type ApiPeer = {
  peer_id: string;
  connections: { remote_addr: string; direction: string; transport: string; opened_at_ns: number }[];
  first_seen_ns: number;
  last_seen_ns: number;
};

type ApiWsMessage = {
  type?: string;
  current_slot?: number;
  slot?: ApiSlotSummary;
  slots?: ApiSlotSummary[];
  source_id?: string;
  peer_count?: number;
};

// ── API-to-UI converters ────────────────────────────────────────────────

function groupBreakdown(rows: ApiBucketBreakdown[]): Record<string, ProtoData> {
  const protos: Record<string, ProtoData> = {};
  for (const row of rows) {
    const pk = row.protocol;
    if (!protos[pk]) protos[pk] = { i: 0, e: 0, topics: {} };
    protos[pk].i += row.bytes_in;
    protos[pk].e += row.bytes_out;
    const tk = row.topic || "(none)";
    if (!protos[pk].topics[tk]) protos[pk].topics[tk] = { i: 0, e: 0 };
    protos[pk].topics[tk].i += row.bytes_in;
    protos[pk].topics[tk].e += row.bytes_out;
  }
  return protos;
}

function apiSummaryToSlotData(s: ApiSlotSummary): SlotData {
  return { slot: s.slot, protos: {}, totalIn: s.bytes_in, totalOut: s.bytes_out, total: s.bytes_in + s.bytes_out, meta: s.meta };
}

function apiDetailToSlotData(d: ApiSlotDetail): SlotData {
  const protos = groupBreakdown(d.breakdown);
  return { slot: d.summary.slot, protos, totalIn: d.summary.bytes_in, totalOut: d.summary.bytes_out, total: d.summary.bytes_in + d.summary.bytes_out };
}

const SLOT_DURATION_MS = 12000;
const TICK_MS = 500;
const POINTS_PER_SLOT = SLOT_DURATION_MS / TICK_MS + 1;
const LIVE_SLOTS_FLUSH_MS = 120;

const KB = 1024, MB = 1048576;
const Y_CEILINGS = [
  10*KB, 20*KB, 50*KB, 100*KB, 200*KB, 500*KB,
  1*MB, 2*MB, 5*MB, 10*MB, 20*MB, 50*MB,
];
function snapCeiling(v: number): number {
  for (const c of Y_CEILINGS) { if (c >= v) return c; }
  return Y_CEILINGS[Y_CEILINGS.length - 1];
}

// ── Diverging chart computation ─────────────────────────────────────────

type Layer = { key: string; color: string; label: string };
type StreamPath = { key: string; color: string; label: string; d: string };

function computeDiverging<T>(
  points: T[],
  layers: Layer[],
  getIn: (pt: T, key: string) => number,
  getOut: (pt: T, key: string) => number,
  getBleedIn?: (pt: T, key: string) => number,
  getBleedOut?: (pt: T, key: string) => number,
): { paths: StreamPath[]; bleedPaths: StreamPath[]; W: number; H: number } {
  const n = points.length, k = layers.length;
  if (!n || !k) return { paths: [], bleedPaths: [], W: 0, H: 0 };

  const W = 1000, H = 400;
  const xStep = W / (n - 1 || 1);
  const mid = H / 2;
  const pad = 20;

  let maxVal = 1;
  for (const pt of points) {
    let up = 0, down = 0;
    for (const l of layers) { up += getOut(pt, l.key); down += getIn(pt, l.key); }
    maxVal = Math.max(maxVal, up, down);
  }
  const ceil = snapCeiling(maxVal);
  const scale = (mid - pad) / ceil;

  const paths: StreamPath[] = [];
  const bleedPaths: StreamPath[] = [];

  // Ingress (upward from center)
  for (let li = 0; li < k; li++) {
    const topPts: [number, number][] = [];
    const botPts: [number, number][] = [];
    for (let pi = 0; pi < n; pi++) {
      const x = pi * xStep;
      let cumul = 0;
      for (let j = 0; j < li; j++) cumul += getIn(points[pi], layers[j].key);
      const val = getIn(points[pi], layers[li].key);
      botPts.push([x, mid - cumul * scale]);
      topPts.push([x, mid - (cumul + val) * scale]);
    }
    paths.push({
      key: layers[li].key + "-in", color: layers[li].color, label: layers[li].label,
      d: `${smooth(topPts)} L${smooth([...botPts].reverse()).slice(1)} Z`,
    });

    // Bleed overlay: outer slice of ingress band
    if (getBleedIn) {
      let hasBleed = false;
      const bleedTop: [number, number][] = [];
      const bleedBot: [number, number][] = [];
      for (let pi = 0; pi < n; pi++) {
        const x = pi * xStep;
        let cumul = 0;
        for (let j = 0; j < li; j++) cumul += getIn(points[pi], layers[j].key);
        const val = getIn(points[pi], layers[li].key);
        const bleed = getBleedIn(points[pi], layers[li].key);
        if (bleed > 0) hasBleed = true;
        const outerY = mid - (cumul + val) * scale;
        const innerY = mid - (cumul + val - Math.min(bleed, val)) * scale;
        bleedTop.push([x, outerY]);
        bleedBot.push([x, innerY]);
      }
      if (hasBleed) {
        bleedPaths.push({
          key: layers[li].key + "-in-bleed", color: layers[li].color, label: layers[li].label,
          d: `${smooth(bleedTop)} L${smooth([...bleedBot].reverse()).slice(1)} Z`,
        });
      }
    }
  }

  // Egress (downward from center)
  for (let li = 0; li < k; li++) {
    const topPts: [number, number][] = [];
    const botPts: [number, number][] = [];
    for (let pi = 0; pi < n; pi++) {
      const x = pi * xStep;
      let cumul = 0;
      for (let j = 0; j < li; j++) cumul += getOut(points[pi], layers[j].key);
      const val = getOut(points[pi], layers[li].key);
      topPts.push([x, mid + cumul * scale]);
      botPts.push([x, mid + (cumul + val) * scale]);
    }
    paths.push({
      key: layers[li].key + "-out", color: layers[li].color, label: layers[li].label,
      d: `${smooth(botPts)} L${smooth([...topPts].reverse()).slice(1)} Z`,
    });

    // Bleed overlay: outer slice of egress band
    if (getBleedOut) {
      let hasBleed = false;
      const bleedOuter: [number, number][] = [];
      const bleedInner: [number, number][] = [];
      for (let pi = 0; pi < n; pi++) {
        const x = pi * xStep;
        let cumul = 0;
        for (let j = 0; j < li; j++) cumul += getOut(points[pi], layers[j].key);
        const val = getOut(points[pi], layers[li].key);
        const bleed = getBleedOut(points[pi], layers[li].key);
        if (bleed > 0) hasBleed = true;
        const outerY = mid + (cumul + val) * scale;
        const innerY = mid + (cumul + val - Math.min(bleed, val)) * scale;
        bleedOuter.push([x, outerY]);
        bleedInner.push([x, innerY]);
      }
      if (hasBleed) {
        bleedPaths.push({
          key: layers[li].key + "-out-bleed", color: layers[li].color, label: layers[li].label,
          d: `${smooth(bleedOuter)} L${smooth([...bleedInner].reverse()).slice(1)} Z`,
        });
      }
    }
  }

  return { paths, bleedPaths, W, H };
}

function smooth(pts: [number, number][]): string {
  if (pts.length < 2) return `M${pts[0][0]},${pts[0][1]}`;
  let d = `M${pts[0][0].toFixed(1)},${pts[0][1].toFixed(1)}`;
  for (let i = 1; i < pts.length; i++) {
    const p = pts[i - 1], c = pts[i], cx = (p[0] + c[0]) / 2;
    d += ` C${cx.toFixed(1)},${p[1].toFixed(1)} ${cx.toFixed(1)},${c[1].toFixed(1)} ${c[0].toFixed(1)},${c[1].toFixed(1)}`;
  }
  return d;
}

// ── Small components ────────────────────────────────────────────────────

function Kbd(props: { children: JSX.Element }) {
  return (
    <kbd style={{
      display: "inline-block", padding: "0 4px", "font-size": "var(--t-sm)",
      "font-family": "var(--m)", color: "var(--3)", background: "var(--0)",
      border: "1px solid var(--2)", "line-height": "var(--lh-dense)",
    }}>
      {props.children}
    </kbd>
  );
}

// ── Command palette ─────────────────────────────────────────────────────

function CmdPalette(props: { onClose: () => void; onCmd: (id: string) => void }) {
  const [q, setQ] = createSignal("");
  const [selIdx, setSelIdx] = createSignal(0);
  let inputRef: HTMLInputElement | undefined;
  let listRef: HTMLDivElement | undefined;

  onMount(() => setTimeout(() => inputRef?.focus(), 0));

  const cmds = [
    { id: "view:slots", l: "View: Slots" },
    { id: "view:peers", l: "View: Peers" },
    { id: "theme", l: "Toggle theme" },
  ];

  const filtered = createMemo(() => {
    const query = q();
    return query ? cmds.filter(c => c.l.toLowerCase().includes(query.toLowerCase())) : cmds;
  });

  const execute = (cmd: typeof cmds[0]) => { props.onCmd(cmd.id); props.onClose(); };

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Escape") { props.onClose(); }
    else if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelIdx(i => Math.min(i + 1, filtered().length - 1));
      listRef?.children[selIdx()]?.scrollIntoView({ block: "nearest" });
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelIdx(i => Math.max(i - 1, 0));
      listRef?.children[selIdx()]?.scrollIntoView({ block: "nearest" });
    } else if (e.key === "Enter") {
      e.preventDefault();
      const cmd = filtered()[selIdx()];
      if (cmd) execute(cmd);
    }
  };

  return (
    <div
      role="dialog"
      aria-label="Command palette"
      style={{
        position: "fixed", inset: "0", "z-index": 100,
        background: "rgba(0,0,0,0.5)", display: "flex",
        "justify-content": "center", "padding-top": "14vh",
      }}
      onClick={props.onClose}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: "420px", "max-height": "340px",
          background: "var(--0)", border: "2px solid var(--fg)", overflow: "hidden",
        }}
      >
        <div style={{
          padding: "8px 12px", "border-bottom": "2px solid var(--2)",
          display: "flex", gap: "8px", "align-items": "center",
        }}>
          <span style={{
            color: "var(--3)", "font-family": "var(--m)",
            "font-size": "var(--t-lg)", "font-weight": "700",
          }}>CMD</span>
          <input
            ref={inputRef!}
            value={q()}
            onInput={(e) => { setQ(e.currentTarget.value); setSelIdx(0); }}
            onKeyDown={handleKeyDown}
            placeholder="type to filter..."
            role="combobox"
            aria-expanded="true"
            aria-autocomplete="list"
            style={{
              flex: 1, background: "none", border: "none", outline: "none",
              color: "var(--fg)", "font-family": "var(--m)", "font-size": "var(--t-lg)",
            }}
          />
        </div>
        <div ref={listRef!} role="listbox" style={{ "max-height": "280px", "overflow-y": "auto" }}>
          <For each={filtered()}>
            {(c, i) => (
              <div
                role="option"
                aria-selected={i() === selIdx()}
                onClick={() => execute(c)}
                onMouseEnter={() => setSelIdx(i())}
                style={{
                  padding: "6px 12px", cursor: "pointer",
                  "font-family": "var(--m)", "font-size": "var(--t-md)",
                  color: "var(--fg)", "border-bottom": "1px solid var(--1)",
                  background: i() === selIdx() ? "var(--1)" : "transparent",
                }}
              >
                {c.l}
              </div>
            )}
          </For>
        </div>
      </div>
    </div>
  );
}

// ── StreamGraph (diverging: ingress up, egress down) ────────────────────

const TICKS = Array.from({ length: POINTS_PER_SLOT }, (_, i) => i * TICK_MS);

function StreamGraph(props: {
  points: FlowTimePoint[];
  flowTotals: FlowRow[];
  theme: ThemeName;
  highlightedFlow: string | null;
  onFlowHover: (flow: string | null) => void;
  followMode?: boolean;
  slotStartNs?: number;
}) {
  const [hoverX, setHoverX] = createSignal<number | null>(null);
  const [hoveredLayer, setHoveredLayer] = createSignal<string | null>(null);
  let svgRef: SVGSVGElement | undefined;

  const effectiveHighlight = () => props.highlightedFlow ?? hoveredLayer();

  // Slot lifecycle ordering: flows appear roughly in this sequence within a slot
  const FLOW_ORDER: Record<string, number> = {
    beacon_block: 0, blob_sidecar: 1, data_column_sidecar: 2,
    attestation: 3, beacon_attestation: 3,
    beacon_aggregate_and_proof: 4, sync_committee: 5,
    sync_committee_contribution_and_proof: 6, voluntary_exit: 7,
    proposer_slashing: 8, attester_slashing: 9,
  };
  const flowOrder = (key: string) => FLOW_ORDER[key] ?? 50;

  const layers = createMemo((): Layer[] => {
    const totals: Record<string, number> = {};
    for (const pt of props.points) {
      for (const [flow, v] of Object.entries(pt.flows)) {
        totals[flow] = (totals[flow] ?? 0) + v.in + v.out;
      }
    }
    return Object.entries(totals)
      .filter(e => e[1] > 0)
      .sort((a, b) => flowOrder(a[0]) - flowOrder(b[0]))
      .slice(0, 12)
      .map(([flow]) => ({ key: flow, color: topicColor(flow, props.theme), label: flow.replace(/_/g, " ") }));
  });

  const getIn = (pt: FlowTimePoint, flow: string): number => pt.flows[flow]?.in ?? 0;
  const getOut = (pt: FlowTimePoint, flow: string): number => pt.flows[flow]?.out ?? 0;
  const getBleedIn = (pt: FlowTimePoint, flow: string): number => pt.flows[flow]?.bleedIn ?? 0;
  const getBleedOut = (pt: FlowTimePoint, flow: string): number => pt.flows[flow]?.bleedOut ?? 0;

  // Bleed distance tint: color based on max distance bucket present
  const BLEED_DIST_COLORS: Record<string, string> = {
    "1": "hsla(0, 40%, 50%, 0.3)",
    "2": "hsla(0, 60%, 45%, 0.4)",
    "3": "hsla(0, 80%, 40%, 0.5)",
    "4+": "hsla(0, 100%, 35%, 0.6)",
  };
  const BLEED_DIST_KEYS = ["1", "2", "3", "4+"];

  // Compute the worst (most distant) bleed bucket across all time points for a given flow
  const bleedTintForFlow = (flow: string): string | null => {
    let maxDist = 0;
    for (const pt of props.points) {
      const f = pt.flows[flow];
      if (!f) continue;
      for (const dk of BLEED_DIST_KEYS) {
        const d = f.bleedByDist[dk];
        if (d && (d.in > 0 || d.out > 0)) {
          const num = dk === "4+" ? 4 : parseInt(dk);
          if (num > maxDist) maxDist = num;
        }
      }
    }
    if (maxDist === 0) return null;
    const key = maxDist >= 4 ? "4+" : String(maxDist);
    return BLEED_DIST_COLORS[key] ?? null;
  };

  const stream = createMemo(() => computeDiverging(props.points, layers(), getIn, getOut, getBleedIn, getBleedOut));
  const streamOp = () => 0.65;
  const dimOp = () => props.theme === "dark" ? 0.12 : 0.08;
  const n = () => props.points.length;

  const [playheadX, setPlayheadX] = createSignal<number | null>(null);
  createEffect(() => {
    if (!props.followMode || !props.slotStartNs) { setPlayheadX(null); return; }
    const startMs = props.slotStartNs / 1e6;
    let raf: number;
    const tick = () => {
      const elapsed = Date.now() - startMs;
      const frac = Math.min(elapsed / SLOT_DURATION_MS, 1);
      setPlayheadX(frac * stream().W);
      if (frac < 1) raf = requestAnimationFrame(tick);
      else setPlayheadX(null);
    };
    raf = requestAnimationFrame(tick);
    onCleanup(() => cancelAnimationFrame(raf));
  });

  const baseKey = (k: string) => k.replace(/-(?:in|out)$/, "");

  const [mousePos, setMousePos] = createSignal<{ x: number; y: number }>({ x: 0, y: 0 });
  const [hoverFrac, setHoverFrac] = createSignal<number | null>(null);

  const handleMouseMove = (e: MouseEvent) => {
    const rect = svgRef?.getBoundingClientRect();
    if (!rect) return;
    const frac = Math.max(0, Math.min((e.clientX - rect.left) / rect.width, 1));
    setHoverFrac(frac);
    setHoverX(Math.max(0, Math.min(Math.round(frac * (n() - 1)), n() - 1)));
    setMousePos({ x: e.clientX - rect.left, y: e.clientY - rect.top });
  };

  return (
    <Show when={stream().paths.length > 0}>
      <div style={{ height: "100%", display: "flex" }}>
        {/* Chart area */}
        <div style={{ flex: 1, position: "relative", "min-width": 0 }}>
        <svg
          ref={svgRef!}
          role="img"
          aria-label="Slot bandwidth: ingress above zero axis, egress below"
          width="100%" height="100%"
          viewBox={`0 0 ${stream().W} ${stream().H}`}
          preserveAspectRatio="none"
          style={{ display: "block", cursor: "default", flex: 1, "min-height": 0 }}
          onMouseMove={handleMouseMove}
          onMouseLeave={() => { setHoverX(null); setHoverFrac(null); setHoveredLayer(null); props.onFlowHover(null); }}
        >
          <defs>
            <pattern id="bleed-hatch" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
              <line x1="0" y1="0" x2="0" y2="4" stroke="var(--0)" stroke-width="1.5" />
            </pattern>
          </defs>

          {/* 500ms time grid */}
          {TICKS.map(ms => {
            const x = (ms / SLOT_DURATION_MS) * stream().W;
            const isSec = ms % 1000 === 0;
            return (
              <line
                x1={x} y1={0} x2={x} y2={stream().H}
                stroke="var(--2)"
                stroke-width={isSec ? 0.6 : 0.3}
                stroke-dasharray={isSec ? "none" : "2,6"}
              />
            );
          })}

          {/* 0 axis */}
          <line
            x1={0} y1={stream().H / 2} x2={stream().W} y2={stream().H / 2}
            stroke="var(--3)" stroke-width={1}
          />

          {/* stream layers (highlighted flow rendered last for z-order) */}
          {(() => {
            const hl = effectiveHighlight();
            const sorted = hl
              ? [...stream().paths].sort((a, b) => {
                  const aHl = baseKey(a.key) === hl ? 1 : 0;
                  const bHl = baseKey(b.key) === hl ? 1 : 0;
                  return aHl - bHl;
                })
              : stream().paths;
            return sorted.map(p => (
              <path
                d={p.d} fill={p.color}
                opacity={hl === null ? streamOp() : hl === baseKey(p.key) ? 0.9 : dimOp()}
                style={{ transition: "opacity 0.15s, d 0.4s ease" }}
                onMouseEnter={() => { setHoveredLayer(baseKey(p.key)); props.onFlowHover(baseKey(p.key)); }}
                onMouseLeave={() => { setHoveredLayer(null); props.onFlowHover(null); }}
              />
            ));
          })()}

          {/* bleed overlays (interactive for hover) */}
          {stream().bleedPaths.map(p => {
            const flowKey = baseKey(p.key.replace(/-bleed$/, ""));
            const hl = effectiveHighlight();
            const isHl = hl === flowKey;
            const tint = isHl ? bleedTintForFlow(flowKey) : null;
            return (
              <>
                {tint && <path d={p.d} fill={tint} opacity={0.9} />}
                <path
                  d={p.d} fill="url(#bleed-hatch)" opacity={isHl ? 0.9 : 0.8}
                  style={{ cursor: "default" }}
                  onMouseEnter={() => { setHoveredLayer(flowKey); props.onFlowHover(flowKey); }}
                  onMouseLeave={() => { setHoveredLayer(null); props.onFlowHover(null); }}
                />
              </>
            );
          })}

          {/* playhead (follow mode) */}
          <Show when={playheadX() !== null}>
            <line
              x1={playheadX()!} y1={0} x2={playheadX()!} y2={stream().H - 6}
              stroke="var(--fg)" stroke-width={0.5} opacity={0.5} stroke-dasharray="1,3"
            />
            <polygon
              points={`${playheadX()! - 3},${stream().H} ${playheadX()! + 3},${stream().H} ${playheadX()!},${stream().H - 6}`}
              fill="var(--fg)" opacity={0.5}
            />
          </Show>

          {/* y-axis ticks */}
          {(() => {
            const { H } = stream();
            const mid = H / 2;
            const pad = 20;
            let maxVal = 1;
            for (const pt of props.points) {
              let up = 0, down = 0;
              for (const l of layers()) { up += getOut(pt, l.key); down += getIn(pt, l.key); }
              maxVal = Math.max(maxVal, up, down);
            }
            const ceil = snapCeiling(maxVal);
            const divisions = 5;
            const step = ceil / divisions;
            const scale = (mid - pad) / ceil;
            const fmt = (v: number) => v >= MB ? (v / MB).toFixed(v % MB === 0 ? 0 : 1) + " MiB" : (v / KB).toFixed(v % KB === 0 ? 0 : 0) + " KiB";
            const ticks: number[] = [];
            for (let i = 1; i <= divisions; i++) ticks.push(i * step);
            return ticks.map(v => (
              <>
                <line x1={0} y1={mid - v * scale} x2={stream().W} y2={mid - v * scale} stroke="var(--2)" stroke-width={0.3} style={{ transition: "y1 0.4s ease, y2 0.4s ease" }} />
                <line x1={0} y1={mid + v * scale} x2={stream().W} y2={mid + v * scale} stroke="var(--2)" stroke-width={0.3} style={{ transition: "y1 0.4s ease, y2 0.4s ease" }} />
                <text x={stream().W - 4} y={mid - v * scale - 3} fill="var(--3)" font-size="9" font-family="var(--m)" text-anchor="end" style={{ transition: "y 0.4s ease" }}>{fmt(v)}</text>
                <text x={stream().W - 4} y={mid + v * scale + 10} fill="var(--3)" font-size="9" font-family="var(--m)" text-anchor="end" style={{ transition: "y 0.4s ease" }}>{fmt(v)}</text>
              </>
            ));
          })()}

          {/* hover crosshair */}
          <Show when={hoverFrac() !== null}>
            {(() => {
              const xPos = () => hoverFrac()! * stream().W;
              const timeMs = () => hoverFrac()! * SLOT_DURATION_MS;
              const timeFmt = () => (timeMs() / 1000).toFixed(2) + "s";
              return (
                <>
                  <line
                    x1={xPos()} y1={0} x2={xPos()} y2={stream().H}
                    stroke="var(--fg)" stroke-width={0.5} opacity={0.15}
                  />
                  <text
                    x={xPos()} y={stream().H - 4}
                    fill="var(--3)" font-size="8" font-family="var(--m)"
                    text-anchor="middle" opacity={0.5}
                  >{timeFmt()}</text>
                </>
              );
            })()}
          </Show>
        </svg>

        {/* direction labels */}
        <span style={{
          position: "absolute", left: "6px", top: "calc(50% - 16px)",
          "font-family": "var(--m)", "font-size": "var(--t-sm)", color: "var(--3)",
          "letter-spacing": "var(--track-caps)", opacity: 0.5, "pointer-events": "none",
        }}>RCVD</span>
        <span style={{
          position: "absolute", left: "6px", top: "calc(50% + 8px)",
          "font-family": "var(--m)", "font-size": "var(--t-sm)", color: "var(--3)",
          "letter-spacing": "var(--track-caps)", opacity: 0.5, "pointer-events": "none",
        }}>SENT</span>

        {/* hover tooltip */}
        <Show when={hoverX() !== null && props.points[hoverX()!]}>
          {(() => {
            const pt = () => props.points[hoverX()!];
            const hl = () => effectiveHighlight();
            const spotData = () => hl() ? pt()?.flows[hl()!] : null;
            const totalData = () => hl() ? props.flowTotals.find(f => f.flow === hl()) : null;
            const fK = (v: number) => v < 50 ? "0" : (v / 1024).toFixed(1);

            const distHisto = (dist: BleedDistData, bleedTotal: number) =>
              BLEED_DIST_KEYS
                .map(k => ({ key: k, val: dist[k] }))
                .filter(e => e.val && (e.val.in > 0 || e.val.out > 0))
                .map(e => {
                  const total = e.val!.in + e.val!.out;
                  const pct = bleedTotal > 0 ? (total / bleedTotal) * 100 : 0;
                  const label = e.key === "4+" ? "-4+ slots" : `-${e.key} slot${e.key === "1" ? "" : "s"}`;
                  return (
                    <div style={{ display: "flex", "align-items": "center", gap: "4px", "margin-top": "1px" }}>
                      <span style={{ width: "6px", height: "6px", "flex-shrink": 0, background: BLEED_DIST_COLORS[e.key], display: "inline-block" }} />
                      <span style={{ color: "var(--3)", width: "52px", "font-size": "var(--t-xs)" }}>{label}</span>
                      <div style={{ flex: 1, height: "3px", background: "var(--1)", "min-width": "20px" }}>
                        <div style={{ height: "100%", background: BLEED_DIST_COLORS[e.key], width: `${Math.min(pct, 100)}%` }} />
                      </div>
                      <span style={{ color: "var(--3)", "font-size": "var(--t-xs)", width: "40px", "text-align": "right" }}>{fK(total)}</span>
                    </div>
                  );
                });

            return (
              <div style={{
                position: "absolute",
                left: `${Math.min(mousePos().x + 14, (svgRef?.clientWidth ?? 300) - 200)}px`,
                top: `${Math.max(mousePos().y - 20, 4)}px`,
                "font-family": "var(--m)", "font-size": "var(--t-sm)", color: "var(--fg)",
                background: "var(--0)", border: "1px solid var(--2)",
                padding: "6px 10px", "pointer-events": "none",
                "min-width": "180px", "z-index": 10,
              }}>
                {/* Header: time + flow name */}
                <div style={{ display: "flex", "align-items": "center", gap: "8px", "font-size": "var(--t-md)" }}>
                  <span style={{ color: "var(--3)" }}>{(pt().offsetMs / 1000).toFixed(1)}s</span>
                  <Show when={hl()}>
                    <span style={{ color: layers().find(l => l.key === hl())?.color, "font-weight": "600" }}>
                      {hl()!.replace(/_/g, " ")}
                    </span>
                  </Show>
                </div>

                <Show when={spotData() && totalData()}>
                  {/* Total + Spot traffic */}
                  <div style={{ "margin-top": "4px", display: "flex", gap: "16px" }}>
                    <div>
                      <div style={{ color: "var(--3)", "font-size": "var(--t-xs)", "letter-spacing": "var(--track-caps)" }}>TOTAL</div>
                      <div>{"\u2193"}{fK(totalData()!.totalIn)} / {"\u2191"}{fK(totalData()!.totalOut)} KiB</div>
                    </div>
                    <div>
                      <div style={{ color: "var(--3)", "font-size": "var(--t-xs)", "letter-spacing": "var(--track-caps)" }}>SPOT</div>
                      <div>{"\u2193"}{fK(spotData()!.in)} / {"\u2191"}{fK(spotData()!.out)} KiB</div>
                    </div>
                  </div>

                  {/* Bleed section with two histograms */}
                  <Show when={(totalData()!.bleedIn + totalData()!.bleedOut > 0) || (spotData()!.bleedIn + spotData()!.bleedOut > 0)}>
                    <div style={{ "margin-top": "4px", "padding-top": "4px", "border-top": "1px solid var(--2)" }}>
                      <div style={{ color: "hsl(0, 60%, 55%)", "font-size": "var(--t-xs)", "letter-spacing": "var(--track-caps)", "margin-bottom": "2px" }}>BLEED</div>

                      {/* Total bleed histogram */}
                      <Show when={totalData()!.bleedIn + totalData()!.bleedOut > 0}>
                        <div style={{ "margin-bottom": "4px" }}>
                          <div style={{ color: "var(--3)", "font-size": "var(--t-xs)", "margin-bottom": "1px" }}>
                            total: {fK(totalData()!.bleedIn)} / {fK(totalData()!.bleedOut)} KiB
                          </div>
                          {distHisto(totalData()!.bleedByDist, totalData()!.bleedIn + totalData()!.bleedOut)}
                        </div>
                      </Show>

                      {/* Spot bleed histogram */}
                      <Show when={spotData()!.bleedIn + spotData()!.bleedOut > 0}>
                        <div>
                          <div style={{ color: "var(--3)", "font-size": "var(--t-xs)", "margin-bottom": "1px" }}>
                            spot: {fK(spotData()!.bleedIn)} / {fK(spotData()!.bleedOut)} KiB
                          </div>
                          {distHisto(spotData()!.bleedByDist, spotData()!.bleedIn + spotData()!.bleedOut)}
                        </div>
                      </Show>
                    </div>
                  </Show>
                </Show>
              </div>
            );
          })()}
        </Show>

        </div>
        {/* Vertical legend (right side) */}
        <div style={{
          "flex-shrink": 0, width: "120px", "overflow-y": "auto",
          padding: "6px 8px", display: "flex", "flex-direction": "column", gap: "2px",
          "border-left": "1px solid var(--1)",
        }}>
          {layers().map(l => (
            <span
              onMouseEnter={() => { setHoveredLayer(l.key); props.onFlowHover(l.key); }}
              onMouseLeave={() => { setHoveredLayer(null); props.onFlowHover(null); }}
              style={{
                "font-size": "var(--t-sm)", "font-family": "var(--m)",
                color: effectiveHighlight() === l.key ? l.color : "var(--3)",
                cursor: "pointer", transition: "color 0.1s",
                display: "flex", "align-items": "center", gap: "5px",
                overflow: "hidden", "text-overflow": "ellipsis", "white-space": "nowrap",
                padding: "1px 0",
              }}
            >
              <span style={{
                width: "6px", height: "6px", background: l.color,
                opacity: effectiveHighlight() === null || effectiveHighlight() === l.key ? 0.8 : 0.2,
                display: "inline-block", "flex-shrink": 0,
              }} />
              {l.label}
            </span>
          ))}
          {/* Bleed legend entry */}
          <div style={{ "border-top": "1px solid var(--2)", "margin-top": "4px", "padding-top": "4px" }}>
            <span style={{
              "font-size": "var(--t-sm)", "font-family": "var(--m)", color: "var(--3)",
              display: "flex", "align-items": "center", gap: "5px",
            }}>
              <svg width="6" height="6" style={{ "flex-shrink": 0 }}>
                <rect width="6" height="6" fill="url(#bleed-hatch)" stroke="var(--3)" stroke-width="0.5" />
              </svg>
              bleed
            </span>
          </div>
        </div>
      </div>
    </Show>
  );
}

// ── Main ────────────────────────────────────────────────────────────────

type ViewId = "slots" | "peers";
type WsStatus = "connecting" | "connected" | "reconnecting" | "disconnected";

const isMac = /Mac|iPhone|iPad|iPod/.test(navigator.platform);
const modKey = isMac ? "Cmd" : "Ctrl";

export default function App() {
  const [sel, setSel] = createSignal(0);
  const [view, setView] = createSignal<ViewId>("slots");
  const [cmdOpen, setCmdOpen] = createSignal(false);
  const [theme, setTheme] = createSignal<ThemeName>("dark");
  const [wsStatus, setWsStatus] = createSignal<WsStatus>("disconnected");
  const [followMode, setFollowMode] = createSignal(true);
  const [highlightedFlow, setHighlightedFlow] = createSignal<string | null>(null);

  // Source management
  const [sources, setSources] = createSignal<{ source_id: string; client_name: string; connected: boolean }[]>([]);
  const [activeSource, setActiveSource] = createSignal("");
  const [peerCount, setPeerCount] = createSignal<number | null>(null);

  // Live data signals
  const [liveSlots, setLiveSlots] = createSignal<SlotData[]>([]);
  const [liveDetail, setLiveDetail] = createSignal<ApiSlotDetail | null>(null);
  const [livePeers, setLivePeers] = createSignal<ApiPeer[]>([]);

  // Search
  const [searchMode, setSearchMode] = createSignal(false);
  const [searchFrom, setSearchFrom] = createSignal("");
  const [searchTo, setSearchTo] = createSignal("");
  const [searchResults, setSearchResults] = createSignal<SlotData[] | null>(null);

  const apiUrl = (path: string, extra?: Record<string, string>) => {
    const params = new URLSearchParams();
    if (activeSource()) params.set("source", activeSource());
    if (extra) for (const [k, v] of Object.entries(extra)) params.set(k, v);
    const qs = params.toString();
    return qs ? `${path}?${qs}` : path;
  };

  onMount(() => {
    fetch("/api/sources")
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!data?.sources) return;
        setSources(data.sources);
        const connected = data.sources.find((s: any) => s.connected);
        setActiveSource(connected?.source_id ?? data.sources[0]?.source_id ?? "");
      })
      .catch(() => {});
  });

  const slots = createMemo(() => liveSlots());
  const selData = createMemo(() => {
    const d = liveDetail();
    if (d) return apiDetailToSlotData(d);
    const s = slots().find(x => x.slot === sel());
    return s ?? { slot: 0, protos: {}, totalIn: 0, totalOut: 0, total: 0 };
  });
  const slotDetail = createMemo((): FlowTimePoint[] => {
    const d = liveDetail();
    return d ? deriveFlowTimeSeries(d.buckets) : [];
  });

  const flowBreakdown = createMemo((): FlowRow[] => {
    const d = liveDetail();
    return d ? deriveFlows(d.breakdown) : [];
  });

  const activeControlKinds = createMemo(() => {
    const kinds = new Set<string>();
    for (const row of flowBreakdown()) {
      for (const k of Object.keys(row.control)) {
        kinds.add(k);
      }
    }
    return CONTROL_KINDS.filter(k => kinds.has(k));
  });

  const protocolBreakdown = createMemo((): ProtocolRow[] => {
    const d = liveDetail();
    return d ? deriveProtocols(d.breakdown) : [];
  });

  const activeProtoControlKinds = createMemo(() => {
    const kinds = new Set<string>();
    for (const row of protocolBreakdown()) {
      for (const k of Object.keys(row.control)) kinds.add(k);
    }
    return CONTROL_KINDS.filter(k => kinds.has(k));
  });

  // Apply theme CSS custom properties to document root
  createEffect(() => {
    const vars = THEME_VARS[theme()];
    for (const [key, value] of Object.entries(vars)) {
      document.documentElement.style.setProperty(key, value);
    }
  });

  // ── WebSocket connection ────────────────────────────────────────────
  createEffect(() => {
    const source = activeSource();
    if (!source) return;
    let closed = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | undefined;
    let pendingSlots = new Map<number, ApiSlotSummary>();
    let flushTimer: number | undefined;

    const flushPending = () => {
      flushTimer = undefined;
      if (closed || pendingSlots.size === 0) return;
      const items = [...pendingSlots.values()];
      pendingSlots = new Map();

      setLiveSlots(prev => {
        const bySlot = new Map(prev.map(s => [s.slot, s]));
        for (const s of items) bySlot.set(s.slot, apiSummaryToSlotData(s));
        const sorted = [...bySlot.values()].sort((a, b) => b.slot - a.slot).slice(0, 512);
        if (followMode() && sorted.length > 0) setSel(sorted[0].slot);
        return sorted;
      });
    };

    const scheduleFlush = () => {
      if (flushTimer !== undefined) return;
      flushTimer = window.setTimeout(flushPending, LIVE_SLOTS_FLUSH_MS);
    };

    let detailRefreshTimer: number | undefined;
    const refreshDetail = () => {
      if (detailRefreshTimer !== undefined) return;
      detailRefreshTimer = window.setTimeout(() => {
        detailRefreshTimer = undefined;
        const s = sel();
        if (s === 0) return;
        fetch(apiUrl(`/api/slots/${s}`))
          .then(r => r.ok ? r.json() as Promise<ApiSlotDetail> : null)
          .then(data => { if (data && sel() === s) setLiveDetail(data); })
          .catch(() => {});
      }, LIVE_SLOTS_FLUSH_MS);
    };

    const connect = () => {
      if (closed) return;
      setWsStatus("connecting");
      socket = new WebSocket(window.location.origin.replace(/^http/, "ws") + apiUrl("/api/ws"));
      socket.onopen = () => {
        if (closed) return;
        setWsStatus("connected");
        fetch(apiUrl("/api/slots"))
          .then(r => r.ok ? r.json() as Promise<{ slots: ApiSlotSummary[] }> : null)
          .then(data => {
            if (!data) return;
            const items = (data.slots ?? []).map(apiSummaryToSlotData);
            setLiveSlots(items);
            if (items.length > 0) setSel(items[0].slot);
          })
          .catch(e => console.warn("[wiretap] slot list fetch failed:", e));
      };
      socket.onmessage = (event) => {
        let payload: ApiWsMessage;
        try { payload = JSON.parse(String(event.data)); } catch (e) { console.warn("[wiretap] WS parse error:", e); return; }
        if (payload.type === "slot_update" && payload.slot) {
          pendingSlots.set(payload.slot.slot, payload.slot);
        }
        if (payload.type === "slot_batch" && payload.slots) {
          for (const slot of payload.slots) pendingSlots.set(slot.slot, slot);
        }
        if (payload.peer_count !== undefined) setPeerCount(payload.peer_count);
        scheduleFlush();
        if (followMode()) refreshDetail();
      };
      socket.onerror = () => { if (!closed) setWsStatus("reconnecting"); };
      socket.onclose = () => {
        socket = null;
        if (closed) return;
        setWsStatus("reconnecting");
        reconnectTimer = window.setTimeout(connect, 1500);
      };
    };

    connect();
    onCleanup(() => {
      closed = true;
      setWsStatus("disconnected");
      if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer);
      if (flushTimer !== undefined) window.clearTimeout(flushTimer);
      if (detailRefreshTimer !== undefined) window.clearTimeout(detailRefreshTimer);
      socket?.close();
    });
  });

  // ── Slot detail fetch ──────────────────────────────────────────────
  createEffect(() => {
    const s = sel();
    if (s === 0) return;
    setLiveDetail(null);
    fetch(apiUrl(`/api/slots/${s}`))
      .then(r => r.ok ? r.json() as Promise<ApiSlotDetail> : null)
      .then(data => { if (data && sel() === s) setLiveDetail(data); })
      .catch(e => console.warn("[wiretap] slot detail fetch failed:", e));
  });

  // ── Peers fetch ────────────────────────────────────────────────────
  createEffect(() => {
    if (view() !== "peers" || !activeSource()) return;
    const fetchPeers = () => {
      fetch(apiUrl("/api/peers"))
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (data?.peers) setLivePeers(data.peers); })
        .catch(() => {});
    };
    fetchPeers();
    const iv = setInterval(fetchPeers, 5000);
    onCleanup(() => clearInterval(iv));
  });

  // Keyboard handler
  onMount(() => {
    const h = (e: KeyboardEvent) => {
      if (cmdOpen()) return;
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault(); setCmdOpen(true);
      } else if (e.key === "j") {
        setSel(s => {
          const i = slots().findIndex(x => x.slot === s);
          const next = i < slots().length - 1 ? slots()[i + 1].slot : s;
          setFollowMode(false);
          return next;
        });
      } else if (e.key === "k" && !e.metaKey && !e.ctrlKey) {
        setSel(s => {
          const i = slots().findIndex(x => x.slot === s);
          const next = i > 0 ? slots()[i - 1].slot : s;
          setFollowMode(next === slots()[0]?.slot);
          return next;
        });
      } else if (e.key === "1") { setView("slots"); }
      else if (e.key === "2") { setView("peers"); }
      else if (e.key === "t") { setTheme(t => t === "dark" ? "light" : "dark"); }
      else if (e.key === "/" && !searchMode()) {
        e.preventDefault();
        setSearchMode(true);
      }
      else if (e.key === "Escape" && searchMode()) {
        setSearchMode(false);
        setSearchResults(null);
      }
    };
    window.addEventListener("keydown", h);
    onCleanup(() => window.removeEventListener("keydown", h));
  });

  const handleCmd = (id: string) => {
    if (id === "theme") setTheme(t => t === "dark" ? "light" : "dark");
    else if (id.startsWith("view:")) setView(id.split(":")[1] as ViewId);
  };

  const pc = (key: string) => getProtoColor(key, theme());

  const liveColor = () => theme() === "dark" ? "#8cb876" : "#4d8a32";
  const inColor = () => pc("req-resp");
  const outColor = () => pc("gossipsub");

  const fmtK = (bytes: number) => bytes < 50 ? "0" : (bytes / 1024).toFixed(1);
  const fmtPair = (inn: number, out: number) => {
    if (inn < 50 && out < 50) return "-";
    return `${fmtK(inn)} / ${fmtK(out)} KiB`;
  };

  return (
    <div style={{
      "font-family": "var(--m)", background: "var(--0)", color: "var(--fg)",
      height: "100vh", display: "flex", "flex-direction": "column", overflow: "hidden",
    }}>

      {/* ── Top bar ──────────────────────────────────── */}
      <div style={{
        height: "36px", "border-bottom": "2px solid var(--2)",
        display: "flex", "align-items": "center", padding: "0 16px",
        gap: "12px", "flex-shrink": 0,
      }}>
        <span style={{ "font-family": "var(--m)", "font-size": "var(--t-lg)", "font-weight": "700", color: "var(--hi)", "letter-spacing": "var(--track-caps)" }}>ETHEREUM WIRETAP</span>
        <div style={{ flex: 1 }} />

        {/* Connection status */}
        <span style={{
          width: "6px", height: "6px",
          background: wsStatus() === "connected" ? liveColor() : "var(--3)",
          animation: wsStatus() === "connected" ? "blink 1.8s step-end infinite" : "none",
        }} />
        <span style={{ "font-family": "var(--m)", "font-size": "var(--t-sm)", color: wsStatus() === "connected" ? liveColor() : "var(--3)" }}>
          {wsStatus()}
        </span>
        <Show when={followMode() && wsStatus() === "connected"}>
          <span style={{
            "font-family": "var(--m)", "font-size": "var(--t-xs)", color: "var(--3)",
            border: "1px solid var(--2)", padding: "1px 8px",
          }}>follow mode: on</span>
        </Show>

        <Show when={peerCount() !== null}>
          <span style={{ "font-family": "var(--m)", "font-size": "var(--t-sm)", color: "var(--3)" }}>
            {peerCount()} peers
          </span>
        </Show>

        <Show when={sources().length > 1} fallback={
          <Show when={sources().length === 1}>
            <span style={{ "font-family": "var(--m)", "font-size": "var(--t-sm)", color: "var(--3)" }}>
              {sources()[0].client_name}
            </span>
          </Show>
        }>
          <select
            value={activeSource()}
            on:change={(e) => setActiveSource(e.currentTarget.value)}
            style={{
              "font-family": "var(--m)", "font-size": "var(--t-sm)",
              background: "var(--1)", color: "var(--fg)", border: "1px solid var(--2)",
              padding: "2px 4px", cursor: "pointer",
            }}
          >
            <For each={sources()}>
              {(src) => <option value={src.source_id}>{src.client_name}{src.connected ? "" : " (offline)"}</option>}
            </For>
          </select>
        </Show>

        <span style={{ color: "var(--2)" }}>|</span>
        <span style={{ "font-family": "var(--m)", "font-size": "var(--t-md)", color: "var(--3)" }}>
          S<span style={{ color: "var(--fg)" }}>{sel()}</span>
        </span>
        <span style={{ "font-family": "var(--m)", "font-size": "var(--t-md)", color: "var(--3)" }}>
          {(selData().total / 1024).toFixed(1)}<span style={{ "font-size": "var(--t-xs)" }}>KiB</span>
        </span>
        <span style={{ color: "var(--2)" }}>|</span>

        <button
          onClick={() => setTheme(t => t === "dark" ? "light" : "dark")}
          aria-label="Toggle theme"
          style={{
            cursor: "pointer", "font-family": "var(--m)", "font-size": "var(--t-sm)",
            color: "var(--3)", display: "flex", "align-items": "center", gap: "4px",
            background: "none", border: "none", padding: 0,
          }}
        >
          <span style={{ "font-size": "var(--t-lg)" }}>{theme() === "dark" ? "○" : "●"}</span>
          <Kbd>t</Kbd>
        </button>

        <button onClick={() => setCmdOpen(true)} aria-label="Open command palette" style={{ cursor: "pointer", opacity: 0.5, background: "none", border: "none", padding: 0 }}><Kbd>{modKey}+K</Kbd></button>
      </div>

      {/* ── Body ─────────────────────────────────────── */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>

        {/* ── Left column ────────────────────────────── */}
        <div style={{
          width: "28%", "border-right": "2px solid var(--2)",
          display: "flex", "flex-direction": "column", overflow: "hidden", "flex-shrink": 0,
        }}>
          {/* View tabs */}
          <div role="tablist" style={{ display: "flex", "border-bottom": "2px solid var(--2)", "flex-shrink": 0 }}>
            {([
              { id: "slots" as const, l: "SLOTS", k: "1" },
              { id: "peers" as const, l: "PEERS", k: "2" },
            ]).map(tab => (
              <button
                role="tab"
                aria-selected={view() === tab.id}
                onClick={() => setView(tab.id)}
                style={{
                  flex: 1, padding: "7px 0", "text-align": "center", cursor: "pointer",
                  "font-family": "var(--m)", "font-size": "var(--t-md)", "font-weight": "700",
                  "letter-spacing": "var(--track-caps)",
                  color: view() === tab.id ? "var(--hi)" : "var(--3)",
                  background: "none", border: "none",
                  "border-bottom": view() === tab.id ? "2px solid var(--fg)" : "2px solid transparent",
                  "margin-bottom": "-2px",
                  "border-right": "1px solid var(--2)",
                }}
              >
                <span style={{ "font-size": "var(--t-xs)", "margin-right": "3px", opacity: 0.35 }}>{tab.k}</span>{tab.l}
              </button>
            ))}
            <button
              on:click={() => { setView("slots"); setSearchMode(m => !m); if (searchMode()) setSearchResults(null); }}
              style={{
                padding: "7px 10px", cursor: "pointer",
                "font-family": "var(--m)", "font-size": "var(--t-sm)",
                color: searchMode() ? "var(--hi)" : "var(--3)",
                background: "none", border: "none",
                "border-bottom": searchMode() ? "2px solid var(--fg)" : "2px solid transparent",
                "margin-bottom": "-2px",
              }}
              aria-label="Search slots"
            >/</button>
          </div>

          {/* View content */}
          <div style={{ flex: 1, "overflow-y": "auto" }}>

            {/* ── Slots view ── */}
            <Show when={view() === "slots"}>
              <Show when={searchMode()}>
                <div style={{ padding: "8px 12px", "border-bottom": "1px solid var(--2)", display: "flex", gap: "8px", "align-items": "center" }}>
                  <input
                    ref={(el) => setTimeout(() => el.focus(), 0)}
                    placeholder="from slot"
                    value={searchFrom()}
                    on:input={(e) => setSearchFrom(e.currentTarget.value)}
                    style={{ width: "80px", "font-family": "var(--m)", "font-size": "var(--t-sm)", background: "var(--0)", color: "var(--fg)", border: "1px solid var(--2)", padding: "2px 4px" }}
                  />
                  <span style={{ color: "var(--3)", "font-size": "var(--t-sm)", "font-family": "var(--m)" }}>to</span>
                  <input
                    placeholder="to slot"
                    value={searchTo()}
                    on:input={(e) => setSearchTo(e.currentTarget.value)}
                    style={{ width: "80px", "font-family": "var(--m)", "font-size": "var(--t-sm)", background: "var(--0)", color: "var(--fg)", border: "1px solid var(--2)", padding: "2px 4px" }}
                  />
                  <button
                    on:click={() => {
                      const params: Record<string, string> = {};
                      if (searchFrom()) params.from_slot = searchFrom();
                      if (searchTo()) params.to_slot = searchTo();
                      fetch(apiUrl("/api/search", params))
                        .then(r => r.ok ? r.json() : null)
                        .then(data => {
                          if (data?.slots) setSearchResults(data.slots.map(apiSummaryToSlotData));
                        })
                        .catch(() => {});
                    }}
                    style={{ "font-family": "var(--m)", "font-size": "var(--t-sm)", background: "var(--1)", color: "var(--fg)", border: "1px solid var(--2)", padding: "2px 8px", cursor: "pointer" }}
                  >search</button>
                  <button
                    on:click={() => { setSearchMode(false); setSearchResults(null); }}
                    style={{ "font-family": "var(--m)", "font-size": "var(--t-xs)", background: "none", color: "var(--3)", border: "none", cursor: "pointer" }}
                  >esc</button>
                </div>
              </Show>
              <For each={searchResults() ?? slots()}>
                {(s) => {
                  const isSel = () => s.slot === sel();
                  const isEpoch = s.slot % 32 === 0;
                  return (
                    <>
                      {isEpoch && (
                        <div style={{
                          padding: "6px 12px", background: "var(--1)",
                          "border-bottom": "1px solid var(--2)", "font-family": "var(--m)",
                          "font-size": "var(--t-sm)", color: "var(--3)", "letter-spacing": "var(--track-caps)",
                        }}>
                          EPOCH {Math.floor(s.slot / 32)}
                        </div>
                      )}
                      <button
                        on:click={() => {
                          setSel(s.slot);
                          setFollowMode(s.slot === slots()[0]?.slot);
                        }}
                        style={{
                          padding: "8px 12px 7px", cursor: "pointer", width: "100%",
                          background: isSel() ? "var(--sel-bg)" : "transparent",
                          border: "none", "border-bottom": "2px solid var(--1)",
                          "border-left": isSel() ? "3px solid var(--sel-border)" : "3px solid transparent",
                          display: "flex", "flex-direction": "column", gap: "5px",
                          "text-align": "left",
                        }}
                        onMouseEnter={(e) => { if (!isSel()) e.currentTarget.style.background = "var(--hover)"; }}
                        onMouseLeave={(e) => { if (!isSel()) e.currentTarget.style.background = "transparent"; }}
                      >
                        {/* 2x2 grid: rows align between columns */}
                        <div style={{
                          display: "grid", width: "100%",
                          "grid-template-columns": "9ch 1fr",
                          "grid-template-rows": "auto auto",
                          "row-gap": "3px",
                        }}>
                          {/* R1C1: slot number */}
                          <span style={{
                            "font-family": "var(--m)", "font-size": "var(--t-md)",
                            color: isSel() ? "var(--hi)" : "var(--fg)",
                            "align-self": "center",
                          }}>{s.slot}</span>
                          {/* R1C2: bar with in/out labels */}
                          <div style={{ display: "flex", "align-items": "center", gap: "4px" }}>
                            <span style={{ "font-family": "var(--m)", "font-size": "var(--t-xs)", color: "var(--fg)", opacity: 0.4, "flex-shrink": 0 }}>in</span>
                            <div style={{ flex: 1, display: "flex", height: "14px" }}>
                              {Object.keys(s.protos).length > 0
                                ? Object.entries(s.protos).map(([key, pd]) => (
                                    <div style={{
                                      width: `${((pd.i + pd.e) / (s.total || 1)) * 100}%`,
                                      height: "100%", background: pc(key),
                                      opacity: isSel() ? 0.8 : 0.35,
                                    }} />
                                  ))
                                : <>
                                    <div style={{ width: `${(s.totalIn / (s.total || 1)) * 100}%`, height: "100%", background: "hsl(210, 8%, 55%)", opacity: isSel() ? 0.7 : 0.3 }} />
                                    <div style={{ width: `${(s.totalOut / (s.total || 1)) * 100}%`, height: "100%", background: "hsl(30, 8%, 45%)", opacity: isSel() ? 0.6 : 0.25 }} />
                                  </>
                              }
                            </div>
                            <span style={{ "font-family": "var(--m)", "font-size": "var(--t-xs)", color: "var(--fg)", opacity: 0.4, "flex-shrink": 0 }}>out</span>
                          </div>
                          {/* R2C1: total traffic */}
                          <span style={{
                            "font-family": "var(--m)", "font-size": "var(--t-md)",
                            color: "var(--3)", "align-self": "center",
                            animation: s.slot === slots()[0]?.slot ? "traffic-pulse 2s ease-in-out infinite" : "none",
                          }}>{(s.total / 1024).toFixed(1)} KiB</span>
                          {/* R2C2: attributes */}
                          <div style={{
                            display: "flex", gap: "4px", "padding-left": "3ch", "align-self": "center",
                            "font-family": "var(--m)", "font-size": "var(--t-sm)", color: "var(--3)",
                          }}>
                            <Show when={s.meta?.tx_count != null}>
                              <span style={{ width: "10ch", "white-space": "nowrap" }}>
                                <span style={{ opacity: 0.6 }}>tx</span> <span style={{ color: "var(--fg)" }}>{s.meta!.tx_count}</span>
                              </span>
                            </Show>
                            <Show when={s.meta?.blob_commitments != null}>
                              <span style={{ width: "10ch", "white-space": "nowrap" }}>
                                <span style={{ opacity: 0.6 }}>blob</span> <span style={{ color: "var(--fg)" }}>{s.meta!.blob_commitments}</span>
                              </span>
                            </Show>
                            <Show when={s.meta?.attestation_count != null}>
                              <span style={{ width: "10ch", "white-space": "nowrap" }}>
                                <span style={{ opacity: 0.6 }}>att</span> <span style={{ color: "var(--fg)" }}>{s.meta!.attestation_count}</span>
                              </span>
                            </Show>
                            <Show when={s.meta?.proposer_index != null}>
                              <span style={{ width: "10ch", "white-space": "nowrap", opacity: 0.6 }}>
                                v{s.meta!.proposer_index}
                              </span>
                            </Show>
                          </div>
                        </div>
                      </button>
                    </>
                  );
                }}
              </For>
            </Show>

            {/* ── Peers view ── */}
            <Show when={view() === "peers"}>
              <div style={{
                padding: "6px 12px", "font-family": "var(--m)", "font-size": "var(--t-sm)",
                color: "var(--3)", "border-bottom": "1px solid var(--2)",
              }}>{livePeers().length} peers</div>
              <For each={livePeers()}>
                {(p) => {
                  const conn = () => p.connections?.[0];
                  const dir = () => conn()?.direction ?? "unknown";
                  const transport = () => conn()?.transport ?? "";
                  const addrShort = () => {
                    const a = conn()?.remote_addr ?? "";
                    const m = a.match(/\/ip[46]\/([^/]+)/);
                    return m ? m[1] : a;
                  };
                  return (
                    <div style={{
                      padding: "6px 12px", display: "flex", "align-items": "center",
                      gap: "8px", "border-bottom": "1px solid var(--1)",
                    }}
                      onMouseEnter={(e) => { e.currentTarget.style.background = "var(--hover)"; }}
                      onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
                    >
                      <span style={{
                        width: "5px", height: "5px", "flex-shrink": 0,
                        background: dir() === "inbound" ? "var(--proto-gsub)" : "var(--proto-reqr)",
                      }} />
                      <div style={{ flex: 1, "min-width": 0 }}>
                        <div style={{
                          "font-family": "var(--m)", "font-size": "var(--t-md)", color: "var(--fg)",
                          overflow: "hidden", "text-overflow": "ellipsis", "white-space": "nowrap",
                        }}>{p.peer_id.length > 20 ? p.peer_id.slice(0, 8) + "\u2026" + p.peer_id.slice(-8) : p.peer_id}</div>
                        <div style={{ "font-family": "var(--m)", "font-size": "var(--t-xs)", color: "var(--3)", "margin-top": "1px" }}>
                          {addrShort()}
                        </div>
                      </div>
                      <div style={{ "font-family": "var(--m)", "font-size": "var(--t-xs)", color: "var(--3)", "text-align": "right", "flex-shrink": 0 }}>
                        <div>{dir()}</div>
                        <Show when={transport()}><div>{transport()}</div></Show>
                      </div>
                    </div>
                  );
                }}
              </For>
              <Show when={livePeers().length === 0}>
                <div style={{ padding: "24px 12px", "text-align": "center", color: "var(--3)", "font-family": "var(--m)", "font-size": "var(--t-md)" }}>
                  No peers connected
                </div>
              </Show>
            </Show>
          </div>

          {/* Keyboard hints */}
          <div style={{
            padding: "6px 12px", "border-top": "1px solid var(--2)",
            "font-size": "var(--t-sm)", "font-family": "var(--m)", color: "var(--3)",
            display: "flex", gap: "8px", "flex-shrink": 0, "flex-wrap": "wrap",
          }}>
            <span><Kbd>j</Kbd><Kbd>k</Kbd> nav</span>
            <span><Kbd>t</Kbd> theme</span>
            <span><Kbd>/</Kbd> search</span>
          </div>
        </div>

        {/* ── Right column ───────────────────────────── */}
        <div style={{ flex: 1, display: "flex", "flex-direction": "column", overflow: "hidden" }}>

          {/* Slot stats header */}
          <div style={{
            display: "flex", "justify-content": "space-between", padding: "6px 8px",
            "flex-shrink": 0, "border-bottom": "1px solid var(--2)",
          }}>
            <span style={{
              "font-family": "var(--m)", "font-size": "var(--t-md)", "font-weight": "700",
              color: "var(--3)", "letter-spacing": "var(--track-caps)",
            }}>SLOT {sel()}</span>
            <span style={{ "font-family": "var(--m)", "font-size": "var(--t-md)", color: "var(--3)" }}>
              <span style={{ color: inColor() }}>{"\u2193"}{(selData().totalIn / 1024).toFixed(1)} KiB</span>
              <span style={{ margin: "0 6px", color: "var(--2)" }}>|</span>
              <span style={{ color: outColor() }}>{"\u2191"}{(selData().totalOut / 1024).toFixed(1)} KiB</span>
              <span style={{ margin: "0 6px", color: "var(--2)" }}>|</span>
              <span style={{ color: "var(--fg)" }}>{(selData().total / 1024).toFixed(1)} KiB</span>
            </span>
          </div>

          {/* Stream chart (50% height) */}
          <div style={{ flex: 1, padding: "4px 8px", "min-height": 0 }}>
            <StreamGraph
              points={slotDetail()}
              flowTotals={flowBreakdown()}
              theme={theme()}
              highlightedFlow={highlightedFlow()}
              onFlowHover={setHighlightedFlow}
              followMode={followMode()}
              slotStartNs={liveDetail()?.summary?.slot_start_ns}
            />
          </div>

          {/* Time axis (right margin matches 120px legend + 8px padding + 1px border) */}
          <div style={{ position: "relative", height: "14px", margin: "0 137px 0 8px", "flex-shrink": 0 }}>
            {[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map(s => (
              <span style={{
                position: "absolute",
                left: `${(s / 12) * 100}%`,
                transform: s === 0 ? "none" : s === 12 ? "translateX(-100%)" : "translateX(-50%)",
                "font-family": "var(--m)", "font-size": "var(--t-sm)",
                color: s % 2 === 0 ? "var(--3)" : "var(--2)",
              }}>{s}s</span>
            ))}
          </div>

          {/* ── Flow breakdown table (50% height) ────── */}
          <div style={{
            flex: 1, "border-top": "2px solid var(--2)",
            display: "flex", "flex-direction": "column", "min-height": 0,
          }}>
            {/* Scrollable flow table */}
            <div style={{ flex: 1, "overflow-x": "auto", "overflow-y": "auto" }}>
              <table style={{
                width: "100%", "min-width": "100%",
                "border-collapse": "collapse",
                "font-family": "var(--m)", "font-size": "var(--t-md)",
              }}>
                <thead>
                  {/* Header row 1: column groups */}
                  <tr style={{ "border-bottom": "1px solid var(--2)" }}>
                    <th colSpan={2} style={{
                      padding: "5px 10px", "text-align": "left",
                      "font-size": "var(--t-sm)", color: "var(--fg)",
                      "letter-spacing": "var(--track-caps)", "font-weight": "700",
                    }}>FLOW</th>
                    <th style={{
                      padding: "5px 10px", "text-align": "right",
                      "font-size": "var(--t-sm)", color: "var(--fg)",
                      "letter-spacing": "var(--track-caps)", "font-weight": "700",
                      "border-left": "2px solid var(--2)",
                    }}>DATA</th>
                    <Show when={activeControlKinds().length > 0}>
                      <th colSpan={activeControlKinds().length} style={{
                        padding: "5px 10px", "text-align": "left",
                        "font-size": "var(--t-sm)", color: "var(--3)",
                        "letter-spacing": "var(--track-caps)", "font-weight": "700",
                        "border-left": "2px solid var(--2)",
                      }}>CONTROL</th>
                    </Show>
                    <th style={{
                      padding: "5px 10px", "text-align": "right",
                      "font-size": "var(--t-sm)", color: "var(--3)",
                      "letter-spacing": "var(--track-caps)", "font-weight": "700",
                      background: "rgba(232, 118, 118, 0.06)",
                      "border-left": "2px solid var(--2)",
                    }}>BLEED</th>
                  </tr>
                  {/* Header row 2: individual columns */}
                  <tr style={{ "border-bottom": "2px solid var(--2)" }}>
                    <th style={{
                      padding: "3px 10px", "text-align": "left",
                      "font-size": "var(--t-xs)", color: "var(--3)",
                      "letter-spacing": "var(--track-caps)", "font-weight": "400",
                    }}>NAME</th>
                    <th style={{
                      padding: "3px 6px", "text-align": "left",
                      "font-size": "var(--t-xs)", color: "var(--3)",
                      "letter-spacing": "var(--track-caps)", "font-weight": "400",
                      "border-right": "1px solid var(--1)",
                    }}>DOMAIN</th>
                    <th style={{
                      padding: "3px 10px", "text-align": "right",
                      "font-size": "var(--t-xs)", color: "var(--3)",
                      "font-weight": "400",
                      "border-left": "2px solid var(--2)",
                      "border-right": "1px solid var(--1)",
                    }}>{"\u2193"}RCVD / {"\u2191"}SENT</th>
                    <For each={activeControlKinds()}>
                      {(kind, i) => (
                        <th style={{
                          padding: "3px 8px", "text-align": "right",
                          "font-size": "var(--t-xs)", color: "var(--3)",
                          "font-weight": "400", "white-space": "nowrap",
                          "border-left": i() === 0 ? "2px solid var(--2)" : "1px solid var(--1)",
                        }}>{kind}</th>
                      )}
                    </For>
                    <th style={{
                      padding: "3px 10px", "text-align": "right",
                      "font-size": "var(--t-xs)", color: "var(--3)",
                      "font-weight": "400",
                      background: "rgba(232, 118, 118, 0.06)",
                      "border-left": "2px solid var(--2)",
                    }}>{"\u2193"}RCVD / {"\u2191"}SENT</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    const [expanded, setExpanded] = createSignal<Set<string>>(new Set());
                    const toggle = (flow: string) => setExpanded(prev => {
                      const next = new Set(prev);
                      next.has(flow) ? next.delete(flow) : next.add(flow);
                      return next;
                    });
                    const chipInfo = (protocol: string): { label: string; color: string } => {
                      const p = protocol.toLowerCase();
                      if (p.includes("meshsub") || p.includes("gossipsub") || p.includes("floodsub") || p === "gossipsub") {
                        return { label: "GSUB", color: "var(--3)" };
                      }
                      return { label: "RPC", color: "var(--3)" };
                    };
                    const chip = (protocol: string) => {
                      const info = chipInfo(protocol);
                      return (
                        <span style={{
                          display: "inline-block", padding: "1px 5px",
                          "font-size": "var(--t-xs)", "font-family": "var(--m)",
                          color: info.color,
                          border: "1px solid var(--2)",
                          background: "transparent",
                          "letter-spacing": "var(--track-caps)",
                        }}>{info.label}</span>
                      );
                    };

                    return (
                      <For each={flowBreakdown()}>
                        {(row) => {
                          const isHl = () => highlightedFlow() === row.flow;
                          const isOpen = () => expanded().has(row.flow);
                          const hasChildren = row.children.length > 0;
                          return (
                            <>
                              {/* Level 1: flow row */}
                              <tr
                                style={{
                                  "border-bottom": "1px solid var(--1)",
                                  background: isHl() ? "var(--hover)" : "transparent",
                                  cursor: hasChildren ? "pointer" : "default",
                                  transition: "background 0.1s",
                                }}
                                on:click={() => { if (hasChildren) toggle(row.flow); }}
                                onMouseEnter={() => setHighlightedFlow(row.flow)}
                                onMouseLeave={() => setHighlightedFlow(null)}
                              >
                                <td style={{
                                  padding: "5px 10px", "white-space": "nowrap",
                                  color: isHl() ? "var(--hi)" : "var(--fg)",
                                }}>
                                  <span style={{ display: "inline-flex", "align-items": "center", gap: "6px" }}>
                                    <span style={{ color: "var(--3)", width: "12px", display: "inline-block", "font-size": "var(--t-md)" }}>
                                      {hasChildren ? (isOpen() ? "\u25BE" : "\u25B8") : ""}
                                    </span>
                                    <span style={{
                                      width: "6px", height: "6px",
                                      background: topicColor(row.flow, theme()),
                                      "flex-shrink": 0, display: "inline-block",
                                    }} />
                                    {row.flow.replace(/_/g, " ")}
                                  </span>
                                </td>
                                <td style={{ padding: "5px 6px", "border-right": "1px solid var(--1)" }}>{chip(row.protocol)}</td>
                                <td style={{ padding: "5px 10px", "text-align": "right", color: "var(--fg)", "white-space": "nowrap", "border-left": "2px solid var(--2)", "border-right": "1px solid var(--1)" }}>{fmtPair(row.dataIn, row.dataOut)}</td>
                                {activeControlKinds().map((kind, i) => {
                                  const cv = row.control[kind];
                                  const hasData = cv && (cv.in > 0 || cv.out > 0);
                                  return (
                                    <td style={{
                                      padding: "5px 8px", "text-align": "right",
                                      color: hasData ? "var(--3)" : "var(--2)", "font-size": "var(--t-sm)",
                                      "white-space": "nowrap",
                                      "border-left": i === 0 ? "2px solid var(--2)" : "1px solid var(--1)",
                                    }}>{hasData ? fmtPair(cv!.in, cv!.out) : "-"}</td>
                                  );
                                })}
                                <td style={{ padding: "5px 10px", "text-align": "right", "white-space": "nowrap", background: "rgba(232, 118, 118, 0.06)", "border-left": "2px solid var(--2)", color: row.bleedIn + row.bleedOut > 50 ? "var(--fg)" : "var(--2)" }}>{fmtPair(row.bleedIn, row.bleedOut)}</td>
                              </tr>

                              {/* Level 2: child rows (individual topics within a grouped flow) */}
                              <Show when={isOpen()}>
                                {row.children.filter(c => c.totalIn + c.totalOut > 100).map(child => (
                                  <tr style={{ "border-bottom": "1px solid var(--1)", background: "transparent" }}
                                    onMouseEnter={(e) => { e.currentTarget.style.background = "var(--hover)"; }}
                                    onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
                                  >
                                    <td style={{
                                      padding: "5px 10px 5px 36px", "white-space": "nowrap",
                                      color: "var(--3)", "font-size": "var(--t-sm)",
                                    }}>
                                      {child.topic.replace(/_/g, " ")}
                                    </td>
                                    <td style={{ padding: "5px 6px", "border-right": "1px solid var(--1)" }}>{chip(child.protocol)}</td>
                                    <td style={{ padding: "5px 10px", "text-align": "right", color: "var(--3)", "font-size": "var(--t-sm)", "white-space": "nowrap", "border-left": "2px solid var(--2)", "border-right": "1px solid var(--1)" }}>{fmtPair(child.dataIn, child.dataOut)}</td>
                                    {activeControlKinds().map((kind, i) => {
                                      const cv = child.control[kind];
                                      const hd = cv && (cv.in > 0 || cv.out > 0);
                                      return (
                                        <td style={{
                                          padding: "5px 8px", "text-align": "right", "font-size": "var(--t-sm)",
                                          color: hd ? "var(--3)" : "var(--2)", "white-space": "nowrap",
                                          "border-left": i === 0 ? "2px solid var(--2)" : "1px solid var(--1)",
                                        }}>{hd ? fmtPair(cv!.in, cv!.out) : "-"}</td>
                                      );
                                    })}
                                    <td style={{ padding: "5px 10px", "text-align": "right", "font-size": "var(--t-sm)", "white-space": "nowrap", background: "rgba(232, 118, 118, 0.06)", "border-left": "2px solid var(--2)", color: child.bleedIn + child.bleedOut > 50 ? "var(--3)" : "var(--2)" }}>{fmtPair(child.bleedIn, child.bleedOut)}</td>
                                  </tr>
                                ))}
                              </Show>
                            </>
                          );
                        }}
                      </For>
                    );
                  })()}
                </tbody>
              </table>
            </div>

            {/* Protocol overhead table */}
            <Show when={protocolBreakdown().length > 0}>
              <div style={{
                "border-top": "1px solid var(--2)", padding: "0",
                "overflow-x": "auto",
                background: theme() === "dark" ? "rgba(255,255,255,0.02)" : "rgba(0,0,0,0.03)",
              }}>
                <table style={{
                  width: "100%", "border-collapse": "collapse",
                  "font-family": "var(--m)", "font-size": "var(--t-sm)",
                }}>
                  <thead>
                    <tr style={{ "border-bottom": "1px solid var(--2)" }}>
                      <th style={{
                        padding: "4px 10px", "text-align": "left",
                        "font-size": "var(--t-xs)", color: "var(--3)",
                        "letter-spacing": "var(--track-caps)", "font-weight": "700",
                      }}>PROTOCOL OVERHEAD</th>
                      <th style={{
                        padding: "4px 10px", "text-align": "right",
                        "font-size": "var(--t-xs)", color: "var(--3)", "font-weight": "400",
                      }}>FRAMING</th>
                      <For each={activeProtoControlKinds()}>
                        {(kind) => (
                          <th style={{
                            padding: "4px 8px", "text-align": "right",
                            "font-size": "var(--t-xs)", color: "var(--3)",
                            "font-weight": "400", "white-space": "nowrap",
                          }}>{kind}</th>
                        )}
                      </For>
                      <th style={{
                        padding: "4px 10px", "text-align": "right",
                        "font-size": "var(--t-xs)", color: "var(--3)", "font-weight": "400",
                      }}>TOTAL</th>
                    </tr>
                  </thead>
                  <tbody>
                    {protocolBreakdown().map(row => (
                      <tr style={{ "border-bottom": "1px solid var(--1)" }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = "var(--hover)"; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
                      >
                        <td style={{
                          padding: "4px 10px", "white-space": "nowrap", color: "var(--fg)",
                        }}>{row.protocol}</td>
                        <td style={{
                          padding: "4px 10px", "text-align": "right", color: "var(--3)",
                          "white-space": "nowrap",
                        }}>{fmtPair(row.framing.in, row.framing.out)}</td>
                        {activeProtoControlKinds().map(kind => {
                          const cv = row.control[kind];
                          const hasData = cv && (cv.in > 0 || cv.out > 0);
                          return (
                            <td style={{
                              padding: "4px 8px", "text-align": "right",
                              color: hasData ? "var(--3)" : "var(--2)",
                              "white-space": "nowrap",
                            }}>{hasData ? fmtPair(cv!.in, cv!.out) : "-"}</td>
                          );
                        })}
                        <td style={{
                          padding: "4px 10px", "text-align": "right", color: "var(--fg)",
                          "font-weight": "600", "white-space": "nowrap",
                        }}>{fmtPair(row.totalIn, row.totalOut)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Show>
          </div>
        </div>
      </div>

      {/* ── Command palette overlay ───────────────────── */}
      <Show when={cmdOpen()}>
        <CmdPalette onClose={() => setCmdOpen(false)} onCmd={handleCmd} />
      </Show>
    </div>
  );
}
