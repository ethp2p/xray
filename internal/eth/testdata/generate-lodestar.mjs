import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const repository = resolve(process.argv[2]);
const require = createRequire(
  resolve(repository, "packages/beacon-node/package.json"),
);
const { ssz } = await import(
  pathToFileURL(resolve(repository, "packages/types/lib/index.js")).href
);
const snappy = await import(require.resolve("snappy"));
const genesis = 1606824023;
const secondsPerSlot = 12;
const slot = 42;
const cases = [
  [
    "legacy_attestation",
    "phase0",
    "beacon_attestation_5",
    "Attestation",
    (v) => {
      v.data.slot = slot;
    },
  ],
  [
    "single_attestation",
    "electra",
    "beacon_attestation_5",
    "SingleAttestation",
    (v) => {
      v.committeeIndex = 3;
      v.attesterIndex = 123;
      v.data.slot = slot;
    },
  ],
  [
    "aggregate",
    "fulu",
    "beacon_aggregate_and_proof",
    "SignedAggregateAndProof",
    (v) => {
      v.message.aggregate.data.slot = slot;
    },
  ],
  [
    "fulu_column",
    "fulu",
    "data_column_sidecar_7",
    "DataColumnSidecar",
    (v) => {
      v.index = 7;
      v.signedBlockHeader.message.slot = slot;
    },
  ],
  [
    "gloas_column",
    "gloas",
    "data_column_sidecar_7",
    "DataColumnSidecar",
    (v) => {
      v.index = 7;
      v.slot = slot;
    },
  ],
  [
    "bid",
    "gloas",
    "execution_payload_bid",
    "SignedExecutionPayloadBid",
    (v) => {
      v.message.slot = slot;
    },
  ],
  [
    "payload_attestation",
    "gloas",
    "payload_attestation_message",
    "PayloadAttestationMessage",
    (v) => {
      v.data.slot = slot;
    },
  ],
  [
    "preferences",
    "gloas",
    "proposer_preferences",
    "SignedProposerPreferences",
    (v) => {
      v.message.proposalSlot = slot;
    },
  ],
  [
    "envelope",
    "gloas",
    "execution_payload",
    "SignedExecutionPayloadEnvelope",
    (v) => {
      v.message.payload.timestamp = genesis + slot * secondsPerSlot;
    },
  ],
  [
    "fulu_block",
    "fulu",
    "beacon_block",
    "SignedBeaconBlock",
    (v) => {
      v.message.slot = slot;
      v.message.proposerIndex = 11;
    },
  ],
  [
    "gloas_block",
    "gloas",
    "beacon_block",
    "SignedBeaconBlock",
    (v) => {
      v.message.slot = slot;
      v.message.proposerIndex = 11;
    },
  ],
];
const fixtures = cases.map(([name, fork, topic, typeName, initialize]) => {
  const type = ssz[fork][typeName];
  const value = type.defaultValue();
  initialize(value);
  const data = type.serialize(value);
  return {
    name,
    fork,
    topic,
    slot: name === "envelope" ? null : slot,
    ssz: Buffer.from(data).toString("base64"),
    snappy: snappy.compressSync(data).toString("base64"),
  };
});
const lodestarCommit = execFileSync(
  "git",
  ["-C", repository, "rev-parse", "HEAD"],
  { encoding: "utf8" },
).trim();
writeFileSync(
  new URL("lodestar.json", import.meta.url),
  JSON.stringify(
    { lodestarCommit, genesis, secondsPerSlot, fixtures },
    null,
    2,
  ) + "\n",
);
