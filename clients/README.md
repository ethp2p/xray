# Xray clients

Hand-written producer SDKs for languages other than Go. Each speaks the xray
ingest protocol defined in `proto/xray/` (handshake + framing in
`proto/xray/wire/`) and ships trace envelopes to an xray backend.

The Go SDK lives at the module root (`package xray`); these clients are
ecosystem-idiomatic implementations for their target language:

- `rust/` — Cargo crate. API mirrors Go: `Wrap::new(host, ...)`.
- `js/` — npm package (`@xray/probe`). Node-targeted; not for browsers.

The low-level protobuf bindings these clients depend on are generated into
`../gen/<lang>/` by `buf generate`.
