# Wiretap clients

Hand-written wiretap producer SDKs for languages other than Go. Each speaks
the wiretap protocol defined in `proto/wiretap/` (handshake + framing in
`proto/wiretap/wire/`) and ships trace envelopes to an xray backend.

The Go SDK lives at the module root (`package xray`); these clients are
ecosystem-idiomatic implementations for their target language:

- `rust/` — Cargo crate. API mirrors Go: `Wiretap::new(host, ...)`.
- `js/` — npm package (`@xray/wiretap`). Node-targeted; not for browsers.

The low-level protobuf bindings these clients depend on are generated into
`../gen/<lang>/` by `buf generate`.
