# Generated bindings

Per-language outputs of `buf generate` for the wiretap protocol defined in
`proto/wiretap/`. Each subdirectory is consumed by the matching client in
`../clients/<lang>/`.

- `rust/` — protobuf-generated Rust types (regenerate with `buf generate`).
- `ts/` — protobuf-generated TypeScript types.

These are checked in so consumers don't need protoc/buf installed to build.
Regenerate after editing `proto/wiretap/wiretap.proto`.
