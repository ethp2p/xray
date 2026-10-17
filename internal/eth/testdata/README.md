# Lodestar SSZ fixtures

`lodestar.json` contains SSZ bytes and Snappy block-compressed gossip payloads
serialized by Lodestar at the commit recorded in the file. Slot 42 and sidecar
index 7 are deliberately different, as are committee index 3 and attester index 123. This catches field-offset mistakes that all-zero fixtures hide.

To regenerate, check out the recorded Lodestar commit, install its dependencies,
and build `@lodestar/types` and its dependencies. Then run from the Xray root:

```sh
node internal/eth/testdata/generate-lodestar.mjs /path/to/lodestar
```

The Go tests consume these checked-in bytes without network access or a Lodestar
checkout. Gloas fixtures describe that pinned revision, not future spec revisions.
