# OpenAI rusty_v8 artifact

RemoteCoder selects the OpenAI-built `rusty-v8-v152.2.0` sandbox archive. Its manifest and archive hashes are pinned by `third_party/v8/rusty_v8_152_2_0_release_manifests.sha256` and verified by RemoteCoder's `scripts/rusty-v8-artifacts.py` before compilation.

The Rust wrapper is [seanh-oai/rusty_v8 at 350c5a859a9b3c84ab8701b3ad51e3b7699b30c0](https://github.com/seanh-oai/rusty_v8/tree/350c5a859a9b3c84ab8701b3ad51e3b7699b30c0). The selected archive embeds V8 [15.2.124.1](https://github.com/v8/v8/tree/15.2.124.1) with Codex's reproducible Bazel integration in `third_party/v8/`. The source commit in RemoteCoder's compatibility record owns these inputs.

Native dependency pins live in `third_party/v8/dependencies.bzl`, `third_party/v8/abseil_repository.bzl` and `MODULE.bazel`: Abseil 20250814.1, fast_float 8.2.10, simdutf 7.7.0, and exact Highway, ICU, dragonbox, FP16 and LLVM libc++/libc++abi/libc revisions. Full native license texts and source links are retained in V8.txt. Additional upstream notices remain conservatively. Rust crate notices for each shipped executable are retained separately.

The upstream GN build can optionally compile LGPL glibc trigonometric functions. The selected OpenAI Bazel archives exclude them. Both aarch64-apple-darwin (1,822 members, SHA-256 `feab9ae0384bebaa99c1d000654462c9d42123d587185c7876aaeef7d76ff749`) and x86_64-unknown-linux-musl (1,817 members, SHA-256 `2f2679773a12576dc6b89b0f7d70e8190e5aea8a57f8f6364efaffdd1497d0e7`) have no `s_sin`, `sincostab` or `branred` members and no `__ieee754_sin`, `__ieee754_cos`, `__branred` or `__sincostab` definitions. These hashes identify the compressed archives. Recheck this exclusion whenever changing the native artifact or build path.
