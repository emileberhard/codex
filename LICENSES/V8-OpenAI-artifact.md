# OpenAI rusty_v8 artifact

RemoteCoder selects the OpenAI-built `rusty-v8-v150.4.0` sandbox archive, not the default denoland prebuilt. Its manifest and archive hashes are pinned by the Codex source's `third_party/v8/rusty_v8_150_4_0_release_manifests.sha256` and verified by `scripts/rusty-v8-artifacts.py`.

The source is V8 [15.0.245.2](https://github.com/v8/v8/tree/15.0.245.2), plus the reproducible Bazel integration patches in the [RemoteCoder Codex fork](https://github.com/emileberhard/codex/tree/main/patches). These patches and the native dependency pins in `MODULE.bazel` are part of the runtime source commit in RemoteCoder's compatibility record.

The selected native dependency inputs include Abseil 20250814.0, Highway 1.2.0, ICU and FP16 from the exact git revisions in `patches/v8_module_deps.patch`, and LLVM libc++ / libc++abi / libc at the exact revisions in `MODULE.bazel`. Full native license texts, including Apache-2.0 with LLVM exceptions, BSD, MIT and Unicode/ICU notices, are retained in V8.txt. Rust crate notices are in codex-code-mode-host-rust.md.

The upstream GN build can optionally compile LGPL glibc trigonometric functions. That library is absent from the selected aarch64-apple-darwin OpenAI Bazel archive: its member table contains no `s_sin`, `sincostab` or `branred` objects and its global symbols contain no `__ieee754_sin`, `__ieee754_cos`, `__branred` or `__sincostab` definitions. This exclusion must be rechecked when changing the native artifact or its build path.
