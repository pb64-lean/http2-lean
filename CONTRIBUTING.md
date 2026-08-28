# Contributing

This project is pre-release, so public APIs may still be refined. Compatibility
and migration consequences must nevertheless be explicit.

Run the authoritative checks from the repository root:

```console
bazel build //...
bazel test //...
```

Run the standalone dependency-mode check independently:

```console
cd integration/downstream
bazel test //... --lockfile_mode=error
```

Lake exists for editor feedback and does not replace Bazel validation. After an
intentional dependency change, refresh the module lock through the complete
build graph and review its full diff:

```console
bazel build --lockfile_mode=update //...
```

Protocol changes require focused split-point, truncation, malformed-peer, and
resource-bound tests. Changes to framing, compression, stream state, flow
control, or shutdown require the applicable interoperability coverage. Keep
parsers bounded, preserve unconsumed input, make ownership explicit, and use
typed failures instead of accepting malformed wire input.

Security reports belong in the private channel described in
[SECURITY.md](SECURITY.md). Participation is governed by
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Contributions are licensed under the
[Apache License 2.0](LICENSE).
