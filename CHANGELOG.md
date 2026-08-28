# Changelog

All notable changes to this project will be documented in this file. The
project remains pre-release; `0.1.0` does not promise API stability.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] - 2026-08-28

### Added

- Initial Bazel module, public Lean import root, Lake editor model, continuous
  integration, dependency-mode smoke test, and project governance files.
- Separate runtime-support targets for callback-safe cancellation, bounded host
  resolution, TLS sessions, trust-anchor loading, and the audited POSIX extern
  boundary, without adding those dependencies to the wire-protocol core.
- Typed connection- and stream-scoped HTTP/2 errors, ordered field sections,
  frame codecs and incremental parsing, HPACK and Huffman coding, RFC 8441
  Extended CONNECT values, and a role-aware connection state machine with
  settings, continuation, lifecycle, and flow-control enforcement.
- Managed h2c and TLS client and server transports for Extended CONNECT,
  connection-wide compression synchronization across reset races, bounded
  graceful shutdown, keepalive supervision, and exact assurance policies for
  both the pure core and runtime boundary.
- A reproducible external HPACK smoke check using digest-pinned h2spec cases,
  explicitly scoped to RFC 7541 rather than the tool's broad RFC 7540 suite.
