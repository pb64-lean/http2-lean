# Changelog

All notable changes to this project will be documented in this file. The
project has not made a stable release; `0.1.0` is a development coordinate.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
