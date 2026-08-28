# http2-lean

[![CI](https://github.com/pb64-lean/http2-lean/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pb64-lean/http2-lean/actions/workflows/ci.yml) [![Assurance](https://github.com/pb64-lean/http2-lean/actions/workflows/assurance.yml/badge.svg?branch=main)](https://github.com/pb64-lean/http2-lean/actions/workflows/assurance.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

HTTP/2 protocol foundations for Lean 4.

The project is in pre-release development. Version `0.1.0` is a development
coordinate, not a compatibility promise, and public APIs may change before the
first stable release.

## Intended scope

The library provides application-independent HTTP/2 machinery:

- bounded incremental frame encoding and decoding;
- HPACK encoding, decoding, and dynamic-table management;
- settings, stream lifecycle, connection state, and error handling;
- connection- and stream-level flow control;
- header-block continuation and validation;
- Extended CONNECT support;
- transport-neutral state machines with explicit effectful adapters;
- managed h2c and TLS client connections for Extended CONNECT; and
- managed h2c and TLS listeners with graceful GOAWAY-based shutdown.

Application protocols and their message formats, metadata policies, status
models, dispatch rules, and service runtimes are outside this library's scope.
HTTP/1.1 remains a separate protocol concern.

Optional runtime-support targets provide callback-safe cancellation, bounded
DNS lookup, canonical numeric destinations, socket-driven TLS sessions, system
trust-anchor loading, and the narrow POSIX descriptor boundary required by
trust loading. These adapters are not imported by the wire-protocol core.

The conformance targets are RFC 9113 for HTTP/2, RFC 7541 for HPACK, and the
HTTP/2 protocol extension defined by RFC 8441. Server push is deliberately
disabled; an endpoint advertises that policy and rejects an unexpected push
promise as a connection protocol error.

## Build

Bazel is the authoritative build and test system:

```console
bazel build //...
bazel test //...
```

An optional Docker-backed interoperability smoke exercises the RFC 7541 HPACK
boundary against a digest-pinned external tool. Its intentionally narrow scope
and invocation are documented in [Conformance/README.md](Conformance/README.md).

The standalone dependency-mode check runs separately:

```console
cd integration/downstream
bazel test //... --lockfile_mode=error
```

Lake supplies an editor project model and a compatibility build:

```console
lake build
```

## Library layers

The protocol core is exposed as `@http2_lean//:http2_core`. It contains no
socket, TLS, DNS, or host-filesystem dependency. `@http2_lean//:http2_client`
and `@http2_lean//:http2_server` add managed transports. The `:http2` facade
exports all three layers, while `:runtime` provides the optional environment
adapters.

Client and server transports require the HTTP/2 connection preface and initial
SETTINGS exchange. The cleartext entry points use prior-knowledge h2c; they do
not implement an HTTP/1.1 Upgrade path. TLS entry points require ALPN `h2`.
Connections retain their reader and writer owners until explicit close or
server shutdown, and tunnel operations surface typed connection-, stream-, and
local-input failures.

## Bazel module

Consumers declare:

```starlark
bazel_dep(
    name = "http2-lean",
    version = "0.1.0",
    repo_name = "http2_lean",
)
```

The public Lean import root is `Http2`. Optional application-independent
adapters are imported through `Http2.Runtime`; their individual targets remain
available when a consumer needs a smaller dependency closure.

## Security

Remote peers and all received bytes are untrusted. Parsers and state machines
must reject invalid input with typed failures, enforce configured bounds, and
avoid silently weakening protocol requirements. The precise trusted boundary
and supported-version policy are documented in [SECURITY.md](SECURITY.md).

Please report suspected vulnerabilities privately rather than opening a public
issue.

## License

Licensed under the [Apache License 2.0](LICENSE).
