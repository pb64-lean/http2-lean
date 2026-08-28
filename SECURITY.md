# Security policy

## Supported versions

Only the current pre-release `main` development line receives security fixes.
There is no stable-version support commitment yet.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
vulnerability-reporting flow from the repository's **Security** tab. If that
flow is unavailable, contact the maintainers privately through the owning
organization.

Include the affected revision and environment, a minimal reproduction, the
expected and observed behavior, potential impact, and any known mitigation.

## Security boundary

Remote peers and received bytes are untrusted. Protocol code is responsible
for validating framing, compression state, stream transitions, flow-control
accounting, configured resource bounds, and connection-level error handling.

The Lean compiler and runtime, operating system, network transport, entropy,
cryptography, name resolution, trust configuration, and application policy are
outside the protocol core's trusted boundary unless a specific adapter states
otherwise. Conformance tests and proved implementation properties do not
establish that external components satisfy their contracts.
