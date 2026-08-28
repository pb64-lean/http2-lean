# External HPACK smoke

This optional smoke check exercises the live h2c server with the dedicated
RFC 7541 cases and HPACK representation cases from h2spec 2.6.0. The container
is pinned by content digest, not by a mutable tag.

Run it from the repository root on a Linux host with Bazel and Docker able to
run `linux/amd64` images:

```console
Conformance/run_h2spec_hpack.sh
```

Set `H2SPEC_PORT` when port 9002 is unavailable. A successful run executes 23
HPACK-focused cases. The script builds and starts its own loopback server and
stops it on exit.

This is deliberately not a broad h2spec HTTP/2 conformance claim. h2spec 2.6.0
targets RFC 7540, while this module targets RFC 9113. RFC 9113 obsoletes RFC
7540 and deprecates its priority scheme and HTTP/1.1 Upgrade path. The broad
suite also assumes a particular application response body, which is outside
this protocol smoke fixture. RFC 9113 behavior is covered by the repository's
focused protocol tests; this external check is limited to the still-current
RFC 7541 compression boundary.
