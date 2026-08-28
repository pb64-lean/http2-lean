"""Shared axiom policy for HTTP/2 assurance targets."""

HTTP2_ALLOWED_AXIOMS = [
    "propext",
    "Classical.choice",
    "Quot.sound",
]

HTTP2_CORE_IMPLEMENTED_BY = {
    "Http2.Frame.decodeChunk": "Http2.Frame.decodeChunkCandidate",
    "Http2.Header.normalizeName": "Http2.Header.normalizeNameByteIndexed",
    "Http2.Headers.getLast?": "Http2.Header.Http2.Headers.getLastUSize",
    "Http2.Headers.listEntrySize": "Http2.Header.Http2.Headers.listEntrySizeCandidate",
    "Http2.Hpack.decodeHuffman": "Http2.Hpack.decodeHuffmanLookup",
    "Http2.Hpack.decodeIntegerRest": "Http2.Hpack.decodeIntegerRestBounded",
    "Http2.Hpack.decodeString": "Http2.Hpack.decodeStringLookup",
    "Http2.Hpack.entrySize": "Http2.Hpack.entrySizeCandidate",
}
