"""Exact statements of the protocol core's principal codec theorems."""

HTTP2_CORE_PRINCIPAL_TYPES = {
    "Http2.Hpack.decodeHuffman_encodeHuffman": "∀ (bytes : ByteArray), Http2.Hpack.decodeHuffman (Http2.Hpack.encodeHuffman bytes) = Except.ok bytes",
    "Http2.Hpack.decodeString_encodeString": "∀ {value : String} {encoded : ByteArray} (henc : Http2.Hpack.encodeString value = Except.ok encoded) (rest : ByteArray), Http2.Hpack.decodeString (encoded ++ rest) 0 = Except.ok { value := value, next := encoded.size }",
    "Http2.Hpack.decodeInteger_encodeInteger": "∀ {prefixBits prefixMask value : Nat} {encoded : ByteArray} (hmask : prefixMask % 2 ^ prefixBits = 0) (hfits : prefixMask + 2 ^ prefixBits ≤ 256) (henc : Http2.Hpack.encodeInteger prefixBits prefixMask value = Except.ok encoded) (rest : ByteArray), Http2.Hpack.decodeInteger prefixBits (encoded ++ rest) 0 = Except.ok { value := value, next := encoded.size }",
    "Http2.Hpack.huffmanPrefixFree": "Http2.Hpack.pairwiseNoBitPrefix Http2.Hpack.huffmanCodeTable = true",
    "Http2.Hpack.dynamicSize_resize_le": "∀ (state : Http2.Hpack.State) (maxSize : Nat), Http2.Hpack.dynamicSize (Http2.Hpack.resize state maxSize).dynamic ≤ maxSize",
    "Http2.Hpack.dynamicSize_insert_le": "∀ (state : Http2.Hpack.State) (header : Http2.Header), Http2.Hpack.dynamicSize (Http2.Hpack.insert state header).dynamic ≤ state.maxSize",
    "Http2.Hpack.dynamicSize_resizeChecked_le": "∀ {state state' : Http2.Hpack.State} {maxSize : Nat} (h : Http2.Hpack.resizeChecked state maxSize = Except.ok state'), Http2.Hpack.dynamicSize state'.dynamic ≤ maxSize",
    "Http2.Hpack.dynamicSize_setMaxAllowedSize_le": "∀ (state : Http2.Hpack.State) (maxSize : Nat), Http2.Hpack.dynamicSize (Http2.Hpack.setMaxAllowedSize state maxSize).dynamic ≤ maxSize",
    "Http2.Frame.decodeAll_encode_append": "∀ {frame : Http2.Frame} {bs : ByteArray} (henc : frame.encode = Except.ok bs) (hft : Http2.FrameType.ofUInt8 frame.header.frameType.toUInt8 = frame.header.frameType) (rest : ByteArray), Http2.Frame.decodeAll (bs ++ rest) = Except.map (fun frames => #[frame] ++ frames) (Http2.Frame.decodeAll rest)",
    "Http2.Frame.decodeHeader_encodeHeader_append": "∀ {header : Http2.FrameHeader} {bs : ByteArray} (henc : Http2.Frame.encodeHeader header = Except.ok bs) (hft : Http2.FrameType.ofUInt8 header.frameType.toUInt8 = header.frameType) (rest : ByteArray), Http2.Frame.decodeHeader (bs ++ rest) = Except.ok header",
}
