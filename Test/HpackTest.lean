import Http2.Hpack

open Http2



def expect (cond : Bool) (msg : String) : IO Unit := do
  if cond then pure () else throw (IO.userError msg)

def expectEq [BEq α] (actual expected : α) (msg : String) : IO Unit := do
  expect (actual == expected) msg

def expectErrorOk (result : Except Error α) : IO α := do
  match result with
  | .ok value => pure value
  | .error error => throw (IO.userError error.messageD)

def bytes (xs : List Nat) : ByteArray :=
  xs.foldl (fun out n => out.push (UInt8.ofNat n)) ByteArray.empty

def byteArrayEq (a b : ByteArray) : Bool :=
  a.data == b.data

def expectHuffmanDecodersEq (input : ByteArray) : IO Unit := do
  let reference := Http2.Hpack.decodeHuffmanReference input
  let lookup := Http2.Hpack.decodeHuffmanLookup input
  let publicResult := Http2.Hpack.decodeHuffman input
  let failDifferent (which : String) : IO Unit :=
    throw (IO.userError s!"{which} Huffman decoder differed for input {repr input.data}")
  match reference, lookup with
  | .ok expected, .ok actual =>
      if !byteArrayEq actual expected then failDifferent "bounded"
  | .error expected, .error actual =>
      if actual != expected then failDifferent "bounded"
  | _, _ => failDifferent "bounded"
  match lookup, publicResult with
  | .ok expected, .ok actual =>
      if !byteArrayEq actual expected then failDifferent "public"
  | .error expected, .error actual =>
      if actual != expected then failDifferent "public"
  | _, _ => failDifferent "public"

def expectHuffmanError (input : ByteArray) (message : String) : IO Unit := do
  match Http2.Hpack.decodeHuffmanLookup input with
  | .ok value =>
      throw (IO.userError s!"expected Huffman error '{message}', decoded {repr value.data}")
  | .error status =>
      expectEq status.messageD message s!"unexpected Huffman error for {repr input.data}"

def expectStringDecodersEq (input : ByteArray) (offset : Nat) : IO Unit := do
  let reference := Http2.Hpack.decodeStringReference input offset
  let lookup := Http2.Hpack.decodeStringLookup input offset
  let publicResult := Http2.Hpack.decodeString input offset
  let failDifferent (which : String) : IO Unit :=
    throw (IO.userError
      s!"{which} HPACK string decoder differed at offset {offset} for {repr input.data}")
  match reference, lookup with
  | .ok expected, .ok actual =>
      if actual != expected then failDifferent "bounded"
  | .error expected, .error actual =>
      if actual != expected then failDifferent "bounded"
  | _, _ => failDifferent "bounded"
  match lookup, publicResult with
  | .ok expected, .ok actual =>
      if actual != expected then failDifferent "public"
  | .error expected, .error actual =>
      if actual != expected then failDifferent "public"
  | _, _ => failDifferent "public"

def expectStringError (input : ByteArray) (offset : Nat) (message : String) : IO Unit := do
  expectStringDecodersEq input offset
  match Http2.Hpack.decodeStringLookup input offset with
  | .ok value =>
      throw (IO.userError s!"expected HPACK string error '{message}', decoded {repr value}")
  | .error status =>
      expectEq status.messageD message
        s!"unexpected HPACK string error at offset {offset} for {repr input.data}"

def testHuffmanKnownVector : IO Unit := do
  let expected := bytes [0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff]
  let encoded ← expectErrorOk (Http2.Hpack.encodeString "www.example.com")
  expect (byteArrayEq encoded expected)
    "encodeString should produce the RFC 7541 Appendix C.4 Huffman bytes for www.example.com"
  let decoded ← expectErrorOk (Http2.Hpack.decodeString encoded 0)
  expectEq decoded.value "www.example.com" "Huffman-encoded string should decode back"

def testHuffmanRoundTrip : IO Unit := do
  let samples := [
    "www.example.com",
    "no-cache",
    "application/octet-stream",
    "custom-key custom-value",
    "0123456789 abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
    ""
  ]
  for sample in samples do
    let raw := sample.toUTF8
    let encoded := Http2.Hpack.encodeHuffman raw
    let decoded ← expectErrorOk (Http2.Hpack.decodeHuffmanLookup encoded)
    expect (byteArrayEq decoded raw) s!"Huffman round trip should preserve: {sample}"
    -- encodeString/decodeString round trip regardless of which form is chosen
    let string ← expectErrorOk (Http2.Hpack.encodeString sample)
    let stringDecoded ← expectErrorOk (Http2.Hpack.decodeString string 0)
    expectEq stringDecoded.value sample s!"encodeString round trip should preserve: {sample}"

def testHuffmanLookupTable : IO Unit := do
  expectEq Http2.Hpack.huffmanCodes.size 257 "RFC Huffman table should contain EOS plus 256 symbols"
  for symbol in [0:Http2.Hpack.huffmanCodes.size] do
    let code := Http2.Hpack.huffmanCodes[symbol]!
    let bits := Http2.Hpack.huffmanCodeLengths[symbol]!
    expectEq (Http2.Hpack.findHuffmanSymbolLookup? code bits 0) (some symbol)
      s!"bounded lookup should find RFC symbol {symbol}"
    expectEq (Http2.Hpack.findHuffmanSymbolLookup? code bits symbol) (some symbol)
      s!"bounded lookup should honor an inclusive start at RFC symbol {symbol}"
    expectEq (Http2.Hpack.findHuffmanSymbolLookup? code bits (symbol + 1)) none
      s!"bounded lookup should reject RFC symbol {symbol} before the requested start"
    for prefixBits in [0:bits] do
      let prefixCode := code / (2 ^ (bits - prefixBits))
      expectEq (Http2.Hpack.findHuffmanSymbolLookup? prefixCode prefixBits 0)
        (Http2.Hpack.findHuffmanSymbol? prefixCode prefixBits 0)
        s!"bounded lookup should agree on proper prefix {prefixBits} of symbol {symbol}"
    if code > 0 then
      expectEq (Http2.Hpack.findHuffmanSymbolLookup? (code - 1) bits 0)
        (Http2.Hpack.findHuffmanSymbol? (code - 1) bits 0)
        s!"bounded lookup should agree below RFC symbol {symbol}"
    expectEq (Http2.Hpack.findHuffmanSymbolLookup? (code + 1) bits 0)
      (Http2.Hpack.findHuffmanSymbol? (code + 1) bits 0)
      s!"bounded lookup should agree above RFC symbol {symbol}"
  for bits in [31:36] do
    for code in [0, 1, (2 ^ 30) - 1, 2 ^ 30] do
      expectEq (Http2.Hpack.findHuffmanSymbolLookup? code bits 0)
        (Http2.Hpack.findHuffmanSymbol? code bits 0)
        s!"out-of-range code length {bits} should have no lookup result"

def testHuffmanDecoderDifferential : IO Unit := do
  -- Every one- and two-octet input exercises all byte boundaries, including
  -- successful symbols, truncated codes, bad padding, and embedded EOS.
  for first in [0:256] do
    expectHuffmanDecodersEq (bytes [first])
    for second in [0:256] do
      expectHuffmanDecodersEq (bytes [first, second])
  -- Every data symbol also crosses the encoder/decoder boundary directly.
  for symbol in [0:256] do
    let raw := bytes [symbol]
    let encoded := Http2.Hpack.encodeHuffman raw
    expectHuffmanDecodersEq encoded
    let decoded ← expectErrorOk (Http2.Hpack.decodeHuffman encoded)
    expect (byteArrayEq decoded raw) s!"bounded decoder should round trip byte {symbol}"

def testHuffmanMalformedInputs : IO Unit := do
  let validA ← expectErrorOk (Http2.Hpack.decodeHuffmanLookup (bytes [0x1f]))
  expect (byteArrayEq validA "a".toUTF8) "three trailing EOS-prefix ones should be valid padding"
  expectHuffmanError (bytes [0x18]) "invalid HPACK Huffman padding"
  expectHuffmanError (bytes [0xff]) "invalid HPACK Huffman padding"
  expectHuffmanError (bytes [0x1f, 0xff]) "invalid HPACK Huffman padding"
  expectHuffmanError (bytes [0xff, 0xff, 0xff, 0xfc])
    "HPACK Huffman EOS appeared in data"
  for input in [bytes [0x18], bytes [0xff], bytes [0x1f, 0xff],
      bytes [0xff, 0xff, 0xff, 0xfc]] do
    expectHuffmanDecodersEq input

def testStringDecoderDifferential : IO Unit := do
  let leading := bytes [0xde, 0xad, 0xbe]
  let trailing := bytes [0xfa, 0xce]
  for sample in ["www.example.com", "raw \\ value", "", "application/octet-stream"] do
    let encoded ← expectErrorOk (Http2.Hpack.encodeString sample)
    let input := (leading.append encoded).append trailing
    expectStringDecodersEq input leading.size
    let decoded ← expectErrorOk (Http2.Hpack.decodeStringLookup input leading.size)
    expectEq decoded.value sample s!"string decoder should recover {repr sample}"
    expectEq decoded.next (leading.size + encoded.size)
      "string decoder should stop before residual bytes"

  -- Exercise a multi-octet raw length independently of encodeString's choice.
  let longRaw := String.ofList (List.replicate 130 'z')
  let rawPrefix ← expectErrorOk (Http2.Hpack.encodeInteger 7 0 longRaw.toUTF8.size)
  let rawEncoded := rawPrefix.append longRaw.toUTF8
  let rawInput := (leading.append rawEncoded).append trailing
  expectStringDecodersEq rawInput leading.size
  let rawDecoded ← expectErrorOk (Http2.Hpack.decodeStringLookup rawInput leading.size)
  expectEq rawDecoded.value longRaw "multi-octet raw string length should decode"
  expectEq rawDecoded.next (leading.size + rawEncoded.size)
    "multi-octet raw decoder should preserve the absolute cursor"

  -- Force the Huffman representation rather than relying on the size heuristic.
  let huffmanValue := "www.example.com"
  let huffmanPayload := Http2.Hpack.encodeHuffman huffmanValue.toUTF8
  let huffmanPrefix ← expectErrorOk
    (Http2.Hpack.encodeInteger 7 128 huffmanPayload.size)
  let huffmanEncoded := huffmanPrefix.append huffmanPayload
  let huffmanInput := (leading.append huffmanEncoded).append trailing
  expectStringDecodersEq huffmanInput leading.size
  let huffmanDecoded ← expectErrorOk
    (Http2.Hpack.decodeStringLookup huffmanInput leading.size)
  expectEq huffmanDecoded.value huffmanValue "explicit Huffman string should decode"
  expectEq huffmanDecoded.next (leading.size + huffmanEncoded.size)
    "Huffman decoder should preserve the absolute cursor"

  let malformed : List (ByteArray × Nat × String) := [
    (ByteArray.empty, 0, "missing HPACK string"),
    (bytes [0xaa], 1, "missing HPACK string"),
    (bytes [0xff], 0, "truncated HPACK integer"),
    (bytes [0x02, 0x61], 0, "truncated HPACK string"),
    (bytes [0x82, 0x1f], 0, "truncated HPACK string"),
    (bytes [0x81, 0x18], 0, "invalid HPACK Huffman padding"),
    (bytes [0x84, 0xff, 0xff, 0xff, 0xfc], 0, "HPACK Huffman EOS appeared in data")
  ]
  for (input, offset, message) in malformed do
    expectStringError input offset message

def testHuffmanShorterFormChosen : IO Unit := do
  -- "www.example.com" Huffman form is 12 bytes vs 15 raw, so the H bit must be set
  let encoded ← expectErrorOk (Http2.Hpack.encodeString "www.example.com")
  expect (encoded[0]!.toNat >= 128) "Huffman form should be used when strictly shorter"
  -- A string of rare characters is longer in Huffman form, so raw must be used
  let rare := "\\\\\\\\"
  let rareEncoded ← expectErrorOk (Http2.Hpack.encodeString rare)
  expect (rareEncoded[0]!.toNat < 128) "raw form should be used when Huffman is not shorter"
  expectEq rareEncoded.size (1 + rare.toUTF8.size) "raw form should carry the raw octets"
  let rareDecoded ← expectErrorOk (Http2.Hpack.decodeString rareEncoded 0)
  expectEq rareDecoded.value rare "raw fallback should decode back"

def testDynamicTableRoundTrip : IO Unit := do
  let firstBlockHeaders := #[
    Header.of ":status" "200",
    Header.of "content-type" "application/octet-stream",
    Header.of "content-encoding" "identity",
    Header.of "x-request-id" "abc123"
  ]
  let secondBlockHeaders := #[
    Header.of ":status" "200",
    Header.of "content-type" "application/octet-stream",
    Header.of "content-encoding" "identity",
    Header.of "x-request-id" "abc123"
  ]
  let thirdBlockHeaders := #[
    Header.of "x-status" "0",
    Header.of "x-request-id" "abc123"
  ]

  let encoder : Http2.Hpack.State := {}
  let firstBlock ← expectErrorOk (Http2.Hpack.encodeHeaderBlock encoder firstBlockHeaders)
  let secondBlock ← expectErrorOk (Http2.Hpack.encodeHeaderBlock firstBlock.2 secondBlockHeaders)
  let thirdBlock ← expectErrorOk (Http2.Hpack.encodeHeaderBlock secondBlock.2 thirdBlockHeaders)

  expect (!firstBlock.2.dynamic.isEmpty)
    "encoder should insert literal headers into its dynamic table"
  expect (secondBlock.1.size < firstBlock.1.size)
    "repeated header block should compress via the dynamic table"
  -- second block should be all single-byte indexed representations
  expectEq secondBlock.1.size secondBlockHeaders.size
    "fully repeated block should use one indexed byte per header"

  let decoder : Http2.Hpack.State := {}
  let firstDecoded ← expectErrorOk (Http2.Hpack.decodeHeaderBlock decoder firstBlock.1)
  expectEq firstDecoded.headers firstBlockHeaders "first block should round trip"
  let secondDecoded ← expectErrorOk (Http2.Hpack.decodeHeaderBlock firstDecoded.state secondBlock.1)
  expectEq secondDecoded.headers secondBlockHeaders "second block should round trip via decoder dynamic table"
  let thirdDecoded ← expectErrorOk (Http2.Hpack.decodeHeaderBlock secondDecoded.state thirdBlock.1)
  expectEq thirdDecoded.headers thirdBlockHeaders "third block should round trip"
  expectEq thirdDecoded.state.dynamic thirdBlock.2.dynamic
    "decoder dynamic table should match encoder dynamic table after three blocks"

def testEncoderDecoderTablesStayInSync : IO Unit := do
  let encoder : Http2.Hpack.State := {}
  let decoder : Http2.Hpack.State := {}
  let blocks := [
    #[Header.of "a-header" "one", Header.of "b-header" "two"],
    #[Header.of "a-header" "one", Header.of "c-header" "three"],
    #[Header.of "b-header" "two", Header.of "c-header" "three", Header.of "a-header" "changed"],
    #[Header.of "a-header" "changed", Header.of "a-header" "one"]
  ]
  let mut encoderState := encoder
  let mut decoderState := decoder
  for headers in blocks do
    let encoded ← expectErrorOk (Http2.Hpack.encodeHeaderBlock encoderState headers)
    encoderState := encoded.2
    let decoded ← expectErrorOk (Http2.Hpack.decodeHeaderBlock decoderState encoded.1)
    decoderState := decoded.state
    expectEq decoded.headers headers "sequential header block should round trip"
    expectEq decoderState.dynamic encoderState.dynamic
      "decoder dynamic table should mirror encoder dynamic table"

def testEviction : IO Unit := do
  -- max size that fits roughly one small entry (name+value+32)
  let encoder := Http2.Hpack.setMaxAllowedSize {} 70
  let decoder := Http2.Hpack.setDecoderMaxAllowedSize {} 70
  let blocks := [
    #[Header.of "header-aa" "value-aa"],
    #[Header.of "header-bb" "value-bb"],
    #[Header.of "header-aa" "value-aa", Header.of "header-bb" "value-bb"]
  ]
  let mut encoderState := encoder
  let mut decoderState := decoder
  for headers in blocks do
    let encoded ← expectErrorOk (Http2.Hpack.encodeHeaderBlock encoderState headers)
    encoderState := encoded.2
    let decoded ← expectErrorOk (Http2.Hpack.decodeHeaderBlock decoderState encoded.1)
    decoderState := decoded.state
    expectEq decoded.headers headers "header block with eviction should round trip"
    expect (Http2.Hpack.dynamicSize encoderState.dynamic <= 70)
      "encoder dynamic table should stay within max size"
    expectEq decoderState.dynamic encoderState.dynamic
      "tables should stay in sync under eviction"

def testDynamicTableSizeUpdateEmitted : IO Unit := do
  let encoder : Http2.Hpack.State := {}
  let firstHeaders := #[Header.of "x-first" "one"]
  let first ← expectErrorOk (Http2.Hpack.encodeHeaderBlock encoder firstHeaders)
  -- peer announces a smaller SETTINGS_HEADER_TABLE_SIZE
  let resized := Http2.Hpack.setMaxAllowedSize first.2 128
  expectEq resized.pendingSizeUpdate (some 128)
    "encoder should record a pending dynamic table size update"
  let secondHeaders := #[Header.of "x-second" "two"]
  let second ← expectErrorOk (Http2.Hpack.encodeHeaderBlock resized secondHeaders)
  expectEq second.2.pendingSizeUpdate (none : Option Nat)
    "pending size update should be cleared after emission"
  -- first byte must be a dynamic table size update (0b001xxxxx)
  expect (second.1[0]!.toNat >= 32 && second.1[0]!.toNat < 64)
    "next header block should start with a dynamic table size update"

  let decoder : Http2.Hpack.State := {}
  let firstDecoded ← expectErrorOk (Http2.Hpack.decodeHeaderBlock decoder first.1)
  let decoderResized := Http2.Hpack.setDecoderMaxAllowedSize firstDecoded.state 128
  let secondDecoded ← expectErrorOk (Http2.Hpack.decodeHeaderBlock decoderResized second.1)
  expectEq secondDecoded.headers secondHeaders
    "block with leading size update should decode"
  expectEq secondDecoded.state.maxSize 128
    "decoder should apply the emitted dynamic table size update"
  expectEq secondDecoded.state.dynamic second.2.dynamic
    "tables should stay in sync across a size update"

def testIntegerImplementationBound : IO Unit := do
  let maximum : Nat := 18446744073709551615
  let encoded ← expectErrorOk (Http2.Hpack.encodeInteger 7 0 maximum)
  let decoded ← expectErrorOk (Http2.Hpack.decodeInteger 7 encoded 0)
  expectEq decoded.value maximum "maximum supported HPACK integer did not round trip"
  let adversarial := bytes (0x7f :: (List.replicate 100 0x80 ++ [0]))
  match Http2.Hpack.decodeInteger 7 adversarial 0 with
  | .error error =>
      expectEq error.code ErrorCode.compressionError
        "oversized HPACK integer used the wrong error code"
  | .ok _ => throw (IO.userError "an unbounded HPACK integer continuation was accepted")

def testDynamicTableMinimumAndFinalUpdates : IO Unit := do
  let seededHeaders := #[Header.of "x-seeded" "value"]
  let seeded ← expectErrorOk
    (Http2.Hpack.encodeHeaderBlock ({} : Http2.Hpack.State) seededHeaders)
  let decodedSeed ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock ({} : Http2.Hpack.State) seeded.1)
  let changed := Http2.Hpack.setMaxAllowedSize
    (Http2.Hpack.setMaxAllowedSize seeded.2 0) Http2.Hpack.defaultDynamicTableSize
  expectEq changed.pendingMinimumSizeUpdate (some 0)
    "inter-block table changes did not retain the minimum"
  expectEq changed.pendingSizeUpdate (some Http2.Hpack.defaultDynamicTableSize)
    "inter-block table changes did not retain the final value"
  let nextHeaders := #[Header.of "x-next" "another"]
  let next ← expectErrorOk (Http2.Hpack.encodeHeaderBlock changed nextHeaders)
  let minimum ← expectErrorOk (Http2.Hpack.decodeInteger 5 next.1 0)
  let final ← expectErrorOk (Http2.Hpack.decodeInteger 5 next.1 minimum.next)
  expectEq minimum.value 0 "first table-size update was not the observed minimum"
  expectEq final.value Http2.Hpack.defaultDynamicTableSize
    "second table-size update was not the final value"
  let decodedNext ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock decodedSeed.state next.1)
  expectEq decodedNext.headers nextHeaders
    "minimum/final table-size updates did not decode"
  expectEq decodedNext.state.dynamic next.2.dynamic
    "encoder and decoder diverged across minimum/final table-size updates"

  let required := Http2.Hpack.setDecoderMaxAllowedSize
    ({} : Http2.Hpack.State) 128
  let ordinary ← expectErrorOk
    (Http2.Hpack.encodeHeaderBlock ({} : Http2.Hpack.State) nextHeaders)
  match Http2.Hpack.decodeHeaderBlock required ordinary.1 with
  | .error error =>
      expectEq error.code ErrorCode.compressionError
        "missing required table-size update used the wrong error"
  | .ok _ => throw (IO.userError
      "decoder accepted a block that omitted a required table-size update")

  let update128 ← expectErrorOk (Http2.Hpack.encodeInteger 5 32 128)
  let update64 ← expectErrorOk (Http2.Hpack.encodeInteger 5 32 64)
  let update32 ← expectErrorOk (Http2.Hpack.encodeInteger 5 32 32)
  match Http2.Hpack.decodeHeaderBlock
      ({} : Http2.Hpack.State) (update32.append update64 |>.append update128) with
  | .error error =>
      expectEq error.code ErrorCode.compressionError
        "third table-size update used the wrong error"
  | .ok _ => throw (IO.userError "decoder accepted more than two table-size updates")
  match Http2.Hpack.decodeHeaderBlock
      ({} : Http2.Hpack.State) (update128.append update64) with
  | .error error =>
      expectEq error.code ErrorCode.compressionError
        "misordered table-size updates used the wrong error"
  | .ok _ => throw (IO.userError
      "decoder accepted table-size updates that did not put the minimum first")

def testWireOctetsArePreserved : IO Unit := do
  let rawValue := bytes [2, 0x80, 0xff]
  let decodedString ← expectErrorOk (Http2.Hpack.decodeString rawValue 0)
  expectEq decodedString.octets? (some (bytes [0x80, 0xff]))
    "non-UTF-8 HPACK value octets were discarded"
  let rawOctets := bytes [0x80, 0xff]
  let huffman := Http2.Hpack.encodeHuffman rawOctets
  let huffmanPrefix ← expectErrorOk
    (Http2.Hpack.encodeInteger 7 128 huffman.size)
  let huffmanDecoded ← expectErrorOk
    (Http2.Hpack.decodeString (huffmanPrefix.append huffman) 0)
  expectEq huffmanDecoded.octets? (some rawOctets)
    "Huffman-coded non-UTF-8 value octets were discarded"

  let encodedName ← expectErrorOk (Http2.Hpack.encodeString "x-obs")
  let literal := (bytes [0]).append encodedName |>.append rawValue
  let decoded ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock ({} : Http2.Hpack.State) literal)
  let header := decoded.headers[0]!
  expectEq header.valueOctets (bytes [0x80, 0xff])
    "decoded field value did not expose its exact wire octets"
  expectEq (Headers.getOctets? decoded.headers "x-obs") (some (bytes [0x80, 0xff]))
    "Headers exact-value accessor did not preserve obs-text"
  expect (Header.validFieldValue header)
    "valid obs-text octets were rejected as HTTP field content"
  expectEq (Headers.listEntrySize header) ("x-obs".utf8ByteSize + 2 + 32)
    "field-list accounting used the String view instead of wire octets"
  let reencoded ← expectErrorOk
    (Http2.Hpack.encodeHeaderBlock ({} : Http2.Hpack.State) decoded.headers)
  let roundTrip ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock ({} : Http2.Hpack.State) reencoded.1)
  expectEq roundTrip.headers[0]!.valueOctets (bytes [0x80, 0xff])
    "HPACK re-encoding changed non-UTF-8 value octets"

  let indexedLiteral := (bytes [0x40]).append encodedName |>.append rawValue
  let indexed ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock ({} : Http2.Hpack.State) indexedLiteral)
  let dynamicIndex ← expectErrorOk
    (Http2.Hpack.encodeInteger 7 128 (Http2.Hpack.staticTableSize + 1))
  let reused ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock indexed.state dynamicIndex)
  expectEq reused.headers[0]!.valueOctets (bytes [0x80, 0xff])
    "dynamic indexing lost non-UTF-8 field-value octets"

  let encodedValue ← expectErrorOk (Http2.Hpack.encodeString "v")
  let invalidNameBlock := (bytes [0, 1, 0xff]).append encodedValue
  let invalidName ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock ({} : Http2.Hpack.State) invalidNameBlock)
  expectEq invalidName.headers[0]!.nameOctets (bytes [0xff])
    "invalid field-name octets were discarded"
  expect (!Header.validFieldName invalidName.headers[0]!)
    "a non-ASCII field name passed the shared HTTP/2 predicate"
  let utf8Name ← expectErrorOk (Http2.Hpack.encodeString "ÿ")
  let utf8NameBlock := (bytes [0]).append utf8Name |>.append encodedValue
  let validUtf8Name ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock ({} : Http2.Hpack.State) utf8NameBlock)
  expect (invalidName.headers[0]! != validUtf8Name.headers[0]!)
    "distinct invalid and UTF-8 field-name octets collapsed to one entry"

def testAuthorizationIsNeverIndexed : IO Unit := do
  let authorization := Header.of "authorization" "Bearer production-secret"
  let ordinary := Header.of "x-request-id" "request-1"
  let encoder : Http2.Hpack.State := {}
  let encoded ← expectErrorOk
    (Http2.Hpack.encodeHeaderBlock encoder #[authorization, ordinary])
  -- The first representation must use the 0001xxxx never-indexed prefix.
  expect (!encoded.1.isEmpty &&
      encoded.1[0]!.toNat >= 16 && encoded.1[0]!.toNat < 32)
    "authorization should use HPACK's never-indexed literal representation"
  expect (!encoded.2.dynamic.contains authorization)
    "authorization should not enter the encoder dynamic table"
  expect (encoded.2.dynamic.contains ordinary)
    "ordinary metadata should remain eligible for dynamic indexing"

  let decoded ← expectErrorOk
    (Http2.Hpack.decodeHeaderBlock ({} : Http2.Hpack.State) encoded.1)
  expectEq decoded.headers #[authorization, ordinary]
    "never-indexed authorization should round trip"
  expect (!decoded.state.dynamic.contains authorization)
    "authorization should not enter the decoder dynamic table"

def main : IO Unit := do
  testHuffmanKnownVector
  testHuffmanRoundTrip
  testHuffmanLookupTable
  testHuffmanDecoderDifferential
  testHuffmanMalformedInputs
  testStringDecoderDifferential
  testHuffmanShorterFormChosen
  testDynamicTableRoundTrip
  testEncoderDecoderTablesStayInSync
  testEviction
  testDynamicTableSizeUpdateEmitted
  testIntegerImplementationBound
  testDynamicTableMinimumAndFinalUpdates
  testWireOctetsArePreserved
  testAuthorizationIsNeverIndexed
  IO.println "hpack tests passed"
