import Http2.Frame
import Http2.Hpack

open Http2

private def expect (condition : Bool) (message : String) : IO Unit := do
  unless condition do
    throw (IO.userError message)

private def expectError (result : Except Error α) (code : ErrorCode)
    (scope : ErrorScope) (message : String) : IO Unit := do
  match result with
  | .ok _ => throw (IO.userError s!"{message}: expected an error")
  | .error error =>
      expect (error.code == code) s!"{message}: wrong HTTP/2 error code"
      expect (error.scope == scope) s!"{message}: wrong containment scope"

private def testErrorCodeRoundTrip : IO Unit := do
  for code in [0:32] do
    expect ((ErrorCode.ofNat code).toNat == code)
      s!"HTTP/2 error code {code} did not round trip"

private def testFrameClassification : IO Unit := do
  expectError (Frame.decodeHeader ByteArray.empty) .frameSizeError .connection
    "truncated frame header"
  expectError (Frame.encodeHeader {
      length := 0,
      frameType := .data,
      streamId := maxStreamId + 1
    }) .protocolError .localInput "oversized local stream id"

private def testHpackClassification : IO Unit := do
  expectError (Hpack.decodeInteger 7 ByteArray.empty 0) .compressionError .connection
    "truncated HPACK integer"
  expectError (Hpack.decodeHuffman (ByteArray.mk #[0xff])) .compressionError .connection
    "invalid HPACK Huffman sequence"

def main : IO Unit := do
  testErrorCodeRoundTrip
  testFrameClassification
  testHpackClassification
  IO.println "HTTP/2 error classification tests passed"
