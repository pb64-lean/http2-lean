import Http2.Connection

open Http2

private def fail (message : String) : IO α :=
  throw (IO.userError message)

private def expect (condition : Bool) (message : String) : IO Unit := do
  unless condition do fail message

private def requireOk (result : Except Error α) : IO α :=
  match result with
  | .ok value => pure value
  | .error error => fail error.message

private def requireError (result : Except Error α) (message : String) : IO Error :=
  match result with
  | .error error => pure error
  | .ok _ => fail message

private def headersFrame (streamId : Nat) (headers : Headers)
    (endHeaders : Bool := true) (endStream : Bool := false) : IO Frame := do
  let (block, _) ← requireOk (Hpack.encodeHeaderBlock {} headers)
  let flags := FrameFlag.combine <|
    (if endHeaders then #[FrameFlag.endHeaders] else #[]) ++
    (if endStream then #[FrameFlag.endStream] else #[])
  pure {
    header := { length := block.size, frameType := .headers, flags, streamId }
    payload := block
  }

private def afterPeerSettings (role : Connection.Role) : IO Connection.State := do
  let frame ← requireOk <| Http2.Settings.frame #[]
  let (state, _, _) ← requireOk <| Connection.processFrame (Connection.initial role) frame
  pure state

private def testSettings : IO Unit := do
  let frame ← requireOk <| Http2.Settings.frame #[
    { id := SettingId.enableConnectProtocol, value := 1 },
    { id := .initialWindowSize, value := 131072 }
  ]
  let (state, automatic, events) ← requireOk <|
    Connection.processFrame (Connection.initial .server) frame
  expect state.peerSettings.enableConnectProtocol
    "SETTINGS_ENABLE_CONNECT_PROTOCOL was not applied"
  expect (state.peerSettings.initialWindowSize == 131072)
    "SETTINGS_INITIAL_WINDOW_SIZE was not applied"
  expect (automatic.size == 1 && Http2.Settings.isAck automatic[0]!)
    "a non-ack SETTINGS frame did not produce one acknowledgement"
  expect (events.size == 1) "the settings transition did not emit one event"

  let invalid ← requireOk <| Http2.Settings.frame #[{ id := .enablePush, value := 1 }]
  let error ← requireError (Connection.processFrame (Connection.initial .client) invalid)
    "a server SETTINGS_ENABLE_PUSH value was accepted"
  expect (error.code == .protocolError) "invalid SETTINGS used the wrong error code"

private def testStreamIdentityAndHeaders : IO Unit := do
  let fields := Headers.empty
    |>.insert ":method" "GET"
    |>.insert ":scheme" "https"
    |>.insert ":authority" "example.test"
    |>.insert ":path" "/"
  let frame ← headersFrame 1 fields true true
  let initial ← afterPeerSettings .server
  let (state, _, events) ← requireOk <|
    Connection.processFrame initial frame
  expect (state.lastPeerStreamId == 1) "the peer stream high-water mark was not advanced"
  match events[0]? with
  | some (Connection.Event.headers 1 received true false) =>
      expect (received == fields) "the decoded field section changed"
  | _ => fail "the initial field section emitted the wrong event"

  let wrongParity ← headersFrame 2 fields
  let initial ← afterPeerSettings .server
  let error ← requireError
    (Connection.processFrame initial wrongParity)
    "a peer stream with local parity was accepted"
  expect (error.code == .protocolError) "wrong stream parity used the wrong error code"

private def testContinuationExclusivity : IO Unit := do
  let first ← headersFrame 1 (Headers.singleton "x-long" "value") false
  let initial ← afterPeerSettings .server
  let (state, _, _) ← requireOk <|
    Connection.processFrame initial first
  let ping ← requireOk <| Ping.frame (ByteArray.mk #[0, 1, 2, 3, 4, 5, 6, 7])
  let error ← requireError (Connection.processFrame state ping)
    "a PING was interleaved into a header block"
  expect (error.code == .protocolError) "header interleaving used the wrong error code"

private def testPaddedFlowControl : IO Unit := do
  let opening ← headersFrame 1 (Headers.singleton ":method" "POST")
  let initial ← afterPeerSettings .server
  let (state, _, _) ← requireOk <|
    Connection.processFrame initial opening
  let payload := ByteArray.mk #[2, 97, 0, 0]
  let data : Frame := {
    header := {
      length := payload.size
      frameType := .data
      flags := FrameFlag.padded
      streamId := 1
    }
    payload
  }
  let (state, automatic, events) ← requireOk <| Connection.processFrame state data
  expect (automatic.size == 2)
    "padded DATA did not immediately restore connection and padding credit"
  match events[0]? with
  | some (Connection.Event.data 1 bytes false) =>
      expect (bytes == ByteArray.mk #[97]) "DATA padding reached the application"
  | _ => fail "padded DATA emitted the wrong event"
  let stream := state.streams.find? (·.id == 1)
  expect (stream.map (·.inboundWindow) == some (Connection.initialWindowSize - 1))
    "only application bytes should remain charged to the stream"
  let (state, updates) ← requireOk <| Connection.acknowledgeData state 1 1
  expect (updates.size == 1) "application consumption did not return stream credit"
  let stream := state.streams.find? (·.id == 1)
  expect (stream.map (·.inboundWindow) == some Connection.initialWindowSize)
    "stream receive credit was not conserved"

private def testOutboundFragmentation : IO Unit := do
  let headers := Headers.singleton ":method" "CONNECT"
  let (state, streamId, frames) ← requireOk <|
    Connection.openStream (Connection.initial .client) headers
  expect (streamId == 1 && state.nextLocalStreamId == 3)
    "the client did not allocate odd increasing stream identifiers"
  expect (!frames.isEmpty && frames[0]!.header.frameType == .headers)
    "opening a stream did not emit HEADERS"
  expect (FrameFlag.has frames[frames.size - 1]!.header.flags FrameFlag.endHeaders)
    "the final header fragment omitted END_HEADERS"

def main : IO Unit := do
  testSettings
  testStreamIdentityAndHeaders
  testContinuationExclusivity
  testPaddedFlowControl
  testOutboundFragmentation
  IO.println "HTTP/2 connection tests passed"
