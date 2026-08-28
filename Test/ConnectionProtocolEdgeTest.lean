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

private def readyState (role : Connection.Role) : Connection.State := {
  Connection.initial role with receivedSettings := true
}

private def headersFrame (streamId : Nat) (block : ByteArray)
    (endStream : Bool := false) : Frame := {
  header := {
    length := block.size
    frameType := .headers
    flags := FrameFlag.combine <| #[FrameFlag.endHeaders] ++
      if endStream then #[FrameFlag.endStream] else #[]
    streamId
  }
  payload := block
}

private def expectStreamError (automatic : Array Frame) (events : Array Connection.Event)
    (streamId : Nat) (code : ErrorCode) : IO Unit := do
  expect (automatic.size == 1 && automatic[0]!.header.streamId == streamId)
    "a decoded stream failure did not produce exactly one matching RST_STREAM"
  expect ((← requireOk <| RstStream.decode automatic[0]!) == code)
    "a decoded stream failure used the wrong RST_STREAM code"
  match events[0]? with
  | some (Connection.Event.streamError eventStreamId eventCode _) =>
      expect (events.size == 1 && eventStreamId == streamId && eventCode == code)
        "a decoded stream failure emitted the wrong terminal event"
  | _ => fail "a decoded stream failure omitted its terminal event"

private def validRequest (path : String := "/") : Headers :=
  Headers.empty
    |>.insert ":method" "GET"
    |>.insert ":scheme" "https"
    |>.insert ":authority" "example.test"
    |>.insert ":path" path

private def testInitialWindowUpdatesAreOrderedAndChecked : IO Unit := do
  let stream : Connection.Stream := {
    id := 1
    phase := .open
    outboundWindow := Int.ofNat Connection.maximumWindowSize - 1
  }
  let state := { readyState .client with streams := #[stream], nextLocalStreamId := 3 }
  let transientOverflow ← requireOk <| Http2.Settings.frame #[
    { id := .initialWindowSize, value := Connection.initialWindowSize + 2 },
    { id := .initialWindowSize, value := Connection.initialWindowSize }
  ]
  let error ← requireError (Connection.processFrame state transientOverflow)
    "duplicate initial-window settings hid a transient stream-window overflow"
  expect (error.scope == .connection && error.code == .flowControlError)
    "initial-window overflow used the wrong error scope or code"

  let stream := { stream with outboundWindow := 10 }
  let state := { state with streams := #[stream] }
  let decrease ← requireOk <| Http2.Settings.frame #[
    { id := .initialWindowSize, value := 0 }
  ]
  let (state, _, _) ← requireOk <| Connection.processFrame state decrease
  let adjusted := state.streams[0]!.outboundWindow
  expect (adjusted == 10 - Int.ofNat Connection.initialWindowSize)
    "a valid SETTINGS_INITIAL_WINDOW_SIZE decrease did not permit a negative window"

private def zeroWindowUpdate (streamId : Nat) : Frame := {
  header := {
    length := 4
    frameType := .windowUpdate
    streamId
  }
  payload := ByteArray.mk #[0, 0, 0, 0]
}

private def testWindowUpdateStateAndScope : IO Unit := do
  let closed : Connection.Stream := {
    id := 1
    phase := .closed
    outboundWindow := Int.ofNat Connection.maximumWindowSize
  }
  let state := { readyState .client with streams := #[closed], nextLocalStreamId := 3 }
  let update ← requireOk <| WindowUpdate.frame 1 1
  let (state, _, _) ← requireOk <| Connection.processFrame state update
  expect (state.streams[0]!.outboundWindow == closed.outboundWindow)
    "WINDOW_UPDATE changed or overflowed a closed stream window"

  let streamError ← requireError (Connection.processFrame state (zeroWindowUpdate 1))
    "a zero stream WINDOW_UPDATE increment was accepted"
  expect (streamError.scope == .stream 1 && streamError.code == .protocolError)
    "a zero stream WINDOW_UPDATE increment used connection scope"
  let connectionError ← requireError (Connection.processFrame state (zeroWindowUpdate 0))
    "a zero connection WINDOW_UPDATE increment was accepted"
  expect (connectionError.scope == .connection && connectionError.code == .protocolError)
    "a zero connection WINDOW_UPDATE increment used the wrong scope"

private def testClosedResetIsIgnored : IO Unit := do
  let closed : Connection.Stream := { id := 1, phase := .closed }
  let state := { readyState .client with streams := #[closed], nextLocalStreamId := 3 }
  let reset ← requireOk <| RstStream.frame 1 .cancel
  let (next, automatic, events) ← requireOk <| Connection.processFrame state reset
  expect (next.streams == state.streams && automatic.isEmpty && events.isEmpty)
    "RST_STREAM on a closed stream was not ignored"

private def testGoAwayRestrictions : IO Unit := do
  let first ← requireOk <| GoAway.frame 9 .noError
  let (state, _, _) ← requireOk <| Connection.processFrame (readyState .client) first
  let decreasing ← requireOk <| GoAway.frame 7 .noError
  let (state, _, _) ← requireOk <| Connection.processFrame state decreasing
  expect (state.peerGoAwayLastStream? == some 7)
    "a decreasing successive GOAWAY was not retained"
  let increasing ← requireOk <| GoAway.frame 9 .noError
  let error ← requireError (Connection.processFrame state increasing)
    "a successive GOAWAY increased its last processed stream identifier"
  expect (error.scope == .connection && error.code == .protocolError)
    "an increasing successive GOAWAY used the wrong error scope or code"

  let state := {
    readyState .client with peerGoAwayLastStream? := some maxStreamId
  }
  let error ← requireError (Connection.openStream state Headers.empty)
    "a new stream was opened after peer GOAWAY"
  expect (error.code == .refusedStream)
    "opening a stream after GOAWAY used the wrong error code"

private def testDecodedFailurePreservesCompressionState : IO Unit := do
  let malformed := (validRequest "/bad")
    |>.insert "x-dynamic" "connection-scoped"
    |>.insert "connection" "close"
  let valid := (validRequest "/good").insert "x-dynamic" "connection-scoped"
  let (malformedBlock, encoder) ← requireOk <| Hpack.encodeHeaderBlock {} malformed
  let (validBlock, _) ← requireOk <| Hpack.encodeHeaderBlock encoder valid
  let state := readyState .server
  let (state, automatic, events) ← requireOk <|
    Connection.processFrame state (headersFrame 1 malformedBlock)
  expectStreamError automatic events 1 .protocolError
  let failed := state.streams.find? (fun stream => stream.id == 1)
  expect (failed.any fun stream => stream.phase == .closed && stream.locallyReset)
    "a decoded field failure did not close and locally reset its stream"
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (headersFrame 3 validBlock true)
  expect automatic.isEmpty "a valid dynamic-reference request produced an automatic frame"
  match events[0]? with
  | some (Connection.Event.headers 3 headers true false) =>
      expect (headers == valid)
        "HPACK state was lost after a stream-scoped field validation failure"
  | _ => fail "the request after a decoded stream failure was not delivered"

private def testInformationalAndTrailerSequence : IO Unit := do
  let stream : Connection.Stream := { id := 1, phase := .open, sentHeaders := true }
  let mut state := {
    readyState .client with streams := #[stream], nextLocalStreamId := 3
  }
  let informational := Headers.singleton ":status" "103"
  let final := Headers.singleton ":status" "200"
  let trailers := Headers.singleton "x-checksum" "complete"
  let (firstBlock, encoder) ← requireOk <| Hpack.encodeHeaderBlock {} informational
  let (secondBlock, encoder) ← requireOk <| Hpack.encodeHeaderBlock encoder informational
  let (finalBlock, encoder) ← requireOk <| Hpack.encodeHeaderBlock encoder final
  let (trailerBlock, _) ← requireOk <| Hpack.encodeHeaderBlock encoder trailers
  for block in #[firstBlock, secondBlock] do
    let (next, automatic, events) ← requireOk <|
      Connection.processFrame state (headersFrame 1 block)
    expect automatic.isEmpty "an informational response produced an automatic frame"
    match events[0]? with
    | some (Connection.Event.headers 1 headers false false) =>
        expect (headers == informational) "an informational response changed during decoding"
    | _ => fail "an informational response was misclassified"
    let current := next.streams[0]!
    expect (!current.receivedHeaders && !current.receivedTrailers)
      "an informational response advanced final-response state"
    state := next
  let (next, _, events) ← requireOk <|
    Connection.processFrame state (headersFrame 1 finalBlock)
  match events[0]? with
  | some (Connection.Event.headers 1 headers false false) =>
      expect (headers == final) "the final response changed during decoding"
  | _ => fail "the final response was misclassified as trailers"
  expect next.streams[0]!.receivedHeaders
    "the final response did not establish response state"
  let (next, _, events) ← requireOk <|
    Connection.processFrame next (headersFrame 1 trailerBlock true)
  match events[0]? with
  | some (Connection.Event.headers 1 headers true true) =>
      expect (headers == trailers) "trailing fields changed during decoding"
  | _ => fail "a valid END_STREAM trailer section was misclassified"
  expect (next.streams[0]!.receivedTrailers && next.streams[0]!.phase == .halfClosedRemote)
    "valid trailers did not close the remote stream half"

private def testMalformedFieldSections : IO Unit := do
  let uppercase : Headers := (validRequest).push { name := "X-Bad", value := "value" }
  let pseudoAfterOrdinary : Headers := #[
    { name := ":method", value := "GET" },
    { name := "x-before", value := "value" },
    { name := ":scheme", value := "https" },
    { name := ":path", value := "/" }
  ]
  let (uppercaseBlock, encoder) ← requireOk <| Hpack.encodeHeaderBlock {} uppercase
  let (layoutBlock, _) ← requireOk <| Hpack.encodeHeaderBlock encoder pseudoAfterOrdinary
  let (state, automatic, events) ← requireOk <|
    Connection.processFrame (readyState .server) (headersFrame 1 uppercaseBlock)
  expectStreamError automatic events 1 .protocolError
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (headersFrame 3 layoutBlock)
  expectStreamError automatic events 3 .protocolError

  let responseStream : Connection.Stream := {
    id := 1, phase := .open, sentHeaders := true
  }
  let client := {
    readyState .client with streams := #[responseStream], nextLocalStreamId := 3
  }
  let (badStatusBlock, _) ← requireOk <|
    Hpack.encodeHeaderBlock {} (Headers.singleton ":status" "101")
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame client (headersFrame 1 badStatusBlock)
  expectStreamError automatic events 1 .protocolError

  let trailerStream := { responseStream with receivedHeaders := true }
  let client := { client with streams := #[trailerStream] }
  let (trailerBlock, _) ← requireOk <|
    Hpack.encodeHeaderBlock {} (Headers.singleton "x-trailer" "value")
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame client (headersFrame 1 trailerBlock)
  expectStreamError automatic events 1 .protocolError

private def testHeaderListLimitOnResetStreams : IO Unit := do
  let fields := (Headers.singleton ":status" "200").insert "x-large" ("a".pushn 'b' 96)
  let (block, encoder) ← requireOk <| Hpack.encodeHeaderBlock {} fields
  let (nextBlock, _) ← requireOk <| Hpack.encodeHeaderBlock encoder fields
  let active : Connection.Stream := { id := 1, phase := .open, sentHeaders := true }
  let limited := {
    readyState .client with
    localSettings := { (readyState .client).localSettings with maxHeaderListSize := some 64 }
    streams := #[active]
    nextLocalStreamId := 3
  }
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame limited (headersFrame 1 block)
  expectStreamError automatic events 1 .enhanceYourCalm

  let reset := { active with phase := .closed, locallyReset := true }
  let discarded := { limited with streams := #[reset] }
  let (discarded, automatic, events) ← requireOk <|
    Connection.processFrame discarded (headersFrame 1 block)
  expect (automatic.isEmpty && events.isEmpty)
    "an oversized field section racing a local reset produced another reset"
  let openStream : Connection.Stream := { id := 3, phase := .open, sentHeaders := true }
  let unlimited := {
    discarded with
    localSettings := { discarded.localSettings with maxHeaderListSize := none }
    streams := discarded.streams.push openStream
    nextLocalStreamId := 5
  }
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame unlimited (headersFrame 3 nextBlock)
  expect automatic.isEmpty "a dynamic reference after discarded headers produced a reset"
  match events[0]? with
  | some (Connection.Event.headers 3 decoded false false) =>
      expect (decoded == fields)
        "discarded oversized headers did not advance the HPACK decoder"
  | _ => fail "headers after an oversized discarded block were not delivered"

private def testOutboundFieldValidationAndResponseState : IO Unit := do
  let invalid : Headers := (validRequest).push { name := "X-Bad", value := "value" }
  let error ← requireError (Connection.openStream (readyState .client) invalid)
    "openStream emitted an uppercase field name"
  expect (error.scope == .localInput && error.code == .protocolError)
    "outbound request validation used the wrong scope or code"
  let extended := Headers.empty
    |>.insert ":method" "CONNECT"
    |>.insert ":protocol" "websocket"
    |>.insert ":scheme" "https"
    |>.insert ":authority" "example.test"
    |>.insert ":path" "/socket"
  let error ← requireError (Connection.openStream (readyState .client) extended)
    "openStream emitted extended CONNECT before peer enablement"
  expect (error.scope == .localInput) "disabled extended CONNECT was not a local input error"

  let requestStream : Connection.Stream := {
    id := 1, phase := .open, receivedHeaders := true
  }
  let mut server := { readyState .server with streams := #[requestStream], lastPeerStreamId := 1 }
  let informational := Headers.singleton ":status" "103"
  for _ in [0, 1] do
    let (next, _) ← requireOk <| Connection.sendHeaders server 1 informational
    expect (!next.streams[0]!.sentHeaders)
      "an outbound informational response established final-response state"
    server := next
  let (finalized, _) ← requireOk <|
    Connection.sendHeaders server 1 (Headers.singleton ":status" "200")
  expect finalized.streams[0]!.sentHeaders
    "an outbound final response did not establish response state"
  let error ← requireError
    (Connection.sendHeaders finalized 1 (Headers.singleton "x-trailer" "value"))
    "outbound trailers omitted END_STREAM"
  expect (error.scope == .localInput && error.code == .protocolError)
    "outbound trailer validation used the wrong scope or code"
  let (trailed, _) ← requireOk <|
    Connection.sendHeaders finalized 1 (Headers.singleton "x-trailer" "value") true
  expect (trailed.streams[0]!.sentTrailers && trailed.streams[0]!.phase == .halfClosedLocal)
    "valid outbound trailers did not close the local stream half"

def main : IO Unit := do
  testInitialWindowUpdatesAreOrderedAndChecked
  testWindowUpdateStateAndScope
  testClosedResetIsIgnored
  testGoAwayRestrictions
  testDecodedFailurePreservesCompressionState
  testInformationalAndTrailerSequence
  testMalformedFieldSections
  testHeaderListLimitOnResetStreams
  testOutboundFieldValidationAndResponseState
  IO.println "HTTP/2 connection protocol edge tests passed"
