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

private def request (method : String := "GET") (path : String := "/") : Headers :=
  Headers.empty
    |>.insert ":method" method
    |>.insert ":scheme" "https"
    |>.insert ":authority" "example.test"
    |>.insert ":path" path

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

private def dataFrame (streamId : Nat) (payload : ByteArray)
    (endStream : Bool := false) : Frame := {
  header := {
    length := payload.size
    frameType := .data
    flags := if endStream then FrameFlag.endStream else 0
    streamId
  }
  payload
}

private def hasStreamError (events : Array Connection.Event)
    (streamId : Nat) (code : ErrorCode) : Bool :=
  events.any fun
  | .streamError actualId actualCode _ => actualId == streamId && actualCode == code
  | _ => false

private def hasReset (frames : Array Frame) (streamId : Nat) (code : ErrorCode) : Bool :=
  frames.any fun frame =>
    frame.header.frameType == .rstStream && frame.header.streamId == streamId &&
      match RstStream.decode frame with
      | .ok actual => actual == code
      | .error _ => false

private def hasConnectionCredit (frames : Array Frame) (amount : Nat) : Bool :=
  frames.any fun frame =>
    frame.header.frameType == .windowUpdate && frame.header.streamId == 0 &&
      match WindowUpdate.decode frame with
      | .ok actual => actual == amount
      | .error _ => false

private def testContentLengthAndNoContent : IO Unit := do
  let fields := request
    |>.insert "content-length" "3, 3"
    |>.insert "content-length" "3"
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} fields
  let (state, automatic, _) ← requireOk <|
    Connection.processFrame (readyState .server) (headersFrame 1 block)
  expect automatic.isEmpty "valid repeated Content-Length produced a reset"
  let payload := ByteArray.mk #[1, 2, 3]
  let (state, automatic, events) ← requireOk <|
    Connection.processFrame state (dataFrame 1 payload true)
  expect (hasConnectionCredit automatic 3)
    "valid request DATA did not restore connection credit"
  match events[0]? with
  | some (Connection.Event.data 1 actual true) =>
      expect (actual == payload) "valid Content-Length DATA changed"
  | _ => fail "valid Content-Length DATA was not delivered"
  expect (state.streams[0]!.inboundBodyBytes == 3)
    "valid Content-Length body bytes were not recorded"

  let conflicting := request
    |>.insert "content-length" "2"
    |>.insert "content-length" "3"
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} conflicting
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame (readyState .server) (headersFrame 1 block)
  expect (hasReset automatic 1 .protocolError && hasStreamError events 1 .protocolError)
    "conflicting Content-Length was not a stream protocol error"

  let overflow := request |>.insert "content-length" "18446744073709551616"
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} overflow
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame (readyState .server) (headersFrame 1 block)
  expect (hasReset automatic 1 .protocolError && hasStreamError events 1 .protocolError)
    "overflowing Content-Length was accepted"

  let short := request |>.insert "content-length" "1"
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} short
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame (readyState .server) (headersFrame 1 block true)
  expect (hasReset automatic 1 .protocolError && hasStreamError events 1 .protocolError)
    "END_STREAM before the declared body length was accepted"

  let exceed := request |>.insert "content-length" "2"
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} exceed
  let (state, _, _) ← requireOk <|
    Connection.processFrame (readyState .server) (headersFrame 1 block)
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (dataFrame 1 payload true)
  expect (hasConnectionCredit automatic 3 && hasReset automatic 1 .protocolError &&
      hasStreamError events 1 .protocolError)
    "DATA beyond Content-Length did not return credit and reset only the stream"

  let responseStream : Connection.Stream := {
    id := 1
    phase := .open
    sentHeaders := true
    requestMethod? := some "GET"
  }
  let initialClient := {
    readyState .client with streams := #[responseStream], nextLocalStreamId := 3
  }
  let noContent := Headers.singleton ":status" "205" |>.insert "content-length" "0"
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} noContent
  let (client, automatic, _) ← requireOk <|
    Connection.processFrame initialClient (headersFrame 1 block)
  expect automatic.isEmpty "valid 205 response was reset"
  expect (client.streams[0]!.inboundBodyKind == .noContent)
    "205 response was not marked no-content"
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame client (dataFrame 1 (ByteArray.mk #[9]))
  expect (hasConnectionCredit automatic 1 && hasReset automatic 1 .protocolError &&
      hasStreamError events 1 .protocolError)
    "nonempty 205 DATA was accepted or leaked connection credit"

  let invalid205 := Headers.singleton ":status" "205" |>.insert "content-length" "1"
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} invalid205
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame initialClient (headersFrame 1 block)
  expect (hasReset automatic 1 .protocolError && hasStreamError events 1 .protocolError)
    "205 response with nonzero Content-Length was accepted"

  let outbound := request |>.insert "content-length" "3"
  let (client, streamId, _) ← requireOk <|
    Connection.openStream (readyState .client) outbound
  let error ← requireError
    (Connection.sendData client streamId (ByteArray.mk #[1, 2]) true)
    "outbound END_STREAM violated Content-Length"
  expect (error.scope == .localInput)
    "outbound Content-Length mismatch was not a local input error"
  let (_, frames) ← requireOk <|
    Connection.sendData client streamId (ByteArray.mk #[1, 2, 3]) true
  expect (frames.size == 1) "exact outbound Content-Length did not produce DATA"

private def testDataFailurePrecedence : IO Unit := do
  let stream : Connection.Stream := { id := 1, phase := .open, sentHeaders := true }
  let state := {
    readyState .client with streams := #[stream], nextLocalStreamId := 3
  }
  let payload := ByteArray.mk #[1, 2, 3]
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (dataFrame 1 payload)
  expect (hasConnectionCredit automatic 3 && hasReset automatic 1 .protocolError &&
      hasStreamError events 1 .protocolError)
    "DATA before a final field section did not return connection credit"

  let closed := { stream with phase := .closed }
  let exhausted := {
    state with streams := #[closed], inboundConnectionWindow := 2
  }
  let error ← requireError (Connection.processFrame exhausted (dataFrame 1 payload))
    "stream-scoped DATA handling masked connection-window exhaustion"
  expect (error.scope == .connection && error.code == .flowControlError)
    "connection-window exhaustion did not take precedence"

  let idleZero : Frame := {
    header := { length := 4, frameType := .windowUpdate, streamId := 3 }
    payload := ByteArray.mk #[0, 0, 0, 0]
  }
  let error ← requireError (Connection.processFrame state idleZero)
    "zero WINDOW_UPDATE on an idle stream was accepted"
  expect (error.scope == .connection && error.code == .protocolError)
    "WINDOW_UPDATE increment validation masked the idle-stream connection error"

private def testProcessBytesContinuesAfterStreamError : IO Unit := do
  let stream : Connection.Stream := { id := 1, phase := .open, sentHeaders := true }
  let state := {
    readyState .client with streams := #[stream], nextLocalStreamId := 3
  }
  let ping ← requireOk <| Ping.frame (ByteArray.mk #[0, 1, 2, 3, 4, 5, 6, 7])
  let bytes ← requireOk <| Frame.encodeBatch #[{
    header := { length := 4, frameType := .windowUpdate, streamId := 1 }
    payload := ByteArray.mk #[0, 0, 0, 0]
  }, ping]
  let result ← requireOk <| Connection.processBytes state bytes
  expect result.error?.isNone "a stream error terminated byte-chunk processing"
  expect (hasReset result.outbound 1 .protocolError &&
      result.outbound.any Ping.isAck && hasStreamError result.events 1 .protocolError)
    "frames following a same-buffer stream error were not processed"

  let oversizedHeader ← requireOk <| Frame.encodeHeader {
    length := defaultMaxFramePayloadLength + 1
    frameType := .data
    streamId := 1
  }
  let oversized ← requireOk <|
    Connection.processBytes (readyState .client) oversizedHeader
  expect (oversized.error?.any fun error =>
      error.scope == .connection && error.code == .frameSizeError)
    "an oversized declared payload was awaited instead of rejected from its header"
  expect oversized.state.decoder.buffered.isEmpty
    "an oversized declared payload header remained buffered"

private def testCompressedBounds : IO Unit := do
  let fields := request |>.insert "x-large" ("a".pushn 'b' 96)
  let (block, _) ← requireOk <| Hpack.encodeHeaderBlock {} fields
  let bounded := {
    readyState .server with
    localSettings := {
      (readyState .server).localSettings with maxCompressedHeaderBlockSize := 4
    }
  }
  let error ← requireError (Connection.processFrame bounded (headersFrame 1 block))
    "compressed field block resource bound was not enforced"
  expect (error.scope == .connection && error.code == .enhanceYourCalm)
    "compressed field block resource failure used the wrong scope"

  let decodedLimited := {
    readyState .server with
    localSettings := {
      (readyState .server).localSettings with
      maxHeaderListSize := some 64
      maxCompressedHeaderBlockSize := 4096
    }
  }
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame decodedLimited (headersFrame 1 block)
  expect (hasReset automatic 1 .enhanceYourCalm &&
      hasStreamError events 1 .enhanceYourCalm)
    "decoded field-list limit was conflated with the compressed-block bound"

private def testClosedStreamRetention : IO Unit := do
  let closed := (List.range Connection.maximumClosedStreamRecords).foldl
    (init := #[]) fun streams index =>
      streams.push ({ id := index * 2 + 1, phase := .closed } : Connection.Stream)
  let activeId := Connection.maximumClosedStreamRecords * 2 + 1
  let active : Connection.Stream := {
    id := activeId, phase := .open, sentHeaders := true, receivedHeaders := true
  }
  let state := {
    readyState .client with
    streams := closed.push active
    nextLocalStreamId := activeId + 2
  }
  let (state, _) ← requireOk <| Connection.resetStream state activeId
  expect (state.streams.size == Connection.maximumClosedStreamRecords &&
      !(state.streams.any (·.id == 1)))
    "closed stream retention was not bounded or did not prune the oldest record"

  let reset ← requireOk <| RstStream.frame 1 .cancel
  let (stateAfterReset, automatic, events) ← requireOk <|
    Connection.processFrame state reset
  expect (automatic.isEmpty && events.isEmpty && stateAfterReset.streams == state.streams)
    "late RST_STREAM on a pruned closed stream was not ignored"
  let update ← requireOk <| WindowUpdate.frame 1 1
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state update
  expect (automatic.isEmpty && events.isEmpty)
    "late WINDOW_UPDATE on a pruned closed stream was not ignored"

  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (dataFrame 1 (ByteArray.mk #[1]))
  expect (hasConnectionCredit automatic 1 && hasReset automatic 1 .streamClosed &&
      hasStreamError events 1 .streamClosed)
    "late DATA on a pruned closed stream was misclassified"

  let (block, _) ← requireOk <|
    Hpack.encodeHeaderBlock {} (Headers.singleton ":status" "200")
  let (afterHeaders, automatic, events) ← requireOk <|
    Connection.processFrame state (headersFrame 1 block)
  expect (hasReset automatic 1 .streamClosed && hasStreamError events 1 .streamClosed &&
      afterHeaders.streams.size == Connection.maximumClosedStreamRecords)
    "late HEADERS on a pruned closed stream lost closed-stream semantics"

private def testPriorityGoAwayUriAndTunnel : IO Unit := do
  let malformedPriority : Frame := {
    header := { length := 4, frameType := .priority, streamId := 1 }
    payload := ByteArray.mk #[0, 0, 0, 0]
  }
  let (priorityState, automatic, events) ← requireOk <|
    Connection.processFrame (readyState .server) malformedPriority
  expect (hasReset automatic 1 .frameSizeError &&
      hasStreamError events 1 .frameSizeError)
    "malformed PRIORITY on an idle stream was not contained to that stream"

  let (requestBlock, _) ← requireOk <| Hpack.encodeHeaderBlock {} request
  let (priorityState, automatic, events) ← requireOk <|
    Connection.processFrame priorityState (headersFrame 1 requestBlock)
  let priorityStream := priorityState.streams.find? (·.id == 1)
  expect (automatic.isEmpty && events.isEmpty && priorityStream.any (·.phase == .closed))
    "HEADERS reopened an idle stream after its automatic RST_STREAM"

  let (localPriorityState, _, _) ← requireOk <|
    Connection.processFrame (readyState .client) malformedPriority
  let (localPriorityState, allocatedId, _) ← requireOk <|
    Connection.openStream localPriorityState request
  expect (allocatedId == 3 && localPriorityState.streams.map (·.id) == #[1, 3])
    "local stream allocation reused an ID closed by an automatic RST_STREAM"

  let afterGoAway := {
    readyState .server with peerGoAwayLastStream? := some 0
  }
  let error ← requireError
    (Connection.processFrame afterGoAway (headersFrame 1 requestBlock))
    "peer opened a stream after sending GOAWAY"
  expect (error.scope == .connection && error.code == .protocolError)
    "peer stream after GOAWAY used the wrong error classification"

  let invalidTargets := #[
    request "GET" "*",
    request "GET" "/bad#fragment",
    request "GET" "/bad%2"
  ]
  for fields in invalidTargets do
    let error ← requireError (Connection.openStream (readyState .client) fields)
      "invalid request target was accepted"
    expect (error.scope == .localInput && error.code == .protocolError)
      "invalid request target used the wrong error classification"
  let (_, _, _) ← requireOk <|
    Connection.openStream (readyState .client) (request "OPTIONS" "*")
  let userinfo := (request).map fun header =>
    if header.name == ":authority" then { header with value := "user@example.test" }
    else header
  let _ ← requireError (Connection.openStream (readyState .client) userinfo)
    "userinfo in https authority was accepted"

  let connect := Headers.empty
    |>.insert ":method" "CONNECT"
    |>.insert ":authority" "example.test:443"
  let (client, streamId, _) ← requireOk <|
    Connection.openStream (readyState .client) connect
  let (responseBlock, _) ← requireOk <|
    Hpack.encodeHeaderBlock {} (Headers.singleton ":status" "200")
  let (client, _, _) ← requireOk <|
    Connection.processFrame client (headersFrame streamId responseBlock)
  let stream := client.streams.find? (·.id == streamId)
  expect (stream.any fun current => current.inboundBodyKind == .tunnel &&
      current.outboundBodyKind == .tunnel)
    "successful CONNECT did not switch both directions to tunnel accounting"
  let tunnelBytes := ByteArray.mk #[4, 5, 6]
  let (client, outbound) ← requireOk <|
    Connection.sendData client streamId tunnelBytes
  expect (outbound.size == 1) "outbound CONNECT tunnel DATA was rejected"
  let (_, _, events) ← requireOk <|
    Connection.processFrame client (dataFrame streamId tunnelBytes)
  match events[0]? with
  | some (Connection.Event.data actualId actual false) =>
      expect (actualId == streamId && actual == tunnelBytes)
        "inbound CONNECT tunnel DATA was not delivered"
  | _ => fail "inbound CONNECT tunnel DATA was rejected"
  let trailers := Headers.singleton "x-trailer" "forbidden"
  let error ← requireError (Connection.sendHeaders client streamId trailers true)
    "outbound field section followed a successful CONNECT response"
  expect (error.scope == .localInput && error.code == .protocolError)
    "outbound CONNECT trailer rejection used the wrong error scope"
  let (trailerBlock, _) ← requireOk <| Hpack.encodeHeaderBlock {} trailers
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame client (headersFrame streamId trailerBlock true)
  expect (hasReset automatic streamId .protocolError &&
      hasStreamError events streamId .protocolError)
    "inbound field section followed a successful CONNECT response"

private def testWireBuilderBoundsAndSettingsOrdering : IO Unit := do
  let maxCode := ErrorCode.unknown maxUInt32Value
  let overflowCode := ErrorCode.unknown (maxUInt32Value + 1)
  let _ ← requireOk <| RstStream.frame 1 maxCode
  let _ ← requireOk <| GoAway.frame 0 maxCode
  let _ ← requireError (RstStream.frame 1 overflowCode)
    "RST_STREAM truncated an oversized error code"
  let _ ← requireError (GoAway.frame 0 overflowCode)
    "GOAWAY truncated an oversized error code"
  let _ ← requireOk <| Http2.Settings.frame #[{
    id := .unknown 65535, value := maxUInt32Value
  }]
  let _ ← requireError (Http2.Settings.frame #[{
    id := .unknown 65535, value := maxUInt32Value + 1
  }]) "SETTINGS truncated an oversized value"

  let settings ← requireOk <| Http2.Settings.frame #[
    { id := .headerTableSize, value := 0 },
    { id := .headerTableSize, value := Hpack.defaultDynamicTableSize }
  ]
  let (state, _, _) ← requireOk <|
    Connection.processFrame (readyState .client) settings
  expect (state.hpackEncode.pendingMinimumSizeUpdate == some 0 &&
      state.hpackEncode.pendingSizeUpdate == some Hpack.defaultDynamicTableSize)
    "duplicate header-table settings lost their minimum/final ordering"

private def testWireFieldOctets : IO Unit := do
  let (requestBlock, _) ← requireOk <| Hpack.encodeHeaderBlock {} request
  let name ← requireOk <| Hpack.encodeString "x-obs"
  let obsLiteral := (ByteArray.mk #[0]).append name |>.append
    (ByteArray.mk #[2, 0x80, 0xff])
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame (readyState .server)
      (headersFrame 1 (requestBlock.append obsLiteral) true)
  expect automatic.isEmpty "valid obs-text field value reset its stream"
  match events[0]? with
  | some (Connection.Event.headers 1 headers true false) =>
      let field := headers.find? (·.name == "x-obs")
      expect (field.any fun header =>
          header.valueOctets == ByteArray.mk #[0x80, 0xff])
        "application headers lost obs-text wire octets"
  | _ => fail "valid obs-text request fields were not delivered"

  let value ← requireOk <| Hpack.encodeString "v"
  let invalidNameLiteral := (ByteArray.mk #[0, 1, 0xff]).append value
  let dynamicRequest := request |>.insert "x-dynamic" "shared"
  let (dynamicBlock, encoder) ← requireOk <|
    Hpack.encodeHeaderBlock {} dynamicRequest
  let (reusedBlock, _) ← requireOk <|
    Hpack.encodeHeaderBlock encoder dynamicRequest
  let (state, automatic, events) ← requireOk <|
    Connection.processFrame (readyState .server)
      (headersFrame 1 (dynamicBlock.append invalidNameLiteral) true)
  expect (hasReset automatic 1 .protocolError &&
      hasStreamError events 1 .protocolError)
    "non-ASCII field-name octets were not rejected at stream scope"
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (headersFrame 3 reusedBlock true)
  expect automatic.isEmpty "dynamic reference after a raw-name failure reset its stream"
  match events[0]? with
  | some (Connection.Event.headers 3 headers true false) =>
      expect (headers == dynamicRequest)
        "raw-name failure did not commit connection-wide HPACK state"
  | _ => fail "later stream could not reuse HPACK state after a raw-name failure"

private def testCompositionQueries : IO Unit := do
  let stream : Connection.Stream := {
    id := 1, phase := .open, outboundWindow := 10
  }
  let state := {
    readyState .client with
    streams := #[stream]
    nextLocalStreamId := 3
    lastPeerStreamId := 7
    outboundConnectionWindow := 5
  }
  expect (Connection.stream? state 1 == some stream &&
      Connection.outboundCredit? state 1 == some 5 &&
      Connection.outboundCredit? state 3 == none)
    "public stream or outbound-credit query returned the wrong state"
  let (state, frame) ← requireOk <| Connection.beginGoAway state
  let decoded ← requireOk <| GoAway.decode frame
  expect (decoded.lastStreamId == 7 && state.localGoAwayLastStream? == some 7)
    "local GOAWAY transition did not retain its advertised boundary"
  let advanced := { state with lastPeerStreamId := 9 }
  let (advanced, frame) ← requireOk <| Connection.beginGoAway advanced
  let decoded ← requireOk <| GoAway.decode frame
  expect (decoded.lastStreamId == 7 && advanced.localGoAwayLastStream? == some 7)
    "repeated local GOAWAY increased its advertised boundary"

private def testLocalSettingsActivateOnAcknowledgement : IO Unit := do
  let configured : Connection.Settings := {
    headerTableSize := 0
    initialWindowSize := 100
  }
  let peerSettings ← requireOk <| Http2.Settings.frame #[]
  let (state, _, _) ← requireOk <|
    Connection.processFrame (Connection.initial .server configured) peerSettings
  expect (state.peerKnownLocalSettings.headerTableSize == Hpack.defaultDynamicTableSize &&
      state.peerKnownLocalSettings.initialWindowSize == Connection.initialWindowSize)
    "local settings became mandatory before peer acknowledgement"

  let fields := request "POST" |>.insert "x-dynamic" "pre-ack"
  let (block, encoder) ← requireOk <| Hpack.encodeHeaderBlock {} fields
  let (state, automatic, _) ← requireOk <|
    Connection.processFrame state (headersFrame 1 block)
  expect automatic.isEmpty "pre-ack request encoded under defaults was rejected"
  let payload := ByteArray.mk (Array.replicate 200 1)
  let (state, _, _) ← requireOk <|
    Connection.processFrame state (dataFrame 1 payload)
  expect ((Connection.stream? state 1).any fun stream => stream.inboundWindow == 65335)
    "pre-ack DATA did not use the default initial window"

  let acknowledgement ← requireOk <| Http2.Settings.frame #[] true
  let (state, _, _) ← requireOk <|
    Connection.processFrame state acknowledgement
  expect (state.peerKnownLocalSettings == configured &&
      (Connection.stream? state 1).any fun stream => stream.inboundWindow == -100)
    "acknowledged initial-window reduction did not preserve outstanding debt"
  let (state, automatic, events) ← requireOk <|
    Connection.processFrame state (dataFrame 1 ByteArray.empty)
  expect (automatic.isEmpty && events.size == 1)
    "zero-length DATA was rejected while the stream window carried negative debt"

  let (unupdated, _) ← requireOk <| Hpack.encodeHeaderBlock encoder fields
  let error ← requireError
    (Connection.processFrame state (headersFrame 3 unupdated))
    "peer omitted the HPACK update required after acknowledging a reduction"
  expect (error.scope == .connection && error.code == .compressionError)
    "missing post-ack HPACK update used the wrong error classification"

  let encoder := Hpack.setMaxAllowedSize encoder 0
  let (updated, _) ← requireOk <| Hpack.encodeHeaderBlock encoder fields
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (headersFrame 3 updated true)
  expect (automatic.isEmpty && events.size == 1)
    "required post-ack HPACK table-size update was rejected"

private def testConfiguredConcurrentStreamCapBeforeAcknowledgement : IO Unit := do
  let configured : Connection.Settings := { maxConcurrentStreams := some 1 }
  let state := {
    Connection.initial .server configured with receivedSettings := true
  }
  expect state.peerKnownLocalSettings.maxConcurrentStreams.isNone
    "configured concurrent-stream limit became a pre-ack protocol setting"
  let (firstBlock, encoder) ← requireOk <|
    Hpack.encodeHeaderBlock {} (request "POST" "/first")
  let (state, automatic, _) ← requireOk <|
    Connection.processFrame state (headersFrame 1 firstBlock)
  expect automatic.isEmpty "first stream was refused below the configured resource cap"
  let (secondBlock, _) ← requireOk <|
    Hpack.encodeHeaderBlock encoder (request "POST" "/second")
  let (_, automatic, events) ← requireOk <|
    Connection.processFrame state (headersFrame 3 secondBlock)
  expect (hasReset automatic 3 .refusedStream &&
      hasStreamError events 3 .refusedStream)
    "a second pre-ack stream exceeded the configured resource cap without refusal"

private def testSkippedPeerStreamCannotBeReopened : IO Unit := do
  let (higherBlock, encoder) ← requireOk <|
    Hpack.encodeHeaderBlock {} (request "GET" "/higher")
  let (lowerBlock, _) ← requireOk <|
    Hpack.encodeHeaderBlock encoder (request "GET" "/lower")
  let higher := headersFrame 5 higherBlock true
  let lower := headersFrame 3 lowerBlock true
  let wire ← requireOk <| Frame.encodeBatch #[higher, lower]
  let state := {
    readyState .server with prefaceReceived := true
  }
  let processed ← requireOk <| Connection.processBytes state wire
  expect (processed.events.size == 1 && processed.state.lastPeerStreamId == 5)
    "a valid higher stream preceding an identifier error was not committed"
  expect (processed.error?.any fun error =>
      error.scope == .connection && error.code == .protocolError)
    "reopening a skipped peer stream was not a connection protocol error"

private def testServerPrefaceOwnership : IO Unit := do
  let split := 7
  let first ← requireOk <|
    Connection.processBytes (Connection.initial .server)
      (connectionPreface.extract 0 split)
  expect (!first.state.prefaceReceived && first.error?.isNone &&
      first.state.prefaceBuffer.size == split)
    "fragmented client preface was not retained"
  let settings ← requireOk <| Http2.Settings.frame #[]
  let settingsWire ← requireOk <| Frame.encode settings
  let trailing := (connectionPreface.extract split connectionPreface.size).append settingsWire
  let completed ← requireOk <| Connection.processBytes first.state trailing
  expect (completed.state.prefaceReceived && completed.state.prefaceBuffer.isEmpty &&
      completed.state.receivedSettings && completed.error?.isNone &&
      completed.outbound.size == 1 && Http2.Settings.isAck completed.outbound[0]!)
    "preface completion did not preserve and process same-chunk frame bytes"

  let mismatched ← requireOk <|
    Connection.processBytes (Connection.initial .server) (ByteArray.mk #[0])
  expect (mismatched.error?.any fun error =>
      error.scope == .connection && error.code == .protocolError)
    "mismatched client preface was not a connection protocol error"
  expect (Connection.initial .client).prefaceReceived
    "client state incorrectly waited for an inbound connection preface"

def main : IO Unit := do
  testContentLengthAndNoContent
  testDataFailurePrecedence
  testProcessBytesContinuesAfterStreamError
  testCompressedBounds
  testClosedStreamRetention
  testPriorityGoAwayUriAndTunnel
  testWireBuilderBoundsAndSettingsOrdering
  testWireFieldOctets
  testCompositionQueries
  testLocalSettingsActivateOnAcknowledgement
  testConfiguredConcurrentStreamCapBeforeAcknowledgement
  testSkippedPeerStreamCannotBeReopened
  testServerPrefaceOwnership
  IO.println "HTTP/2 connection compliance tests passed"
