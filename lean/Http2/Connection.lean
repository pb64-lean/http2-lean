module

public import Http2.Frame
public import Http2.Hpack

public section

namespace Http2.Connection

/-- The endpoint's role determines locally and remotely initiated stream IDs. -/
inductive Role where
  | client
  | server
  deriving Inhabited, Repr, DecidableEq

namespace Role

def firstLocalStreamId : Role → Nat
  | .client => 1
  | .server => 2

def isLocalStreamId (role : Role) (streamId : Nat) : Bool :=
  streamId != 0 && streamId % 2 == role.firstLocalStreamId % 2

def isPeerStreamId (role : Role) (streamId : Nat) : Bool :=
  streamId != 0 && !role.isLocalStreamId streamId

end Role

def initialWindowSize : Nat := 65535
def maximumWindowSize : Nat := maxStreamId

/-- The effective settings for one side of a connection. -/
structure Settings where
  headerTableSize : Nat := Hpack.defaultDynamicTableSize
  enablePush : Bool := true
  maxConcurrentStreams : Option Nat := none
  initialWindowSize : Nat := Connection.initialWindowSize
  maxFrameSize : Nat := defaultMaxFramePayloadLength
  maxHeaderListSize : Option Nat := none
  enableConnectProtocol : Bool := false
  deriving Inhabited, Repr, DecidableEq

private def boolSetting (name : String) (value : Nat) : Except Error Bool :=
  if value == 0 then .ok false
  else if value == 1 then .ok true
  else .error (Error.connection .protocolError s!"{name} must be zero or one")

/-- Validate and apply one peer setting. Unknown settings are ignored. -/
def Settings.apply (settings : Settings) (setting : Setting) : Except Error Settings := do
  match setting.id with
  | .headerTableSize => pure { settings with headerTableSize := setting.value }
  | .enablePush =>
      pure { settings with enablePush := ← boolSetting "SETTINGS_ENABLE_PUSH" setting.value }
  | .maxConcurrentStreams =>
      pure { settings with maxConcurrentStreams := some setting.value }
  | .initialWindowSize =>
      if setting.value > maximumWindowSize then
        throw (Error.connection .flowControlError
          "SETTINGS_INITIAL_WINDOW_SIZE exceeds 2^31-1")
      pure { settings with initialWindowSize := setting.value }
  | .maxFrameSize =>
      if setting.value < defaultMaxFramePayloadLength ||
          setting.value > maxFramePayloadLength then
        throw (Error.connection .protocolError
          "SETTINGS_MAX_FRAME_SIZE is outside 16384..16777215")
      pure { settings with maxFrameSize := setting.value }
  | .maxHeaderListSize =>
      pure { settings with maxHeaderListSize := some setting.value }
  | .unknown id =>
      if id == SettingId.enableConnectProtocol.toNat then
        let enabled ← boolSetting "SETTINGS_ENABLE_CONNECT_PROTOCOL" setting.value
        if settings.enableConnectProtocol && !enabled then
          throw (Error.connection .protocolError
            "SETTINGS_ENABLE_CONNECT_PROTOCOL cannot be disabled after being enabled")
        pure { settings with enableConnectProtocol := enabled }
      else
        pure settings

inductive StreamPhase where
  | idle
  | open
  | halfClosedLocal
  | halfClosedRemote
  | closed
  deriving Inhabited, Repr, DecidableEq

namespace StreamPhase

def localOpen : StreamPhase → Bool
  | .open | .halfClosedRemote => true
  | _ => false

def remoteOpen : StreamPhase → Bool
  | .open | .halfClosedLocal => true
  | _ => false

def closeLocal : StreamPhase → StreamPhase
  | .open => .halfClosedLocal
  | .halfClosedRemote => .closed
  | phase => phase

def closeRemote : StreamPhase → StreamPhase
  | .open => .halfClosedRemote
  | .halfClosedLocal => .closed
  | phase => phase

end StreamPhase

structure Stream where
  id : Nat
  phase : StreamPhase := .idle
  inboundWindow : Nat := initialWindowSize
  outboundWindow : Int := initialWindowSize
  receivedHeaders : Bool := false
  receivedTrailers : Bool := false
  sentHeaders : Bool := false
  sentTrailers : Bool := false
  deriving Inhabited, Repr, DecidableEq

structure PendingHeaders where
  streamId : Nat
  block : ByteArray
  endStream : Bool
  trailers : Bool
  deriving Inhabited, DecidableEq

inductive Event where
  | headers (streamId : Nat) (headers : Headers) (endStream trailers : Bool)
  | data (streamId : Nat) (bytes : ByteArray) (endStream : Bool)
  | reset (streamId : Nat) (code : ErrorCode)
  | settingsChanged (settings : Settings)
  | settingsAcknowledged
  | pingAcknowledged (payload : ByteArray)
  | goAway (lastStreamId : Nat) (code : ErrorCode) (debugData : ByteArray)
  | priority (streamId : Nat)
  deriving Inhabited

structure State where
  role : Role
  localSettings : Settings := {}
  peerSettings : Settings := {}
  streams : Array Stream := #[]
  nextLocalStreamId : Nat
  lastPeerStreamId : Nat := 0
  inboundConnectionWindow : Nat := initialWindowSize
  outboundConnectionWindow : Int := initialWindowSize
  decoder : Frame.DecodeState := {}
  hpackDecode : Hpack.State := {}
  hpackEncode : Hpack.State := {}
  pendingHeaders : Option PendingHeaders := none
  receivedSettings : Bool := false
  localGoAwayLastStream? : Option Nat := none
  peerGoAwayLastStream? : Option Nat := none
  deriving Inhabited

def initial (role : Role) (localSettings : Settings := {}) : State := {
  role
  localSettings
  nextLocalStreamId := role.firstLocalStreamId
  hpackDecode := Hpack.setMaxAllowedSize {} localSettings.headerTableSize
}

private def findStream? (state : State) (streamId : Nat) : Option Stream :=
  state.streams.find? (·.id == streamId)

private def replaceStream (state : State) (stream : Stream) : State :=
  { state with streams := state.streams.map fun current =>
      if current.id == stream.id then stream else current }

private def insertStream (state : State) (stream : Stream) : State :=
  { state with streams := state.streams.push stream }

private def activePeerStreams (state : State) : Nat :=
  state.streams.foldl (init := 0) fun count stream =>
    if state.role.isPeerStreamId stream.id && stream.phase != .closed then count + 1 else count

private def ensureFrameSize (state : State) (frame : Frame) : Except Error Unit :=
  if frame.payload.size > state.localSettings.maxFrameSize then
    .error (Error.connection .frameSizeError "received frame exceeds SETTINGS_MAX_FRAME_SIZE")
  else
    .ok ()

private def requireConnectionFrame (frame : Frame) (name : String) : Except Error Unit :=
  if frame.header.streamId == 0 then .ok ()
  else .error (Error.connection .protocolError s!"{name} must use stream 0")

private def requireStreamFrame (frame : Frame) (name : String) : Except Error Unit :=
  if frame.header.streamId == 0 then
    .error (Error.connection .protocolError s!"{name} must use a nonzero stream")
  else
    .ok ()

private def requireNoInterleavedHeaderBlock (state : State) (frame : Frame) : Except Error Unit :=
  match state.pendingHeaders with
  | none => pure ()
  | some pending =>
      unless frame.header.frameType == .continuation &&
          frame.header.streamId == pending.streamId do
        throw (Error.connection .protocolError
          "a header block was interleaved with another frame")

private def clearBit (flags bit : UInt8) : UInt8 :=
  UInt8.ofNat (flags.toNat - if FrameFlag.has flags bit then bit.toNat else 0)

private def unpad (frame : Frame) : Except Error (ByteArray × Nat) := do
  if !FrameFlag.has frame.header.flags FrameFlag.padded then
    pure (frame.payload, 0)
  else if frame.payload.isEmpty then
    throw (Error.connection .protocolError "padded frame omitted the pad length")
  else
    let padding := frame.payload[0]!.toNat
    if padding >= frame.payload.size then
      throw (Error.connection .protocolError "frame padding exceeds its payload")
    pure (frame.payload.extract 1 (frame.payload.size - padding), padding + 1)

private def headersFragment (frame : Frame) : Except Error ByteArray := do
  let (payload, _) ← unpad frame
  if FrameFlag.has frame.header.flags FrameFlag.priority then
    if payload.size < 5 then
      throw (Error.connection .frameSizeError "HEADERS priority section is truncated")
    pure (payload.extract 5 payload.size)
  else
    pure payload

private def peerStreamForHeaders (state : State) (streamId : Nat) : Except Error (State × Stream × Bool) := do
  match findStream? state streamId with
  | some stream =>
      unless stream.phase.remoteOpen do
        throw (Error.stream streamId .streamClosed "HEADERS arrived on a remotely closed stream")
      pure (state, stream, true)
  | none =>
      unless state.role.isPeerStreamId streamId do
        throw (Error.connection .protocolError "peer opened a stream with the wrong parity")
      unless streamId > state.lastPeerStreamId do
        throw (Error.connection .protocolError "peer stream identifiers are not increasing")
      if let some limit := state.localSettings.maxConcurrentStreams then
        if activePeerStreams state >= limit then
          throw (Error.stream streamId .refusedStream "maximum concurrent streams reached")
      let stream : Stream := {
        id := streamId
        phase := .open
        inboundWindow := state.localSettings.initialWindowSize
        outboundWindow := state.peerSettings.initialWindowSize
      }
      pure ({ (insertStream state stream) with lastPeerStreamId := streamId }, stream, false)

private def finishHeaders (state : State) (pending : PendingHeaders) : Except Error (State × Event) := do
  let decoded ← Hpack.decodeHeaderBlock state.hpackDecode pending.block
  Headers.validateListSize state.localSettings.maxHeaderListSize decoded.headers
  let some stream := findStream? state pending.streamId
    | throw (Error.connection .internalError "header stream disappeared")
  let phase := if pending.endStream then stream.phase.closeRemote else stream.phase
  let stream := {
    stream with
    phase
    receivedHeaders := true
    receivedTrailers := stream.receivedHeaders
  }
  let state := replaceStream { state with hpackDecode := decoded.state, pendingHeaders := none } stream
  pure (state, .headers pending.streamId decoded.headers pending.endStream pending.trailers)

private def processHeaders (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "HEADERS"
  let (state, stream, existed) ← peerStreamForHeaders state frame.header.streamId
  if existed && stream.receivedTrailers then
    throw (Error.stream frame.header.streamId .protocolError "a second trailer block was received")
  let fragment ← headersFragment frame
  let pending : PendingHeaders := {
    streamId := frame.header.streamId
    block := fragment
    endStream := FrameFlag.has frame.header.flags FrameFlag.endStream
    trailers := stream.receivedHeaders
  }
  if FrameFlag.has frame.header.flags FrameFlag.endHeaders then
    let (state, event) ← finishHeaders state pending
    pure (state, #[], #[event])
  else
    pure ({ state with pendingHeaders := some pending }, #[], #[])

private def processContinuation (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "CONTINUATION"
  let some pending := state.pendingHeaders
    | throw (Error.connection .protocolError "CONTINUATION arrived without a header block")
  unless pending.streamId == frame.header.streamId do
    throw (Error.connection .protocolError "CONTINUATION changed streams")
  let pending := { pending with block := pending.block.append frame.payload }
  if pending.block.size > state.localSettings.maxHeaderListSize.getD maxFramePayloadLength then
    throw (Error.connection .enhanceYourCalm "compressed header block exceeds configured limit")
  if FrameFlag.has frame.header.flags FrameFlag.endHeaders then
    let (state, event) ← finishHeaders state pending
    pure (state, #[], #[event])
  else
    pure ({ state with pendingHeaders := some pending }, #[], #[])

private def processData (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "DATA"
  let some stream := findStream? state frame.header.streamId
    | throw (Error.connection .protocolError "DATA arrived on an idle stream")
  unless stream.receivedHeaders && stream.phase.remoteOpen do
    throw (Error.stream frame.header.streamId .streamClosed "DATA arrived on a closed stream")
  let consumed := frame.payload.size
  if consumed > state.inboundConnectionWindow then
    throw (Error.connection .flowControlError "connection receive window was exceeded")
  if consumed > stream.inboundWindow then
    throw (Error.stream frame.header.streamId .flowControlError "stream receive window was exceeded")
  let (payload, paddingCredit) ← unpad frame
  let endStream := FrameFlag.has frame.header.flags FrameFlag.endStream
  let stream := {
    stream with
    inboundWindow := stream.inboundWindow - payload.size
    phase := if endStream then stream.phase.closeRemote else stream.phase
  }
  let mut automatic := #[]
  if consumed > 0 then
    automatic := automatic.push (← WindowUpdate.frame 0 consumed)
  if paddingCredit > 0 then
    automatic := automatic.push (← WindowUpdate.frame frame.header.streamId paddingCredit)
  let state := replaceStream state stream
  pure (state, automatic, #[.data frame.header.streamId payload endStream])

private def adjustOutboundWindows (state : State) (old new : Nat) : State :=
  let delta : Int := Int.ofNat new - Int.ofNat old
  { state with streams := state.streams.map fun stream =>
      { stream with outboundWindow := stream.outboundWindow + delta } }

private def processSettings (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireConnectionFrame frame "SETTINGS"
  let values ← Http2.Settings.decode frame
  if Http2.Settings.isAck frame then
    pure (state, #[], #[.settingsAcknowledged])
  else
    let oldWindow := state.peerSettings.initialWindowSize
    let mut peer := state.peerSettings
    for setting in values do
      if state.role == .client && setting.id == .enablePush then
        throw (Error.connection .protocolError
          "a server endpoint must not send SETTINGS_ENABLE_PUSH")
      peer ← peer.apply setting
    let state := adjustOutboundWindows { state with
      peerSettings := peer
      receivedSettings := true
      hpackEncode := Hpack.setMaxAllowedSize state.hpackEncode peer.headerTableSize
    } oldWindow peer.initialWindowSize
    let ack ← Http2.Settings.frame #[] true
    pure (state, #[ack], #[.settingsChanged peer])

private def processPing (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireConnectionFrame frame "PING"
  let payload ← Ping.decode frame
  if Ping.isAck frame then
    pure (state, #[], #[.pingAcknowledged payload])
  else
    pure (state, #[← Ping.frame payload true], #[])

private def processWindowUpdate (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  let increment ← WindowUpdate.decode frame
  if frame.header.streamId == 0 then
    let next := state.outboundConnectionWindow + Int.ofNat increment
    if next > Int.ofNat maximumWindowSize then
      throw (Error.connection .flowControlError "connection send window overflowed")
    pure ({ state with outboundConnectionWindow := next }, #[], #[])
  else
    match findStream? state frame.header.streamId with
    | none => pure (state, #[], #[])
    | some stream =>
        let next := stream.outboundWindow + Int.ofNat increment
        if next > Int.ofNat maximumWindowSize then
          throw (Error.stream stream.id .flowControlError "stream send window overflowed")
        pure (replaceStream state { stream with outboundWindow := next }, #[], #[])

private def processReset (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "RST_STREAM"
  let code ← RstStream.decode frame
  let some stream := findStream? state frame.header.streamId
    | throw (Error.connection .protocolError "RST_STREAM arrived on an idle stream")
  let state := replaceStream state { stream with phase := .closed }
  pure (state, #[], #[.reset stream.id code])

private def processGoAway (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireConnectionFrame frame "GOAWAY"
  let decoded ← GoAway.decode frame
  pure ({ state with peerGoAwayLastStream? := some decoded.lastStreamId }, #[],
    #[.goAway decoded.lastStreamId decoded.errorCode decoded.debugData])

/-- Apply one fully decoded peer frame and produce automatic control frames and events. -/
def processFrame (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  ensureFrameSize state frame
  requireNoInterleavedHeaderBlock state frame
  unless state.receivedSettings do
    unless frame.header.frameType == .settings && !Http2.Settings.isAck frame do
      throw (Error.connection .protocolError
        "the first peer frame must be a non-acknowledgement SETTINGS frame")
  match frame.header.frameType with
  | .headers => processHeaders state frame
  | .continuation => processContinuation state frame
  | .data => processData state frame
  | .settings => processSettings state frame
  | .ping => processPing state frame
  | .windowUpdate => processWindowUpdate state frame
  | .rstStream => processReset state frame
  | .goAway => processGoAway state frame
  | .priority =>
      requireStreamFrame frame "PRIORITY"
      discard <| Priority.decode frame
      pure (state, #[], #[.priority frame.header.streamId])
  | .pushPromise =>
      throw (Error.connection .protocolError "server push is disabled by this endpoint")
  | .unknown _ => pure (state, #[], #[])

/-- Decode a byte chunk, preserving incomplete input, and apply all complete frames. -/
def processBytes (state : State) (bytes : ByteArray) :
    Except Error (State × Array Frame × Array Event) := do
  let decoded ← Frame.decodeChunk state.decoder bytes
  let mut state := { state with decoder := { decoded with frames := #[] } }
  let mut outbound := #[]
  let mut events := #[]
  for frame in decoded.frames do
    let (next, automatic, emitted) ← processFrame state frame
    state := next
    outbound := outbound ++ automatic
    events := events ++ emitted
  pure (state, outbound, events)

private def headerFrames (streamId : Nat) (block : ByteArray) (maxSize : Nat)
    (endStream : Bool) : Array Frame :=
  let size := max 1 maxSize
  let count := max 1 ((block.size + size - 1) / size)
  (List.range count).foldl (init := #[]) fun out index =>
      let offset := index * size
      let stop := min block.size (offset + size)
      let payload := block.extract offset stop
      let first := index == 0
      let last := index + 1 == count
      let flags := FrameFlag.combine <|
        (if last then #[FrameFlag.endHeaders] else #[]) ++
        (if first && endStream then #[FrameFlag.endStream] else #[])
      out.push {
        header := {
          length := payload.size
          frameType := if first then .headers else .continuation
          flags
          streamId
        }
        payload
      }

/-- Open a locally initiated stream and encode its initial field section. -/
def openStream (state : State) (headers : Headers) (endStream : Bool := false) :
    Except Error (State × Nat × Array Frame) := do
  let streamId := state.nextLocalStreamId
  if streamId > maxStreamId then
    throw (Error.connection .protocolError "no local stream identifiers remain")
  if let some last := state.peerGoAwayLastStream? then
    if streamId > last then
      throw (Error.stream streamId .refusedStream "peer GOAWAY excludes the new stream")
  if let some limit := state.peerSettings.maxConcurrentStreams then
    let active := state.streams.foldl (init := 0) fun count stream =>
      if state.role.isLocalStreamId stream.id && stream.phase != .closed then count + 1 else count
    if active >= limit then
      throw (Error.stream streamId .refusedStream "peer concurrent-stream limit reached")
  Headers.validateListSize state.peerSettings.maxHeaderListSize headers
  let (block, hpack) ← Hpack.encodeHeaderBlock state.hpackEncode headers
  let stream : Stream := {
    id := streamId
    phase := if endStream then .halfClosedLocal else .open
    inboundWindow := state.localSettings.initialWindowSize
    outboundWindow := state.peerSettings.initialWindowSize
    sentHeaders := true
  }
  let state := insertStream {
    state with
    nextLocalStreamId := streamId + 2
    hpackEncode := hpack
  } stream
  pure (state, streamId,
    headerFrames streamId block state.peerSettings.maxFrameSize endStream)

/-- Encode a subsequent field section on an existing stream. -/
def sendHeaders (state : State) (streamId : Nat) (headers : Headers)
    (endStream : Bool := false) : Except Error (State × Array Frame) := do
  let some stream := findStream? state streamId
    | throw (Error.stream streamId .streamClosed "cannot send headers on an idle stream")
  unless stream.phase.localOpen do
    throw (Error.stream streamId .streamClosed "local side of stream is closed")
  if stream.sentTrailers then
    throw (Error.stream streamId .protocolError "trailers were already sent")
  Headers.validateListSize state.peerSettings.maxHeaderListSize headers
  let (block, hpack) ← Hpack.encodeHeaderBlock state.hpackEncode headers
  let stream := {
    stream with
    phase := if endStream then stream.phase.closeLocal else stream.phase
    sentHeaders := true
    sentTrailers := stream.sentHeaders
  }
  let state := replaceStream { state with hpackEncode := hpack } stream
  pure (state, headerFrames streamId block state.peerSettings.maxFrameSize endStream)

/-- Encode DATA that fits the currently available connection and stream windows. -/
def sendData (state : State) (streamId : Nat) (bytes : ByteArray)
    (endStream : Bool := false) : Except Error (State × Array Frame) := do
  let some stream := findStream? state streamId
    | throw (Error.stream streamId .streamClosed "cannot send DATA on an idle stream")
  unless stream.sentHeaders && stream.phase.localOpen do
    throw (Error.stream streamId .streamClosed "local side of stream is closed")
  let connectionAvailable :=
    if state.outboundConnectionWindow <= 0 then 0 else state.outboundConnectionWindow.toNat
  let streamAvailable :=
    if stream.outboundWindow <= 0 then 0 else stream.outboundWindow.toNat
  let available := min connectionAvailable streamAvailable
  if bytes.size > available then
    throw (Error.stream streamId .flowControlError "insufficient outbound flow-control credit")
  let size := max 1 state.peerSettings.maxFrameSize
  let count := max 1 ((bytes.size + size - 1) / size)
  let frames := (List.range count).foldl (init := #[]) fun out index =>
      let offset := index * size
      let stop := min bytes.size (offset + size)
      let payload := bytes.extract offset stop
      let last := index + 1 == count
      out.push {
        header := {
          length := payload.size
          frameType := .data
          flags := if last && endStream then FrameFlag.endStream else 0
          streamId
        }
        payload
      }
  let stream := {
    stream with
    outboundWindow := stream.outboundWindow - Int.ofNat bytes.size
    phase := if endStream then stream.phase.closeLocal else stream.phase
  }
  let state := replaceStream {
    state with outboundConnectionWindow := state.outboundConnectionWindow - Int.ofNat bytes.size
  } stream
  pure (state, frames)

/-- Return consumed application-byte stream credit to the peer.

Connection-level credit and DATA padding credit are restored automatically by
`processFrame`; callers pass only payload bytes removed from their bounded
application queue. -/
def acknowledgeData (state : State) (streamId amount : Nat) :
    Except Error (State × Array Frame) := do
  if amount == 0 then return (state, #[])
  let some stream := findStream? state streamId
    | throw (Error.stream streamId .streamClosed "cannot credit an idle stream")
  if stream.inboundWindow + amount > maximumWindowSize then
    throw (Error.stream streamId .flowControlError "stream receive window overflowed")
  unless stream.phase.remoteOpen do
    return (state, #[])
  let streamUpdate ← WindowUpdate.frame streamId amount
  let stream := { stream with inboundWindow := stream.inboundWindow + amount }
  pure (replaceStream state stream, #[streamUpdate])

/-- Close local stream bookkeeping and construct its RST_STREAM frame.
Repeated resets of an already closed stream produce no frame. -/
def resetStream (state : State) (streamId : Nat) (code : ErrorCode := .cancel) :
    Except Error (State × Option Frame) := do
  let some stream := findStream? state streamId
    | throw (Error.stream streamId .streamClosed "cannot reset an idle stream")
  if stream.phase == .closed then
    pure (state, none)
  else
    let frame ← RstStream.frame streamId code
    pure (replaceStream state { stream with phase := .closed }, some frame)

end Http2.Connection
