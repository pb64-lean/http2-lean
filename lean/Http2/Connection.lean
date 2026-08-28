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
/-- The protocol default is a valid 31-bit flow-control window. -/
theorem initialWindowSize_le_maximum : initialWindowSize ≤ Http2.maxStreamId := by
  decide
/-- Default local resource limit for one compressed HPACK field block. This
is independent of the decoded field-list limit advertised to the peer. -/
def defaultMaxCompressedHeaderBlockSize : Nat := 1048576
/-- Maximum number of closed stream records retained for late-frame handling. -/
def maximumClosedStreamRecords : Nat := 256

/-- The effective settings for one side of a connection. -/
structure Settings where
  headerTableSize : Nat := Hpack.defaultDynamicTableSize
  enablePush : Bool := true
  maxConcurrentStreams : Option Nat := none
  initialWindowSize : Nat := Connection.initialWindowSize
  maxFrameSize : Nat := defaultMaxFramePayloadLength
  maxHeaderListSize : Option Nat := none
  maxCompressedHeaderBlockSize : Nat := defaultMaxCompressedHeaderBlockSize
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
  | .enableConnectProtocol =>
      let enabled ← boolSetting "SETTINGS_ENABLE_CONNECT_PROTOCOL" setting.value
      if settings.enableConnectProtocol && !enabled then
        throw (Error.connection .protocolError
          "SETTINGS_ENABLE_CONNECT_PROTOCOL cannot be disabled after being enabled")
      pure { settings with enableConnectProtocol := enabled }
  | .unknown _ => pure settings

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

inductive BodyKind where
  | ordinary
  | noContent
  | tunnel
  deriving Inhabited, Repr, DecidableEq

structure Stream where
  id : Nat
  phase : StreamPhase := .idle
  inboundWindow : Int := initialWindowSize
  outboundWindow : Int := initialWindowSize
  /-- Frames already in flight can arrive after this endpoint sends RST_STREAM.
  Header blocks on such a stream still have to advance the connection-wide
  HPACK decoder before their application semantics are discarded. -/
  locallyReset : Bool := false
  receivedHeaders : Bool := false
  receivedTrailers : Bool := false
  sentHeaders : Bool := false
  sentTrailers : Bool := false
  requestMethod? : Option String := none
  inboundContentLength? : Option Nat := none
  inboundBodyBytes : Nat := 0
  inboundBodyKind : BodyKind := .ordinary
  outboundContentLength? : Option Nat := none
  outboundBodyBytes : Nat := 0
  outboundBodyKind : BodyKind := .ordinary
  deriving Inhabited, Repr, DecidableEq

structure PendingHeaders where
  streamId : Nat
  block : ByteArray
  endStream : Bool
  trailers : Bool
  discard : Bool := false
  failure? : Option Error := none
  deriving Inhabited, DecidableEq

inductive Event where
  | headers (streamId : Nat) (headers : Headers) (endStream trailers : Bool)
  | data (streamId : Nat) (bytes : ByteArray) (endStream : Bool)
  | reset (streamId : Nat) (code : ErrorCode)
  /-- A decoded field section failed stream-scoped HTTP validation. The
  corresponding RST_STREAM is included in the automatic outbound frames. -/
  | streamError (streamId : Nat) (code : ErrorCode) (message : String)
  | settingsChanged (settings : Settings)
  | settingsAcknowledged
  | pingAcknowledged (payload : ByteArray)
  | goAway (lastStreamId : Nat) (code : ErrorCode) (debugData : ByteArray)
  | priority (streamId : Nat)
  deriving Inhabited

structure State where
  role : Role
  /-- Locally configured settings, advertised by the transport preface. -/
  localSettings : Settings := {}
  /-- Local settings the peer has acknowledged and can therefore be required
  to obey. Before the initial acknowledgement, protocol defaults apply. -/
  peerKnownLocalSettings : Settings := {}
  peerSettings : Settings := {}
  streams : Array Stream := #[]
  nextLocalStreamId : Nat
  lastPeerStreamId : Nat := 0
  inboundConnectionWindow : Nat := initialWindowSize
  outboundConnectionWindow : Int := initialWindowSize
  decoder : Frame.DecodeState := {}
  /-- Servers own the exact client connection preface before frame decoding;
  clients have no inbound magic preface. -/
  prefaceReceived : Bool := false
  prefaceBuffer : ByteArray := ByteArray.empty
  hpackDecode : Hpack.State := {}
  hpackEncode : Hpack.State := {}
  pendingHeaders : Option PendingHeaders := none
  receivedSettings : Bool := false
  localGoAwayLastStream? : Option Nat := none
  peerGoAwayLastStream? : Option Nat := none
  deriving Inhabited

def initial (role : Role) (localSettings : Settings := {}) : State := {
  role
  localSettings := if role == .client then { localSettings with enablePush := false }
    else localSettings
  peerKnownLocalSettings := if role == .client then { ({} : Settings) with enablePush := false }
    else {}
  nextLocalStreamId := role.firstLocalStreamId
  hpackDecode := {}
  prefaceReceived := role == .client
}

/-- Serialize the settings that this endpoint advertises at connection start.
Settings used only as local implementation bounds are intentionally omitted.
Servers never send `SETTINGS_ENABLE_PUSH`; clients explicitly disable push. -/
def initialSettingsValues (state : State) : Array Setting := Id.run do
  let settings := state.localSettings
  let mut values : Array Setting := #[]
  if settings.headerTableSize != Hpack.defaultDynamicTableSize then
    values := values.push { id := .headerTableSize, value := settings.headerTableSize }
  if state.role == .client then
    values := values.push { id := .enablePush, value := if settings.enablePush then 1 else 0 }
  if let some maximum := settings.maxConcurrentStreams then
    values := values.push { id := .maxConcurrentStreams, value := maximum }
  if settings.initialWindowSize != initialWindowSize then
    values := values.push { id := .initialWindowSize, value := settings.initialWindowSize }
  if settings.maxFrameSize != defaultMaxFramePayloadLength then
    values := values.push { id := .maxFrameSize, value := settings.maxFrameSize }
  if let some maximum := settings.maxHeaderListSize then
    values := values.push { id := .maxHeaderListSize, value := maximum }
  if settings.enableConnectProtocol then
    values := values.push { id := .enableConnectProtocol, value := 1 }
  return values

/-- Construct the initial SETTINGS frame from the same state that will enforce
the advertised values. -/
def initialSettingsFrame (state : State) : Except Error Frame :=
  Http2.Settings.frame (initialSettingsValues state)

/-- Encode an endpoint's connection-opening bytes. Clients prepend the HTTP/2
connection preface; servers send only their initial SETTINGS frame. -/
def initialWireBytes (state : State) : Except Error ByteArray := do
  let frame ← initialSettingsFrame state
  let wire ← Frame.encode frame
  pure <| if state.role == .client then connectionPreface.append wire else wire

/-- Look up retained protocol state for one stream identifier. -/
def stream? (state : State) (streamId : Nat) : Option Stream :=
  state.streams.find? (·.id == streamId)

private def findStream? (state : State) (streamId : Nat) : Option Stream :=
  stream? state streamId

/-- Currently usable outbound DATA credit, bounded by both connection and
stream windows. `none` means the stream has no retained protocol state. -/
def outboundCredit? (state : State) (streamId : Nat) : Option Nat := do
  let stream ← stream? state streamId
  let connectionCredit := if state.outboundConnectionWindow <= 0 then 0
    else state.outboundConnectionWindow.toNat
  let streamCredit := if stream.outboundWindow <= 0 then 0
    else stream.outboundWindow.toNat
  pure (min connectionCredit streamCredit)

/-- Enter local GOAWAY state and construct the matching control frame. A
repeated call never increases the previously advertised last stream ID. -/
def beginGoAway (state : State) (code : ErrorCode := .noError)
    (debugData : ByteArray := ByteArray.empty) : Except Error (State × Frame) := do
  let lastStreamId := min state.lastPeerStreamId
    (state.localGoAwayLastStream?.getD state.lastPeerStreamId)
  let frame ← GoAway.frame lastStreamId code debugData
  pure ({ state with localGoAwayLastStream? := some lastStreamId }, frame)

private def pruneClosedStreams (streams : Array Stream) : Array Stream :=
  let closed := streams.foldl (init := 0) fun count stream =>
    if stream.phase == .closed then count + 1 else count
  let excess := closed - maximumClosedStreamRecords
  if excess == 0 then streams
  else
    (streams.foldl (init := (#[], excess)) fun (kept, remaining) stream =>
      if remaining > 0 && stream.phase == .closed then
        (kept, remaining - 1)
      else
        (kept.push stream, remaining)).1

private def replaceStream (state : State) (stream : Stream) : State :=
  { state with streams := pruneClosedStreams <| state.streams.map fun current =>
      if current.id == stream.id then stream else current }

private def insertStream (state : State) (stream : Stream) : State :=
  { state with streams := pruneClosedStreams (state.streams.push stream) }

private def streamIdIsIdle (state : State) (streamId : Nat) : Bool :=
  if state.role.isPeerStreamId streamId then
    streamId > state.lastPeerStreamId
  else
    streamId >= state.nextLocalStreamId

private def activePeerStreams (state : State) : Nat :=
  state.streams.foldl (init := 0) fun count stream =>
    if state.role.isPeerStreamId stream.id && stream.phase != .closed then count + 1 else count

private def ensureFrameSize (state : State) (frame : Frame) : Except Error Unit :=
  if frame.payload.size > state.peerKnownLocalSettings.maxFrameSize then
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

private def peerStreamForHeaders (state : State) (streamId : Nat) :
    Except Error (State × Stream × Bool × Option Error) := do
  match findStream? state streamId with
  | some stream =>
      if stream.phase.remoteOpen then
        pure (state, stream, false, none)
      else if stream.locallyReset then
        pure (state, stream, true, none)
      else
        pure (state, stream, false, some <|
          Error.stream streamId .streamClosed "HEADERS arrived on a remotely closed stream")
  | none =>
      if !streamIdIsIdle state streamId then
        let stream : Stream := {
          id := streamId
          phase := .closed
          inboundWindow := state.peerKnownLocalSettings.initialWindowSize
          outboundWindow := state.peerSettings.initialWindowSize
        }
        if state.role.isPeerStreamId streamId then
          -- Opening a higher peer stream proves that the peer knows every
          -- skipped lower identifier is closed. Decompress the field block,
          -- then reject an attempt to open such an identifier again.
          return (state, stream, false, some <|
            Error.connection .protocolError
              "peer reused an implicitly closed stream identifier")
        let state := insertStream state stream
        return (state, stream, false, some <|
          Error.stream streamId .streamClosed "HEADERS arrived on a closed stream")
      unless state.role.isPeerStreamId streamId do
        throw (Error.connection .protocolError "peer opened a stream with the wrong parity")
      if state.peerGoAwayLastStream?.isSome then
        throw (Error.connection .protocolError "peer opened a new stream after sending GOAWAY")
      if state.role == .client then
        throw (Error.connection .protocolError
          "a server opened a stream without a PUSH_PROMISE")
      unless streamId > state.lastPeerStreamId do
        throw (Error.connection .protocolError "peer stream identifiers are not increasing")
      let base : Stream := {
        id := streamId
        phase := .open
        inboundWindow := state.peerKnownLocalSettings.initialWindowSize
        outboundWindow := state.peerSettings.initialWindowSize
      }
      let beyondGoAway := state.localGoAwayLastStream?.any (streamId > ·)
      -- The configured value is also the endpoint's immediate resource cap.
      -- A peer need not treat it as a protocol constraint until SETTINGS is
      -- acknowledged, but accepting more live streams in the meantime would
      -- defeat the purpose of the limit.
      let overLimit := state.localSettings.maxConcurrentStreams.any fun limit =>
        activePeerStreams state >= limit
      let stream := if beyondGoAway then
          { base with phase := .closed, locallyReset := true }
        else base
      let failure? := if !beyondGoAway && overLimit then
          some (Error.stream streamId .refusedStream "maximum concurrent streams reached")
        else none
      pure ({ (insertStream state stream) with lastPeerStreamId := streamId },
        stream, beyondGoAway, failure?)

private def fieldTokenCharacter (character : Char) : Bool :=
  Header.isTokenCharacter character

private def connectionSpecificField (name : String) : Bool :=
  name == "connection" || name == "keep-alive" || name == "proxy-connection" ||
    name == "transfer-encoding" || name == "upgrade"

private def malformedFields (streamId : Nat) (message : String) : Error :=
  Error.stream streamId .protocolError message

private def decimalDigit (character : Char) : Bool :=
  let value := character.toNat
  0x30 <= value && value <= 0x39

private def maximumContentLength : Nat := 18446744073709551615

private def splitCommaMembers : List Char → List (List Char)
  | [] => [[]]
  | ',' :: rest => [] :: splitCommaMembers rest
  | character :: rest =>
      match splitCommaMembers rest with
      | [] => [[character]]
      | first :: remaining => (character :: first) :: remaining

private def contentLengthWhitespace (character : Char) : Bool :=
  character == ' ' || character.toNat == 0x09

private def trimOptionalWhitespace (characters : List Char) : List Char :=
  ((characters.dropWhile contentLengthWhitespace).reverse.dropWhile
    contentLengthWhitespace).reverse

private def parseContentLengthDigits (characters : List Char) : Option Nat :=
  if characters.isEmpty then none
  else
    characters.foldlM (init := 0) fun total character => do
      if !decimalDigit character then none else pure ()
      let digit := character.toNat - '0'.toNat
      if total > (maximumContentLength - digit) / 10 then none
      else some (total * 10 + digit)

private def contentLength? (streamId : Nat) (headers : Headers) : Except Error (Option Nat) := do
  let mut expected : Option Nat := none
  for header in headers do
    if header.name == "content-length" then
      for raw in splitCommaMembers header.value.toList do
        let member := trimOptionalWhitespace raw
        let some parsed := parseContentLengthDigits member
          | throw (malformedFields streamId "invalid or overflowing Content-Length value")
        match expected with
        | none => expected := some parsed
        | some previous =>
            unless parsed == previous do
              throw (malformedFields streamId "conflicting Content-Length values")
  pure expected

private def validateFieldSectionSyntax (streamId : Nat) (headers : Headers)
    (allowedPseudoFields : Array String) : Except Error Unit := do
  let mut seenOrdinary := false
  let mut seenPseudoFields : Array String := #[]
  for header in headers do
    unless Header.validFieldValue header do
      throw (malformedFields streamId s!"invalid HTTP/2 field value for {header.name}")
    if header.name.startsWith ":" then
      if seenOrdinary then
        throw (malformedFields streamId
          s!"HTTP/2 pseudo-header {header.name} appeared after an ordinary field")
      unless allowedPseudoFields.contains header.name do
        throw (malformedFields streamId s!"invalid HTTP/2 pseudo-header {header.name}")
      if seenPseudoFields.contains header.name then
        throw (malformedFields streamId s!"duplicate HTTP/2 pseudo-header {header.name}")
      seenPseudoFields := seenPseudoFields.push header.name
    else
      seenOrdinary := true
      unless Header.validFieldName header do
        throw (malformedFields streamId s!"invalid HTTP/2 field name {header.name}")
      if connectionSpecificField header.name then
        throw (malformedFields streamId
          s!"HTTP/2 connection-specific field is forbidden: {header.name}")
      if header.name == "te" && header.value.toLower != "trailers" then
        throw (malformedFields streamId "HTTP/2 TE field value must be trailers")

private def validateTrailerFields (streamId : Nat) (headers : Headers) : Except Error Unit := do
  validateFieldSectionSyntax streamId headers #[]
  unless (headers.getAll "content-length").isEmpty do
    throw (malformedFields streamId "trailing fields must not contain Content-Length")

private def requiredPseudoField (streamId : Nat) (headers : Headers)
    (name : String) : Except Error String := do
  let values := headers.getAll name
  unless values.size == 1 do
    throw (malformedFields streamId
      s!"HTTP/2 field section requires exactly one {name} pseudo-header")
  pure values[0]!

private def validateRequestFields (connectProtocolEnabled : Bool) (streamId : Nat)
    (headers : Headers) : Except Error Unit := do
  validateFieldSectionSyntax streamId headers
    #[":method", ":scheme", ":authority", ":path", ":protocol"]
  let method ← requiredPseudoField streamId headers ":method"
  unless !method.isEmpty && method.all fieldTokenCharacter do
    throw (malformedFields streamId "HTTP/2 :method is not a valid token")
  let protocols := headers.getAll ":protocol"
  if protocols.isEmpty then
    if method == "CONNECT" then
      let authority ← requiredPseudoField streamId headers ":authority"
      unless RequestTarget.validAuthority "https" authority do
        throw (malformedFields streamId "CONNECT :authority is invalid or contains userinfo")
      unless (headers.getAll ":scheme").isEmpty && (headers.getAll ":path").isEmpty do
        throw (malformedFields streamId "CONNECT must omit :scheme and :path")
    else
      let scheme ← requiredPseudoField streamId headers ":scheme"
      let path ← requiredPseudoField streamId headers ":path"
      unless RequestTarget.valid method scheme path do
        throw (malformedFields streamId "HTTP/2 request target pseudo-headers are invalid")
      if scheme.toLower == "http" || scheme.toLower == "https" then
        if let some authority := headers.get? ":authority" then
          unless RequestTarget.validAuthority scheme authority do
            throw (malformedFields streamId
              "HTTP/2 :authority must not contain userinfo for http or https")
  else
    unless method == "CONNECT" do
      throw (malformedFields streamId "the :protocol pseudo-header requires :method CONNECT")
    unless connectProtocolEnabled do
      throw (malformedFields streamId
        "extended CONNECT was used without SETTINGS_ENABLE_CONNECT_PROTOCOL")
    let protocol ← requiredPseudoField streamId headers ":protocol"
    unless !protocol.isEmpty && protocol.all fieldTokenCharacter do
      throw (malformedFields streamId "HTTP/2 :protocol is not a valid token")
    let scheme ← requiredPseudoField streamId headers ":scheme"
    let authority ← requiredPseudoField streamId headers ":authority"
    let path ← requiredPseudoField streamId headers ":path"
    if authority.isEmpty then
      throw (malformedFields streamId
        "extended CONNECT :authority must not be empty")
    unless RequestTarget.valid method scheme path do
      throw (malformedFields streamId "extended CONNECT request target is invalid")
    unless RequestTarget.validAuthority scheme authority do
      throw (malformedFields streamId
        "extended CONNECT :authority must not contain userinfo for http or https")

private def parseStatus? (value : String) : Option Nat := do
  if value.utf8ByteSize != 3 then none else pure ()
  let status ← value.toNat?
  if 100 <= status && status <= 599 then some status else none

private def validateResponseFields (streamId : Nat) (headers : Headers) : Except Error Nat := do
  validateFieldSectionSyntax streamId headers #[":status"]
  let raw ← requiredPseudoField streamId headers ":status"
  let some status := parseStatus? raw
    | throw (malformedFields streamId s!"invalid HTTP/2 :status {raw}")
  if status == 101 then
    throw (malformedFields streamId "HTTP/2 responses must not use status 101")
  pure status

private inductive FieldSectionClass where
  | initial
  | informational
  | trailers
  deriving DecidableEq

private structure FieldSectionMetadata where
  classification : FieldSectionClass
  requestMethod? : Option String := none
  contentLength? : Option Nat := none
  bodyKind : BodyKind := .ordinary

private def responseMetadata (streamId : Nat) (requestMethod? : Option String)
    (status : Nat) (declaredLength? : Option Nat) : Except Error FieldSectionMetadata := do
  if status < 200 then
    if declaredLength?.isSome then
      throw (malformedFields streamId
        "informational responses must not contain Content-Length")
    pure { classification := .informational }
  else if requestMethod? == some "CONNECT" && status < 300 then
    if declaredLength?.isSome then
      throw (malformedFields streamId
        "successful CONNECT responses must not contain Content-Length")
    pure { classification := .initial, bodyKind := .tunnel }
  else if requestMethod? == some "HEAD" || status == 204 || status == 304 || status == 205 then
    if status == 204 && declaredLength?.isSome then
      throw (malformedFields streamId "204 responses must not contain Content-Length")
    if status == 205 && declaredLength?.any (· != 0) then
      throw (malformedFields streamId "205 responses can only declare Content-Length zero")
    pure { classification := .initial, bodyKind := .noContent }
  else
    pure { classification := .initial, contentLength? := declaredLength? }

private def validateInboundFieldSection (state : State) (pending : PendingHeaders)
    (headers : Headers) : Except Error FieldSectionMetadata := do
  if pending.trailers then
    if (findStream? state pending.streamId).any (·.inboundBodyKind == .tunnel) then
      throw (malformedFields pending.streamId
        "field sections are forbidden after a successful CONNECT response")
    unless pending.endStream do
      throw (malformedFields pending.streamId "trailing fields must carry END_STREAM")
    validateTrailerFields pending.streamId headers
    pure { classification := .trailers }
  else if state.role == .server then
    validateRequestFields state.peerKnownLocalSettings.enableConnectProtocol
      pending.streamId headers
    let method ← requiredPseudoField pending.streamId headers ":method"
    let length ← contentLength? pending.streamId headers
    pure {
      classification := .initial
      requestMethod? := some method
      contentLength? := length
    }
  else
    let status ← validateResponseFields pending.streamId headers
    let declaredLength ← contentLength? pending.streamId headers
    let requestMethod? := (findStream? state pending.streamId).bind (·.requestMethod?)
    let metadata ← responseMetadata pending.streamId requestMethod? status declaredLength
    if metadata.classification == .informational && pending.endStream then
      throw (malformedFields pending.streamId
        "an informational response must not carry END_STREAM")
    pure metadata

private def localizeFieldError (result : Except Error α) : Except Error α :=
  match result with
  | .ok value => .ok value
  | .error error => .error { error with scope := .localInput }

private def validateOutboundFieldSection (state : State) (stream : Stream)
    (headers : Headers) (endStream : Bool) : Except Error FieldSectionMetadata := do
  if stream.sentHeaders then
    if stream.outboundBodyKind == .tunnel then
      throw (Error.invalidArgument
        "field sections are forbidden after a successful CONNECT response")
    unless endStream do
      throw (Error.invalidArgument "trailing fields must carry END_STREAM")
    localizeFieldError (validateTrailerFields stream.id headers)
    pure { classification := .trailers }
  else
    unless state.role == .server && state.role.isPeerStreamId stream.id do
      throw (Error.invalidArgument "only a server response can precede final outbound headers")
    let status ← localizeFieldError (validateResponseFields stream.id headers)
    let declaredLength ← localizeFieldError (contentLength? stream.id headers)
    let metadata ← localizeFieldError <|
      responseMetadata stream.id stream.requestMethod? status declaredLength
    if metadata.classification == .informational && endStream then
      throw (Error.invalidArgument "an informational response must not carry END_STREAM")
    pure metadata

private def failStream (state : State) (streamId : Nat) (error : Error) :
    Except Error (State × Array Frame × Array Event) := do
  let reset ← RstStream.frame streamId error.code
  let state := match findStream? state streamId with
    | none => insertStream state {
        id := streamId
        phase := .closed
        inboundWindow := state.peerKnownLocalSettings.initialWindowSize
        outboundWindow := state.peerSettings.initialWindowSize
        locallyReset := true
      }
    | some stream => replaceStream state { stream with phase := .closed, locallyReset := true }
  pure (state, #[reset], #[.streamError streamId error.code error.message])

private def finishHeaders (state : State) (pending : PendingHeaders) :
    Except Error (State × Array Frame × Array Event) := do
  let decoded ← match Hpack.decodeHeaderBlock state.hpackDecode pending.block with
    | .ok decoded => pure decoded
    | .error error =>
        throw (Error.connection .compressionError error.message)
  let state := {
    state with
    hpackDecode := decoded.state
    pendingHeaders := none
  }
  match Headers.validateListSize state.localSettings.maxHeaderListSize decoded.headers with
  | .error error =>
      if pending.discard then pure (state, #[], #[])
      else
        failStream state pending.streamId
          (Error.stream pending.streamId .enhanceYourCalm error.message)
  | .ok _ =>
      if pending.discard then pure (state, #[], #[])
      else if let some error := pending.failure? then
        match error.scope with
        | .connection => throw error
        | .stream _ => failStream state pending.streamId error
        | .localInput =>
            throw (Error.connection .internalError error.message)
      else
        match validateInboundFieldSection state pending decoded.headers with
        | .error error => failStream state pending.streamId error
        | .ok metadata =>
            let some stream := findStream? state pending.streamId
              | throw (Error.connection .internalError "header stream disappeared")
            let classification := metadata.classification
            let expectedLength? := if classification == .trailers then
                stream.inboundContentLength?
              else metadata.contentLength?
            if pending.endStream && metadata.bodyKind != .tunnel &&
                expectedLength?.any (· != stream.inboundBodyBytes) then
              failStream state pending.streamId (Error.stream pending.streamId .protocolError
                "END_STREAM body length did not match Content-Length")
            else
              let phase := if pending.endStream then stream.phase.closeRemote else stream.phase
              let stream := {
                stream with
                phase
                receivedHeaders := stream.receivedHeaders || classification == .initial
                receivedTrailers := stream.receivedTrailers || classification == .trailers
                requestMethod? := metadata.requestMethod?.orElse fun _ => stream.requestMethod?
                inboundContentLength? := if classification == .initial then
                    metadata.contentLength?
                  else stream.inboundContentLength?
                inboundBodyKind := if classification == .initial then metadata.bodyKind
                  else stream.inboundBodyKind
                outboundContentLength? := if metadata.bodyKind == .tunnel then none
                  else stream.outboundContentLength?
                outboundBodyKind := if metadata.bodyKind == .tunnel then .tunnel
                  else stream.outboundBodyKind
              }
              let state := replaceStream state stream
              pure (state, #[], #[.headers pending.streamId decoded.headers pending.endStream
                (classification == .trailers)])

private def processHeaders (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "HEADERS"
  let (state, stream, discard, failure?) ←
    peerStreamForHeaders state frame.header.streamId
  let fragment ← headersFragment frame
  if fragment.size > state.localSettings.maxCompressedHeaderBlockSize then
    throw (Error.connection .enhanceYourCalm
      "compressed header block exceeds the local resource limit")
  let failure? := if !discard && stream.receivedTrailers then
      some (Error.stream frame.header.streamId .protocolError
        "a second trailer block was received")
    else failure?
  let pending : PendingHeaders := {
    streamId := frame.header.streamId
    block := fragment
    endStream := FrameFlag.has frame.header.flags FrameFlag.endStream
    trailers := stream.receivedHeaders
    discard
    failure?
  }
  if FrameFlag.has frame.header.flags FrameFlag.endHeaders then
    finishHeaders state pending
  else
    pure ({ state with pendingHeaders := some pending }, #[], #[])

private def processContinuation (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "CONTINUATION"
  let some pending := state.pendingHeaders
    | throw (Error.connection .protocolError "CONTINUATION arrived without a header block")
  unless pending.streamId == frame.header.streamId do
    throw (Error.connection .protocolError "CONTINUATION changed streams")
  let pending := { pending with block := pending.block.append frame.payload }
  if pending.block.size > state.localSettings.maxCompressedHeaderBlockSize then
    throw (Error.connection .enhanceYourCalm
      "compressed header block exceeds the local resource limit")
  if FrameFlag.has frame.header.flags FrameFlag.endHeaders then
    finishHeaders state pending
  else
    pure ({ state with pendingHeaders := some pending }, #[], #[])

private def connectionDataCredit (consumed : Nat) : Except Error (Array Frame) := do
  if consumed == 0 then pure #[]
  else pure #[← WindowUpdate.frame 0 consumed]

private def failDataStream (state : State) (streamId consumed : Nat)
    (code : ErrorCode) (message : String) :
    Except Error (State × Array Frame × Array Event) := do
  let credit ← connectionDataCredit consumed
  let (state, reset, events) ← failStream state streamId
    (Error.stream streamId code message)
  pure (state, credit ++ reset, events)

private def processData (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "DATA"
  let consumed := frame.payload.size
  if consumed > state.inboundConnectionWindow then
    throw (Error.connection .flowControlError "connection receive window was exceeded")
  let (payload, paddingCredit) ← unpad frame
  let some stream := findStream? state frame.header.streamId
    | if streamIdIsIdle state frame.header.streamId then
        throw (Error.connection .protocolError "DATA arrived on an idle stream")
      else
        return ← failDataStream state frame.header.streamId consumed .streamClosed
          "DATA arrived on a closed stream"
  if stream.locallyReset && !stream.phase.remoteOpen then
    return (state, ← connectionDataCredit consumed, #[])
  if consumed > 0 && Int.ofNat consumed > stream.inboundWindow then
    return ← failDataStream state stream.id consumed .flowControlError
      "stream receive window was exceeded"
  unless stream.receivedHeaders do
    return ← failDataStream state stream.id consumed .protocolError
      "DATA arrived before the initial or final field section"
  unless stream.phase.remoteOpen do
    return ← failDataStream state stream.id consumed .streamClosed
      "DATA arrived on a closed stream"
  if stream.inboundBodyKind == .noContent && !payload.isEmpty then
    return ← failDataStream state stream.id consumed .protocolError
      "DATA carried content for a no-content message"
  let endStream := FrameFlag.has frame.header.flags FrameFlag.endStream
  let bodyBytes := stream.inboundBodyBytes + payload.size
  if stream.inboundBodyKind == .ordinary &&
      stream.inboundContentLength?.any (bodyBytes > ·) then
    return ← failDataStream state stream.id consumed .protocolError
      "DATA exceeded Content-Length"
  if endStream && stream.inboundBodyKind == .ordinary &&
      stream.inboundContentLength?.any (bodyBytes != ·) then
    return ← failDataStream state stream.id consumed .protocolError
      "END_STREAM body length did not match Content-Length"
  let stream := {
    stream with
    inboundWindow := stream.inboundWindow - Int.ofNat payload.size
    inboundBodyBytes := bodyBytes
    phase := if endStream then stream.phase.closeRemote else stream.phase
  }
  let mut automatic ← connectionDataCredit consumed
  if paddingCredit > 0 then
    automatic := automatic.push (← WindowUpdate.frame frame.header.streamId paddingCredit)
  let state := replaceStream state stream
  pure (state, automatic, #[.data frame.header.streamId payload endStream])

private def adjustOutboundWindows (state : State) (old new : Nat) : Except Error State := do
  let delta : Int := Int.ofNat new - Int.ofNat old
  let mut streams := #[]
  for stream in state.streams do
    if stream.phase.localOpen then
      let next := stream.outboundWindow + delta
      if next > Int.ofNat maximumWindowSize then
        throw (Error.connection .flowControlError
          "SETTINGS_INITIAL_WINDOW_SIZE overflowed a stream send window")
      streams := streams.push { stream with outboundWindow := next }
    else
      streams := streams.push stream
  pure { state with streams }

private def activateAcknowledgedLocalSettings (state : State) : State :=
  if state.peerKnownLocalSettings == state.localSettings then state
  else
    let oldWindow := state.peerKnownLocalSettings.initialWindowSize
    let newWindow := state.localSettings.initialWindowSize
    let delta := Int.ofNat newWindow - Int.ofNat oldWindow
    let streams := state.streams.map fun stream =>
      if stream.phase == .closed then stream
      else { stream with inboundWindow := stream.inboundWindow + delta }
    {
      state with
      streams
      peerKnownLocalSettings := state.localSettings
      hpackDecode := Hpack.setDecoderMaxAllowedSize state.hpackDecode
        state.localSettings.headerTableSize
    }

private def processSettings (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireConnectionFrame frame "SETTINGS"
  let values ← Http2.Settings.decode frame
  if Http2.Settings.isAck frame then
    pure (activateAcknowledgedLocalSettings state, #[], #[.settingsAcknowledged])
  else
    let mut updated := state
    let mut peer := updated.peerSettings
    for setting in values do
      if updated.role == .client && setting.id == .enablePush then
        throw (Error.connection .protocolError
          "a server endpoint must not send SETTINGS_ENABLE_PUSH")
      let oldWindow := peer.initialWindowSize
      let oldHeaderTableSize := peer.headerTableSize
      peer ← peer.apply setting
      if peer.initialWindowSize != oldWindow then
        updated ← adjustOutboundWindows updated oldWindow peer.initialWindowSize
      if peer.headerTableSize != oldHeaderTableSize then
        updated := { updated with
          hpackEncode := Hpack.setMaxAllowedSize updated.hpackEncode peer.headerTableSize
        }
    let nextState := { updated with
      peerSettings := peer
      receivedSettings := true
    }
    let ack ← Http2.Settings.frame #[] true
    pure (nextState, #[ack], #[.settingsChanged peer])

private def processPing (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireConnectionFrame frame "PING"
  let payload ← Ping.decode frame
  if Ping.isAck frame then
    pure (state, #[], #[.pingAcknowledged payload])
  else
    pure (state, #[← Ping.frame payload true], #[])

private def processWindowUpdate (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  if frame.header.streamId != 0 && (findStream? state frame.header.streamId).isNone &&
      streamIdIsIdle state frame.header.streamId then
    throw (Error.connection .protocolError "WINDOW_UPDATE arrived on an idle stream")
  let increment ← WindowUpdate.decode frame
  if frame.header.streamId == 0 then
    let next := state.outboundConnectionWindow + Int.ofNat increment
    if next > Int.ofNat maximumWindowSize then
      throw (Error.connection .flowControlError "connection send window overflowed")
    pure ({ state with outboundConnectionWindow := next }, #[], #[])
  else
    match findStream? state frame.header.streamId with
    | none =>
        if streamIdIsIdle state frame.header.streamId then
          throw (Error.connection .protocolError "WINDOW_UPDATE arrived on an idle stream")
        pure (state, #[], #[])
    | some stream =>
        if stream.phase == .closed then
          return (state, #[], #[])
        let next := stream.outboundWindow + Int.ofNat increment
        if next > Int.ofNat maximumWindowSize then
          throw (Error.stream stream.id .flowControlError "stream send window overflowed")
        pure (replaceStream state { stream with outboundWindow := next }, #[], #[])

private def processReset (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireStreamFrame frame "RST_STREAM"
  let code ← RstStream.decode frame
  let some stream := findStream? state frame.header.streamId
    | if streamIdIsIdle state frame.header.streamId then
        throw (Error.connection .protocolError "RST_STREAM arrived on an idle stream")
      else return (state, #[], #[])
  if stream.phase == .closed then
    return (state, #[], #[])
  let state := replaceStream state { stream with phase := .closed }
  pure (state, #[], #[.reset stream.id code])

private def processGoAway (state : State) (frame : Frame) : Except Error (State × Array Frame × Array Event) := do
  requireConnectionFrame frame "GOAWAY"
  let decoded ← GoAway.decode frame
  if let some previous := state.peerGoAwayLastStream? then
    if decoded.lastStreamId > previous then
      throw (Error.connection .protocolError
        "successive GOAWAY increased the last processed stream identifier")
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
  | .priority => do
      requireStreamFrame frame "PRIORITY"
      match Priority.decode frame with
      | .ok _ => pure (state, #[], #[.priority frame.header.streamId])
      | .error error =>
          match error.scope with
          | .stream streamId => failStream state streamId error
          | .localInput | .connection => throw error
  | .pushPromise =>
      throw (Error.connection .protocolError "server push is disabled by this endpoint")
  | .unknown _ => pure (state, #[], #[])

/-- Partial result from processing a byte chunk. Complete frames preceding a
terminal frame error remain committed and observable. Frames after `error?`
are intentionally not applied. -/
structure ProcessBytesResult where
  state : State
  outbound : Array Frame := #[]
  events : Array Event := #[]
  error? : Option Error := none

/-- Decode a byte chunk, preserving incomplete input and all successful
per-frame transitions that precede a failing frame. -/
def processBytes (state : State) (bytes : ByteArray) : Except Error ProcessBytesResult := do
  let (state, bytes) ← if state.prefaceReceived then
      pure (state, bytes)
    else
      let buffered := state.prefaceBuffer.append bytes
      let compared := min buffered.size connectionPreface.size
      unless buffered.extract 0 compared == connectionPreface.extract 0 compared do
        return {
          state := { state with prefaceBuffer := ByteArray.empty }
          error? := some (Error.connection .protocolError
            "invalid HTTP/2 client connection preface")
        }
      if buffered.size < connectionPreface.size then
        return { state := { state with prefaceBuffer := buffered } }
      else
        pure ({ state with
          prefaceReceived := true
          prefaceBuffer := ByteArray.empty
        }, buffered.extract connectionPreface.size buffered.size)
  let decoded ← Frame.decodeChunkBounded state.decoder bytes
    state.peerKnownLocalSettings.maxFrameSize
  let mut state := { state with decoder := { decoded.state with frames := #[] } }
  let mut outbound := #[]
  let mut events := #[]
  let mut error? : Option Error := none
  for frame in decoded.state.frames do
    if error?.isNone then
      match processFrame state frame with
      | .ok (next, automatic, emitted) =>
          state := next
          outbound := outbound ++ automatic
          events := events ++ emitted
      | .error error =>
          match error.scope with
          | .stream streamId =>
              let (next, automatic, emitted) ← failStream state streamId error
              state := next
              outbound := outbound ++ automatic
              events := events ++ emitted
          | .localInput | .connection => error? := some error
  if error?.isNone then
    error? := decoded.error?
  pure { state, outbound, events, error? }

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
  let streamId := (List.range (state.streams.size + 1)).foldl
    (init := state.nextLocalStreamId) fun candidate _ =>
      if state.streams.any (·.id == candidate) then candidate + 2 else candidate
  unless state.role == .client do
    throw (Error.invalidArgument
      "opening server-initiated streams requires server-push state")
  if streamId > maxStreamId then
    throw (Error.connection .protocolError "no local stream identifiers remain")
  if state.peerGoAwayLastStream?.isSome then
    throw (Error.stream streamId .refusedStream
      "cannot open a stream after receiving GOAWAY")
  if let some limit := state.peerSettings.maxConcurrentStreams then
    let active := state.streams.foldl (init := 0) fun count stream =>
      if state.role.isLocalStreamId stream.id && stream.phase != .closed then count + 1 else count
    if active >= limit then
      throw (Error.stream streamId .refusedStream "peer concurrent-stream limit reached")
  localizeFieldError <|
    validateRequestFields state.peerSettings.enableConnectProtocol streamId headers
  let method ← localizeFieldError <| requiredPseudoField streamId headers ":method"
  let declaredLength ← localizeFieldError <| contentLength? streamId headers
  if endStream && declaredLength.any (· != 0) then
    throw (Error.invalidArgument "END_STREAM body length did not match Content-Length")
  Headers.validateListSize state.peerSettings.maxHeaderListSize headers
  let (block, hpack) ← Hpack.encodeHeaderBlock state.hpackEncode headers
  let stream : Stream := {
    id := streamId
    phase := if endStream then .halfClosedLocal else .open
    inboundWindow := state.peerKnownLocalSettings.initialWindowSize
    outboundWindow := state.peerSettings.initialWindowSize
    sentHeaders := true
    requestMethod? := some method
    outboundContentLength? := declaredLength
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
  let metadata ← validateOutboundFieldSection state stream headers endStream
  let classification := metadata.classification
  let expectedLength? := if classification == .trailers then
      stream.outboundContentLength?
    else metadata.contentLength?
  if endStream && metadata.bodyKind != .tunnel &&
      expectedLength?.any (· != stream.outboundBodyBytes) then
    throw (Error.invalidArgument "END_STREAM body length did not match Content-Length")
  Headers.validateListSize state.peerSettings.maxHeaderListSize headers
  let (block, hpack) ← Hpack.encodeHeaderBlock state.hpackEncode headers
  let stream := {
    stream with
    phase := if endStream then stream.phase.closeLocal else stream.phase
    sentHeaders := stream.sentHeaders || classification == .initial
    sentTrailers := stream.sentTrailers || classification == .trailers
    outboundContentLength? := if classification == .initial then
        metadata.contentLength?
      else stream.outboundContentLength?
    outboundBodyKind := if classification == .initial then metadata.bodyKind
      else stream.outboundBodyKind
    inboundContentLength? := if metadata.bodyKind == .tunnel then none
      else stream.inboundContentLength?
    inboundBodyKind := if metadata.bodyKind == .tunnel then .tunnel
      else stream.inboundBodyKind
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
  if stream.outboundBodyKind == .noContent && !bytes.isEmpty then
    throw (Error.invalidArgument "cannot send DATA content for a no-content message")
  let bodyBytes := stream.outboundBodyBytes + bytes.size
  if stream.outboundBodyKind == .ordinary &&
      stream.outboundContentLength?.any (bodyBytes > ·) then
    throw (Error.invalidArgument "DATA exceeds Content-Length")
  if endStream && stream.outboundBodyKind == .ordinary &&
      stream.outboundContentLength?.any (bodyBytes != ·) then
    throw (Error.invalidArgument "END_STREAM body length does not match Content-Length")
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
    outboundBodyBytes := bodyBytes
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
  let nextWindow := stream.inboundWindow + Int.ofNat amount
  if nextWindow > Int.ofNat maximumWindowSize then
    throw (Error.stream streamId .flowControlError "stream receive window overflowed")
  unless stream.phase.remoteOpen do
    return (state, #[])
  let streamUpdate ← WindowUpdate.frame streamId amount
  let stream := { stream with inboundWindow := nextWindow }
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
    pure (replaceStream state { stream with phase := .closed, locallyReset := true }, some frame)

end Http2.Connection
