module

public import Std.Async.TCP
public import Std.Async.Timer
public import Std.Sync.CancellationToken
public import Std.Sync.Channel
public import Std.Sync.Mutex
public import Std.Sync.Notify
public import Http2.CancellationToken
public import Http2.Connection
public import Http2.ExtendedConnect
public import Http2.Tls.Session
public import Tls.Server

public section

namespace Http2.Server

open Std
open Std.Net
open Std.Async

/-- HTTP/2 listener, connection, flow-control, and keepalive policy. -/
structure Config where
  address : SocketAddress := .v4 {
    addr := IPv4Addr.ofParts 127 0 0 1
    port := 8080
  }
  backlog : UInt32 := 1024
  readSize : UInt64 := 16384
  noDelay : Bool := true
  headerTableSize : Nat := Hpack.defaultDynamicTableSize
  maxConcurrentStreams : Option Nat := some 100
  initialWindowSize : Nat := Connection.initialWindowSize
  maxFrameSize : Nat := defaultMaxFramePayloadLength
  maxHeaderListSize : Option Nat := some 65536
  /-- Local bound for a compressed HPACK block before decoding. -/
  maxCompressedHeaderBlockSize : Nat := Connection.defaultMaxCompressedHeaderBlockSize
  /-- Milliseconds between server-initiated PINGs; `none` disables keepalive. -/
  keepaliveIntervalMs : Option Nat := none
  /-- Milliseconds allowed for the matching PING acknowledgement. -/
  keepaliveTimeoutMs : Nat := 20000
  deriving Inhabited

/-- Application protocols mounted on one HTTP/2 listener. -/
structure Applications where
  /-- RFC 8441 requests are admitted only when this handler is present. -/
  extendedConnect : Option ExtendedConnect.Handler := none

/-- Static TLS identity and ALPN policy. Per-connection randomness is generated
when a client is accepted. -/
structure TlsConfig where
  /-- DER certificates, leaf first. -/
  certificateChain : Array ByteArray
  /-- Ed25519 private scalar matching the leaf certificate. -/
  signingKey : ByteArray
  alpnProtocols : List String := ["h2"]

/-- The terminal reason retained for a managed connection. -/
inductive CloseCause where
  | peerClosed
  | serverShutdown
  | keepaliveTimeout
  | protocolError (error : Error)
  | transportError (message : String)
  deriving Inhabited, Repr

namespace CloseCause

def errorCode : CloseCause -> ErrorCode
  | .peerClosed => .noError
  | .serverShutdown => .noError
  | .keepaliveTimeout => .noError
  | .protocolError error => error.code
  | .transportError _ => .internalError

def describe : CloseCause -> String
  | .peerClosed => "peer closed the connection"
  | .serverShutdown => "server shutdown"
  | .keepaliveTimeout => "keepalive ping timeout"
  | .protocolError error => error.message
  | .transportError message => "transport failure: " ++ message

def notifiesPeer : CloseCause -> Bool
  | .peerClosed => false
  | _ => true

end CloseCause

/-- One recently completed connection and its terminal reason. -/
structure ClosedConnection where
  id : Nat
  cause : CloseCause
  deriving Inhabited, Repr

def ipv4Address (a b c d : UInt8) (port : UInt16) : SocketAddress :=
  .v4 { addr := IPv4Addr.ofParts a b c d, port }

def loopback (port : UInt16) : SocketAddress :=
  ipv4Address 127 0 0 1 port

def anyIPv4 (port : UInt16) : SocketAddress :=
  ipv4Address 0 0 0 0 port

private def asIO {alpha : Type} (result : Except Error alpha) : IO alpha :=
  match result with
  | .ok value => pure value
  | .error error => throw (IO.userError error.message)

private def transportError (error : IO.Error) : Error :=
  Error.localInput (toString error)

private structure WriteRequest where
  bytes : ByteArray
  completion : Option (IO.Promise (Except IO.Error Unit)) := none

/-- One FIFO owns all application writes for a connection. This layer is also
used above TLS so frame order and completion semantics do not depend on the
underlying transport. -/
private structure Wire where
  outbound : Std.CloseableChannel WriteRequest
  task : AsyncTask Unit
  failure : IO.Ref (Option IO.Error)

private partial def failQueuedWrites (outbound : Std.CloseableChannel WriteRequest)
    (error : IO.Error) : Async Unit := do
  match <- await (← outbound.recv) with
  | none => pure ()
  | some request =>
      if let some completion := request.completion then
        completion.resolve (.error error)
      failQueuedWrites outbound error

private partial def wireLoop (outbound : Std.CloseableChannel WriteRequest)
    (write : ByteArray -> Async Unit) (failure : IO.Ref (Option IO.Error))
    (onError : IO.Error -> IO Unit) : Async Unit := do
  match <- await (← outbound.recv) with
  | none => pure ()
  | some request =>
      try
        write request.bytes
        if let some completion := request.completion then
          completion.resolve (.ok ())
        wireLoop outbound write failure onError
      catch error =>
        failure.set (some error)
        if let some completion := request.completion then
          completion.resolve (.error error)
        discard <| outbound.close.toBaseIO
        failQueuedWrites outbound error
        onError error

private def startWire (write : ByteArray -> Async Unit)
    (onError : IO.Error -> IO Unit) : IO Wire := do
  let outbound <- Std.CloseableChannel.new none
  let failure <- IO.mkRef (none : Option IO.Error)
  let task <- Async.toIO (wireLoop outbound write failure onError)
  pure { outbound, task, failure }

private def Wire.send (wire : Wire) (bytes : ByteArray) : IO Unit := do
  unless bytes.isEmpty do
    let admitted <- (Std.CloseableChannel.Sync.send wire.outbound { bytes }).toBaseIO
    match admitted with
    | .ok () => pure ()
    | .error _ =>
        throw ((← wire.failure.get).getD (IO.userError "HTTP/2 writer is closed"))

private def Wire.enqueueAcknowledged (wire : Wire) (bytes : ByteArray) :
    IO (Except Error (IO.Promise (Except IO.Error Unit))) := do
  let completion : IO.Promise (Except IO.Error Unit) <- IO.Promise.new
  let admitted <- (Std.CloseableChannel.Sync.send wire.outbound {
    bytes
    completion := some completion
  }).toBaseIO
  match admitted with
  | .ok () => pure (.ok completion)
  | .error _ =>
      let error := (← wire.failure.get).getD (IO.userError "HTTP/2 writer is closed")
      pure (.error (transportError error))

private def awaitWrite (completion : IO.Promise (Except IO.Error Unit)) :
    Async (Except Error Unit) := do
  match <- Async.ofTask completion.result? with
  | some (.ok ()) => pure (.ok ())
  | some (.error error) => pure (.error (transportError error))
  | none => pure (.error (Error.localInput "HTTP/2 write acknowledgement was dropped"))

private def Wire.sendAcknowledged (wire : Wire) (bytes : ByteArray) :
    Async (Except Error Unit) := do
  if bytes.isEmpty then return .ok ()
  match <- wire.enqueueAcknowledged bytes with
  | .error error => pure (.error error)
  | .ok completion => awaitWrite completion

private def waitTaskWithin (task : AsyncTask alpha) (timeoutMs : Nat) : Async Bool := do
  let mut finished <- IO.hasFinished task
  for _ in [0:timeoutMs] do
    if finished then break
    Async.sleep (Std.Time.Millisecond.Offset.ofNat 1)
    finished <- IO.hasFinished task
  pure finished

private def retireFinishedTask (task : AsyncTask alpha) : Async Unit := do
  if <- IO.hasFinished task then
    try discard <| Async.ofAsyncTask task catch _ => pure ()

private def drainWire (wire : Wire) : Async Unit := do
  discard <| wire.outbound.close.toBaseIO
  unless <- waitTaskWithin wire.task 200 do IO.cancel wire.task
  retireFinishedTask wire.task

private def shutdownSocket (socket : TCP.Socket.Client) : Async Unit := do
  let task <- Async.toIO socket.shutdown
  unless <- waitTaskWithin task 200 do IO.cancel task
  retireFinishedTask task

private inductive TunnelPhase where
  | deciding
  | active
  | terminal
  deriving Inhabited, DecidableEq

private structure TunnelOwner where
  inbound : Std.CloseableChannel (Except Error (ByteArray × Nat))
  wakeup : Std.Notify
  terminal : IO.Ref (Option (Except Error Unit))
  terminalSignal : IO.Promise Unit
  task : IO.Ref (Option (AsyncTask Unit))

private structure ManagedTunnel where
  streamId : Nat
  owner : TunnelOwner
  phase : TunnelPhase := .deciding
  sendClosed : Bool := false
  recvClosed : Bool := false

private structure ManagedState where
  protocol : Connection.State
  tunnels : Array ManagedTunnel := #[]
  closing : Bool := false
  pendingPing : Option ByteArray := none

private structure ConnectionContext where
  state : Std.Mutex ManagedState
  wire : Wire
  stopToken : Std.CancellationToken

private def findTunnel? (state : ManagedState) (streamId : Nat) : Option ManagedTunnel :=
  state.tunnels.find? (·.streamId == streamId)

private def replaceTunnel (state : ManagedState) (tunnel : ManagedTunnel) : ManagedState :=
  { state with tunnels := state.tunnels.map fun current =>
      if current.streamId == tunnel.streamId then tunnel else current }

private def findProtocolStream? (state : Connection.State) (streamId : Nat) :
    Option Connection.Stream :=
  state.streams.find? (·.id == streamId)

private def closeInbound (owner : TunnelOwner) : BaseIO Unit := do
  discard <| owner.inbound.close.toBaseIO

private def signalInboundError (owner : TunnelOwner) (error : Error) : BaseIO Unit := do
  discard <| owner.inbound.trySend (.error error)
  closeInbound owner

private def setTerminal (owner : TunnelOwner) (result : Except Error Unit) : IO Unit := do
  owner.terminal.modify fun
    | some existing => some existing
    | none => some result
  owner.terminalSignal.resolve ()
  owner.wakeup.notify

private def notifyTunnels (state : ManagedState) : IO Unit := do
  for tunnel in state.tunnels do tunnel.owner.wakeup.notify

private def encodeFrames (frames : Array Frame) : Except Error ByteArray :=
  Frame.encodeBatch frames

private def settingsFor (config : Config) (applications : Applications) : Connection.Settings := {
  headerTableSize := config.headerTableSize
  enablePush := false
  maxConcurrentStreams := config.maxConcurrentStreams
  initialWindowSize := config.initialWindowSize
  maxFrameSize := config.maxFrameSize
  maxHeaderListSize := config.maxHeaderListSize
  maxCompressedHeaderBlockSize := config.maxCompressedHeaderBlockSize
  enableConnectProtocol := applications.extendedConnect.isSome
}

private def initialSettings (config : Config) (applications : Applications) :
    Except Error Frame := do
  let mut values : Array Setting := #[]
  if config.headerTableSize != Hpack.defaultDynamicTableSize then
    values := values.push { id := .headerTableSize, value := config.headerTableSize }
  if let some maximum := config.maxConcurrentStreams then
    values := values.push { id := .maxConcurrentStreams, value := maximum }
  if config.initialWindowSize != Connection.initialWindowSize then
    values := values.push { id := .initialWindowSize, value := config.initialWindowSize }
  if config.maxFrameSize != defaultMaxFramePayloadLength then
    values := values.push { id := .maxFrameSize, value := config.maxFrameSize }
  if let some maximum := config.maxHeaderListSize then
    values := values.push { id := .maxHeaderListSize, value := maximum }
  if applications.extendedConnect.isSome then
    values := values.push { id := SettingId.enableConnectProtocol, value := 1 }
  Http2.Settings.frame values

private def newManagedState (config : Config) (applications : Applications) : ManagedState := {
  protocol := Connection.initial .server (settingsFor config applications)
}

private def newTunnelOwner : IO TunnelOwner := do
  let inbound <- Std.CloseableChannel.new none
  let wakeup <- Std.Notify.new
  let terminal <- IO.mkRef (none : Option (Except Error Unit))
  let terminalSignal : IO.Promise Unit <- IO.Promise.new
  let task <- IO.mkRef (none : Option (AsyncTask Unit))
  pure { inbound, wakeup, terminal, terminalSignal, task }

private def resetManagedStream (context : ConnectionContext) (streamId : Nat)
    (code : ErrorCode) (failure : Error) (cancelTask : Bool := true) : IO Unit := do
  let affected : Option (TunnelOwner × Bool) <- context.state.atomically do
    let state <- get
    let (protocol, frame?) <- match Connection.resetStream state.protocol streamId code with
      | .ok result => pure result
      | .error _ => pure (state.protocol, some (← asIO (RstStream.frame streamId code)))
    let tunnel? := findTunnel? state streamId
    let state := match tunnel? with
      | none => { state with protocol }
      | some tunnel => replaceTunnel { state with protocol } {
          tunnel with
          phase := .terminal
          sendClosed := true
          recvClosed := true
        }
    set state
    if let some frame := frame? then
      context.wire.send (← asIO (Frame.encode frame))
    pure (tunnel?.map fun tunnel => (tunnel.owner, cancelTask))
  match affected with
  | none => pure ()
  | some (owner, cancelOwnerTask) =>
      setTerminal owner (.error failure)
      signalInboundError owner failure
      if cancelOwnerTask then
        if let some task <- owner.task.get then IO.cancel task

private def completeTunnelIfClosed (context : ConnectionContext) (streamId : Nat) : IO Unit := do
  let owner? : Option TunnelOwner <- context.state.atomically do
    let state <- get
    match findTunnel? state streamId with
    | some tunnel =>
        if tunnel.phase != .terminal && tunnel.sendClosed && tunnel.recvClosed then
          set (replaceTunnel state { tunnel with phase := .terminal })
          pure (some tunnel.owner)
        else
          pure none
    | none => pure none
  if let some owner := owner? then
    setTerminal owner (.ok ())

private inductive TunnelSendStep where
  | wrote (count : Nat) (completion : IO.Promise (Except IO.Error Unit))
  | wait (waiter : AsyncTask Unit)
  | failed (error : Error)

private partial def sendTunnelBytes (context : ConnectionContext) (streamId : Nat)
    (bytes : ByteArray) (offset : Nat) : Async (Except Error Unit) := do
  if offset >= bytes.size then return .ok ()
  let step : TunnelSendStep <- context.state.atomically do
    let state <- get
    match findTunnel? state streamId with
    | none => pure (.failed (Error.stream streamId .streamClosed "tunnel is not active"))
    | some tunnel =>
        if tunnel.phase != .active then
          pure (.failed (Error.stream streamId .streamClosed "tunnel is not active"))
        else if tunnel.sendClosed then
          pure (.failed (Error.stream streamId .streamClosed "tunnel send side is closed"))
        else
          match findProtocolStream? state.protocol streamId with
          | none => pure (.failed (Error.stream streamId .streamClosed "HTTP/2 stream is closed"))
          | some stream =>
              let connectionCredit :=
                if state.protocol.outboundConnectionWindow <= 0 then 0
                else state.protocol.outboundConnectionWindow.toNat
              let streamCredit :=
                if stream.outboundWindow <= 0 then 0 else stream.outboundWindow.toNat
              let available := min connectionCredit streamCredit
              if available == 0 then
                pure (.wait (← tunnel.owner.wakeup.wait))
              else
                let count := min (min available state.protocol.peerSettings.maxFrameSize)
                  (bytes.size - offset)
                let chunk := bytes.extract offset (offset + count)
                match Connection.sendData state.protocol streamId chunk with
                | .error error => pure (.failed error)
                | .ok (protocol, frames) =>
                    match encodeFrames frames with
                    | .error error => pure (.failed error)
                    | .ok encoded =>
                        set { state with protocol }
                        match <- context.wire.enqueueAcknowledged encoded with
                        | .error error => pure (.failed error)
                        | .ok completion => pure (.wrote count completion)
  match step with
  | .failed error => pure (.error error)
  | .wait waiter =>
      try discard <| Async.ofAsyncTask waiter catch _ => pure ()
      sendTunnelBytes context streamId bytes offset
  | .wrote count completion =>
      match <- awaitWrite completion with
      | .error error => pure (.error error)
      | .ok () => sendTunnelBytes context streamId bytes (offset + count)

private def receiveTunnelBytes (context : ConnectionContext) (streamId : Nat)
    (owner : TunnelOwner) : Async (Except Error (Option ByteArray)) := do
  match <- Async.ofTask (← owner.inbound.recv) with
  | none =>
      match <- owner.terminal.get with
      | some (.error error) => pure (.error error)
      | _ => pure (.ok none)
  | some (.error error) => pure (.error error)
  | some (.ok (bytes, credit)) =>
      let credited : Except Error Unit <- context.state.atomically do
        let state <- get
        match Connection.acknowledgeData state.protocol streamId credit with
        | .error error => pure (.error error)
        | .ok (protocol, frames) =>
            match encodeFrames frames with
            | .error error => pure (.error error)
            | .ok encoded =>
                set { state with protocol }
                try
                  context.wire.send encoded
                  pure (.ok ())
                catch error =>
                  pure (.error (transportError error))
      match credited with
      | .error error => pure (.error error)
      | .ok () => pure (.ok (some bytes))

private def closeTunnelSend (context : ConnectionContext) (streamId : Nat) :
    Async (Except Error Unit) := do
  let prepared : Except Error (Option (IO.Promise (Except IO.Error Unit))) <-
      context.state.atomically do
    let state <- get
    match findTunnel? state streamId with
    | none => pure (.error (Error.stream streamId .streamClosed "tunnel is not active"))
    | some tunnel =>
        if tunnel.phase == .terminal then
          pure (.error (Error.stream streamId .streamClosed "tunnel is closed"))
        else if tunnel.sendClosed then
          pure (.ok none)
        else
          match Connection.sendData state.protocol streamId ByteArray.empty true with
          | .error error => pure (.error error)
          | .ok (protocol, frames) =>
              match encodeFrames frames with
              | .error error => pure (.error error)
              | .ok encoded =>
                  let tunnel := { tunnel with sendClosed := true }
                  set (replaceTunnel { state with protocol } tunnel)
                  match <- context.wire.enqueueAcknowledged encoded with
                  | .error error => pure (.error error)
                  | .ok completion => pure (.ok (some completion))
  match prepared with
  | .error error => pure (.error error)
  | .ok none => pure (.ok ())
  | .ok (some completion) =>
      match <- awaitWrite completion with
      | .error error => pure (.error error)
      | .ok () =>
          completeTunnelIfClosed context streamId
          pure (.ok ())

private partial def waitTunnel (context : ConnectionContext) (streamId : Nat)
    (owner : TunnelOwner) : Async (Except Error Unit) := do
  match <- owner.terminal.get with
  | some result => pure result
  | none =>
      discard <| Async.ofTask owner.terminalSignal.result?
      waitTunnel context streamId owner

private def makeTunnel (context : ConnectionContext) (streamId : Nat)
    (owner : TunnelOwner) : ExtendedConnect.Tunnel := {
  sendBytesImpl := fun bytes => sendTunnelBytes context streamId bytes 0
  recvBytesImpl := receiveTunnelBytes context streamId owner
  closeSendImpl := closeTunnelSend context streamId
  cancelImpl := resetManagedStream context streamId .cancel
    (Error.stream streamId .cancel "tunnel cancelled locally") false
  waitImpl := waitTunnel context streamId owner
}

private def normalizedRejection (rejection : ExtendedConnect.Rejection) :
    ExtendedConnect.Response :=
  let response : ExtendedConnect.Response := {
    status := rejection.status
    headers := rejection.headers
  }
  if response.status < 200 || response.status >= 300 then response else { status := 500 }

private def commitRejection (context : ConnectionContext) (streamId : Nat)
    (rejection : ExtendedConnect.Rejection) : IO (Except Error Unit) := do
  let requested := normalizedRejection rejection
  let response := match ExtendedConnect.encodeResponse requested with
    | .ok headers => (requested, headers)
    | .error _ =>
        let fallback : ExtendedConnect.Response := { status := 500 }
        (fallback, (ExtendedConnect.encodeResponse fallback).toOption.getD
          (Headers.singleton ":status" "500"))
  let committed : Except Error TunnelOwner <- context.state.atomically do
    let state <- get
    match findTunnel? state streamId with
    | none => pure (.error (Error.stream streamId .streamClosed "request is no longer active"))
    | some tunnel =>
        if tunnel.phase == .terminal then
          pure (.error (Error.stream streamId .streamClosed "request is no longer active"))
        else
          match Connection.sendHeaders state.protocol streamId response.2 true with
          | .error error => pure (.error error)
          | .ok (protocol, responseFrames) =>
              let (protocol, resetFrames) <-
                match findProtocolStream? protocol streamId with
                | some stream =>
                    if stream.phase.remoteOpen then
                      match Connection.resetStream protocol streamId .noError with
                      | .ok (protocol, some frame) => pure (protocol, #[frame])
                      | .ok (protocol, none) => pure (protocol, #[])
                      | .error _ => pure (protocol, #[])
                    else pure (protocol, #[])
                | none => pure (protocol, #[])
              let frames := responseFrames ++ resetFrames
              match encodeFrames frames with
              | .error error => pure (.error error)
              | .ok encoded =>
                  let tunnel := {
                    tunnel with phase := .terminal, sendClosed := true, recvClosed := true
                  }
                  set (replaceTunnel { state with protocol } tunnel)
                  try
                    context.wire.send encoded
                    pure (.ok tunnel.owner)
                  catch error => pure (.error (transportError error))
  match committed with
  | .error error => pure (.error error)
  | .ok owner =>
      closeInbound owner
      setTerminal owner (.ok ())
      pure (.ok ())

private def commitAcceptance (context : ConnectionContext) (streamId : Nat)
    (acceptance : ExtendedConnect.Acceptance) : IO (Except Error TunnelOwner) := do
  let response : ExtendedConnect.Response := {
    status := acceptance.status
    headers := acceptance.headers
  }
  unless ExtendedConnect.isSuccess response do
    return .error (Error.invalidArgument "extended CONNECT acceptance must use a 2xx status")
  let headers <- match ExtendedConnect.encodeResponse response with
    | .ok headers => pure headers
    | .error error => return .error error
  context.state.atomically do
    let state <- get
    match findTunnel? state streamId with
    | none => pure (.error (Error.stream streamId .streamClosed "request is no longer active"))
    | some tunnel =>
        if tunnel.phase != .deciding then
          pure (.error (Error.stream streamId .streamClosed "request is no longer pending"))
        else
          match Connection.sendHeaders state.protocol streamId headers false with
          | .error error => pure (.error error)
          | .ok (protocol, frames) =>
              match encodeFrames frames with
              | .error error => pure (.error error)
              | .ok encoded =>
                  let tunnel := { tunnel with phase := .active }
                  set (replaceTunnel { state with protocol } tunnel)
                  try
                    context.wire.send encoded
                    pure (.ok tunnel.owner)
                  catch error => pure (.error (transportError error))

private def retireTunnelApplication (context : ConnectionContext) (streamId : Nat) :
    Async Unit := do
  match <- closeTunnelSend context streamId with
  | .error _ => pure ()
  | .ok () =>
      let remoteClosed : Bool <- context.state.atomically do
        pure (((findTunnel? (← get) streamId).map (·.recvClosed)).getD true)
      if remoteClosed then
        completeTunnelIfClosed context streamId
      else
        resetManagedStream context streamId .cancel
          (Error.stream streamId .cancel "tunnel application completed") false

private def runExtendedConnect (context : ConnectionContext) (streamId : Nat)
    (request : ExtendedConnect.Request) (handler : ExtendedConnect.Handler) : Async Unit := do
  let decision <- try handler request catch _ => pure (.reject { status := 500 })
  match decision with
  | .reject rejection =>
      discard <| commitRejection context streamId rejection
  | .accept acceptance =>
      match <- commitAcceptance context streamId acceptance with
      | .error _ =>
          discard <| commitRejection context streamId { status := 500 }
      | .ok owner =>
          let tunnel := makeTunnel context streamId owner
          try
            acceptance.run tunnel
            retireTunnelApplication context streamId
          catch error =>
            resetManagedStream context streamId .internalError
              (Error.stream streamId .internalError
                s!"tunnel application failed: {error}") false

private def removeManagedTunnel (context : ConnectionContext) (streamId : Nat) :
    BaseIO Unit :=
  context.state.atomically do
    modify fun state => {
      state with tunnels := state.tunnels.filter (·.streamId != streamId)
    }

private def spawnExtendedConnect (context : ConnectionContext) (streamId : Nat)
    (request : ExtendedConnect.Request) (handler : ExtendedConnect.Handler)
    (endStream : Bool) : IO (Except Error Unit) := do
  let owner <- newTunnelOwner
  let registered : Bool <- context.state.atomically do
    let state <- get
    if state.closing then
      pure false
    else if (findTunnel? state streamId).isSome then
      pure false
    else
      set { state with tunnels := state.tunnels.push {
        streamId
        owner
        recvClosed := endStream
      } }
      pure true
  unless registered do
    return .error (Error.stream streamId .refusedStream "connection is draining")
  if endStream then closeInbound owner
  let gate : IO.Promise Unit <- IO.Promise.new
  let task <- Async.toIO do
    try
      match <- Async.ofTask gate.result? with
      | none => pure ()
      | some () =>
          let pending <- context.state.atomically do
            pure ((findTunnel? (← get) streamId).any (·.phase == .deciding))
          if pending then runExtendedConnect context streamId request handler
    finally
      removeManagedTunnel context streamId
  owner.task.set (some task)
  gate.resolve ()
  pure (.ok ())

private def sendFinalResponse (context : ConnectionContext) (streamId status : Nat) :
    IO (Except Error Unit) := do
  let response : ExtendedConnect.Response := { status }
  let headers <- match ExtendedConnect.encodeResponse response with
    | .ok headers => pure headers
    | .error error => return .error error
  context.state.atomically do
    let state <- get
    match Connection.sendHeaders state.protocol streamId headers true with
    | .error error => pure (.error error)
    | .ok (protocol, responseFrames) =>
        let (protocol, resetFrames) <-
          match findProtocolStream? protocol streamId with
          | some stream =>
              if stream.phase.remoteOpen then
                match Connection.resetStream protocol streamId .noError with
                | .ok (protocol, some reset) => pure (protocol, #[reset])
                | .ok (protocol, none) => pure (protocol, #[])
                | .error _ => pure (protocol, #[])
              else pure (protocol, #[])
          | none => pure (protocol, #[])
        match encodeFrames (responseFrames ++ resetFrames) with
        | .error error => pure (.error error)
        | .ok encoded =>
            set { state with protocol }
            try
              context.wire.send encoded
              pure (.ok ())
            catch error => pure (.error (transportError error))

private def requestTokenCharacter (character : Char) : Bool :=
  Header.isTokenCharacter character

private def connectionSpecificField (name : String) : Bool :=
  name == "connection" || name == "keep-alive" || name == "proxy-connection" ||
    name == "transfer-encoding" || name == "upgrade"

private def validateRequestFieldSection (headers : Headers) : Except Error Unit := do
  let mut seenOrdinary := false
  let mut pseudoFields : Array String := #[]
  for header in headers do
    unless Header.validFieldValue header do
      throw (Error.invalidArgument s!"invalid HTTP/2 field value for {header.name}")
    if header.name.startsWith ":" then
      if seenOrdinary then
        throw (Error.invalidArgument
          s!"HTTP/2 pseudo-header {header.name} appeared after an ordinary field")
      unless header.name == ":method" || header.name == ":scheme" ||
          header.name == ":authority" || header.name == ":path" ||
          header.name == ":protocol" do
        throw (Error.invalidArgument s!"invalid request pseudo-header {header.name}")
      if pseudoFields.contains header.name then
        throw (Error.invalidArgument s!"duplicate HTTP/2 pseudo-header {header.name}")
      pseudoFields := pseudoFields.push header.name
    else
      seenOrdinary := true
      unless Header.validFieldName header do
        throw (Error.invalidArgument s!"invalid HTTP/2 field name {header.name}")
      if connectionSpecificField header.name then
        throw (Error.invalidArgument
          s!"HTTP/2 connection-specific field is forbidden: {header.name}")
      if header.name == "te" && header.value.toLower != "trailers" then
        throw (Error.invalidArgument "HTTP/2 TE field value must be trailers")
  let methods := headers.getAll ":method"
  unless methods.size == 1 do
    throw (Error.invalidArgument "HTTP/2 request requires exactly one :method")
  let method := methods[0]!
  unless !method.isEmpty && method.all requestTokenCharacter do
    throw (Error.invalidArgument "HTTP/2 :method is not a valid token")
  let protocols := headers.getAll ":protocol"
  if protocols.size > 1 then
    throw (Error.invalidArgument "HTTP/2 request contains duplicate :protocol")
  if protocols.isEmpty then
    if method == "CONNECT" then
      unless (headers.getAll ":authority").size == 1 do
        throw (Error.invalidArgument "CONNECT requires exactly one :authority")
      if ((headers.get? ":authority").getD "").isEmpty then
        throw (Error.invalidArgument "CONNECT :authority must not be empty")
      unless (headers.getAll ":scheme").isEmpty && (headers.getAll ":path").isEmpty do
        throw (Error.invalidArgument "CONNECT must omit :scheme and :path")
    else
      unless (headers.getAll ":scheme").size == 1 &&
          (headers.getAll ":path").size == 1 do
        throw (Error.invalidArgument
          "HTTP/2 request requires exactly one :scheme and :path")
      if ((headers.get? ":scheme").getD "").isEmpty ||
          ((headers.get? ":path").getD "").isEmpty then
        throw (Error.invalidArgument "HTTP/2 :scheme and :path must not be empty")
      if (headers.getAll ":authority").size > 1 then
        throw (Error.invalidArgument "HTTP/2 request contains duplicate :authority")
  else unless method == "CONNECT" do
    throw (Error.invalidArgument "the :protocol pseudo-header requires :method CONNECT")

private def handleHeaders (context : ConnectionContext) (applications : Applications)
    (streamId : Nat) (headers : Headers) (endStream trailers : Bool) : IO Unit := do
  let closing <- context.state.atomically do pure (← get).closing
  if closing then
    resetManagedStream context streamId .refusedStream
      (Error.stream streamId .refusedStream "connection is draining")
    return
  if trailers then
    resetManagedStream context streamId .protocolError
      (Error.stream streamId .protocolError
        "extended CONNECT tunnels do not accept trailing field sections")
    return
  match validateRequestFieldSection headers with
  | .error error =>
      resetManagedStream context streamId .protocolError {
        error with scope := .stream streamId, code := .protocolError
      }
      return
  | .ok () => pure ()
  let method? := headers.get? ":method"
  let protocol? := headers.get? ":protocol"
  if protocol?.isSome && method? != some "CONNECT" then
    resetManagedStream context streamId .protocolError
      (Error.stream streamId .protocolError
        "the :protocol pseudo-header requires :method CONNECT")
    return
  if method? != some "CONNECT" || protocol?.isNone then
    match <- sendFinalResponse context streamId 501 with
    | .ok () => pure ()
    | .error error => resetManagedStream context streamId error.code error
    return
  let request <- match ExtendedConnect.decodeRequest headers with
    | .ok request => pure request
    | .error error =>
        resetManagedStream context streamId .protocolError {
          error with scope := .stream streamId, code := .protocolError
        }
        return
  match applications.extendedConnect with
  | none =>
      match <- sendFinalResponse context streamId 501 with
      | .ok () => pure ()
      | .error error => resetManagedStream context streamId error.code error
  | some handler =>
      match <- spawnExtendedConnect context streamId request handler endStream with
      | .ok () => pure ()
      | .error error => resetManagedStream context streamId error.code error

private def handleTunnelData (context : ConnectionContext) (streamId : Nat)
    (bytes : ByteArray) (endStream : Bool) : IO Unit := do
  let target? : Option TunnelOwner <- context.state.atomically do
    let state <- get
    match findTunnel? state streamId with
    | none => pure none
    | some tunnel =>
        if tunnel.phase == .terminal || tunnel.recvClosed then
          pure none
        else
          let tunnel := { tunnel with recvClosed := endStream }
          set (replaceTunnel state tunnel)
          pure (some tunnel.owner)
  match target? with
  | none =>
      resetManagedStream context streamId .streamClosed
        (Error.stream streamId .streamClosed "DATA arrived without an active tunnel")
  | some owner =>
      let admitted <- if bytes.isEmpty then pure true
        else owner.inbound.trySend (.ok (bytes, bytes.size))
      unless admitted do
        resetManagedStream context streamId .cancel
          (Error.stream streamId .cancel "tunnel receive side is closed")
        return
      if endStream then closeInbound owner
      owner.wakeup.notify
      completeTunnelIfClosed context streamId

private def handleStreamFailure (context : ConnectionContext) (streamId : Nat)
    (failure : Error) : IO Unit := do
  let affected : Option TunnelOwner <- context.state.atomically do
    let state <- get
    match findTunnel? state streamId with
    | none => pure none
    | some tunnel =>
        if tunnel.phase == .terminal then pure none
        else
          set (replaceTunnel state {
            tunnel with phase := .terminal, sendClosed := true, recvClosed := true
          })
          pure (some tunnel.owner)
  if let some owner := affected then
    setTerminal owner (.error failure)
    signalInboundError owner failure
    if let some task <- owner.task.get then IO.cancel task

private def handlePeerReset (context : ConnectionContext) (streamId : Nat)
    (code : ErrorCode) : IO Unit :=
  handleStreamFailure context streamId
    (Error.stream streamId code "peer reset the HTTP/2 stream")

private def handleEvent (context : ConnectionContext) (applications : Applications) :
    Connection.Event -> IO Unit
  | .headers streamId headers endStream trailers =>
      handleHeaders context applications streamId headers endStream trailers
  | .data streamId bytes endStream =>
      handleTunnelData context streamId bytes endStream
  | .reset streamId code => handlePeerReset context streamId code
  | .streamError streamId code message =>
      handleStreamFailure context streamId (Error.stream streamId code message)
  | .settingsChanged _ => pure ()
  | .settingsAcknowledged => pure ()
  | .pingAcknowledged payload =>
      context.state.atomically do
        let state <- get
        if state.pendingPing == some payload then
          set { state with pendingPing := none }
  | .goAway _ _ _ => pure ()
  | .priority _ => pure ()

private structure ProcessedBytes where
  events : Array Connection.Event := #[]
  error? : Option Error := none

private def processPlaintext (context : ConnectionContext) (applications : Applications)
    (bytes : ByteArray) : IO (Except Error Unit) := do
  let processed : Except Error ProcessedBytes <- context.state.atomically do
    let state <- get
    match Connection.processBytes state.protocol bytes with
    | .error error => pure (.error error)
    | .ok result =>
        match encodeFrames result.outbound with
        | .error error => pure (.error error)
        | .ok encoded =>
            let next := { state with protocol := result.state }
            set next
            try
              context.wire.send encoded
              notifyTunnels next
              pure (.ok { events := result.events, error? := result.error? })
            catch error => pure (.error (transportError error))
  match processed with
  | .error error =>
      match error.scope with
      | .stream streamId =>
          resetManagedStream context streamId error.code error
          pure (.ok ())
      | .connection => pure (.error error)
      | .localInput =>
          pure (.error (Error.connection .internalError error.message))
  | .ok processed =>
      for event in processed.events do handleEvent context applications event
      match processed.error? with
      | none => pure (.ok ())
      | some error =>
          match error.scope with
          | .stream streamId =>
              resetManagedStream context streamId error.code error
              pure (.ok ())
          | .connection => pure (.error error)
          | .localInput =>
              pure (.error (Error.connection .internalError error.message))

private def isDrainedState (state : ManagedState) : Bool :=
  state.protocol.streams.all (·.phase == .closed) &&
    state.tunnels.all (·.phase == .terminal)

private def isDrained (context : ConnectionContext) : IO Bool :=
  context.state.atomically do pure (isDrainedState (← get))

private structure ActiveConnection where
  id : Nat
  client : TCP.Socket.Client
  state : Std.Mutex ManagedState
  stopToken : Std.CancellationToken
  closeCause : IO.Ref (Option CloseCause)
  wire : IO.Ref (Option Wire)
  tlsSession : IO.Ref (Option Http2.Tls.ServerSession)

private structure ConnectionTask where
  id : Nat
  task : AsyncTask Unit

/-- A bound listener together with all owned accept and connection tasks. -/
structure Server where
  socket : TCP.Socket.Server
  localAddress : SocketAddress
  config : Config
  private shutdownToken : Std.CancellationToken
  private acceptTask : IO.Ref (Option (AsyncTask Unit))
  private activeConnections : Std.Mutex (Array ActiveConnection)
  private connectionTasks : Std.Mutex (Array ConnectionTask)
  private nextConnectionId : IO.Ref Nat
  private closedConnections : Std.Mutex (Array ClosedConnection)
  private acceptFailure : IO.Ref (Option IO.Error)
  private started : IO.Ref Bool

private def validateConfig (config : Config) : IO Unit := do
  if config.readSize == 0 then
    throw (IO.userError "HTTP/2 readSize must be positive")
  if config.initialWindowSize > Connection.maximumWindowSize then
    throw (IO.userError "HTTP/2 initialWindowSize exceeds 2^31-1")
  if config.maxCompressedHeaderBlockSize == 0 then
    throw (IO.userError "HTTP/2 maxCompressedHeaderBlockSize must be positive")
  if config.maxFrameSize < defaultMaxFramePayloadLength ||
      config.maxFrameSize > maxFramePayloadLength then
    throw (IO.userError "HTTP/2 maxFrameSize is outside 16384..16777215")
  let maximumSettingValue : Nat := 4294967295
  if config.headerTableSize > maximumSettingValue then
    throw (IO.userError "HTTP/2 headerTableSize exceeds the SETTINGS wire range")
  if config.maxConcurrentStreams.any (· > maximumSettingValue) then
    throw (IO.userError "HTTP/2 maxConcurrentStreams exceeds the SETTINGS wire range")
  if config.maxHeaderListSize.any (· > maximumSettingValue) then
    throw (IO.userError "HTTP/2 maxHeaderListSize exceeds the SETTINGS wire range")
  if config.keepaliveIntervalMs == some 0 then
    throw (IO.userError "HTTP/2 keepaliveIntervalMs must be positive")
  if config.keepaliveIntervalMs.isSome && config.keepaliveTimeoutMs == 0 then
    throw (IO.userError "HTTP/2 keepaliveTimeoutMs must be positive")

/-- Bind and listen without starting an accept owner. -/
def bind (config : Config := {}) : IO Server := do
  validateConfig config
  let socket <- TCP.Socket.Server.mk
  socket.bind config.address
  socket.listen config.backlog
  if config.noDelay then socket.noDelay
  let localAddress <- socket.getSockName
  let shutdownToken <- Std.CancellationToken.new
  let acceptTask <- IO.mkRef (none : Option (AsyncTask Unit))
  let activeConnections <- Std.Mutex.new #[]
  let connectionTasks <- Std.Mutex.new #[]
  let nextConnectionId <- IO.mkRef 0
  let closedConnections <- Std.Mutex.new #[]
  let acceptFailure <- IO.mkRef (none : Option IO.Error)
  let started <- IO.mkRef false
  pure {
    socket
    localAddress
    config
    shutdownToken
    acceptTask
    activeConnections
    connectionTasks
    nextConnectionId
    closedConnections
    acceptFailure
    started
  }

private def reportCloseCause (reference : IO.Ref (Option CloseCause))
    (cause : CloseCause) : IO Unit :=
  reference.modify fun
    | some existing => some existing
    | none => some cause

private def nextConnectionId (server : Server) : IO Nat := do
  let id <- server.nextConnectionId.get
  server.nextConnectionId.set (id + 1)
  pure id

private def registerActive (server : Server) (connection : ActiveConnection) : IO Unit :=
  server.activeConnections.atomically do modify (·.push connection)

private def unregisterActive (server : Server) (id : Nat) : IO Unit :=
  server.activeConnections.atomically do modify (·.filter fun item => item.id != id)

private def activeSnapshot (server : Server) : IO (Array ActiveConnection) :=
  server.activeConnections.atomically get

private def retainTask (server : Server) (owner : ConnectionTask) : IO Unit :=
  server.connectionTasks.atomically do modify (·.push owner)

private def taskSnapshot (server : Server) : IO (Array ConnectionTask) :=
  server.connectionTasks.atomically get

private def pruneFinishedTasks (server : Server) : IO Unit := do
  let owners <- taskSnapshot server
  let mut finished : Array Nat := #[]
  for owner in owners do
    if <- IO.hasFinished owner.task then
      match owner.task.get with
      | .ok () => pure ()
      | .error _ => pure ()
      finished := finished.push owner.id
  unless finished.isEmpty do
    server.connectionTasks.atomically do
      modify (·.filter fun owner => !finished.contains owner.id)

private def maxClosedConnectionRecords : Nat := 64

private def recordClosed (server : Server) (id : Nat) (cause : CloseCause) : IO Unit :=
  server.closedConnections.atomically do
    modify fun records =>
      let records := records.push { id, cause }
      if records.size > maxClosedConnectionRecords then
        records.extract (records.size - maxClosedConnectionRecords) records.size
      else records

/-- The bounded set of most recently completed connections. -/
def closedConnectionRecords (server : Server) : IO (Array ClosedConnection) :=
  server.closedConnections.atomically get

private def connectionContext? (connection : ActiveConnection) : IO (Option ConnectionContext) := do
  pure ((← connection.wire.get).map fun wire => {
    state := connection.state
    wire
    stopToken := connection.stopToken
  })

private def gracefulGoAway (connection : ActiveConnection) : IO Unit := do
  reportCloseCause connection.closeCause .serverShutdown
  let wire? <- connection.wire.get
  let encoded? <- connection.state.atomically do
    let state <- get
    let state := { state with closing := true }
    match wire? with
    | none =>
        set state
        pure none
    | some _ =>
        if !state.protocol.prefaceReceived || state.protocol.localGoAwayLastStream?.isSome then
          set state
          pure none
        else
          match GoAway.frame state.protocol.lastPeerStreamId .noError with
          | .error _ =>
              set state
              pure none
          | .ok frame =>
              match Frame.encode frame with
              | .error _ =>
                  set state
                  pure none
              | .ok bytes =>
                  let protocol := {
                    state.protocol with
                    localGoAwayLastStream? := some state.protocol.lastPeerStreamId
                  }
                  set { state with protocol }
                  pure (some bytes)
  match wire?, encoded? with
  | some wire, some bytes =>
      try wire.send bytes catch _ => pure ()
  | _, _ => pure ()
  let drained <- connection.state.atomically do pure (isDrainedState (← get))
  if drained then
    discard <| Http2.CancellationToken.cancel connection.stopToken
      (reason := Std.CancellationReason.shutdown)

private def causeGoAway (connection : ActiveConnection) (cause : CloseCause) : IO Unit := do
  unless cause.notifiesPeer do return
  let wire? <- connection.wire.get
  let encoded? <- connection.state.atomically do
    let state <- get
    if !state.protocol.prefaceReceived || state.protocol.localGoAwayLastStream?.isSome then
      pure none
    else
      match GoAway.frame state.protocol.lastPeerStreamId cause.errorCode
          cause.describe.toUTF8 with
      | .error _ => pure none
      | .ok frame =>
          match Frame.encode frame with
          | .error _ => pure none
          | .ok bytes =>
              let protocol := {
                state.protocol with
                localGoAwayLastStream? := some state.protocol.lastPeerStreamId
              }
              set { state with protocol }
              pure (some bytes)
  match wire?, encoded? with
  | some wire, some bytes =>
      try wire.send bytes catch _ => pure ()
  | _, _ => pure ()

private def cancelOwnedTunnels (connection : ActiveConnection) (failure : Error) :
    Async Unit := do
  let owners <- connection.state.atomically do
    let state <- get
    let (tunnels, owners) := state.tunnels.foldl (init := (#[], #[])) fun result tunnel =>
      let (tunnels, owners) := result
      if tunnel.phase == .terminal then
        (tunnels.push tunnel, owners.push tunnel.owner)
      else
        (tunnels.push {
          tunnel with phase := .terminal, sendClosed := true, recvClosed := true
        }, owners.push tunnel.owner)
    set { state with closing := true, tunnels }
    pure owners
  for owner in owners do
    setTerminal owner (.error failure)
    signalInboundError owner failure
    if let some task <- owner.task.get then IO.cancel task
  for owner in owners do
    if let some task <- owner.task.get then
      try discard <| Async.ofAsyncTask task catch _ => pure ()

private def sendServerSettings (context : ConnectionContext) (config : Config)
    (applications : Applications) : IO Unit := do
  let frame <- asIO (initialSettings config applications)
  context.wire.send (← asIO (Frame.encode frame))

private inductive KeepaliveEvent where
  | tick
  | stop

private def waitKeepalive (token : Std.CancellationToken) (milliseconds : Nat) :
    Async KeepaliveEvent := do
  let timer <- Selector.sleep (Std.Time.Millisecond.Offset.ofNat milliseconds)
  Selectable.one #[
    Selectable.case timer fun _ => pure .tick,
    Selectable.case token.selector fun _ => pure .stop
  ]

private def keepalivePayload : ByteArray :=
  ByteArray.mk #[0x68, 0x74, 0x74, 0x70, 0x32, 0x70, 0x69, 0x6e]

private def emitKeepalivePing (context : ConnectionContext) : IO Bool :=
  context.state.atomically do
    let state <- get
    if state.closing || state.pendingPing.isSome then
      pure false
    else
      match Ping.frame keepalivePayload with
      | .error _ => pure false
      | .ok frame =>
          match Frame.encode frame with
          | .error _ => pure false
          | .ok bytes =>
              set { state with pendingPing := some keepalivePayload }
              try
                context.wire.send bytes
                pure true
              catch _ => pure false

private partial def keepaliveLoop (context : ConnectionContext)
    (closeCause : IO.Ref (Option CloseCause)) (intervalMs timeoutMs : Nat) : Async Unit := do
  match <- waitKeepalive context.stopToken intervalMs with
  | .stop => pure ()
  | .tick =>
      if !(← emitKeepalivePing context) then
        keepaliveLoop context closeCause intervalMs timeoutMs
      else
        match <- waitKeepalive context.stopToken timeoutMs with
        | .stop => pure ()
        | .tick =>
            let pending <- context.state.atomically do pure (← get).pendingPing
            if pending == some keepalivePayload then
              reportCloseCause closeCause .keepaliveTimeout
              discard <| Http2.CancellationToken.cancel context.stopToken
                (reason := Std.CancellationReason.shutdown)
            else
              keepaliveLoop context closeCause intervalMs timeoutMs

private def spawnKeepalive (context : ConnectionContext) (config : Config)
    (closeCause : IO.Ref (Option CloseCause)) : IO (Option (AsyncTask Unit)) :=
  match config.keepaliveIntervalMs with
  | none => pure none
  | some interval => some <$> Async.toIO
      (keepaliveLoop context closeCause interval config.keepaliveTimeoutMs)

private inductive PlainConnectionEvent where
  | received (bytes : Option ByteArray)
  | stop

private def nextPlainEvent (connection : ActiveConnection) (readSize : UInt64) :
    Async PlainConnectionEvent :=
  Selectable.one #[
    Selectable.case (connection.client.recvSelector readSize) fun bytes =>
      pure (.received bytes),
    Selectable.case connection.stopToken.selector fun _ => pure .stop
  ]

private partial def servePlainLoop (context : ConnectionContext)
    (connection : ActiveConnection) (applications : Applications) (config : Config) :
    Async Unit := do
  match <- nextPlainEvent connection config.readSize with
  | .stop => reportCloseCause connection.closeCause .serverShutdown
  | .received none => reportCloseCause connection.closeCause .peerClosed
  | .received (some bytes) =>
      if !bytes.isEmpty then
        match <- processPlaintext context applications bytes with
        | .error error =>
            reportCloseCause connection.closeCause (.protocolError error)
            return
        | .ok () => pure ()
      let closingAndDrained <- connection.state.atomically do
        let state <- get
        pure (state.closing && isDrainedState state)
      if closingAndDrained then
        reportCloseCause connection.closeCause .serverShutdown
      else
        servePlainLoop context connection applications config

private inductive TlsConnectionEvent where
  | received (bytes : Option ByteArray)
  | writerFailed
  | stop

private def nextTlsEvent (connection : ActiveConnection)
    (session : Http2.Tls.ServerSession) (readSize : UInt64) : Async TlsConnectionEvent :=
  Selectable.one #[
    Selectable.case (session.socket.recvSelector readSize) fun bytes => pure (.received bytes),
    Selectable.case session.writerFailureSelector fun _ => pure .writerFailed,
    Selectable.case connection.stopToken.selector fun _ => pure .stop
  ]

private partial def serveTlsLoop (context : ConnectionContext)
    (connection : ActiveConnection) (applications : Applications) (config : Config)
    (session : Http2.Tls.ServerSession) : Async Unit := do
  match <- nextTlsEvent connection session config.readSize with
  | .stop => reportCloseCause connection.closeCause .serverShutdown
  | .writerFailed =>
      let message := (← session.writerFailure?).map toString |>.getD
        "TLS record writer stopped"
      reportCloseCause connection.closeCause (.transportError message)
  | .received none => reportCloseCause connection.closeCause .peerClosed
  | .received (some raw) =>
      match <- session.feedInbound raw with
      | none => reportCloseCause connection.closeCause .peerClosed
      | some plaintext =>
          if !plaintext.isEmpty then
            match <- processPlaintext context applications plaintext with
            | .error error =>
                reportCloseCause connection.closeCause (.protocolError error)
                return
            | .ok () => pure ()
          let closingAndDrained <- connection.state.atomically do
            let state <- get
            pure (state.closing && isDrainedState state)
          if closingAndDrained then
            reportCloseCause connection.closeCause .serverShutdown
          else
            serveTlsLoop context connection applications config session

private def waitUntilPublished (gate : IO.Promise Unit) : Async Unit := do
  match <- Async.ofTask gate.result? with
  | some () => pure ()
  | none => throw (IO.userError "connection ownership gate was dropped")

private def freshTlsConfig (config : TlsConfig) : IO (_root_.Tls.Server.Config) := do
  let entropy <- IO.getRandomBytes 64
  pure {
    serverRandom := entropy.extract 0 32
    x25519Private := entropy.extract 32 64
    certificateChain := config.certificateChain
    signingKey := config.signingKey
    alpnProtocols := config.alpnProtocols
  }

private def tunnelFailureFor (cause : CloseCause) : Error :=
  match cause with
  | .protocolError error => error
  | .serverShutdown => Error.connection .cancel "server shutdown"
  | .peerClosed => Error.connection .cancel "peer closed the connection"
  | .keepaliveTimeout => Error.connection .cancel "keepalive ping timeout"
  | .transportError message => Error.connection .internalError message

private def finishConnection (server : Server) (connection : ActiveConnection)
    (keepaliveTask? : Option (AsyncTask Unit)) : Async Unit := do
  discard <| Http2.CancellationToken.cancel connection.stopToken
    (reason := Std.CancellationReason.shutdown)
  match keepaliveTask? with
  | none => pure ()
  | some task =>
      try discard <| Async.ofAsyncTask task catch _ => pure ()
  let cause := (← connection.closeCause.get).getD .peerClosed
  causeGoAway connection cause
  cancelOwnedTunnels connection (tunnelFailureFor cause)
  match <- connection.wire.get with
  | none => pure ()
  | some wire => drainWire wire
  match <- connection.tlsSession.get with
  | some session => session.close
  | none => shutdownSocket connection.client
  recordClosed server connection.id cause
  unregisterActive server connection.id

private def makeConnection (server : Server) (applications : Applications)
    (client : TCP.Socket.Client) : IO ActiveConnection := do
  let id <- nextConnectionId server
  let state <- Std.Mutex.new (newManagedState server.config applications)
  let stopToken <- Std.CancellationToken.new
  let closeCause <- IO.mkRef (none : Option CloseCause)
  let wire <- IO.mkRef (none : Option Wire)
  let tlsSession <- IO.mkRef (none : Option Http2.Tls.ServerSession)
  pure { id, client, state, stopToken, closeCause, wire, tlsSession }

private def servePlainConnection (server : Server) (applications : Applications)
    (connection : ActiveConnection) (published : IO.Promise Unit) : Async Unit := do
  let mut keepaliveTask? : Option (AsyncTask Unit) := none
  let failure? <- try
      waitUntilPublished published
      if server.config.noDelay then connection.client.noDelay
      let wire <- startWire (fun bytes => connection.client.send bytes) fun error => do
        reportCloseCause connection.closeCause (.transportError (toString error))
        discard <| Http2.CancellationToken.cancel connection.stopToken
          (reason := Std.CancellationReason.shutdown)
      connection.wire.set (some wire)
      let context : ConnectionContext := {
        state := connection.state
        wire
        stopToken := connection.stopToken
      }
      sendServerSettings context server.config applications
      let closing <- connection.state.atomically do pure (← get).closing
      if closing then gracefulGoAway connection
      keepaliveTask? <- spawnKeepalive context server.config connection.closeCause
      servePlainLoop context connection applications server.config
      pure none
    catch error =>
      reportCloseCause connection.closeCause (.transportError (toString error))
      pure (some error)
  finishConnection server connection keepaliveTask?
  if let some error := failure? then throw error

private def serveTlsConnection (server : Server) (applications : Applications)
    (tlsConfig : TlsConfig) (connection : ActiveConnection)
    (published : IO.Promise Unit) : Async Unit := do
  let mut keepaliveTask? : Option (AsyncTask Unit) := none
  let failure? <- try
      waitUntilPublished published
      if server.config.noDelay then connection.client.noDelay
      let config <- freshTlsConfig tlsConfig
      let (session, leftover) <- Http2.Tls.ServerSession.establishWithLeftover
        connection.client config server.config.readSize (stopToken := some connection.stopToken)
      unless (← session.alpnSelected) == some "h2" do
        throw (IO.userError "TLS peer did not negotiate the h2 ALPN protocol")
      connection.tlsSession.set (some session)
      let wire <- startWire (fun bytes => session.sendAcknowledged bytes) fun error => do
        reportCloseCause connection.closeCause (.transportError (toString error))
        discard <| Http2.CancellationToken.cancel connection.stopToken
          (reason := Std.CancellationReason.shutdown)
      connection.wire.set (some wire)
      let context : ConnectionContext := {
        state := connection.state
        wire
        stopToken := connection.stopToken
      }
      sendServerSettings context server.config applications
      unless leftover.isEmpty do
        match <- processPlaintext context applications leftover with
        | .ok () => pure ()
        | .error error =>
            reportCloseCause connection.closeCause (.protocolError error)
            throw (IO.userError error.message)
      let closing <- connection.state.atomically do pure (← get).closing
      if closing then gracefulGoAway connection
      keepaliveTask? <- spawnKeepalive context server.config connection.closeCause
      serveTlsLoop context connection applications server.config session
      pure none
    catch error =>
      reportCloseCause connection.closeCause (.transportError (toString error))
      pure (some error)
  finishConnection server connection keepaliveTask?
  if let some error := failure? then throw error

private def launchPlainConnection (server : Server) (applications : Applications)
    (client : TCP.Socket.Client) : IO Unit := do
  let connection <- makeConnection server applications client
  registerActive server connection
  let published : IO.Promise Unit <- IO.Promise.new
  let task <- try
      Async.toIO (servePlainConnection server applications connection published)
    catch error =>
      published.resolve ()
      unregisterActive server connection.id
      Async.block (shutdownSocket client)
      throw error
  retainTask server { id := connection.id, task }
  published.resolve ()
  if <- server.shutdownToken.isCancelled then gracefulGoAway connection
  pruneFinishedTasks server

private def launchTlsConnection (server : Server) (applications : Applications)
    (tlsConfig : TlsConfig) (client : TCP.Socket.Client) : IO Unit := do
  let connection <- makeConnection server applications client
  registerActive server connection
  let published : IO.Promise Unit <- IO.Promise.new
  let task <- try
      Async.toIO (serveTlsConnection server applications tlsConfig connection published)
    catch error =>
      published.resolve ()
      unregisterActive server connection.id
      Async.block (shutdownSocket client)
      throw error
  retainTask server { id := connection.id, task }
  published.resolve ()
  if <- server.shutdownToken.isCancelled then gracefulGoAway connection
  pruneFinishedTasks server

private inductive AcceptEvent where
  | accepted (client : TCP.Socket.Client)
  | shutdown

private def nextAcceptEvent (server : Server) : Async AcceptEvent :=
  Selectable.one #[
    Selectable.case server.socket.acceptSelector fun client => pure (.accepted client),
    Selectable.case server.shutdownToken.selector fun _ => pure .shutdown
  ]

private partial def acceptPlainLoop (server : Server) (applications : Applications) :
    Async Unit := do
  match <- nextAcceptEvent server with
  | .shutdown => pure ()
  | .accepted client =>
      launchPlainConnection server applications client
      acceptPlainLoop server applications

private partial def acceptTlsLoop (server : Server) (applications : Applications)
    (tlsConfig : TlsConfig) : Async Unit := do
  match <- nextAcceptEvent server with
  | .shutdown => pure ()
  | .accepted client =>
      launchTlsConnection server applications tlsConfig client
      acceptTlsLoop server applications tlsConfig

private def ownAcceptLoop (server : Server) (loop : Async Unit) : Async Unit := do
  try loop
  catch error =>
    server.acceptFailure.set (some error)
    throw error

private def startServer (server : Server) (loop : Async Unit) : IO Server := do
  if <- server.started.get then
    throw (IO.userError "HTTP/2 server accept loop is already running")
  server.started.set true
  let task <- Async.toIO (ownAcceptLoop server loop)
  server.acceptTask.set (some task)
  pure server

/-- Bind and serve cleartext HTTP/2 using the client connection preface
(prior-knowledge h2c). -/
def serveApplications (applications : Applications) (config : Config := {}) : IO Server := do
  let server <- bind config
  startServer server (acceptPlainLoop server applications)

/-- Bind and serve HTTP/2 over TLS 1.3 with ALPN `h2`. -/
def serveTlsApplications (applications : Applications) (tlsConfig : TlsConfig)
    (config : Config := {}) : IO Server := do
  unless tlsConfig.alpnProtocols.contains "h2" do
    throw (IO.userError "TLS server ALPN policy must include h2")
  let server <- bind config
  startServer server (acceptTlsLoop server applications tlsConfig)

/-- Convenience entry point for one extended CONNECT handler over h2c. -/
def serveExtendedConnect (handler : ExtendedConnect.Handler) (config : Config := {}) :
    IO Server :=
  serveApplications { extendedConnect := some handler } config

/-- Convenience entry point for one extended CONNECT handler over TLS. -/
def serveTlsExtendedConnect (handler : ExtendedConnect.Handler) (tlsConfig : TlsConfig)
    (config : Config := {}) : IO Server :=
  serveTlsApplications { extendedConnect := some handler } tlsConfig config

private def signalGracefulShutdown (server : Server) : IO Unit := do
  for connection in <- activeSnapshot server do
    try gracefulGoAway connection catch _ => pure ()

/-- Stop accepting and initiate GOAWAY-based graceful drain on every connection.
Repeated calls are safe. -/
def shutdown (server : Server) : IO Unit := do
  let elected <- Http2.CancellationToken.cancel server.shutdownToken
    (reason := Std.CancellationReason.shutdown)
  if elected then signalGracefulShutdown server

private def forceStopConnections (server : Server) : IO Unit := do
  for connection in <- activeSnapshot server do
    reportCloseCause connection.closeCause .serverShutdown
    discard <| Http2.CancellationToken.cancel connection.stopToken
      (reason := Std.CancellationReason.shutdown)

private partial def waitActiveConnections (server : Server)
    (remainingMs : Option Nat) : IO Bool := do
  let connections <- activeSnapshot server
  if connections.isEmpty then return true
  for connection in connections do
    let drained <- connection.state.atomically do pure (isDrainedState (← get))
    if drained then
      discard <| Http2.CancellationToken.cancel connection.stopToken
        (reason := Std.CancellationReason.shutdown)
  match remainingMs with
  | some 0 => pure false
  | _ =>
      IO.sleep 1
      waitActiveConnections server (remainingMs.map (· - 1))

private def waitTaskFinishedWithinIO (task : AsyncTask alpha) (timeoutMs : Nat) : IO Bool := do
  for _ in [0:timeoutMs] do
    if <- IO.hasFinished task then return true
    IO.sleep 1
  IO.hasFinished task

private def waitConnectionOwnersWithin (server : Server) (timeoutMs : Nat) : IO Bool := do
  for _ in [0:timeoutMs] do
    pruneFinishedTasks server
    if (← taskSnapshot server).isEmpty then return true
    IO.sleep 1
  pruneFinishedTasks server
  pure (← taskSnapshot server).isEmpty

private def ownerCleanupTimeoutMs : Nat := 3000

/-- Wait for the accept owner and all connection owners.

When shutdown has already started, `drainTimeoutMs` bounds graceful stream
drain. At the deadline active connections are cancelled and receive a further
bounded cleanup interval. Passing `none` requests an unbounded graceful drain.
Calling `wait` before shutdown is the serving process's blocking join. -/
def wait (server : Server) (drainTimeoutMs : Option Nat := some 30000) : IO Unit := do
  let shuttingDown <- server.shutdownToken.isCancelled
  let acceptTask? <- server.acceptTask.get
  let acceptFinished <- match acceptTask? with
    | none => pure true
    | some task =>
        if shuttingDown then waitTaskFinishedWithinIO task ownerCleanupTimeoutMs
        else
          match task.get with
          | .ok () => pure true
          | .error error =>
              server.acceptFailure.set (some error)
              pure true
  unless acceptFinished do
    if let some task := acceptTask? then IO.cancel task
    throw (IO.userError "HTTP/2 accept owner did not stop within its shutdown bound")
  if shuttingDown then signalGracefulShutdown server
  let drained <- waitActiveConnections server (if shuttingDown then drainTimeoutMs else none)
  unless drained do forceStopConnections server
  let ownersFinished <- waitConnectionOwnersWithin server ownerCleanupTimeoutMs
  unless ownersFinished && (← activeSnapshot server).isEmpty do
    throw (IO.userError "HTTP/2 connection owner did not retire within its shutdown bound")
  if let some error <- server.acceptFailure.get then throw error

/-- Whether shutdown has been requested. -/
def isShutdown (server : Server) : IO Bool :=
  server.shutdownToken.isCancelled

/-- Whether the accept owner has stopped and no managed connection remains. -/
def isClosed (server : Server) : IO Bool := do
  let acceptStopped <- match <- server.acceptTask.get with
    | none => pure !(← server.started.get)
    | some task => IO.hasFinished task
  pure (acceptStopped && (← activeSnapshot server).isEmpty)

/-- Number of currently owned connections. -/
def activeConnectionCount (server : Server) : IO Nat :=
  return (← activeSnapshot server).size

namespace TestSupport

/-- Number of per-stream tunnel owners retained by active connections. -/
def managedTunnelCount (server : Server) : IO Nat := do
  let mut count := 0
  for connection in ← activeSnapshot server do
    count := count + (← connection.state.atomically do pure (← get).tunnels.size)
  pure count

end TestSupport

/-- The accept-loop failure, once observable. -/
def acceptFailure? (server : Server) : IO (Option IO.Error) :=
  server.acceptFailure.get

/-- Whether the server is currently accepting. An accept-owner failure is
re-thrown so callers do not mistake a dead listener for a healthy one. -/
def checkAccepting (server : Server) : IO Bool := do
  if let some error <- server.acceptFailure.get then throw error
  pure ((← server.started.get) && !(← server.shutdownToken.isCancelled))

end Http2.Server
