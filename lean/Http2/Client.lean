module

public import Std.Async.TCP
public import Std.Async.Timer
public import Std.Sync.CancellationToken
public import Std.Sync.Channel
public import Std.Sync.Notify
public import Http2.CancellationToken
public import Http2.Connection
public import Http2.ExtendedConnect
public import Http2.Tls.Session

public section

namespace Http2.Client

open Std
open Std.Async
open Std.Net

structure Config where
  address : SocketAddress := .v4 {
    addr := IPv4Addr.ofParts 127 0 0 1
    port := 80
  }
  authority : String := "localhost"
  scheme : String := "http"
  readSize : UInt64 := 16384
  initialWindowSize : Nat := Http2.Connection.initialWindowSize
  maxHeaderListSize : Option Nat := some 65536
  /-- Local bound for a compressed HPACK block before decoding. -/
  maxCompressedHeaderBlockSize : Nat :=
    Http2.Connection.defaultMaxCompressedHeaderBlockSize
  deriving Inhabited

structure TlsConfig where
  serverName : Option String := none
  alpnProtocols : Array String := #["h2"]
  /-- PEM trust anchors used for certificate-chain validation. Required unless
  `insecureSkipVerification` is explicitly enabled. -/
  trustAnchorsPEM : Option String := none
  /-- Verify the peer certificate identity after chain validation. -/
  verifyHostname : Bool := true
  /-- Certificate identity independent of SNI. When absent, `serverName` is
  used; one of the two is required while hostname verification is enabled. -/
  verificationName : Option String := none
  /-- Disable certificate-chain and hostname verification. This is intended
  only for explicitly insecure development connections. -/
  insecureSkipVerification : Bool := false

/-- A verified TLS transport before an application protocol consumes it. -/
structure TlsBootstrap where
  session : Http2.Tls.ClientSession
  initialInbound : ByteArray := ByteArray.empty

namespace TlsBootstrap

def close (bootstrap : TlsBootstrap) : Async Unit :=
  bootstrap.session.close

end TlsBootstrap

private structure TunnelHandleState where
  inbound : Array ByteArray := #[]
  terminal? : Option (Except Error Unit) := none

private structure TunnelRecord where
  streamId : Nat
  handle : IO.Ref TunnelHandleState
  inbound : Array ByteArray := #[]
  response : Option ExtendedConnect.Response := none
  failure : Option Error := none
  sendClosed : Bool := false
  recvClosed : Bool := false

private structure State where
  protocol : Http2.Connection.State
  tunnels : Array TunnelRecord := #[]
  dead : Option Error := none
  deriving Inhabited

private structure BackgroundTasks where
  writer : Option (AsyncTask Unit) := none
  reader : Option (AsyncTask Unit) := none

private structure OutboundWrite where
  bytes : ByteArray
  completion : Option (IO.Promise (Except IO.Error Unit)) := none

/-- A multiplexed HTTP/2 transport with retained reader and writer owners. -/
structure Connection where
  socket : TCP.Socket.Client
  config : Config
  private state : Std.Mutex State
  private outbound : Std.CloseableChannel OutboundWrite
  private wakeup : Std.Notify
  private stopToken : Std.CancellationToken
  private writerFailure : IO.Ref (Option IO.Error)
  private writerFailureToken : Std.CancellationToken
  private background : IO.Ref BackgroundTasks
  private tls : Option Http2.Tls.ClientSession := none
  private closeClaimed : Std.Mutex Bool
  private closed : IO.Promise Unit

private def validateConfig (config : Config) : IO Unit := do
  if config.readSize == 0 then
    throw (IO.userError "HTTP/2 client readSize must be positive")
  if config.initialWindowSize > Http2.Connection.maximumWindowSize then
    throw (IO.userError "HTTP/2 client initial window exceeds 2^31-1")
  if config.maxCompressedHeaderBlockSize == 0 then
    throw (IO.userError "HTTP/2 client maxCompressedHeaderBlockSize must be positive")

private def transportError (error : IO.Error) : Error :=
  Error.connection .internalError (toString error)

private def localCancellation (message : String) : Error :=
  Error.localInput message .cancel

private def requireOk (result : Except Error α) : IO α :=
  match result with
  | .ok value => pure value
  | .error error => throw (IO.userError error.message)

private def findTunnel? (tunnels : Array TunnelRecord) (streamId : Nat) :
    Option TunnelRecord :=
  tunnels.find? fun tunnel => tunnel.streamId == streamId

private def replaceTunnel (tunnels : Array TunnelRecord) (tunnel : TunnelRecord) :
    Array TunnelRecord :=
  (tunnels.filter fun current => current.streamId != tunnel.streamId).push tunnel

private def removeTunnel (tunnels : Array TunnelRecord) (streamId : Nat) :
    Array TunnelRecord :=
  tunnels.filter fun tunnel => tunnel.streamId != streamId

private def failTunnelRecord (error : Error) (tunnel : TunnelRecord) : TunnelRecord :=
  if tunnel.failure.isSome then tunnel else { tunnel with failure := some error }

private def retireTunnelRecord (tunnel : TunnelRecord)
    (result : Except Error Unit) : BaseIO Unit := do
  tunnel.handle.modify fun retained =>
    if retained.terminal?.isSome then retained
    else { inbound := tunnel.inbound, terminal? := some result }

private def failState (state : State) (error : Error) : State := {
  state with
  dead := some (state.dead.getD error)
  tunnels := state.tunnels.map (failTunnelRecord error)
}

private def wake (connection : Connection) : BaseIO Unit :=
  connection.wakeup.notify

private def failConnection (connection : Connection) (error : Error) : IO Unit := do
  connection.state.atomically do modify fun state => failState state error
  wake connection

private def enqueueBytes (connection : Connection) (bytes : ByteArray) : BaseIO Unit := do
  unless bytes.isEmpty do
    discard <| connection.outbound.send ({ bytes := bytes } : OutboundWrite)

private def enqueueFrame (connection : Connection) (frame : Frame) : BaseIO Unit := do
  match Frame.encode frame with
  | .ok bytes => enqueueBytes connection bytes
  | .error _ => pure ()

private def enqueueFrames (connection : Connection) (frames : Array Frame) : BaseIO Unit := do
  unless frames.isEmpty do
    match Frame.encodeBatch frames with
    | .ok bytes => enqueueBytes connection bytes
    | .error _ => pure ()

private structure OutboundTicket where
  completion : IO.Promise (Except IO.Error Unit)

private def enqueueBytesAcknowledged (connection : Connection) (bytes : ByteArray) :
    IO (Except Error OutboundTicket) := do
  let completion : IO.Promise (Except IO.Error Unit) ← IO.Promise.new
  if bytes.isEmpty then
    completion.resolve (.ok ())
    pure (.ok { completion })
  else
    let request : OutboundWrite := {
      bytes
      completion := some completion
    }
    let admitted ← (Std.CloseableChannel.Sync.send connection.outbound request).toBaseIO
    match admitted with
    | .ok () => pure (.ok { completion })
    | .error _ =>
        let error := (← connection.writerFailure.get).getD
          (IO.userError "HTTP/2 connection writer is closed")
        pure (.error (transportError error))

private def enqueueFramesAcknowledged (connection : Connection) (frames : Array Frame) :
    IO (Except Error OutboundTicket) :=
  match Frame.encodeBatch frames with
  | .ok bytes => enqueueBytesAcknowledged connection bytes
  | .error error => pure (.error error)

private def awaitOutboundTicket (ticket : OutboundTicket) : Async (Except Error Unit) := do
  match ← Async.ofTask ticket.completion.result? with
  | some (.ok ()) => pure (.ok ())
  | some (.error error) => pure (.error (transportError error))
  | none => pure (.error (Error.connection .internalError
      "HTTP/2 writer dropped a send acknowledgement"))

private def awaitWaiter (waiter : AsyncTask Unit) : Async Unit := do
  try Async.ofAsyncTask waiter catch _ => pure ()

private def asyncTaskSelector (task : AsyncTask α) : Selector α := {
  tryFn := do
    if ← IO.hasFinished task then some <$> Async.ofAsyncTask task else pure none
  registerFn := fun waiter => do
    discard <| IO.mapTask (t := task) (sync := true) fun result =>
      waiter.race (pure ()) fun promise => promise.resolve result
  unregisterFn := pure ()
}

private def streamError (streamId : Nat) (code : ErrorCode) (message : String) : Error :=
  Error.stream streamId code message

private def resetTunnelState (state : State) (streamId : Nat) (code : ErrorCode)
    (error : Error) : State × Array Frame :=
  let tunnels := match findTunnel? state.tunnels streamId with
    | none => state.tunnels
    | some tunnel => replaceTunnel state.tunnels (failTunnelRecord error tunnel)
  match Http2.Connection.resetStream state.protocol streamId code with
  | .ok (protocol, some frame) => ({ state with protocol, tunnels }, #[frame])
  | .ok (protocol, none) => ({ state with protocol, tunnels }, #[])
  | .error _ => ({ state with tunnels }, #[])

private def failTunnelProtocol (state : State) (streamId : Nat) (message : String) :
    State × Array Frame :=
  resetTunnelState state streamId .protocolError
    (streamError streamId .protocolError message)

private def handleHeadersEvent (state : State) (streamId : Nat) (headers : Headers)
    (endStream : Bool) : State × Array Frame :=
  match findTunnel? state.tunnels streamId with
  | none => (state, #[])
  | some tunnel =>
      if tunnel.failure.isSome || tunnel.response.isSome then
        failTunnelProtocol state streamId
          "peer sent HEADERS after the extended CONNECT final response"
      else
        match ExtendedConnect.decodeResponse headers with
        | .error error => failTunnelProtocol state streamId error.message
        | .ok response =>
            if response.status < 200 then
              if endStream then
                failTunnelProtocol state streamId
                  "peer ended an extended CONNECT stream with an informational response"
              else (state, #[])
            else
              let tunnel := { tunnel with response := some response, recvClosed := endStream }
              ({ state with tunnels := replaceTunnel state.tunnels tunnel }, #[])

private def handleDataEvent (state : State) (streamId : Nat) (bytes : ByteArray)
    (endStream : Bool) : Except Error (State × Array Frame) := do
  let some tunnel := findTunnel? state.tunnels streamId | return (state, #[])
  if tunnel.failure.isSome then return (state, #[])
  let some response := tunnel.response
    | return failTunnelProtocol state streamId
        "peer sent DATA before the extended CONNECT final response"
  if !ExtendedConnect.isSuccess response then
    let (protocol, credit) ← Http2.Connection.acknowledgeData
      state.protocol streamId bytes.size
    let tunnel := { tunnel with recvClosed := tunnel.recvClosed || endStream }
    pure ({ state with protocol, tunnels := replaceTunnel state.tunnels tunnel }, credit)
  else if tunnel.recvClosed then
    pure (failTunnelProtocol state streamId
      "peer sent DATA after END_STREAM on an extended CONNECT tunnel")
  else
    let tunnel := {
      tunnel with
      inbound := if bytes.isEmpty then tunnel.inbound else tunnel.inbound.push bytes
      recvClosed := endStream
    }
    pure ({ state with tunnels := replaceTunnel state.tunnels tunnel }, #[])

private def handleEvent (state : State) (event : Http2.Connection.Event) :
    Except Error (State × Array Frame) := do
  match event with
  | .headers streamId headers endStream _ =>
      pure (handleHeadersEvent state streamId headers endStream)
  | .data streamId bytes endStream => handleDataEvent state streamId bytes endStream
  | .reset streamId code =>
      let error := streamError streamId code
        s!"peer reset extended CONNECT stream with HTTP/2 error {code.toNat}"
      let tunnels := match findTunnel? state.tunnels streamId with
        | none => state.tunnels
        | some tunnel =>
            -- A final unsuccessful response can be followed by
            -- RST_STREAM(NO_ERROR) when the peer declines the request body.
            -- Preserve the response so opening reports an HTTP rejection.
            if code == .noError && tunnel.response.any (fun response =>
                !ExtendedConnect.isSuccess response) then
              state.tunnels
            else
              replaceTunnel state.tunnels (failTunnelRecord error tunnel)
      pure ({ state with tunnels }, #[])
  | .streamError streamId code message =>
      let error := streamError streamId code message
      let tunnels := match findTunnel? state.tunnels streamId with
        | none => state.tunnels
        | some tunnel => replaceTunnel state.tunnels (failTunnelRecord error tunnel)
      pure ({ state with tunnels }, #[])
  | .goAway lastStreamId code _ =>
      let tunnels := state.tunnels.map fun tunnel =>
        if tunnel.streamId > lastStreamId then
          failTunnelRecord (streamError tunnel.streamId .refusedStream
            s!"peer GOAWAY excluded stream {tunnel.streamId} with error {code.toNat}") tunnel
        else tunnel
      pure ({ state with tunnels }, #[])
  | .settingsChanged _ | .settingsAcknowledged | .pingAcknowledged _ | .priority _ =>
      pure (state, #[])

private structure InboundOutcome where
  frames : Array Frame := #[]
  shouldContinue : Bool := true

private def goAwayFor (state : State) (error : Error) : Option Frame :=
  (GoAway.frame state.protocol.lastPeerStreamId error.code error.message.toUTF8).toOption

private def processInboundChunk (connection : Connection) (chunk : ByteArray) : IO Bool := do
  let outcome : InboundOutcome ← connection.state.atomically do
    let initial ← get
    if initial.dead.isSome then return { shouldContinue := false }
    match Frame.decodeChunkBounded initial.protocol.decoder chunk
        initial.protocol.peerKnownLocalSettings.maxFrameSize with
    | .error error =>
        let state := failState initial error
        set state
        pure {
          frames := match goAwayFor state error with
            | some frame => #[frame]
            | none => #[]
          shouldContinue := false
        }
    | .ok decoded =>
        let mut state := { initial with
          protocol := {
            initial.protocol with decoder := { decoded.state with frames := #[] }
          }
        }
        let mut outbound := #[]
        let mut keepGoing := true
        for frame in decoded.state.frames do
          if !keepGoing then break
          match Http2.Connection.processFrame state.protocol frame with
          | .error error =>
              match error.scope with
              | .stream streamId =>
                  let (next, reset) := resetTunnelState state streamId error.code error
                  state := next
                  outbound := outbound ++ reset
              | .localInput | .connection =>
                  state := failState state error
                  if let some goAway := goAwayFor state error then
                    outbound := outbound.push goAway
                  keepGoing := false
          | .ok (protocol, automatic, events) =>
              state := { state with protocol }
              outbound := outbound ++ automatic
              for event in events do
                match handleEvent state event with
                | .ok (next, frames) =>
                    state := next
                    outbound := outbound ++ frames
                | .error error =>
                    state := failState state error
                    if let some goAway := goAwayFor state error then
                      outbound := outbound.push goAway
                    keepGoing := false
                    break
        if keepGoing then
          if let some error := decoded.error? then
            state := failState state error
            if let some goAway := goAwayFor state error then
              outbound := outbound.push goAway
            keepGoing := false
        set state
        pure { frames := outbound, shouldContinue := keepGoing }
  enqueueFrames connection outcome.frames
  wake connection
  pure outcome.shouldContinue

private inductive ReaderEvent where
  | received (chunk? : Option ByteArray)
  | stop
  | writerFailed (error : IO.Error)

private def nextReaderEvent (connection : Connection) : Async ReaderEvent := do
  let cases := #[
    Selectable.case (connection.socket.recvSelector connection.config.readSize) fun chunk? =>
      pure (.received chunk?),
    Selectable.case connection.stopToken.selector fun _ => pure .stop,
    Selectable.case connection.writerFailureToken.selector fun _ => do
      let error := (← connection.writerFailure.get).getD
        (IO.userError "HTTP/2 connection writer failed")
      pure (.writerFailed error)
  ]
  match connection.tls with
  | none => Selectable.one cases
  | some session =>
      Selectable.one <| cases.push <|
        Selectable.case session.writerFailureSelector fun _ => do
          let error := (← session.writerFailure?).getD
            (IO.userError "TLS record writer failed")
          pure (.writerFailed error)

private partial def failQueuedWrites (connection : Connection) (error : IO.Error) :
    Async Unit := do
  match ← await (← connection.outbound.recv) with
  | none => pure ()
  | some request =>
      if let some completion := request.completion then completion.resolve (.error error)
      failQueuedWrites connection error

private partial def writerLoop (connection : Connection) : Async Unit := do
  match ← await (← connection.outbound.recv) with
  | none => pure ()
  | some request =>
      try
        match connection.tls with
        | some session => session.sendAcknowledged request.bytes
        | none => connection.socket.send request.bytes
        if let some completion := request.completion then completion.resolve (.ok ())
        writerLoop connection
      catch error =>
        if let some completion := request.completion then completion.resolve (.error error)
        if (← connection.writerFailure.get).isNone then
          connection.writerFailure.set (some error)
        discard <| connection.outbound.close.toBaseIO
        failQueuedWrites connection error
        discard <| Http2.CancellationToken.cancel connection.writerFailureToken
          (reason := Std.CancellationReason.shutdown)

private def waitTaskWithin (task : AsyncTask α) (timeoutMs : Nat) : Async Bool := do
  let mut finished ← IO.hasFinished task
  for _ in [0:timeoutMs] do
    if finished then break
    Std.Async.sleep (Std.Time.Millisecond.Offset.ofNat 1)
    finished ← IO.hasFinished task
  pure finished

private def retireTaskWithin (task : AsyncTask Unit) (timeoutMs : Nat := 200) : Async Unit := do
  unless ← waitTaskWithin task timeoutMs do IO.cancel task
  if ← IO.hasFinished task then
    try Async.ofAsyncTask task catch _ => pure ()

private def shutdownSocket (socket : TCP.Socket.Client) : Async Unit := do
  let task ← Async.toIO socket.shutdown
  retireTaskWithin task

private def joinBackgroundTasks (connection : Connection) : Async Unit := do
  let tasks ← connection.background.get
  if let some writer := tasks.writer then retireTaskWithin writer
  if let some reader := tasks.reader then retireTaskWithin reader

private def performShutdown (connection : Connection) : Async Unit := do
  failConnection connection (Error.connection .cancel "HTTP/2 connection closed locally")
  discard <| Http2.CancellationToken.cancel connection.stopToken
    (reason := Std.CancellationReason.shutdown)
  discard <| connection.outbound.close.toBaseIO
  match connection.tls with
  | some session =>
      joinBackgroundTasks connection
      session.close
  | none =>
      joinBackgroundTasks connection
      shutdownSocket connection.socket

/-- Cooperatively close the connection and join its exact reader and writer. -/
def close (connection : Connection) : Async Unit := do
  let owner ← connection.closeClaimed.atomically do
    if ← get then pure false else set true; pure true
  if owner then
    try performShutdown connection finally connection.closed.resolve ()
  else
    discard <| Async.ofTask connection.closed.result?

/-- Elect transport retirement without making the reader join itself. The
spawned close owner drains the already-queued terminal frames, waits for this
reader to return, and resolves the shared completion promise. -/
private def requestTransportRetirement (connection : Connection) : IO Unit := do
  discard <| Async.toIO (close connection)

private def failAndRetire (connection : Connection) (error : Error) : IO Unit := do
  failConnection connection error
  requestTransportRetirement connection

private partial def readerLoop (connection : Connection)
    (pending? : Option ByteArray := none) : Async Unit := do
  if let some chunk := pending? then
    if ← processInboundChunk connection chunk then
      readerLoop connection none
    else
      requestTransportRetirement connection
    return
  let event ← try Except.ok <$> nextReaderEvent connection
    catch error => pure (Except.error error)
  match event with
  | .error error => failAndRetire connection (transportError error)
  | .ok .stop =>
      -- The close owner cancelled this token and already owns retirement.
      failConnection connection (Error.connection .cancel "HTTP/2 connection closed locally")
  | .ok (.writerFailed error) => failAndRetire connection (transportError error)
  | .ok (.received none) =>
      failAndRetire connection (Error.connection .internalError
        "HTTP/2 connection closed by peer")
  | .ok (.received (some raw)) =>
      let plaintext? : Except IO.Error (Option ByteArray) ← try
          match connection.tls with
          | none => pure (Except.ok (some raw))
          | some session => pure (Except.ok (← session.feedInbound raw))
        catch error => pure (Except.error error)
      match plaintext? with
      | .error error => failAndRetire connection (transportError error)
      | .ok none =>
          failAndRetire connection (Error.connection .internalError
            "TLS peer closed the HTTP/2 connection")
      | .ok (some plaintext) =>
          if ← processInboundChunk connection plaintext then
            readerLoop connection none
          else
            requestTransportRetirement connection

private def startBackgroundTasks (connection : Connection)
    (initialInbound : ByteArray := ByteArray.empty) : IO Unit := do
  let pending? := if initialInbound.isEmpty then none else some initialInbound
  let reader ← Async.toIO (readerLoop connection pending?)
  connection.background.set { reader := some reader }
  let writer ← Async.toIO (writerLoop connection)
  connection.background.set { writer := some writer, reader := some reader }

def backgroundTasksFinished (connection : Connection) : IO Bool := do
  let tasks ← connection.background.get
  let writer ← match tasks.writer with
    | none => pure true
    | some task => IO.hasFinished task
  let reader ← match tasks.reader with
    | none => pure true
    | some task => IO.hasFinished task
  pure (writer && reader)

private def localSettings (config : Config) : Http2.Connection.Settings := {
  enablePush := false
  initialWindowSize := config.initialWindowSize
  maxHeaderListSize := config.maxHeaderListSize
  maxCompressedHeaderBlockSize := config.maxCompressedHeaderBlockSize
}

private def prefaceWire (config : Config) : IO ByteArray := do
  requireOk <| Http2.Connection.initialWireBytes
    (Http2.Connection.initial .client (localSettings config))

private def initializeConnection (socket : TCP.Socket.Client) (config : Config)
    (wire : ByteArray) (tls : Option Http2.Tls.ClientSession := none)
    (initialInbound : ByteArray := ByteArray.empty) : IO Connection := do
  validateConfig config
  let connection : Connection := {
    socket
    config
    state := ← Std.Mutex.new {
      protocol := Http2.Connection.initial .client (localSettings config)
    }
    outbound := ← Std.CloseableChannel.new
    wakeup := ← Std.Notify.new
    stopToken := ← Std.CancellationToken.new
    writerFailure := ← IO.mkRef (none : Option IO.Error)
    writerFailureToken := ← Std.CancellationToken.new
    background := ← IO.mkRef {}
    tls
    closeClaimed := ← Std.Mutex.new false
    closed := ← IO.Promise.new
  }
  try
    -- Preface admission precedes both tasks, so an early peer SETTINGS
    -- acknowledgement cannot overtake the mandatory connection preface.
    enqueueBytes connection wire
    startBackgroundTasks connection initialInbound
    pure connection
  catch error =>
    Async.block (close connection)
    throw error

private def openingCancelled (token? : Option Std.CancellationToken) : BaseIO Bool :=
  match token? with
  | none => pure false
  | some token => token.isCancelled

private def openingCancelledError (phase : String) : IO.Error :=
  IO.userError s!"{phase} cancelled"

def connectAsync (config : Config := {})
    (cancellation? : Option Std.CancellationToken := none) : Async Connection := do
  validateConfig config
  let wire ← prefaceWire config
  if ← openingCancelled cancellation? then
    throw (openingCancelledError "HTTP/2 connection opening")
  let socket ← TCP.Socket.Client.mk
  try
    socket.connect config.address
    if ← openingCancelled cancellation? then
      shutdownSocket socket
      throw (openingCancelledError "HTTP/2 connection opening")
    socket.noDelay
    let connection ← initializeConnection socket config wire
    if ← openingCancelled cancellation? then
      close connection
      throw (openingCancelledError "HTTP/2 connection opening")
    pure connection
  catch error =>
    shutdownSocket socket
    throw error

def connect (config : Config := {}) : IO Connection :=
  Async.block (connectAsync config)

private def verifyTlsPeer (session : Http2.Tls.ClientSession)
    (trustStore? : Option TLS13.X509.Chain.TrustStore) (tlsConfig : TlsConfig) : IO Unit := do
  if tlsConfig.insecureSkipVerification then return
  match trustStore? with
  | none => throw (IO.userError
      "TLS trust anchors are required unless insecureSkipVerification is enabled")
  | some trustStore =>
      let certificates ← session.peerCertificates
      let some leaf := certificates[0]? | throw (IO.userError "TLS peer sent no certificate")
      let presented := certificates.extract 1 certificates.size
      let now := (← Std.Time.Timestamp.now).toSecondsSinceUnixEpoch.toInt
      let verified ← match TLS13.X509.Chain.validate now leaf presented trustStore with
        | .ok verified => pure verified
        | .error failure => throw (IO.userError s!"TLS certificate chain: {repr failure}")
      if tlsConfig.verifyHostname then
        match tlsConfig.verificationName.orElse fun _ => tlsConfig.serverName with
        | none => throw (IO.userError
            "TLS hostname verification requires a verificationName or serverName")
        | some host =>
            match TLS13.X509.Hostname.verifyHostname host verified.leaf with
            | .ok () => pure ()
            | .error failure => throw (IO.userError s!"TLS hostname: {repr failure}")

def bootstrapTlsAsync (config : Config := {}) (tlsConfig : TlsConfig := {})
    (cancellation? : Option Std.CancellationToken := none) : Async TlsBootstrap := do
  validateConfig config
  if !tlsConfig.insecureSkipVerification && tlsConfig.verifyHostname &&
      tlsConfig.verificationName.isNone && tlsConfig.serverName.isNone then
    throw (IO.userError
      "TLS hostname verification requires a verificationName or serverName")
  let trustStore? ← if tlsConfig.insecureSkipVerification then
      pure none
    else match tlsConfig.trustAnchorsPEM with
    | none => throw (IO.userError
        "TLS trust anchors are required unless insecureSkipVerification is enabled")
    | some pem =>
        match TLS13.X509.Chain.TrustStore.decodePEM pem with
        | .ok store => pure (some store)
        | .error message => throw (IO.userError s!"TLS trust anchors: {message}")
  if ← openingCancelled cancellation? then
    throw (openingCancelledError "TLS connection opening")
  let socket ← TCP.Socket.Client.mk
  let sessionRef ← IO.mkRef (none : Option Http2.Tls.ClientSession)
  try
    socket.connect config.address
    if ← openingCancelled cancellation? then
      shutdownSocket socket
      throw (openingCancelledError "TLS connection opening")
    socket.noDelay
    let entropy ← IO.getRandomBytes 96
    let clientConfig : _root_.Tls.Client.Config := {
      clientRandom := entropy.extract 0 32
      x25519Private := entropy.extract 32 64
      legacySessionId := entropy.extract 64 96
      serverName := tlsConfig.serverName
      alpnProtocols := tlsConfig.alpnProtocols
    }
    let (session, initialInbound) ← Http2.Tls.ClientSession.establishWithLeftover
      socket clientConfig config.readSize cancellation?
    sessionRef.set (some session)
    if ← openingCancelled cancellation? then
      session.close
      throw (openingCancelledError "TLS connection opening")
    verifyTlsPeer session trustStore? tlsConfig
    if ← openingCancelled cancellation? then
      session.close
      throw (openingCancelledError "TLS peer verification")
    pure { session, initialInbound }
  catch error =>
    match ← sessionRef.get with
    | some session => session.close
    | none => shutdownSocket socket
    throw error

def bootstrapTls (config : Config := {}) (tlsConfig : TlsConfig := {}) : IO TlsBootstrap :=
  Async.block (bootstrapTlsAsync config tlsConfig)

namespace Connection

def adoptTlsH2 (config : Config) (bootstrap : TlsBootstrap) : IO Client.Connection := do
  unless (← bootstrap.session.alpnSelected) == some "h2" do
    throw (IO.userError "TLS peer did not negotiate the h2 ALPN protocol")
  let wire ← prefaceWire config
  initializeConnection bootstrap.session.socket config wire
    (some bootstrap.session) bootstrap.initialInbound

def adoptTlsH2Async (config : Config) (bootstrap : TlsBootstrap)
    (cancellation? : Option Std.CancellationToken := none) : Async Client.Connection := do
  if ← openingCancelled cancellation? then
    throw (openingCancelledError "HTTP/2 TLS adoption")
  let connection ← adoptTlsH2 config bootstrap
  if ← openingCancelled cancellation? then
    Client.close connection
    throw (openingCancelledError "HTTP/2 TLS adoption")
  pure connection

end Connection

def connectTlsAsync (config : Config := {}) (tlsConfig : TlsConfig := {})
    (cancellation? : Option Std.CancellationToken := none) : Async Connection := do
  let bootstrap ← bootstrapTlsAsync config tlsConfig cancellation?
  try Connection.adoptTlsH2Async config bootstrap cancellation?
  catch error => bootstrap.close; throw error

def connectTls (config : Config := {}) (tlsConfig : TlsConfig := {}) : IO Connection :=
  Async.block (connectTlsAsync config tlsConfig)

private def awaitWaiterOrCancel (connection : Connection) (waiter : AsyncTask Unit)
    (cancellation? : Option Std.CancellationToken) : Async Bool := do
  match cancellation? with
  | none => awaitWaiter waiter; pure false
  | some cancellation =>
      let cancelled ← Selectable.one #[
        Selectable.case (asyncTaskSelector waiter) fun _ => pure false,
        Selectable.case cancellation.selector fun _ => pure true
      ]
      if cancelled then wake connection
      pure cancelled

private def localStreamCapacityAvailable (protocol : Http2.Connection.State) : Bool :=
  match protocol.peerSettings.maxConcurrentStreams with
  | none => true
  | some limit =>
      let active := protocol.streams.foldl (init := 0) fun count stream =>
        if protocol.role.isLocalStreamId stream.id && stream.phase != .closed then count + 1
        else count
      active < limit

private inductive CapabilityStep where
  | ready (enabled : Bool)
  | wait (waiter : AsyncTask Unit)
  | failed (error : Error)

private partial def awaitCapability (connection : Connection)
    (cancellation? : Option Std.CancellationToken) : Async (Except Error Bool) := do
  let step : CapabilityStep ← connection.state.atomically do
    let state ← get
    if ← openingCancelled cancellation? then
      pure (.failed (localCancellation "extended CONNECT capability wait was cancelled"))
    else if let some error := state.dead then pure (.failed error)
    else if state.protocol.receivedSettings then
      pure (.ready state.protocol.peerSettings.enableConnectProtocol)
    else pure (.wait (← connection.wakeup.wait))
  match step with
  | .ready enabled => pure (.ok enabled)
  | .failed error => pure (.error error)
  | .wait waiter =>
      if ← awaitWaiterOrCancel connection waiter cancellation? then
        pure (.error (localCancellation "extended CONNECT capability wait was cancelled"))
      else awaitCapability connection cancellation?

private inductive BeginStep where
  | opened (streamId : Nat)
  | wait (waiter : AsyncTask Unit)
  | failed (error : Error)

private partial def beginExtendedConnect (connection : Connection) (headers : Headers)
    (handle : IO.Ref TunnelHandleState)
    (cancellation? : Option Std.CancellationToken) : Async (Except Error Nat) := do
  let step : BeginStep ← connection.state.atomically do
    let state ← get
    if ← openingCancelled cancellation? then
      pure (.failed (localCancellation "extended CONNECT opening was cancelled"))
    else if let some error := state.dead then pure (.failed error)
    else if state.protocol.peerGoAwayLastStream?.isSome then
      pure (.failed (Error.connection .refusedStream
        "connection is shutting down after peer GOAWAY"))
    else if !state.protocol.receivedSettings then
      pure (.wait (← connection.wakeup.wait))
    else if !state.protocol.peerSettings.enableConnectProtocol then
      pure (.failed (Error.localInput
        "peer did not enable the extended CONNECT protocol" .connectError))
    else if !localStreamCapacityAvailable state.protocol then
      pure (.wait (← connection.wakeup.wait))
    else
      match Http2.Connection.openStream state.protocol headers with
      | .error error => pure (.failed error)
      | .ok (protocol, streamId, frames) =>
          let tunnel : TunnelRecord := { streamId, handle }
          set { state with protocol, tunnels := state.tunnels.push tunnel }
          enqueueFrames connection frames
          pure (.opened streamId)
  match step with
  | .opened streamId => pure (.ok streamId)
  | .failed error => pure (.error error)
  | .wait waiter =>
      if ← awaitWaiterOrCancel connection waiter cancellation? then
        pure (.error (localCancellation "extended CONNECT opening was cancelled"))
      else beginExtendedConnect connection headers handle cancellation?

private def abandonOpening (connection : Connection) (streamId : Nat) : IO Unit := do
  connection.state.atomically do
    let state ← get
    let (protocol, reset?) := match Http2.Connection.resetStream
        state.protocol streamId .cancel with
      | .ok result => result
      | .error _ => (state.protocol, none)
    set { state with protocol, tunnels := removeTunnel state.tunnels streamId }
    if let some reset := reset? then enqueueFrame connection reset
  wake connection

private inductive OpenStep where
  | accepted (response : ExtendedConnect.Response)
  | rejected (response : ExtendedConnect.Response)
  | wait (waiter : AsyncTask Unit)
  | cancelled
  | failed (error : Error)

private inductive SendStep where
  | wait (waiter : AsyncTask Unit)
  | sent (count : Nat) (ticket : OutboundTicket)
  | failed (error : Error)

private partial def sendChunks (connection : Connection) (streamId : Nat)
    (handle : IO.Ref TunnelHandleState) (bytes : ByteArray) (offset : Nat) :
    Async (Except Error Unit) := do
  if offset >= bytes.size then return .ok ()
  let step : SendStep ← connection.state.atomically do
    let state ← get
    if let some error := state.dead then return .failed error
    let some tunnel := findTunnel? state.tunnels streamId
      | match (← handle.get).terminal? with
        | some (.error error) => return .failed error
        | _ => return .failed (streamError streamId .streamClosed
            "extended CONNECT tunnel is no longer active")
    if let some error := tunnel.failure then return .failed error
    if tunnel.sendClosed then return .failed (streamError streamId .streamClosed
      "send after extended CONNECT closeSend")
    let some stream := state.protocol.streams.find? fun stream => stream.id == streamId
      | return .failed (streamError streamId .streamClosed
          "extended CONNECT protocol stream is no longer active")
    let available := min state.protocol.outboundConnectionWindow.toNat
      stream.outboundWindow.toNat
    if available == 0 then return .wait (← connection.wakeup.wait)
    let count := min (min available state.protocol.peerSettings.maxFrameSize)
      (bytes.size - offset)
    let chunk := bytes.extract offset (offset + count)
    match Http2.Connection.sendData state.protocol streamId chunk with
    | .error error => pure (.failed error)
    | .ok (protocol, frames) =>
        match ← enqueueFramesAcknowledged connection frames with
        | .error error => pure (.failed error)
        | .ok ticket =>
            set { state with protocol }
            pure (.sent count ticket)
  match step with
  | .wait waiter => awaitWaiter waiter; sendChunks connection streamId handle bytes offset
  | .failed error => pure (.error error)
  | .sent count ticket =>
      match ← awaitOutboundTicket ticket with
      | .error error => pure (.error error)
      | .ok () => sendChunks connection streamId handle bytes (offset + count)

private inductive ReceiveStep where
  | chunk (bytes : ByteArray)
  | done
  | wait (waiter : AsyncTask Unit)
  | failed (error : Error)

private partial def receiveChunk (connection : Connection) (streamId : Nat)
    (handle : IO.Ref TunnelHandleState) :
    Async (Except Error (Option ByteArray)) := do
  let step : ReceiveStep ← connection.state.atomically do
    let state ← get
    let some tunnel := findTunnel? state.tunnels streamId
      | let retained ← handle.get
        match retained.inbound[0]? with
        | some bytes =>
            match Http2.Connection.acknowledgeData state.protocol streamId bytes.size with
            | .error error => return .failed error
            | .ok (protocol, frames) =>
                handle.set { retained with
                  inbound := retained.inbound.extract 1 retained.inbound.size }
                set { state with protocol }
                enqueueFrames connection frames
                return .chunk bytes
        | none =>
            match retained.terminal? with
            | some (.ok ()) => return .done
            | some (.error error) => return .failed error
            | none => return .failed (state.dead.getD
                (streamError streamId .streamClosed
                  "extended CONNECT tunnel is no longer active"))
    match tunnel.inbound[0]? with
    | some bytes =>
        let tunnel := { tunnel with inbound := tunnel.inbound.extract 1 tunnel.inbound.size }
        match Http2.Connection.acknowledgeData state.protocol streamId bytes.size with
        | .error error => pure (.failed error)
        | .ok (protocol, frames) =>
            set { state with protocol, tunnels := replaceTunnel state.tunnels tunnel }
            enqueueFrames connection frames
            pure (.chunk bytes)
    | none =>
        if let some error := tunnel.failure then pure (.failed error)
        else if tunnel.recvClosed then pure .done
        else if let some error := state.dead then pure (.failed error)
        else pure (.wait (← connection.wakeup.wait))
  match step with
  | .chunk bytes => pure (.ok (some bytes))
  | .done => pure (.ok none)
  | .failed error => pure (.error error)
  | .wait waiter => awaitWaiter waiter; receiveChunk connection streamId handle

private inductive CloseSendStep where
  | done
  | sent (ticket : OutboundTicket)
  | failed (error : Error)

private def closeTunnelSend (connection : Connection) (streamId : Nat)
    (handle : IO.Ref TunnelHandleState) : Async (Except Error Unit) := do
  let step : CloseSendStep ← connection.state.atomically do
    let state ← get
    if let some error := state.dead then return .failed error
    let some tunnel := findTunnel? state.tunnels streamId
      | match (← handle.get).terminal? with
        | some (.ok ()) => return .done
        | some (.error error) => return .failed error
        | none => return .failed (streamError streamId .streamClosed
            "extended CONNECT tunnel is no longer active")
    if let some error := tunnel.failure then return .failed error
    if tunnel.sendClosed then return .done
    match Http2.Connection.sendData state.protocol streamId ByteArray.empty true with
    | .error error => pure (.failed error)
    | .ok (protocol, frames) =>
        match ← enqueueFramesAcknowledged connection frames with
        | .error error => pure (.failed error)
        | .ok ticket =>
            set { state with
              protocol
              tunnels := replaceTunnel state.tunnels { tunnel with sendClosed := true }
            }
            pure (.sent ticket)
  match step with
  | .done => pure (.ok ())
  | .failed error => pure (.error error)
  | .sent ticket =>
      let result ← awaitOutboundTicket ticket
      wake connection
      pure result

private def cancelTunnel (connection : Connection) (streamId : Nat) : Async Unit := do
  connection.state.atomically do
    let state ← get
    let some tunnel := findTunnel? state.tunnels streamId | return
    if tunnel.failure.isSome || (tunnel.sendClosed && tunnel.recvClosed) then return
    let error := streamError streamId .cancel "extended CONNECT tunnel cancelled locally"
    let (protocol, reset?) := match Http2.Connection.resetStream
        state.protocol streamId .cancel with
      | .ok result => result
      | .error _ => (state.protocol, none)
    set { state with
      protocol
      tunnels := replaceTunnel state.tunnels (failTunnelRecord error tunnel)
    }
    if let some reset := reset? then enqueueFrame connection reset
  wake connection

private inductive WaitStep where
  | done
  | wait (waiter : AsyncTask Unit)
  | failed (error : Error)

private partial def waitTunnel (connection : Connection) (streamId : Nat)
    (handle : IO.Ref TunnelHandleState) : Async (Except Error Unit) := do
  let step : WaitStep ← connection.state.atomically do
    let state ← get
    let some tunnel := findTunnel? state.tunnels streamId
      | match (← handle.get).terminal? with
        | some (.ok ()) => return .done
        | some (.error error) => return .failed error
        | none => return .failed (state.dead.getD (streamError streamId .streamClosed
            "extended CONNECT tunnel is no longer active"))
    if let some error := tunnel.failure then
      retireTunnelRecord tunnel (.error error)
      set { state with tunnels := removeTunnel state.tunnels streamId }
      pure (.failed error)
    else if tunnel.sendClosed && tunnel.recvClosed then
      retireTunnelRecord tunnel (.ok ())
      set { state with tunnels := removeTunnel state.tunnels streamId }
      pure .done
    else pure (.wait (← connection.wakeup.wait))
  match step with
  | .done => pure (.ok ())
  | .failed error => pure (.error error)
  | .wait waiter => awaitWaiter waiter; waitTunnel connection streamId handle

private def makeTunnel (connection : Connection) (streamId : Nat)
    (handle : IO.Ref TunnelHandleState) : ExtendedConnect.Tunnel := {
  sendBytesImpl := fun bytes => sendChunks connection streamId handle bytes 0
  recvBytesImpl := receiveChunk connection streamId handle
  closeSendImpl := closeTunnelSend connection streamId handle
  cancelImpl := cancelTunnel connection streamId
  waitImpl := waitTunnel connection streamId handle
}

private partial def awaitResponse (connection : Connection) (streamId : Nat)
    (handle : IO.Ref TunnelHandleState)
    (cancellation? : Option Std.CancellationToken) :
    Async (Except Error ExtendedConnect.OpenResult) := do
  let step : OpenStep ← connection.state.atomically do
    let state ← get
    if ← openingCancelled cancellation? then
      pure .cancelled
    else match findTunnel? state.tunnels streamId with
    | none => pure (.failed (state.dead.getD
        (streamError streamId .streamClosed "extended CONNECT stream is no longer active")))
    | some tunnel =>
        if let some error := tunnel.failure then
          set { state with tunnels := removeTunnel state.tunnels streamId }
          pure (.failed error)
        else
          match tunnel.response with
          | none => pure (.wait (← connection.wakeup.wait))
          | some response =>
              if ExtendedConnect.isSuccess response then pure (.accepted response)
              else
                let (protocol, reset?) := match Http2.Connection.resetStream
                    state.protocol streamId .cancel with
                  | .ok result => result
                  | .error _ => (state.protocol, none)
                set { state with protocol, tunnels := removeTunnel state.tunnels streamId }
                if let some reset := reset? then enqueueFrame connection reset
                pure (.rejected response)
  match step with
  | .accepted response =>
      pure (.ok (.accepted response (makeTunnel connection streamId handle)))
  | .rejected response => pure (.ok (.rejected response))
  | .cancelled =>
      abandonOpening connection streamId
      pure (.error (localCancellation "extended CONNECT opening was cancelled"))
  | .failed error => pure (.error error)
  | .wait waiter =>
      if ← awaitWaiterOrCancel connection waiter cancellation? then
        abandonOpening connection streamId
        pure (.error (localCancellation "extended CONNECT opening was cancelled"))
      else awaitResponse connection streamId handle cancellation?

namespace Connection

def peerExtendedConnectEnabled (connection : Client.Connection)
    (cancellation? : Option Std.CancellationToken := none) : Async (Except Error Bool) :=
  awaitCapability connection cancellation?

def openExtendedConnect (connection : Client.Connection)
    (request : ExtendedConnect.Request)
    (cancellation? : Option Std.CancellationToken := none) :
    Async (Except Error ExtendedConnect.OpenResult) := do
  match ExtendedConnect.encodeRequest request with
  | .error error => pure (.error error)
  | .ok headers =>
      let handle ← IO.mkRef ({} : TunnelHandleState)
      match ← beginExtendedConnect connection headers handle cancellation? with
      | .error error => pure (.error error)
      | .ok streamId => awaitResponse connection streamId handle cancellation?

end Connection

namespace TestSupport

def activeTunnelCount (connection : Connection) : IO Nat :=
  connection.state.atomically do pure (← get).tunnels.size

end TestSupport

end Http2.Client
