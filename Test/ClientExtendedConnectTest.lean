import Http2

open Std.Async

namespace Http2.ClientExtendedConnectTest

def expect (condition : Bool) (message : String) : IO Unit := do
  unless condition do throw (IO.userError message)

def expectEq [BEq α] (actual expected : α) (message : String) : IO Unit :=
  expect (actual == expected) message

def requireOk (result : Except Error α) : IO α :=
  match result with
  | .ok value => pure value
  | .error error => throw (IO.userError error.message)

def expectError (result : Except Error α) (message : String) : IO Unit :=
  match result with
  | .error _ => pure ()
  | .ok _ => throw (IO.userError message)

def expectErrorCode (result : Except Error α) (code : ErrorCode)
    (message : String) : IO Unit :=
  match result with
  | .error error => expectEq error.code code message
  | .ok _ => throw (IO.userError message)

def portOf (address : Std.Net.SocketAddress) : UInt16 :=
  match address with
  | .v4 address => address.port
  | .v6 address => address.port

partial def echo (tunnel : ExtendedConnect.Tunnel) : Async Unit := do
  match ← tunnel.recv? with
  | .error _ => pure ()
  | .ok none => discard <| tunnel.closeSend
  | .ok (some bytes) =>
      match ← tunnel.send bytes with
      | .error _ => pure ()
      | .ok () => echo tunnel

partial def drain (tunnel : ExtendedConnect.Tunnel) : Async Unit := do
  match ← tunnel.recv? with
  | .error _ | .ok none => pure ()
  | .ok (some _) => drain tunnel

partial def receiveExactly (tunnel : ExtendedConnect.Tunnel) (size : Nat)
    (accumulator : ByteArray := ByteArray.empty) : Async (Except Error ByteArray) := do
  if accumulator.size >= size then
    pure (.ok accumulator)
  else
    match ← tunnel.recv? with
    | .error error => pure (.error error)
    | .ok none => pure (.error (Error.connection .internalError
        "extended CONNECT tunnel ended before the expected bytes arrived"))
    | .ok (some bytes) => receiveExactly tunnel size (accumulator.append bytes)

def repeatByte (count : Nat) (value : UInt8) : ByteArray :=
  ByteArray.mk (Array.replicate count value)

def awaitPromiseWithin (promise : IO.Promise Unit) (milliseconds : Nat) : IO Bool :=
  Async.block <| Async.race
    (do pure (← Async.ofTask promise.result?).isSome)
    (do
      Std.Async.sleep (Std.Time.Millisecond.Offset.ofNat milliseconds)
      pure false)

partial def awaitNoManagedTunnels (server : Server.Server) (remainingMs : Nat := 2000) :
    IO Bool := do
  if (← Server.TestSupport.managedTunnelCount server) == 0 then
    pure true
  else if remainingMs == 0 then
    pure false
  else
    IO.sleep 1
    awaitNoManagedTunnels server (remainingMs - 1)

def request (path : String := "/echo") : ExtendedConnect.Request := {
  protocol := "websocket"
  scheme := "http"
  authority := "localhost"
  path
  headers := Headers.singleton "origin" "https://example.test"
}

def handler : ExtendedConnect.Handler := fun incoming => do
  if incoming.path == "/reject" then
    pure (.reject {
      status := 403
      headers := Headers.singleton "x-rejected" "true"
    })
  else if incoming.path == "/half-close" then
    pure (.accept {
      run := fun tunnel => do
        discard <| tunnel.closeSend
        drain tunnel
    })
  else if incoming.path == "/reset-after-data" then
    pure (.accept {
      run := fun tunnel => do
        discard <| tunnel.send "before reset".toUTF8
        tunnel.cancel
    })
  else
    pure (.accept {
      headers := Headers.singleton "x-tunnel" "ready"
      run := echo
    })

def openTunnel (connection : Client.Connection) (path : String := "/echo") :
    IO (ExtendedConnect.Response × ExtendedConnect.Tunnel) := do
  match ← Async.block (connection.openExtendedConnect (request path)) with
  | .error error => throw (IO.userError error.message)
  | .ok (.rejected response) =>
      throw (IO.userError s!"extended CONNECT was rejected with {response.status}")
  | .ok (.accepted response tunnel) => pure (response, tunnel)

def testCapabilityDisabled : IO Unit := do
  let server ← Server.serveApplications {} { address := Server.loopback 0 }
  try
    let connection ← Client.connect {
      address := Server.loopback (portOf server.localAddress)
      authority := "localhost"
    }
    try
      expectEq (← requireOk (← Async.block connection.peerExtendedConnectEnabled)) false
        "client reported extended CONNECT without the server setting"
      expectError (← Async.block (connection.openExtendedConnect request))
        "client opened extended CONNECT without peer capability"
    finally
      Async.block (Client.close connection)
      expect (← Client.backgroundTasksFinished connection)
        "client background tasks remained after close"
  finally
    Server.shutdown server
    Server.wait server (some 3000)

def testRuntime : IO Unit := do
  let openingEntered ← IO.Promise.new
  let openingRelease ← IO.Promise.new
  let openingExited ← IO.Promise.new
  let runtimeHandler : ExtendedConnect.Handler := fun incoming => do
    if incoming.path == "/pending" then
      openingEntered.resolve ()
      try
        discard <| Async.ofTask openingRelease.result?
        handler incoming
      finally
        openingExited.resolve ()
    else
      handler incoming
  let server ← Server.serveExtendedConnect runtimeHandler {
    address := Server.loopback 0
    maxConcurrentStreams := some 8
  }
  try
    let connection ← Client.connect {
      address := Server.loopback (portOf server.localAddress)
      authority := "localhost"
    }
    try
      expectEq (← requireOk (← Async.block connection.peerExtendedConnectEnabled)) true
        "client did not observe SETTINGS_ENABLE_CONNECT_PROTOCOL"

      let cancellation ← Std.CancellationToken.new
      let pending ← Async.toIO <|
        connection.openExtendedConnect (request "/pending") (some cancellation)
      expect (← awaitPromiseWithin openingEntered 2000)
        "pending extended CONNECT handler did not start"
      discard <| Http2.CancellationToken.cancel cancellation
      expectError (← Async.block (Async.ofAsyncTask pending))
        "cancelled extended CONNECT opening succeeded"
      expectEq (← Client.TestSupport.activeTunnelCount connection) 0
        "cancelled opening retained an untransferred tunnel"
      openingRelease.resolve ()
      expect (← awaitPromiseWithin openingExited 2000)
        "cancelled extended CONNECT handler did not retire"

      let (response, tunnel) ← openTunnel connection
      expectEq response.status 200 "accepted tunnel response status differed"
      expectEq (response.headers.get? "x-tunnel") (some "ready")
        "accepted tunnel response headers differed"

      let payload := repeatByte 70000 0x5a
      let receiveTask ← Async.toIO (receiveExactly tunnel payload.size)
      discard <| requireOk (← Async.block (tunnel.send payload))
      expectEq (← requireOk (← Async.block (Async.ofAsyncTask receiveTask))) payload
        "tunnel payload changed across HTTP/2 frames or flow-control windows"
      discard <| requireOk (← Async.block tunnel.closeSend)
      expectEq (← requireOk (← Async.block tunnel.recv?)) none
        "peer END_STREAM was not surfaced as end-of-input"
      discard <| requireOk (← Async.block tunnel.wait)

      let (_, halfClosed) ← openTunnel connection "/half-close"
      expectEq (← requireOk (← Async.block halfClosed.recv?)) none
        "server half-close was not surfaced as end-of-input"
      let terminalWait ← Async.toIO halfClosed.wait
      discard <| requireOk (← Async.block halfClosed.closeSend)
      discard <| requireOk (← Async.block (Async.ofAsyncTask terminalWait))

      let (_, resetAfterData) ← openTunnel connection "/reset-after-data"
      let resetWait ← Async.toIO resetAfterData.wait
      expectEq (← requireOk (← Async.block resetAfterData.recv?))
        (some "before reset".toUTF8)
        "concurrent tunnel wait discarded DATA preceding RST_STREAM"
      expectErrorCode (← Async.block resetAfterData.recv?) .cancel
        "tunnel receive lost the peer RST_STREAM code"
      expectErrorCode (← Async.block (Async.ofAsyncTask resetWait)) .cancel
        "tunnel wait lost the peer RST_STREAM code"

      match ← Async.block (connection.openExtendedConnect (request "/reject")) with
      | .ok (.rejected rejection) =>
          expectEq rejection.status 403 "rejection response status differed"
          expectEq (rejection.headers.get? "x-rejected") (some "true")
            "rejection response headers differed"
      | .ok (.accepted _ tunnel) =>
          Async.block tunnel.cancel
          throw (IO.userError "rejected extended CONNECT request was accepted")
      | .error error => throw (IO.userError error.message)

      let (_, cancelled) ← openTunnel connection
      Async.block cancelled.cancel
      expectError (← Async.block cancelled.wait)
        "cancelled tunnel completed successfully"
      expectEq (← Client.TestSupport.activeTunnelCount connection) 0
        "terminal tunnels remained owned by the client"
      expect (← awaitNoManagedTunnels server)
        "terminal tunnels remained owned by the server connection"
    finally
      Async.block (Client.close connection)
      expect (← Client.backgroundTasksFinished connection)
        "client background tasks remained after close"
  finally
    Server.shutdown server
    Server.wait server (some 3000)

def run : IO Unit := do
  testCapabilityDisabled
  testRuntime
  IO.println "HTTP/2 client extended CONNECT tests passed"

end Http2.ClientExtendedConnectTest

def main : IO Unit :=
  Http2.ClientExtendedConnectTest.run
