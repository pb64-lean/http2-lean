import Http2

open Std.Async

namespace Http2.ServerLifecycleTest

def expect (condition : Bool) (message : String) : IO Unit := do
  unless condition do throw (IO.userError message)

def expectEq [BEq alpha] (actual expected : alpha) (message : String) : IO Unit :=
  expect (actual == expected) message

def requireOk (result : Except Error alpha) : IO alpha :=
  match result with
  | .ok value => pure value
  | .error error => throw (IO.userError error.message)

def connect (address : Std.Net.SocketAddress) : IO Std.Async.TCP.Socket.Client := do
  let client <- Std.Async.TCP.Socket.Client.mk
  Async.block (client.connect address)
  pure client

partial def receiveFrame (client : Std.Async.TCP.Socket.Client)
    (decoder : Frame.DecodeState := {}) : IO Frame := do
  let some bytes <- Async.block (client.recv? 16384)
    | throw (IO.userError "server closed before sending an HTTP/2 frame")
  let decoded <- requireOk (Frame.decodeChunk decoder bytes)
  match decoded.frames[0]? with
  | some frame => pure frame
  | none => receiveFrame client { decoded with frames := #[] }

partial def receiveFrameType (client : Std.Async.TCP.Socket.Client) (frameType : FrameType)
    (decoder : Frame.DecodeState := {}) : IO Frame := do
  let some bytes <- Async.block (client.recv? 16384)
    | throw (IO.userError "server closed before sending the expected HTTP/2 frame")
  let decoded <- requireOk (Frame.decodeChunk decoder bytes)
  match decoded.frames.find? (·.header.frameType == frameType) with
  | some frame => pure frame
  | none => receiveFrameType client frameType { decoded with frames := #[] }

def clientPrefaceAndSettings : IO ByteArray := do
  let settings <- requireOk (Http2.Settings.frame #[])
  pure (connectionPreface.append (← requireOk (Frame.encode settings)))

partial def waitForClosedRecord (server : Server.Server) (remainingMs : Nat := 3000) :
    IO Server.ClosedConnection := do
  if let some record := (← Server.closedConnectionRecords server).back? then
    pure record
  else if remainingMs == 0 then
    throw (IO.userError "server did not publish a closed-connection record")
  else
    IO.sleep 1
    waitForClosedRecord server (remainingMs - 1)

def retireClient (client : Std.Async.TCP.Socket.Client) : IO Unit := do
  try Async.block client.shutdown catch _ => pure ()

def testSettingsAndInvalidPreface : IO Unit := do
  let handler : ExtendedConnect.Handler := fun _ => pure (.reject { status := 403 })
  let server <- Server.serveExtendedConnect handler {
    address := Server.loopback 0
    maxConcurrentStreams := some 7
    maxHeaderListSize := some 4096
  }
  let client <- connect server.localAddress
  try
    let first <- receiveFrame client
    expectEq first.header.frameType FrameType.settings
      "server preface did not begin with SETTINGS"
    let values <- requireOk (Http2.Settings.decode first)
    expect (values.contains { id := SettingId.enableConnectProtocol, value := 1 })
      "server did not advertise SETTINGS_ENABLE_CONNECT_PROTOCOL"
    expect (values.contains { id := .maxConcurrentStreams, value := 7 })
      "server did not advertise its concurrent-stream limit"
    expect (values.contains { id := .maxHeaderListSize, value := 4096 })
      "server did not advertise its header-list limit"

    Async.block (client.send "X".toUTF8)
    let record <- waitForClosedRecord server
    match record.cause with
    | Server.CloseCause.protocolError error =>
        expectEq error.code ErrorCode.protocolError
          "invalid preface used the wrong HTTP/2 error code"
    | _ => throw (IO.userError "invalid preface was not recorded as a protocol error")
  finally
    retireClient client
    Server.shutdown server
    Server.wait server (some 3000)
  expect (← Server.isClosed server) "server retained work after invalid-preface shutdown"

def testDefaultResourceSettings : IO Unit := do
  let defaults : Server.Config := {}
  expectEq defaults.maxConcurrentStreams (some 100)
    "managed server default omitted its concurrent-stream bound"
  expectEq defaults.maxHeaderListSize (some 65536)
    "managed server default omitted its decoded-header bound"
  let server <- Server.serveApplications {} { address := Server.loopback 0 }
  let client <- connect server.localAddress
  try
    let first <- receiveFrame client
    let values <- requireOk (Http2.Settings.decode first)
    expect (values.contains { id := .maxConcurrentStreams, value := 100 })
      "server did not advertise its default concurrent-stream bound"
    expect (values.contains { id := .maxHeaderListSize, value := 65536 })
      "server did not advertise its default decoded-header bound"
  finally
    retireClient client
    Server.shutdown server
    Server.wait server (some 3000)

def testOversizedHeaderRejectedBeforePayload : IO Unit := do
  let server <- Server.serveApplications {} { address := Server.loopback 0 }
  let client <- connect server.localAddress
  try
    let first <- receiveFrame client
    expectEq first.header.frameType FrameType.settings
      "server preface did not begin with SETTINGS"
    let settings <- requireOk (Http2.Settings.frame #[])
    let settingsWire <- requireOk (Frame.encode settings)
    let oversized <- requireOk <| Frame.encodeHeader {
      length := defaultMaxFramePayloadLength + 1
      frameType := .data
      streamId := 1
    }
    Async.block (client.send <|
      connectionPreface.append settingsWire |>.append oversized)
    let record <- waitForClosedRecord server
    match record.cause with
    | Server.CloseCause.protocolError error =>
        expectEq error.code ErrorCode.frameSizeError
          "header-only oversized frame used the wrong managed-server error"
    | _ => throw (IO.userError
        "header-only oversized frame did not terminate the managed connection")
  finally
    retireClient client
    Server.shutdown server
    Server.wait server (some 3000)

def testMalformedRequestIsStreamScoped : IO Unit := do
  let server <- Server.serveApplications {} { address := Server.loopback 0 }
  let client <- connect server.localAddress
  try
    let first <- receiveFrame client
    expectEq first.header.frameType FrameType.settings
      "server preface did not begin with SETTINGS"
    let settings <- requireOk (Http2.Settings.frame #[])
    let invalid : Headers := #[
      { name := ":method", value := "GET" },
      { name := ":scheme", value := "http" },
      { name := ":path", value := "/" },
      { name := "X-Uppercase", value := "invalid" }
    ]
    let (block, _) <- requireOk (Hpack.encodeHeaderBlock {} invalid)
    let headers : Frame := {
      header := {
        length := block.size
        frameType := .headers
        flags := FrameFlag.combine #[FrameFlag.endStream, FrameFlag.endHeaders]
        streamId := 1
      }
      payload := block
    }
    let requestFrames <- requireOk (Frame.encodeBatch #[settings, headers])
    Async.block (client.send (connectionPreface.append requestFrames))
    let reset <- receiveFrameType client .rstStream
    expectEq reset.header.streamId 1 "malformed request reset the wrong stream"
    expectEq (← requireOk (RstStream.decode reset)) ErrorCode.protocolError
      "malformed request used the wrong stream error"
    expect (← Server.checkAccepting server)
      "stream-scoped request error stopped the accept owner"
  finally
    retireClient client
    Server.shutdown server
    Server.wait server (some 3000)
  expect (← Server.isClosed server) "server retained work after stream-error shutdown"

def testKeepaliveTimeout : IO Unit := do
  let server <- Server.serveApplications {} {
    address := Server.loopback 0
    keepaliveIntervalMs := some 10
    keepaliveTimeoutMs := 10
  }
  let client <- connect server.localAddress
  try
    let first <- receiveFrame client
    expectEq first.header.frameType FrameType.settings
      "keepalive server preface did not begin with SETTINGS"
    Async.block (client.send (← clientPrefaceAndSettings))
    let record <- waitForClosedRecord server
    match record.cause with
    | Server.CloseCause.keepaliveTimeout => pure ()
    | _ => throw (IO.userError "unacknowledged PING did not cause a keepalive timeout")
  finally
    retireClient client
    Server.shutdown server
    Server.wait server (some 3000)
  expect (← Server.isClosed server) "keepalive server retained work after shutdown"

def testShutdownStatus : IO Unit := do
  let server <- Server.serveApplications {} { address := Server.loopback 0 }
  expect (← Server.checkAccepting server) "new server was not accepting"
  expect (!(← Server.isShutdown server)) "new server was already shut down"
  expect (!(← Server.isClosed server)) "live server reported closed"
  Server.shutdown server
  Server.wait server (some 3000)
  expect (← Server.isShutdown server) "shutdown status was not sticky"
  expect (← Server.isClosed server) "server was not closed after wait"
  expect (!(← Server.checkAccepting server)) "closed server still reported accepting"
  expectEq (← Server.activeConnectionCount server) 0
    "closed server retained an active connection"

def run : IO Unit := do
  testSettingsAndInvalidPreface
  testDefaultResourceSettings
  testOversizedHeaderRejectedBeforePayload
  testMalformedRequestIsStreamScoped
  testKeepaliveTimeout
  testShutdownStatus
  IO.println "HTTP/2 server lifecycle tests passed"

end Http2.ServerLifecycleTest

def main : IO Unit :=
  Http2.ServerLifecycleTest.run
