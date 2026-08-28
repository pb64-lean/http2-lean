import Http2.Connection

open Http2

def expect (condition : Bool) (message : String) : IO Unit := do
  unless condition do throw (IO.userError message)

def requireOk (result : Except Error α) : IO α :=
  match result with
  | .ok value => pure value
  | .error error => throw (IO.userError error.message)

def streamWindow (state : Connection.State) (streamId : Nat) : Int :=
  (state.streams.find? fun stream => stream.id == streamId).map
    (fun stream => stream.inboundWindow) |>.getD 0

def updateIncrement (frame : Frame) : IO Nat :=
  requireOk (WindowUpdate.decode frame)

def headerFrame (streamId : Nat) (block : ByteArray) : Frame := {
  header := {
    length := block.size
    frameType := .headers
    flags := FrameFlag.endHeaders
    streamId
  }
  payload := block
}

def testPaddingCredit : IO Unit := do
  let stream : Connection.Stream := {
    id := 1
    phase := .open
    inboundWindow := 32
    outboundWindow := 32
    receivedHeaders := true
    sentHeaders := true
  }
  let state : Connection.State := {
    Connection.initial .client { initialWindowSize := 32 } with
    receivedSettings := true
    inboundConnectionWindow := 32
    streams := #[stream]
  }
  let padded : Frame := {
    header := {
      length := 5
      frameType := .data
      flags := FrameFlag.padded
      streamId := 1
    }
    payload := ByteArray.mk #[2, 65, 66, 0, 0]
  }
  let (state, automatic, events) ← requireOk (Connection.processFrame state padded)
  expect (state.inboundConnectionWindow == 32)
    "connection credit was not restored immediately"
  expect (streamWindow state 1 == 30)
    "padding remained charged to the stream window"
  expect (automatic.size == 2 && (← updateIncrement automatic[0]!) == 5 &&
      (← updateIncrement automatic[1]!) == 3)
    "automatic connection or padding credit differed"
  match events[0]? with
  | some (Http2.Connection.Event.data 1 bytes false) =>
      expect (bytes == ByteArray.mk #[65, 66]) "padded DATA payload was not normalized"
  | _ => throw (IO.userError "padded DATA did not emit its payload event")

  let (state, credit) ← requireOk (Connection.acknowledgeData state 1 2)
  expect (streamWindow state 1 == 32 && credit.size == 1 &&
      (← updateIncrement credit[0]!) == 2)
    "application consumption did not restore exact stream credit"

  let paddingOnly : Frame := {
    header := {
      length := 2
      frameType := .data
      flags := FrameFlag.padded
      streamId := 1
    }
    payload := ByteArray.mk #[1, 0]
  }
  let (state, automatic, _) ← requireOk (Connection.processFrame state paddingOnly)
  expect (streamWindow state 1 == 32 && automatic.size == 2)
    "padding-only DATA leaked flow-control credit"

def testSettingsRules : IO Unit := do
  let ping ← requireOk (Ping.frame (ByteArray.mk #[0, 1, 2, 3, 4, 5, 6, 7]))
  match Connection.processFrame (Connection.initial .client) ping with
  | .error error => do
      expect (error.code == .protocolError) "wrong error for a non-SETTINGS first frame"
  | .ok _ => throw (IO.userError "a non-SETTINGS first peer frame was accepted")

  let enabled : Connection.Settings := { enableConnectProtocol := true }
  match enabled.apply { id := SettingId.enableConnectProtocol, value := 0 } with
  | .error error => do
      expect (error.code == .protocolError)
        "wrong error for extended CONNECT setting retraction"
  | .ok _ => throw (IO.userError "extended CONNECT capability was retracted")

def testResetHeaderCompressionSynchronization : IO Unit := do
  let headers := Headers.empty
    |>.insert ":status" "200"
    |>.insert "x-dynamic" "connection-scoped"
  let (firstBlock, encoder) ← requireOk (Hpack.encodeHeaderBlock {} headers)
  let (secondBlock, _) ← requireOk (Hpack.encodeHeaderBlock encoder headers)
  let resetStream : Connection.Stream := {
    id := 1
    phase := .closed
    locallyReset := true
    sentHeaders := true
  }
  let activeStream : Connection.Stream := {
    id := 3
    phase := .open
    sentHeaders := true
  }
  let state : Connection.State := {
    Connection.initial .client with
    receivedSettings := true
    streams := #[resetStream, activeStream]
    nextLocalStreamId := 5
  }
  let split := max 1 (firstBlock.size / 2)
  let first : Frame := {
    header := {
      length := split
      frameType := .headers
      streamId := 1
    }
    payload := firstBlock.extract 0 split
  }
  let continuation : Frame := {
    header := {
      length := firstBlock.size - split
      frameType := .continuation
      flags := FrameFlag.endHeaders
      streamId := 1
    }
    payload := firstBlock.extract split firstBlock.size
  }
  let (state, _, firstEvents) ← requireOk (Connection.processFrame state first)
  let (state, _, continuationEvents) ← requireOk
    (Connection.processFrame state continuation)
  expect (firstEvents.isEmpty && continuationEvents.isEmpty)
    "fragmented HEADERS racing a local reset reached the application"
  let (_, _, events) ← requireOk
    (Connection.processFrame state (headerFrame 3 secondBlock))
  match events[0]? with
  | some (Http2.Connection.Event.headers 3 decoded false false) =>
      expect (decoded == headers)
        "discarded HEADERS did not preserve connection-wide HPACK state"
  | _ => throw (IO.userError "the header block following a reset was not decoded")

  let data : Frame := {
    header := {
      length := 3
      frameType := .data
      streamId := 1
    }
    payload := ByteArray.mk #[1, 2, 3]
  }
  let (_, automatic, events) ← requireOk (Connection.processFrame state data)
  expect (events.isEmpty && automatic.size == 1 &&
      (← updateIncrement automatic[0]!) == 3)
    "DATA racing a local reset did not preserve connection flow credit"

def main : IO Unit := do
  testPaddingCredit
  testSettingsRules
  testResetHeaderCompressionSynchronization
  IO.println "connection flow-control tests passed"
