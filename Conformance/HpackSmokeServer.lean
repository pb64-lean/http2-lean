import Http2

namespace Http2.Conformance.HpackSmokeServer

private def portFromArgs : List String → IO UInt16
  | [] => pure 9002
  | value :: _ =>
      match value.toNat? with
      | some port =>
          if port == 0 || port > 65535 then
            throw (IO.userError "port must be between 1 and 65535")
          else
            pure (UInt16.ofNat port)
      | none => throw (IO.userError s!"invalid port {value.quote}")

end Http2.Conformance.HpackSmokeServer

def main (args : List String) : IO Unit := do
  let port ← Http2.Conformance.HpackSmokeServer.portFromArgs args
  let server ← Http2.Server.serveApplications {} {
    address := Http2.Server.loopback port
  }
  IO.println s!"HPACK smoke server listening on 127.0.0.1:{port}"
  (← IO.getStdout).flush
  Http2.Server.wait server none
