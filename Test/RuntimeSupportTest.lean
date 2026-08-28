import Http2.Runtime

private def fail (message : String) : IO α :=
  throw <| IO.userError message

private def testCancellation : IO Unit := do
  let token ← Std.CancellationToken.new
  let first ← Http2.CancellationToken.cancel token
  let second ← Http2.CancellationToken.cancel token
  unless first && !second do
    fail "cancellation did not elect exactly one caller"

private def testResolution : IO Unit := do
  let result ← Http2.NameResolver.resolveHostWith
    (fun _ _ => pure (.ok #["127.0.0.1", "127.0.0.1", "::1"]))
    "example.invalid" 443
  let addresses ← match result with
    | .ok addresses => pure addresses
    | .error error => fail s!"address refinement failed: {error}"
  unless addresses.size == 2 do
    fail "canonical duplicate addresses were not removed"
  unless addresses[0]!.numericHost == "127.0.0.1" &&
      addresses[1]!.numericHost == "::1" do
    fail "address order or canonical form changed"

  let literal := Http2.NameResolver.Address.ofIP
    (.v4 (Std.Net.IPv4Addr.ofParts 192 0 2 1)) 8443
  unless literal.numericHost == "192.0.2.1" &&
      literal.authority == "192.0.2.1:8443" &&
      literal.family == .ipv4 do
    fail "a parsed literal IP did not retain its canonical endpoint"

private def testTrustBoundary : IO Unit := do
  match Http2.TrustAnchors.validateBundle ByteArray.empty with
  | .error .empty => pure ()
  | _ => fail "an empty trust bundle was accepted"
  if (Http2.Posix.strerror Http2.Posix.Errno.invalidArgument).isEmpty then
    fail "the POSIX error adapter returned an empty message"

def main : IO Unit := do
  testCancellation
  testResolution
  testTrustBoundary
