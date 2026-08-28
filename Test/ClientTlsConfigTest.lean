import Http2.Client

open Std.Async

private def fail (message : String) : IO α :=
  throw (IO.userError message)

private def expectFailureContains (operation : Async α) (fragment : String) : IO Unit := do
  let result : Except IO.Error Unit ← try
      discard <| Async.block operation
      pure (Except.ok ())
    catch error => pure (Except.error error)
  match result with
  | Except.ok () => fail s!"operation unexpectedly succeeded; expected {fragment}"
  | Except.error error =>
      unless (toString error).contains fragment do
        fail s!"failure did not contain {fragment}: {error}"

private def testSecureDefaultsFailClosed : IO Unit := do
  expectFailureContains
    (Http2.Client.bootstrapTlsAsync (tlsConfig := { serverName := some "localhost" }))
    "trust anchors are required"
  expectFailureContains
    (Http2.Client.bootstrapTlsAsync (tlsConfig := {
      trustAnchorsPEM := some "not a certificate"
    }))
    "hostname verification requires"
  expectFailureContains
    (Http2.Client.bootstrapTlsAsync (tlsConfig := {
      serverName := some "localhost"
      trustAnchorsPEM := some "not a certificate"
    }))
    "TLS trust anchors"

private def testExplicitInsecureMode : IO Unit := do
  let cancellation ← Std.CancellationToken.new
  discard <| Http2.CancellationToken.cancel cancellation
  expectFailureContains
    (Http2.Client.bootstrapTlsAsync (tlsConfig := {
      trustAnchorsPEM := some "deliberately ignored"
      insecureSkipVerification := true
    }) (cancellation? := some cancellation))
    "cancelled"

def main : IO Unit := do
  testSecureDefaultsFailClosed
  testExplicitInsecureMode
  IO.println "HTTP/2 client TLS configuration tests passed"
