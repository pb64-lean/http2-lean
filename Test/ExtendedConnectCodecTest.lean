import Http2.ExtendedConnect

open Http2

def expect (condition : Bool) (message : String) : IO Unit := do
  unless condition do throw (IO.userError message)

def requireOk (result : Except Error α) : IO α :=
  match result with
  | .ok value => pure value
  | .error error => throw (IO.userError error.message)

def expectError (result : Except Error α) (message : String) : IO Unit :=
  match result with
  | .error _ => pure ()
  | .ok _ => throw (IO.userError message)

def validRequest : ExtendedConnect.Request := {
  protocol := "websocket"
  scheme := "https"
  authority := "example.test"
  path := "/chat?room=blue"
  headers := Headers.singleton "origin" "https://origin.test"
}

def testRequestRoundTrip : IO Unit := do
  expect (ExtendedConnect.validProtocolToken "websocket")
    "an ordinary protocol token was rejected"
  expect (ExtendedConnect.validProtocolToken "a!#$%&'*+-.^_`|~9")
    "valid token punctuation was rejected"
  expect (!ExtendedConnect.validProtocolToken "") "an empty protocol token was accepted"
  expect (!ExtendedConnect.validProtocolToken "web socket")
    "a protocol token containing SP was accepted"
  expect (!ExtendedConnect.validProtocolToken "wébsocket")
    "a non-ASCII protocol token was accepted"

  let encoded ← requireOk (ExtendedConnect.encodeRequest validRequest)
  expect (encoded.map (fun header => header.name) ==
      #[":method", ":protocol", ":scheme", ":authority", ":path", "origin"])
    "request fields were not emitted in canonical pseudo-header order"
  let decoded ← requireOk (ExtendedConnect.decodeRequest encoded)
  expect (decoded == validRequest) "extended CONNECT request did not round-trip"

  let generic : ExtendedConnect.Request := {
    validRequest with
    headers := Headers.empty
      |>.insert "x!trace" "left\tright"
      |>.insert "x-bin" "literal-not-base64"
      |>.insert "te" "Trailers"
  }
  let decoded ← requireOk (ExtendedConnect.decodeRequest
    (← requireOk (ExtendedConnect.encodeRequest generic)))
  expect (decoded == generic)
    "generic HTTP fields were treated as protocol-specific metadata"

def testRequestFailures : IO Unit := do
  let encoded ← requireOk (ExtendedConnect.encodeRequest validRequest)
  expectError (ExtendedConnect.decodeRequest (encoded.insert ":protocol" "duplicate"))
    "a duplicate :protocol was accepted"
  expectError (ExtendedConnect.decodeRequest
      ((encoded.filter (fun header => header.name != ":path"))))
    "a missing :path was accepted"
  expectError (ExtendedConnect.decodeRequest
      (encoded.push { name := ":unknown", value := "value" }))
    "an unknown request pseudo-header was accepted"
  expectError (ExtendedConnect.decodeRequest
      (encoded.push { name := "X-Upper", value := "bad" }))
    "an uppercase ordinary field name was accepted"
  expectError (ExtendedConnect.decodeRequest
      ((Headers.singleton "ordinary" "first").append encoded))
    "a pseudo-header after an ordinary field was accepted"
  expectError (ExtendedConnect.decodeRequest
      (encoded.insert "connection" "keep-alive"))
    "a connection-specific field was accepted"
  expectError (ExtendedConnect.decodeRequest (encoded.insert "te" "gzip"))
    "a TE value other than trailers was accepted"
  expectError (ExtendedConnect.decodeRequest (encoded.insert "x-value" "line\rbreak"))
    "a field value containing CR was accepted"

  expectError (ExtendedConnect.encodeRequest {
      validRequest with protocol := "web socket"
    }) "an invalid outbound :protocol was accepted"
  expectError (ExtendedConnect.encodeRequest {
      validRequest with authority := "example.test\ninvalid"
    }) "an invalid outbound :authority value was accepted"
  expectError (ExtendedConnect.encodeRequest {
      validRequest with path := " /leading-space"
    }) "leading whitespace in outbound :path was accepted"
  expectError (ExtendedConnect.encodeRequest {
      validRequest with headers := #[{ name := ":status", value := "200" }]
    }) "an application-supplied pseudo-header was accepted"

def testResponses : IO Unit := do
  let response : ExtendedConnect.Response := {
    status := 204
    headers := Headers.singleton "x-selected" "yes"
  }
  let encoded ← requireOk (ExtendedConnect.encodeResponse response)
  let decoded ← requireOk (ExtendedConnect.decodeResponse encoded)
  expect (decoded == response) "extended CONNECT response did not round-trip"
  expect (ExtendedConnect.isSuccess response) "status 204 was not successful"
  expect (!ExtendedConnect.isSuccess { status := 300 }) "status 300 was successful"

  let informational ← requireOk
    (ExtendedConnect.decodeResponse (Headers.singleton ":status" "103"))
  expect (informational.status == 103) "an informational response was not preserved"
  expectError (ExtendedConnect.encodeResponse { status := 199 })
    "an informational response was encoded as final"
  expectError (ExtendedConnect.encodeResponse { status := 600 })
    "status 600 was encoded"
  expectError (ExtendedConnect.decodeResponse (Headers.singleton ":status" "101"))
    "HTTP/2 status 101 was accepted"
  expectError (ExtendedConnect.decodeResponse (Headers.singleton ":status" "099"))
    "status 099 was accepted"
  expectError (ExtendedConnect.decodeResponse (Headers.singleton ":status" "600"))
    "status 600 was accepted"
  expectError (ExtendedConnect.decodeResponse
      ((Headers.singleton ":status" "200").insert ":status" "204"))
    "duplicate :status was accepted"
  expectError (ExtendedConnect.decodeResponse
      ((Headers.singleton "x-first" "yes").insert ":status" "200"))
    "a response pseudo-header after an ordinary field was accepted"
  expectError (ExtendedConnect.decodeResponse
      ((Headers.singleton ":status" "200").insert ":method" "CONNECT"))
    "a request pseudo-header was accepted in a response"

def main : IO Unit := do
  testRequestRoundTrip
  testRequestFailures
  testResponses
  IO.println "extended CONNECT codec tests passed"
