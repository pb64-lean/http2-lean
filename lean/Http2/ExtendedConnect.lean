module

public import Std.Async
public import Http2.Header

public section

namespace Http2.ExtendedConnect

open Std.Async

/-- A validated extended CONNECT request. `headers` contains only ordinary
HTTP fields; the five request pseudo-fields are projected into named fields. -/
structure Request where
  protocol : String
  scheme : String
  authority : String
  path : String
  headers : Headers := #[]
  deriving Inhabited, Repr, DecidableEq

/-- An HTTP response to an extended CONNECT request. `headers` excludes
`:status`. A response below 200 is informational rather than final. -/
structure Response where
  status : Nat
  headers : Headers := #[]
  deriving Inhabited, Repr, DecidableEq

/-- A bidirectional byte stream carried by one HTTP/2 stream.

The implementation closures belong to the owning HTTP/2 connection. Consumers
should use the namespace operations rather than invoke the fields directly. -/
structure Tunnel where
  sendBytesImpl : ByteArray → Async (Except Error Unit)
  recvBytesImpl : Async (Except Error (Option ByteArray))
  closeSendImpl : Async (Except Error Unit)
  cancelImpl : Async Unit
  waitImpl : Async (Except Error Unit)

namespace Tunnel

/-- Send bytes in order, waiting for connection and stream flow-control credit. -/
def send (tunnel : Tunnel) (bytes : ByteArray) : Async (Except Error Unit) :=
  tunnel.sendBytesImpl bytes

/-- Receive the next nonempty byte chunk. `none` denotes peer END_STREAM. -/
def recv? (tunnel : Tunnel) : Async (Except Error (Option ByteArray)) :=
  tunnel.recvBytesImpl

/-- Half-close the local send side with END_STREAM. Implementations are idempotent. -/
def closeSend (tunnel : Tunnel) : Async (Except Error Unit) :=
  tunnel.closeSendImpl

/-- Abort the stream with RST_STREAM(CANCEL). Implementations are idempotent. -/
def cancel (tunnel : Tunnel) : Async Unit :=
  tunnel.cancelImpl

/-- Wait until both halves end or a terminal stream failure is observed. -/
def wait (tunnel : Tunnel) : Async (Except Error Unit) :=
  tunnel.waitImpl

end Tunnel

/-- A successful server decision and its lifetime-scoped tunnel application. -/
structure Acceptance where
  status : Nat := 200
  headers : Headers := #[]
  run : Tunnel → Async Unit

/-- A final unsuccessful response. -/
structure Rejection where
  status : Nat
  headers : Headers := #[]
  deriving Inhabited, Repr, DecidableEq

inductive Decision where
  | accept (value : Acceptance)
  | reject (value : Rejection)

abbrev Handler := Request → Async Decision

inductive OpenResult where
  | accepted (response : Response) (tunnel : Tunnel)
  | rejected (response : Response)

private def singletonPseudo (headers : Headers) (name : String) : Except Error String := do
  let values := headers.getAll name
  if values.size != 1 then
    throw (Error.invalidArgument
      s!"extended CONNECT requires exactly one {name} pseudo-header")
  pure values[0]!

private def isTokenChar (c : Char) : Bool :=
  Header.isTokenCharacter c

/-- HTTP token syntax used by the `:protocol` pseudo-header. -/
def validProtocolToken (value : String) : Bool :=
  !value.isEmpty && value.all isTokenChar

private def requestPseudoHeader (name : String) : Bool :=
  name == ":method" || name == ":protocol" || name == ":scheme" ||
    name == ":authority" || name == ":path"

private def forbiddenHttp2FieldName (name : String) : Bool :=
  name == "connection" || name == "keep-alive" ||
    name == "proxy-connection" || name == "transfer-encoding" ||
    name == "upgrade"

private def validateFieldValue (kind value : String) : Except Error Unit := do
  unless Header.validFieldValue (Header.of "x-value" value) do
    throw (Error.invalidArgument s!"invalid HTTP/2 {kind} value")

private def validateOrdinaryHeader (header : Header) : Except Error Unit := do
  unless Header.validFieldName header do
    throw (Error.invalidArgument s!"invalid HTTP/2 field name {header.name}")
  if forbiddenHttp2FieldName header.name then
    throw (Error.invalidArgument
      s!"HTTP/2 connection-specific field is forbidden: {header.name}")
  if header.name == "te" && header.value.toLower != "trailers" then
    throw (Error.invalidArgument "HTTP/2 TE field value must be trailers")
  unless Header.validFieldValue header do
    throw (Error.invalidArgument s!"invalid HTTP/2 field value for {header.name}")

private def validatePseudoHeaderLayout (headers : Headers) : Except Error Unit := do
  let _ ← headers.foldlM (init := (false, (#[] : Array String))) fun state header => do
    let (seenOrdinary, seenPseudo) := state
    if header.name.startsWith ":" then
      if seenOrdinary then
        throw (Error.invalidArgument
          s!"HTTP/2 pseudo-header {header.name} appeared after an ordinary field")
      else if seenPseudo.contains header.name then
        throw (Error.invalidArgument s!"duplicate HTTP/2 pseudo-header {header.name}")
      else
        pure (seenOrdinary, seenPseudo.push header.name)
    else
      pure (true, seenPseudo)
  pure ()

private def validateRequestHeader (header : Header) : Except Error Unit := do
  if header.name.startsWith ":" then
    unless requestPseudoHeader header.name do
      throw (Error.invalidArgument
        s!"invalid extended CONNECT pseudo-header {header.name}")
    validateFieldValue s!"pseudo-header {header.name}" header.value
  else
    validateOrdinaryHeader header

private def ordinaryHeaders (headers : Headers) : Headers :=
  headers.filter fun header => !header.name.startsWith ":"

/-- Validate and project an RFC 8441 extended CONNECT request field section. -/
def decodeRequest (headers : Headers) : Except Error Request := do
  validatePseudoHeaderLayout headers
  headers.forM validateRequestHeader
  let method ← singletonPseudo headers ":method"
  unless method == "CONNECT" do
    throw (Error.invalidArgument "extended CONNECT requires :method CONNECT")
  let protocol ← singletonPseudo headers ":protocol"
  unless validProtocolToken protocol do
    throw (Error.invalidArgument
      "extended CONNECT :protocol is not a valid HTTP token")
  let scheme ← singletonPseudo headers ":scheme"
  let authority ← singletonPseudo headers ":authority"
  let path ← singletonPseudo headers ":path"
  unless RequestTarget.valid "CONNECT" scheme path &&
      RequestTarget.validAuthority scheme authority do
    throw (Error.invalidArgument
      "extended CONNECT scheme, authority, or path is not a valid request target")
  pure { protocol, scheme, authority, path, headers := ordinaryHeaders headers }

private def validateOrdinaryOutbound (kind : String) (headers : Headers) :
    Except Error Unit := do
  for header in headers do
    if header.name.startsWith ":" then
      throw (Error.invalidArgument
        s!"{kind} headers must not contain pseudo-header {header.name}")
    validateOrdinaryHeader header

/-- Construct a validated RFC 8441 extended CONNECT request field section. -/
def encodeRequest (request : Request) : Except Error Headers := do
  unless validProtocolToken request.protocol do
    throw (Error.invalidArgument
      "extended CONNECT :protocol is not a valid HTTP token")
  unless RequestTarget.valid "CONNECT" request.scheme request.path &&
      RequestTarget.validAuthority request.scheme request.authority do
    throw (Error.invalidArgument
      "extended CONNECT scheme, authority, or path is not a valid request target")
  validateFieldValue "`:scheme` pseudo-header" request.scheme
  validateFieldValue "`:authority` pseudo-header" request.authority
  validateFieldValue "`:path` pseudo-header" request.path
  validateOrdinaryOutbound "extended CONNECT request" request.headers
  pure <| Headers.empty
    |>.insert ":method" "CONNECT"
    |>.insert ":protocol" request.protocol
    |>.insert ":scheme" request.scheme
    |>.insert ":authority" request.authority
    |>.insert ":path" request.path
    |>.append request.headers

private def parseStatus (value : String) : Option Nat := do
  if value.utf8ByteSize != 3 then none else pure ()
  let status ← value.toNat?
  if 100 ≤ status && status ≤ 599 then some status else none

private def validateResponseHeader (header : Header) : Except Error Unit := do
  if header.name.startsWith ":" then
    unless header.name == ":status" do
      throw (Error.invalidArgument
        s!"invalid extended CONNECT response pseudo-header {header.name}")
    validateFieldValue "`:status` pseudo-header" header.value
  else
    validateOrdinaryHeader header

/-- Validate and project one extended CONNECT response field section.
Informational responses are returned to let the connection state enforce their
sequencing. Status 101 is forbidden in HTTP/2. -/
def decodeResponse (headers : Headers) : Except Error Response := do
  validatePseudoHeaderLayout headers
  headers.forM validateResponseHeader
  let raw ← singletonPseudo headers ":status"
  let status ← match parseStatus raw with
    | some status => pure status
    | none => throw (Error.invalidArgument s!"invalid HTTP/2 :status {raw}")
  if status == 101 then
    throw (Error.invalidArgument "HTTP/2 responses must not use status 101")
  pure { status, headers := ordinaryHeaders headers }

/-- Construct a validated final extended CONNECT response field section. -/
def encodeResponse (response : Response) : Except Error Headers := do
  if response.status < 200 || response.status > 599 then
    throw (Error.invalidArgument
      "extended CONNECT final response status is outside 200..599")
  validateOrdinaryOutbound "extended CONNECT response" response.headers
  pure <| (Headers.singleton ":status" (toString response.status)).append response.headers

/-- Whether a final response establishes the extended CONNECT tunnel. -/
def isSuccess (response : Response) : Bool :=
  200 ≤ response.status && response.status < 300

end Http2.ExtendedConnect
