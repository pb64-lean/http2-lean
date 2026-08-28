module

public section

namespace Http2

/-- Error codes carried by RST_STREAM and GOAWAY (RFC 9113, section 7). -/
inductive ErrorCode where
  | noError
  | protocolError
  | internalError
  | flowControlError
  | settingsTimeout
  | streamClosed
  | frameSizeError
  | refusedStream
  | cancel
  | compressionError
  | connectError
  | enhanceYourCalm
  | inadequateSecurity
  | http11Required
  | unknown (value : Nat)
  deriving Inhabited, Repr, DecidableEq

namespace ErrorCode

def toNat : ErrorCode -> Nat
  | .noError => 0
  | .protocolError => 1
  | .internalError => 2
  | .flowControlError => 3
  | .settingsTimeout => 4
  | .streamClosed => 5
  | .frameSizeError => 6
  | .refusedStream => 7
  | .cancel => 8
  | .compressionError => 9
  | .connectError => 10
  | .enhanceYourCalm => 11
  | .inadequateSecurity => 12
  | .http11Required => 13
  | .unknown value => value

def ofNat : Nat -> ErrorCode
  | 0 => .noError
  | 1 => .protocolError
  | 2 => .internalError
  | 3 => .flowControlError
  | 4 => .settingsTimeout
  | 5 => .streamClosed
  | 6 => .frameSizeError
  | 7 => .refusedStream
  | 8 => .cancel
  | 9 => .compressionError
  | 10 => .connectError
  | 11 => .enhanceYourCalm
  | 12 => .inadequateSecurity
  | 13 => .http11Required
  | value => .unknown value

end ErrorCode

/-- The boundary at which an HTTP/2 failure applies. -/
inductive ErrorScope where
  /-- Invalid input or a failed local operation before bytes are accepted. -/
  | localInput
  /-- A connection error that terminates the HTTP/2 connection. -/
  | connection
  /-- A stream error contained to one stream. -/
  | stream (streamId : Nat)
  deriving Inhabited, Repr, DecidableEq

/-- A protocol-neutral HTTP/2 failure with its wire code and containment scope. -/
structure Error where
  code : ErrorCode
  scope : ErrorScope
  message : String
  deriving Inhabited, Repr, DecidableEq

namespace Error

def localInput (message : String) (code : ErrorCode := .internalError) : Error :=
  { code, scope := .localInput, message }

def connection (code : ErrorCode) (message : String) : Error :=
  { code, scope := .connection, message }

def stream (streamId : Nat) (code : ErrorCode) (message : String) : Error :=
  { code, scope := .stream streamId, message }

/-- Compatibility-free constructor for an invariant or invalid local input. -/
def internal (message : String) : Error :=
  localInput message

def invalidArgument (message : String) : Error :=
  localInput message .protocolError

def resourceExhausted (message : String) : Error :=
  localInput message .enhanceYourCalm

def protocol (message : String) : Error :=
  connection .protocolError message

def compression (message : String) : Error :=
  connection .compressionError message

def frameSize (message : String) : Error :=
  connection .frameSizeError message

def flowControl (message : String) : Error :=
  connection .flowControlError message

def messageD (error : Error) : String :=
  error.message

end Error

instance : ToString Error where
  toString error := error.message

end Http2
