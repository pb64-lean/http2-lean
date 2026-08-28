module

public import Http2.Error

public section

namespace Http2

/-- One ordered HTTP field. Names created with `Header.of` are lowercase. -/
structure Header where
  name : String
  value : String
  /-- Original wire octets when the field name is not valid UTF-8. -/
  nameOctets? : Option ByteArray := none
  /-- Original wire octets when the field value is not valid UTF-8. -/
  valueOctets? : Option ByteArray := none
  deriving Inhabited, DecidableEq

instance : Repr Header where
  reprPrec header precedence := reprPrec
    (header.name, header.value,
      header.nameOctets?.map (·.data.toList),
      header.valueOctets?.map (·.data.toList)) precedence

/-- An ordered field section. Order and repeated names are significant. -/
abbrev Headers := Array Header

namespace Header

/-- Produce a source-friendly string view while retaining non-UTF-8 wire
octets for exact accounting and re-encoding. -/
@[expose] def decodeWireString (bytes : ByteArray) : String × Option ByteArray :=
  match String.fromUTF8? bytes with
  | some value => (value, none)
  | none =>
      (String.ofList (bytes.data.toList.map fun byte => Char.ofNat byte.toNat), some bytes)

/-- Exact field-name bytes used for HPACK accounting and re-encoding. -/
def nameOctets (header : Header) : ByteArray :=
  header.nameOctets?.getD header.name.toUTF8

/-- Exact field-value bytes used for HPACK accounting and re-encoding. -/
def valueOctets (header : Header) : ByteArray :=
  header.valueOctets?.getD header.value.toUTF8

def isTokenCharacter (character : Char) : Bool :=
  let value := character.toNat
  (0x30 <= value && value <= 0x39) ||
    (0x41 <= value && value <= 0x5a) ||
    (0x61 <= value && value <= 0x7a) ||
    character == '!' || character == '#' || character == '$' || character == '%' ||
    character == '&' || character == '\'' || character == '*' || character == '+' ||
    character == '-' || character == '.' || character == '^' || character == '_' ||
    character == '`' || character == '|' || character == '~'

/-- RFC field-name syntax for a decoded HTTP/2 ordinary field. -/
def validFieldName (header : Header) : Bool :=
  header.nameOctets?.isNone && !header.name.isEmpty && header.name.all fun character =>
    character.toLower == character && isTokenCharacter character

private def optionalWhitespace (byte : UInt8) : Bool :=
  byte == 0x20 || byte == 0x09

/-- HTTP field-content syntax, including obs-text octets 0x80..0xff. -/
def validFieldValue (header : Header) : Bool :=
  let bytes := header.valueOctets.data
  bytes.all fun byte =>
    let value := byte.toNat
    value == 0x09 || (0x20 <= value && value != 0x7f)
  &&
  (match bytes.toList with
  | [] => true
  | first :: rest =>
      !optionalWhitespace first && !(optionalWhitespace (rest.getLastD first)))

@[inline] private def isAsciiNonUpperByte (byte : UInt8) : Bool :=
  byte < 128 && !(65 <= byte && byte <= 90)

private def isAsciiWithoutUpperFrom (name : String) (index : Nat) : Bool :=
  if h : index < name.utf8ByteSize then
    let byte := name.getUTF8Byte ⟨index⟩ (by
      simpa only [String.Pos.Raw.lt_iff, String.byteIdx_rawEndPos] using h)
    if isAsciiNonUpperByte byte then
      isAsciiWithoutUpperFrom name (index + 1)
    else
      false
  else
    true
termination_by name.utf8ByteSize - index

/-- Allocation-free fast path for names that already contain no uppercase ASCII. -/
def normalizeNameByteIndexed (name : String) : String :=
  if isAsciiWithoutUpperFrom name 0 then name else name.toLower

/-- Normalize an HTTP field name to lowercase. -/
@[implemented_by normalizeNameByteIndexed]
def normalizeName (name : String) : String :=
  name.toLower

theorem normalizeName_eq_self (name : String) (h : name.toLower = name) :
    normalizeName name = name := by
  unfold normalizeName
  exact h

@[expose] def of (name value : String) : Header :=
  { name := normalizeName name, value }

end Header

namespace RequestTarget

private def asciiAlpha (character : Char) : Bool :=
  let value := character.toNat
  (0x41 <= value && value <= 0x5a) || (0x61 <= value && value <= 0x7a)

private def asciiDigit (character : Char) : Bool :=
  let value := character.toNat
  0x30 <= value && value <= 0x39

private def asciiHexDigit (character : Char) : Bool :=
  asciiDigit character ||
    let value := character.toNat
    (0x41 <= value && value <= 0x46) || (0x61 <= value && value <= 0x66)

/-- RFC URI scheme syntax. -/
def validScheme (scheme : String) : Bool :=
  match scheme.toList with
  | [] => false
  | first :: rest => asciiAlpha first && rest.all fun character =>
      asciiAlpha character || asciiDigit character ||
        character == '+' || character == '-' || character == '.'

private def validPathQueryCharacters : List Char → Bool
  | [] => true
  | '%' :: high :: low :: rest =>
      asciiHexDigit high && asciiHexDigit low && validPathQueryCharacters rest
  | '%' :: _ => false
  | character :: rest =>
      let unreserved := asciiAlpha character || asciiDigit character ||
        character == '-' || character == '.' || character == '_' || character == '~'
      let subDelimiter := character == '!' || character == '$' || character == '&' ||
        character == '\'' || character == '(' || character == ')' || character == '*' ||
        character == '+' || character == ',' || character == ';' || character == '='
      (unreserved || subDelimiter || character == ':' || character == '@' ||
        character == '/' || character == '?') && validPathQueryCharacters rest

/-- Validate the scheme and path pseudo-fields as one request target. -/
def valid (method scheme path : String) : Bool :=
  validScheme scheme &&
    if path == "*" then method == "OPTIONS"
    else
      !path.isEmpty && validPathQueryCharacters path.toList &&
        if scheme.toLower == "http" || scheme.toLower == "https" then
          path.startsWith "/"
        else true

/-- Validate authority constraints imposed by HTTP URI schemes. -/
def validAuthority (scheme authority : String) : Bool :=
  let rec validCharacters : List Char → Bool
    | [] => true
    | '%' :: high :: low :: rest =>
        asciiHexDigit high && asciiHexDigit low && validCharacters rest
    | '%' :: _ => false
    | character :: rest =>
        let unreserved := asciiAlpha character || asciiDigit character ||
          character == '-' || character == '.' || character == '_' || character == '~'
        let subDelimiter := character == '!' || character == '$' || character == '&' ||
          character == '\'' || character == '(' || character == ')' || character == '*' ||
          character == '+' || character == ',' || character == ';' || character == '='
        (unreserved || subDelimiter || character == ':' || character == '[' ||
          character == ']' || character == '@') && validCharacters rest
  !authority.isEmpty && validCharacters authority.toList &&
    if scheme.toLower == "http" || scheme.toLower == "https" then
      !authority.any (· == '@')
    else true

end RequestTarget

namespace Headers

def empty : Headers := #[]

def insert (headers : Headers) (name value : String) : Headers :=
  headers.push (Header.of name value)

def singleton (name value : String) : Headers :=
  empty.insert name value

@[expose] def getAll (headers : Headers) (name : String) : Array String :=
  let key := Header.normalizeName name
  headers.filterMap fun header =>
    if header.name == key then some header.value else none

@[expose] def get? (headers : Headers) (name : String) : Option String :=
  (getAll headers name)[0]?

/-- Exact field-value octets for every matching field, preserving order. -/
def getAllOctets (headers : Headers) (name : String) : Array ByteArray :=
  let key := Header.normalizeName name
  headers.filterMap fun header =>
    if header.name == key then some header.valueOctets else none

/-- Exact field-value octets for the first matching field. -/
def getOctets? (headers : Headers) (name : String) : Option ByteArray :=
  (getAllOctets headers name)[0]?

private def getLastUSizeLoop (headers : Headers) (key : String)
    (i : USize) (bound : i.toNat ≤ headers.size) : Option String :=
  if atStart : i = 0 then
    none
  else
    have positive : 0 < i.toNat := by
      have nonzero : i.toNat ≠ 0 := by
        intro zero
        apply atStart
        exact USize.toNat_inj.mp (by simpa using zero)
      omega
    have oneLe : (1 : USize) ≤ i := by
      rw [USize.le_iff_toNat_le]
      simpa using positive
    let previous := i - 1
    have previousToNat : previous.toNat = i.toNat - 1 := by
      simpa [previous] using USize.toNat_sub_of_le i 1 oneLe
    have previousBound : previous.toNat < headers.size := by
      omega
    let header := headers.uget previous previousBound
    if header.name == key then
      some header.value
    else
      getLastUSizeLoop headers key previous (Nat.le_of_lt previousBound)
termination_by i.toNat
decreasing_by
  rw [USize.toNat_sub_of_le i 1 oneLe]
  rw [USize.toNat_one]
  omega

private def getLastUSize (headers : Headers) (name : String) : Option String :=
  let key := Header.normalizeName name
  getLastUSizeLoop headers key headers.usize (by
    simp only [Array.usize, Nat.toUSize_eq, USize.toNat_ofNat']
    exact Nat.mod_le _ _)

@[implemented_by getLastUSize, expose]
def getLast? (headers : Headers) (name : String) : Option String :=
  let key := Header.normalizeName name
  headers.findSomeRev? fun header =>
    if header.name == key then some header.value else none

theorem getLast?_eq_getAll_back? (headers : Headers) (name : String) :
    getLast? headers name = (getAll headers name).back? := by
  simp [getLast?, getAll]

def contains (headers : Headers) (name value : String) : Bool :=
  (getAll headers name).contains value

def append (left right : Headers) : Headers :=
  right.foldl (fun acc header => acc.push header) left

private def listEntrySizeReference (header : Header) : Nat :=
  header.nameOctets.size + header.valueOctets.size + 32

private def listEntrySizeCandidate (header : Header) : Nat :=
  header.nameOctets.size + header.valueOctets.size + 32

private theorem listEntrySizeCandidate_eq_reference (header : Header) :
    listEntrySizeCandidate header = listEntrySizeReference header := by
  unfold listEntrySizeCandidate listEntrySizeReference
  rfl

/-- RFC 7541 field-list accounting, including the fixed 32-byte overhead. -/
@[implemented_by listEntrySizeCandidate]
def listEntrySize (header : Header) : Nat :=
  listEntrySizeReference header

def listSize (headers : Headers) : Nat :=
  headers.foldl (fun total header => total + listEntrySize header) 0

def validateListSize (maxSize? : Option Nat) (headers : Headers) : Except Error Unit := do
  match maxSize? with
  | none => pure ()
  | some maxSize =>
      let actual := listSize headers
      if actual > maxSize then
        throw (Error.resourceExhausted
          s!"HTTP field section exceeds configured size limit {maxSize}")
      else
        pure ()

end Headers
end Http2
