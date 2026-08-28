module

public import Http2.Error

public section

namespace Http2

/-- One ordered HTTP field. Names created with `Header.of` are lowercase. -/
structure Header where
  name : String
  value : String
  deriving Inhabited, Repr, DecidableEq

/-- An ordered field section. Order and repeated names are significant. -/
abbrev Headers := Array Header

namespace Header

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
  header.name.toUTF8.size + header.value.toUTF8.size + 32

private def listEntrySizeCandidate (header : Header) : Nat :=
  header.name.utf8ByteSize + header.value.utf8ByteSize + 32

private theorem listEntrySizeCandidate_eq_reference (header : Header) :
    listEntrySizeCandidate header = listEntrySizeReference header := by
  unfold listEntrySizeCandidate listEntrySizeReference
  rw [String.toUTF8_eq_toByteArray, String.toUTF8_eq_toByteArray,
    String.size_toByteArray, String.size_toByteArray]

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
