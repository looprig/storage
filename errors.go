package storage

import "strconv"

// This file defines the storage-canonical error taxonomy: the concrete error
// types a backend returns for each distinct failure mode. Backends wrap their
// own cause with fmt.Errorf("...: %w", &storage.XxxError{...}); callers
// classify by recovering the concrete type with errors.As, never by string.
//
// Every Error() is prefixed "storage: " and names its subject — the Name/Key
// and any relevant number — with the string subject strconv.Quote'd so an
// untrusted key cannot inject newlines or control bytes into a log line.

// ConflictError reports a ledger compare-and-swap that failed because the caller
// appended at the wrong expected sequence (the head had moved).
type ConflictError struct {
	Name     string
	Expected uint64
}

func (e *ConflictError) Error() string {
	return "storage: ledger " + strconv.Quote(e.Name) + " conflict: wrong expected seq " + strconv.FormatUint(e.Expected, 10)
}

// AmbiguousError reports a ledger append whose acknowledgement was lost or timed
// out: the record may or may not have been committed at Expected. Cause carries
// the underlying transport/timeout error and may be nil.
type AmbiguousError struct {
	Name     string
	Expected uint64
	Cause    error
}

func (e *AmbiguousError) Error() string {
	return "storage: ledger " + strconv.Quote(e.Name) + " append ambiguous at expected seq " + strconv.FormatUint(e.Expected, 10)
}

// Unwrap returns the underlying cause (possibly nil).
func (e *AmbiguousError) Unwrap() error {
	return e.Cause
}

// RecordNotFoundError reports that a ledger has no record at the requested Seq.
type RecordNotFoundError struct {
	Name string
	Seq  uint64
}

func (e *RecordNotFoundError) Error() string {
	return "storage: ledger " + strconv.Quote(e.Name) + " has no record at seq " + strconv.FormatUint(e.Seq, 10)
}

// KeyNotFoundError reports that a KV key is absent.
type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return "storage: kv key " + strconv.Quote(e.Key) + " not found"
}

// BlobNotFoundError reports that a blob is absent at Key.
type BlobNotFoundError struct {
	Key string
}

func (e *BlobNotFoundError) Error() string {
	return "storage: blob " + strconv.Quote(e.Key) + " not found"
}

// BlobConflictError reports a blob Put where Key already exists with different
// content (blob writes are content-addressed and immutable per key).
type BlobConflictError struct {
	Key string
}

func (e *BlobConflictError) Error() string {
	return "storage: blob " + strconv.Quote(e.Key) + " already exists with different content"
}

// LeaseHeldError reports an Acquire that was refused because another holder owns
// the lease at HolderEpoch.
type LeaseHeldError struct {
	Name        string
	HolderEpoch uint64
}

func (e *LeaseHeldError) Error() string {
	return "storage: lease " + strconv.Quote(e.Name) + " held by epoch " + strconv.FormatUint(e.HolderEpoch, 10)
}

// LeaseLostError reports a write attempted after the caller's lease at Epoch was
// lost or expired (fenced by a newer holder).
type LeaseLostError struct {
	Name  string
	Epoch uint64
}

func (e *LeaseLostError) Error() string {
	return "storage: lease " + strconv.Quote(e.Name) + " lost at epoch " + strconv.FormatUint(e.Epoch, 10)
}

// InvalidStableKeyError reports an OrderedID StableKey that is empty, too long,
// or not valid UTF-8. Stable keys are opaque and are not constrained by the
// storage name grammar. StableKeyLength is the input's byte length capped at
// 65,535; the error never retains or renders the raw key.
type InvalidStableKeyError struct {
	Rule            string
	StableKeyLength uint16
}

func (e *InvalidStableKeyError) Error() string {
	return "storage: invalid ordered stable key with " + strconv.Itoa(int(e.StableKeyLength)) + " bytes: " + e.Rule
}

const maxInvalidStableKeyDiagnosticBytes = 1<<16 - 1

// newInvalidStableKeyError preserves only safe diagnostics for a rejected
// StableKey. It is intentionally the sole constructor used by validation.
func newInvalidStableKeyError(key StableKey, rule string) *InvalidStableKeyError {
	length := len(key)
	if length > maxInvalidStableKeyDiagnosticBytes {
		length = maxInvalidStableKeyDiagnosticBytes
	}
	return &InvalidStableKeyError{Rule: rule, StableKeyLength: uint16(length)}
}

// InvalidDueError reports a Due state that is not canonical or uses an unknown
// discriminator.
type InvalidDueError struct {
	Due  Due
	Rule string
}

func (e *InvalidDueError) Error() string {
	return "storage: invalid ordered due state " + strconv.FormatUint(uint64(e.Due.State), 10) + " at unix millis " + strconv.FormatInt(e.Due.UnixMillis, 10) + ": " + e.Rule
}

// InvalidOrderedRecordError reports an OrderedRecord that violates an
// invariant that crosses multiple fields.
type InvalidOrderedRecordError struct {
	ID   OrderedID
	Rule string
}

func (e *InvalidOrderedRecordError) Error() string {
	return "storage: invalid ordered record " + orderedIDSubject(e.ID) + ": " + e.Rule
}

// OrderedValueTooLargeError reports an OrderedIndex value over the supported
// maximum size in bytes.
type OrderedValueTooLargeError struct {
	Size int
	Max  int
}

func (e *OrderedValueTooLargeError) Error() string {
	return "storage: ordered value size " + strconv.Itoa(e.Size) + " exceeds maximum " + strconv.Itoa(e.Max)
}

// InvalidOrderedLimitError reports a List* limit outside the inclusive range
// 1..Max.
type InvalidOrderedLimitError struct {
	Limit int
	Max   int
}

func (e *InvalidOrderedLimitError) Error() string {
	return "storage: invalid ordered page limit " + strconv.Itoa(e.Limit) + ": must be in [1, " + strconv.Itoa(e.Max) + "]"
}

// OrderedCursorKind identifies the query family that rejected a cursor.
type OrderedCursorKind string

const (
	// RankedCursorKind identifies a ListRanked continuation cursor.
	RankedCursorKind OrderedCursorKind = "ranked"

	// DueCursorKind identifies a ListDue continuation cursor.
	DueCursorKind OrderedCursorKind = "due"
)

func (k OrderedCursorKind) String() string {
	switch k {
	case RankedCursorKind:
		return "ranked"
	case DueCursorKind:
		return "due"
	default:
		return "unknown"
	}
}

// OrderedCursorRule classifies why a provider rejected an opaque cursor. Its
// String form is fixed so error rendering never relies on provider-supplied
// token text.
type OrderedCursorRule uint8

const (
	// OrderedCursorMalformed reports a syntactically malformed token.
	OrderedCursorMalformed OrderedCursorRule = iota + 1

	// OrderedCursorUnknownVersion reports an unsupported token version.
	OrderedCursorUnknownVersion

	// OrderedCursorWrongKind reports a token issued for another cursor family.
	OrderedCursorWrongKind

	// OrderedCursorQueryMismatch reports a token bound to another query.
	OrderedCursorQueryMismatch

	// OrderedCursorExpired reports a token that is no longer valid.
	OrderedCursorExpired
)

func (r OrderedCursorRule) String() string {
	switch r {
	case OrderedCursorMalformed:
		return "malformed"
	case OrderedCursorUnknownVersion:
		return "unknown version"
	case OrderedCursorWrongKind:
		return "wrong kind"
	case OrderedCursorQueryMismatch:
		return "query mismatch"
	case OrderedCursorExpired:
		return "expired"
	default:
		return "invalid"
	}
}

// InvalidOrderedCursorError reports a malformed, unknown-version, wrong-kind,
// expired, cross-query, or otherwise unusable opaque OrderedIndex continuation
// cursor. Kind identifies the listing operation that rejected it. Rule is a
// safe classification. CursorLength is the raw token's bounded byte length;
// neither it nor Error exposes raw opaque cursor contents.
type InvalidOrderedCursorError struct {
	Kind         OrderedCursorKind
	Rule         OrderedCursorRule
	CursorLength uint16
}

func (e *InvalidOrderedCursorError) Error() string {
	return "storage: invalid " + e.Kind.String() + " ordered cursor: " + e.Rule.String()
}

const maxInvalidOrderedCursorLength = 1<<16 - 1

// NewInvalidOrderedCursorError constructs an error for cursor while retaining
// only its safe, bounded byte length. Providers must use it rather than placing
// raw cursor contents in an error or log message.
func NewInvalidOrderedCursorError(kind OrderedCursorKind, cursor string, rule OrderedCursorRule) *InvalidOrderedCursorError {
	length := len(cursor)
	if length > maxInvalidOrderedCursorLength {
		length = maxInvalidOrderedCursorLength
	}
	return &InvalidOrderedCursorError{Kind: kind, Rule: rule, CursorLength: uint16(length)}
}

// OrderedRevisionConflictError reports an OrderedIndex compare-and-swap whose
// current revision did not equal ExpectedRevision. ActualRevision is the
// current revision observed by a backend when it can determine it; a backend
// that cannot safely disclose it leaves it zero.
type OrderedRevisionConflictError struct {
	ID               OrderedID
	ExpectedRevision uint64
	ActualRevision   uint64
}

func (e *OrderedRevisionConflictError) Error() string {
	actual := "unknown"
	if e.ActualRevision != 0 {
		actual = strconv.FormatUint(e.ActualRevision, 10)
	}
	return "storage: ordered record " + orderedIDSubject(e.ID) + " revision conflict: expected " + strconv.FormatUint(e.ExpectedRevision, 10) + ", actual " + actual
}

// OrderedRevisionExhaustedError reports a mutation that cannot advance a live
// OrderedRecord revision without overflowing uint64. The provider leaves the
// record unchanged.
type OrderedRevisionExhaustedError struct {
	ID       OrderedID
	Revision uint64
}

func (e *OrderedRevisionExhaustedError) Error() string {
	return "storage: ordered record " + orderedIDSubject(e.ID) + " revision exhausted at " + strconv.FormatUint(e.Revision, 10)
}

// OrderedRecordNotFoundError reports an ordered identity that has never been
// created. Tombstoned records are distinct from absent records.
type OrderedRecordNotFoundError struct {
	ID OrderedID
}

func (e *OrderedRecordNotFoundError) Error() string {
	return "storage: ordered record " + orderedIDSubject(e.ID) + " not found"
}

// OrderedDeletedError reports an Update attempted against a logical tombstone.
// Tombstones cannot be resurrected through OrderedIndex.
type OrderedDeletedError struct {
	ID OrderedID
}

func (e *OrderedDeletedError) Error() string {
	return "storage: ordered record " + orderedIDSubject(e.ID) + " is deleted"
}

// OrderedOperation identifies a mutation whose outcome could be ambiguous.
type OrderedOperation string

const (
	// OrderedCreateOperation identifies Create.
	OrderedCreateOperation OrderedOperation = "create"

	// OrderedUpdateOperation identifies Update.
	OrderedUpdateOperation OrderedOperation = "update"

	// OrderedDeleteOperation identifies Delete.
	OrderedDeleteOperation OrderedOperation = "delete"
)

// OrderedAmbiguousError reports a networked OrderedIndex mutation whose
// acknowledgement was lost or timed out, so the mutation may or may not have
// committed. Cause carries the underlying transport error and may be nil.
type OrderedAmbiguousError struct {
	Operation OrderedOperation
	ID        OrderedID
	Cause     error
}

func (e *OrderedAmbiguousError) Error() string {
	return "storage: ordered " + string(e.Operation) + " ambiguous for " + orderedIDSubject(e.ID)
}

// Unwrap returns the underlying cause (possibly nil).
func (e *OrderedAmbiguousError) Unwrap() error {
	return e.Cause
}

// orderedIDSubject formats an OrderedID for a typed error message without ever
// interpreting StableKey as a name or a path.
func orderedIDSubject(id OrderedID) string {
	return "(namespace " + strconv.Quote(id.Namespace) + ", ordering scope " + strconv.Quote(id.OrderingScope) + ", stable key " + strconv.Quote(string(id.StableKey)) + ")"
}
