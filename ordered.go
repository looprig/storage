package storage

import (
	"context"
	"unicode/utf8"
)

// MaxStableKeyBytes is the inclusive maximum length, in UTF-8 bytes, of an
// OrderedID StableKey.
const MaxStableKeyBytes = 256

// MaxOrderedValueBytes is the largest value an OrderedIndex implementation
// must accept. Callers that need larger values must store the bulk content in
// Blobs and keep a reference in Value.
const MaxOrderedValueBytes = 1 << 20

// MaxOrderedPageLimit is the inclusive maximum number of records a single
// OrderedIndex listing may request.
const MaxOrderedPageLimit = 1000

// StableKey is the opaque, stable identity component of an ordered record.
// It is deliberately not a storage name or path: any valid UTF-8 value from
// 1 through MaxStableKeyBytes bytes is accepted, including slashes, uppercase
// letters, punctuation, and Unicode.
type StableKey string

// OrderedID identifies an ordered record. The (Namespace, OrderingScope) pair
// is the order scope: the unit within which Order strictly increases and is
// never reused. The same OrderingScope in another Namespace is a separate order
// scope with its own independent stream, though a provider may serve every
// scope from one shared underlying sequence, so order values are not
// comparable across scopes.
type OrderedID struct {
	Namespace     string
	OrderingScope string
	StableKey     StableKey
}

// DueState records whether an OrderedRecord participates in due-time
// listings.
type DueState uint8

const (
	// NotDue means a record is absent from ListDue results. Its UnixMillis must
	// be zero, which gives the non-due state one canonical representation.
	NotDue DueState = iota

	// DueAt means UnixMillis is an absolute UTC Unix timestamp in milliseconds
	// and the record participates in ListDue results.
	DueAt
)

// Due is the mutable due-time state of an ordered record.
type Due struct {
	State      DueState
	UnixMillis int64
}

// Rank is the mutable rank state of an ordered record. A record with Ranked
// false is absent from ListRanked results.
type Rank struct {
	Ranked bool
	Value  int64
}

// OrderedRecord is the complete state of one ordered index row. Revision and
// Order are provider-assigned: Revision is always nonzero; Create assigns 1,
// and every successful Update or live Delete advances it exactly once. Revision
// never wraps to zero: if a provider cannot advance the maximal uint64 it
// returns *OrderedRevisionExhaustedError without changing state. Order is
// nonzero, immutable, strictly increasing within its order scope, and never
// reused there — including after a tombstone. It is deliberately NOT required
// to be contiguous or 1-based: a provider may allocate order from a JetStream
// stream sequence or a shared SQL sequence, so orders may be sparse and may be
// shared across order scopes. Callers resume from an exclusive order cursor,
// for which density buys nothing, and must never infer a position, a count, or
// a scope from an order value. Callers own
// Value after it is returned; implementations must not retain or later mutate
// caller-owned Value slices.
type OrderedRecord struct {
	ID           OrderedID
	RankingScope string
	Revision     uint64
	Order        uint64
	Due          Due
	Rank         Rank
	Value        []byte
	Deleted      bool
}

// RankedCursor is a provider-issued, opaque, versioned continuation token for
// ListRanked. A nonempty token is valid only for the exact namespace, ranking
// scope, and ranked query that issued it. ListRanked must return
// *InvalidOrderedCursorError with Kind RankedCursorKind for a malformed token,
// an unknown token version, a token issued for another cursor kind, or a query
// mismatch. A cursor conveys position, not authority: a provider re-checks the
// namespace and ranking scope against the live request and never trusts them
// from the token.
type RankedCursor string

// DueCursor is a provider-issued, opaque, versioned continuation token for
// ListDue. A nonempty token is valid only for the exact namespace, due bound,
// and due query that issued it. ListDue must return
// *InvalidOrderedCursorError with Kind DueCursorKind for a malformed token, an
// unknown token version, a token issued for another cursor kind, or a query
// mismatch. A cursor conveys position, not authority: a provider re-checks the
// namespace and due bound against the live request and never trusts them from
// the token.
type DueCursor string

// OrderedPage is a page from ListOrdered. NextAfterOrder is zero when no rows
// were returned; otherwise it is the final immutable order in Records and may
// be passed as afterOrder to resume the acceptance-order stream.
type OrderedPage struct {
	Records        []OrderedRecord
	NextAfterOrder uint64
}

// RankedPage is a page from ListRanked. An empty NextCursor denotes an
// exhausted result set.
type RankedPage struct {
	Records    []OrderedRecord
	NextCursor RankedCursor
}

// DuePage is a page from ListDue. An empty NextCursor denotes an exhausted
// result set.
type DuePage struct {
	Records    []OrderedRecord
	NextCursor DueCursor
}

// OrderedIndex provides a durable record collection with immutable
// acceptance order plus current ranked and due views. Implementations validate
// relevant inputs at their public boundary using the validators below. Each
// method's validation, lookup, and CAS precedence is authoritative; this
// overview does not impose an order beyond those method-specific rules. Every
// method that accepts an OrderedID (Get, Create, Update, and Delete) validates
// it first with ValidateOrderedID before inspecting or mutating a record. A
// canceled context may return its ordinary context error. A networked mutation
// with an indeterminate outcome returns *OrderedAmbiguousError; local
// implementations never do.
// Nonempty ranked and due cursors are provider-issued opaque versioned tokens;
// implementations must fail closed with *InvalidOrderedCursorError of the
// matching cursor Kind if one is malformed, has an unknown version, has the
// wrong cursor kind, or does not bind to the exact request query.
//
// All returned Records and their Value slices are snapshots owned by the
// caller. Implementations must copy caller Value before retaining it and must
// copy Value before returning records.
type OrderedIndex interface {
	// Get validates id first, then returns the current record, including a
	// logical tombstone. An identity that has never been created returns
	// *OrderedRecordNotFoundError.
	Get(ctx context.Context, id OrderedID) (OrderedRecord, error)

	// Create validates id first and then atomically inspects that identity. If
	// it already exists, including as a tombstone, Create returns its canonical
	// stored record with created == false without validating the candidate
	// rankingScope, value, rank, or due state. Only when the identity is absent
	// does Create validate those candidate fields, assign revision 1, and
	// allocate a nonzero immutable order strictly greater than every order the
	// scope has allocated before. It need not be the next integer: allocation
	// may be sparse.
	Create(ctx context.Context, id OrderedID, rankingScope string, value []byte, rank Rank, due Due) (record OrderedRecord, created bool, err error)

	// Update validates id first. An absent record returns
	// *OrderedRecordNotFoundError and a tombstone returns *OrderedDeletedError,
	// in both cases regardless of expectedRevision. For a live record, Update
	// validates candidate Value, Rank, and Due before comparing expectedRevision:
	// an invalid candidate returns its validation error, while a valid stale
	// revision returns *OrderedRevisionConflictError. A valid match replaces
	// only Value, Rank, and Due and advances Revision exactly once. It cannot
	// change identity, ranking scope, or immutable order. If Revision cannot
	// advance, it returns *OrderedRevisionExhaustedError unchanged.
	Update(ctx context.Context, id OrderedID, expectedRevision uint64, value []byte, rank Rank, due Due) (OrderedRecord, error)

	// Delete validates id first. An absent record returns
	// *OrderedRecordNotFoundError. A tombstone returns its canonical existing
	// state regardless of expectedRevision, including a retry with the
	// pre-delete revision. For a live record, a stale expectedRevision returns
	// *OrderedRevisionConflictError; a matching revision advances exactly once,
	// sets Deleted, clears Rank to Rank{} and Due to Due{State: NotDue,
	// UnixMillis: 0}, and preserves Value, RankingScope, identity, and immutable
	// Order. If Revision cannot advance, it returns
	// *OrderedRevisionExhaustedError unchanged.
	Delete(ctx context.Context, id OrderedID, expectedRevision uint64) (OrderedRecord, error)

	// ListOrdered returns the immutable acceptance-order stream, including
	// tombstones, in ascending Order after the exclusive numeric afterOrder.
	// Passing zero starts at the beginning of the (Namespace, OrderingScope)
	// stream.
	ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (OrderedPage, error)

	// ListRanked returns current, nondeleted ranked records in descending
	// (rank, stable_key, ordering_scope) order. OrderingScope is consulted only
	// after the frozen (rank, stable_key) pair ties, making pagination total
	// without narrowing valid identities. after is a provider-issued opaque,
	// versioned, query-bound token. A malformed, unknown-version, wrong-kind,
	// or query-mismatched token returns *InvalidOrderedCursorError with Kind
	// RankedCursorKind.
	//
	// Pagination resumes from the frozen (rank, stable_key, ordering_scope)
	// tuple the cursor names, not from a snapshot of the result set. A record
	// whose rank changes between two pages therefore moves relative to that
	// frozen position: it is skipped if it moves to an already-passed position
	// and returned twice if it moves ahead of one. This is inherent to
	// keyset pagination over a live view; a sweep that must see every record
	// exactly once has to reconcile by identity, not by page.
	ListRanked(ctx context.Context, namespace string, rankingScope string, after RankedCursor, limit int) (RankedPage, error)

	// ListDue returns current, nondeleted DueAt records with UnixMillis no later
	// than dueAtOrBefore in ascending (due_at, stable_key, ordering_scope)
	// order. OrderingScope is consulted only after the frozen (due_at,
	// stable_key) pair ties. after is opaque and bound to namespace, the fixed
	// due bound, and this exact due query. A malformed, unknown-version,
	// wrong-kind, or query-mismatched token returns *InvalidOrderedCursorError
	// with Kind DueCursorKind.
	//
	// Like ListRanked, ListDue resumes from the frozen (due_at, stable_key,
	// ordering_scope) tuple the cursor names, so a record whose due time moves
	// across that position between pages is skipped or returned twice.
	ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after DueCursor, limit int) (DuePage, error)
}

// ValidateStableKey reports whether key is a valid opaque StableKey.
func ValidateStableKey(key StableKey) error {
	if len(key) == 0 {
		return newInvalidStableKeyError(key, "empty")
	}
	if len(key) > MaxStableKeyBytes {
		return newInvalidStableKeyError(key, "too long")
	}
	if !utf8.ValidString(string(key)) {
		return newInvalidStableKeyError(key, "invalid UTF-8")
	}
	return nil
}

// ValidateOrderedID validates the storage-name components of id and its opaque
// StableKey. Namespace and OrderingScope deliberately retain ValidateName's
// canonical path grammar; StableKey deliberately does not.
func ValidateOrderedID(id OrderedID) error {
	if err := ValidateName(id.Namespace); err != nil {
		return err
	}
	if err := ValidateName(id.OrderingScope); err != nil {
		return err
	}
	return ValidateStableKey(id.StableKey)
}

// ValidateDue validates Due's discriminated state and its canonical NotDue
// representation.
func ValidateDue(due Due) error {
	switch due.State {
	case NotDue:
		if due.UnixMillis != 0 {
			return &InvalidDueError{Due: due, Rule: "not due must have zero UnixMillis"}
		}
	case DueAt:
		return nil
	default:
		return &InvalidDueError{Due: due, Rule: "unknown due state"}
	}
	return nil
}

// ValidateOrderedValue reports whether value falls within OrderedIndex's
// required value capacity. Nil and empty values are valid.
func ValidateOrderedValue(value []byte) error {
	if len(value) > MaxOrderedValueBytes {
		return &OrderedValueTooLargeError{Size: len(value), Max: MaxOrderedValueBytes}
	}
	return nil
}

// ValidateOrderedLimit reports whether limit is a valid OrderedIndex page
// limit.
func ValidateOrderedLimit(limit int) error {
	if limit <= 0 || limit > MaxOrderedPageLimit {
		return &InvalidOrderedLimitError{Limit: limit, Max: MaxOrderedPageLimit}
	}
	return nil
}

// ValidateOrderedRecord validates an OrderedRecord's externally observable
// representation. Providers use it when constructing a record snapshot and
// callers may use it before asserting a returned record in their own code.
func ValidateOrderedRecord(record OrderedRecord) error {
	if err := ValidateOrderedID(record.ID); err != nil {
		return err
	}
	if record.Revision == 0 {
		return &InvalidOrderedRecordError{ID: record.ID, Rule: "revision must be nonzero"}
	}
	if err := ValidateName(record.RankingScope); err != nil {
		return err
	}
	if err := ValidateDue(record.Due); err != nil {
		return err
	}
	if err := ValidateOrderedValue(record.Value); err != nil {
		return err
	}
	if record.Deleted && (record.Rank != (Rank{}) || record.Due != (Due{State: NotDue, UnixMillis: 0})) {
		return &InvalidOrderedRecordError{ID: record.ID, Rule: "deleted record must have zero rank and canonical not-due state"}
	}
	if record.Order == 0 {
		return &InvalidOrderedRecordError{ID: record.ID, Rule: "order must be nonzero"}
	}
	return nil
}
