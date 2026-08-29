package memstore

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/looprig/storage"
)

const (
	// orderedCursorVersionField is the encoded payload's version token. It is
	// carried inside the payload as well as in the token header so a decoder
	// never infers a version from the header alone.
	orderedCursorVersionField = "1"

	rankedCursorTokenKind = "ranked"
	dueCursorTokenKind    = "due"

	rankedCursorHeader = "v1:r:"
	dueCursorHeader    = "v1:d:"

	// orderedCursorFieldCount is the exact field count of both encoded cursor
	// payloads: version, kind, and five query/position fields.
	orderedCursorFieldCount = 7

	// maxOrderedCursorBytes is deliberately well above the largest valid
	// cursor generated from the contract's bounded names and stable key, while
	// keeping untrusted parsing allocation-bounded.
	maxOrderedCursorBytes = 8 << 10
	maxOrderedUint64      = ^uint64(0)
)

// orderedIdentity is the comparable in-memory representation of an ordered
// record identity. The public OrderedID remains embedded in every stored
// record; this key exists solely for the backing maps and current-index slices.
type orderedIdentity struct {
	namespace     string
	orderingScope string
	stableKey     storage.StableKey
}

// orderedScope identifies the independent immutable-order sequence shared by
// all identities in one namespace and ordering scope.
type orderedScope struct {
	namespace     string
	orderingScope string
}

// rankedScope identifies one current ranked view. RankingScope is deliberately
// separate from an OrderedID's OrderingScope: the former selects a cross-order
// catalog, while the latter owns immutable acceptance order.
type rankedScope struct {
	namespace    string
	rankingScope string
}

// contextMutex is a single-permit mutex that lets a waiter abandon lock
// acquisition when its context ends. Unlike sync.Mutex and sync.RWMutex, its
// wait path selects directly on ctx.Done, so orderedStore never begins a public
// operation after the caller gave up while another operation holds the store.
type contextMutex struct {
	permit chan struct{}
}

func newContextMutex() contextMutex {
	permit := make(chan struct{}, 1)
	permit <- struct{}{}
	return contextMutex{permit: permit}
}

func (m *contextMutex) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.permit:
	}
	if err := ctx.Err(); err != nil {
		m.unlock()
		return err
	}
	return nil
}

func (m *contextMutex) unlock() {
	m.permit <- struct{}{}
}

// orderedStore is the in-memory OrderedIndex reference provider. Its one mutex
// protects authoritative records, scope high-water marks, and all three current
// indexes as one linearizable state transition. The index slices contain only
// identities; record state lives authoritatively in records and is copied out
// before every public return.
type orderedStore struct {
	mu contextMutex

	records   map[orderedIdentity]storage.OrderedRecord
	highWater map[orderedScope]uint64
	ordered   map[orderedScope][]orderedIdentity
	ranked    map[rankedScope][]orderedIdentity
	due       map[string][]orderedIdentity
}

// newOrderedStore returns an empty orderedStore.
func newOrderedStore() *orderedStore {
	return &orderedStore{
		mu:        newContextMutex(),
		records:   make(map[orderedIdentity]storage.OrderedRecord),
		highWater: make(map[orderedScope]uint64),
		ordered:   make(map[orderedScope][]orderedIdentity),
		ranked:    make(map[rankedScope][]orderedIdentity),
		due:       make(map[string][]orderedIdentity),
	}
}

// Compile-time proof that *orderedStore honors the OrderedIndex contract.
var _ storage.OrderedIndex = (*orderedStore)(nil)

// Get returns a caller-owned snapshot of id's current record, including a
// tombstone. It validates identity before lookup and never exposes the backing
// Value slice.
func (s *orderedStore) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, err
	}

	if err := s.mu.lock(ctx); err != nil {
		return storage.OrderedRecord{}, err
	}
	defer s.mu.unlock()
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, err
	}

	record, ok := s.records[orderedIdentityFor(id)]
	if !ok {
		return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: id}
	}
	return cloneOrderedRecord(record), nil
}

// Create atomically returns the existing canonical record for a duplicate or,
// after validating a previously absent candidate, allocates the next immutable
// order in id's order scope. Candidate validation deliberately occurs after the
// duplicate lookup, as required for idempotent retry behavior.
func (s *orderedStore) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, false, err
	}

	key := orderedIdentityFor(id)
	if err := s.mu.lock(ctx); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	defer s.mu.unlock()
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	if record, ok := s.records[key]; ok {
		return cloneOrderedRecord(record), false, nil
	}
	if err := validateOrderedCandidate(rankingScope, value, due); err != nil {
		return storage.OrderedRecord{}, false, err
	}

	scope := orderedScopeFor(id)
	order := s.highWater[scope]
	if order == maxOrderedUint64 {
		return storage.OrderedRecord{}, false, &orderedOrderExhaustedError{}
	}
	order++
	record := storage.OrderedRecord{
		ID:           id,
		RankingScope: rankingScope,
		Revision:     1,
		Order:        order,
		Due:          due,
		Rank:         rank,
		Value:        cloneOrderedBytes(value),
	}
	s.records[key] = record
	s.highWater[scope] = order
	s.ordered[scope] = append(s.ordered[scope], key)
	s.insertCurrentIndexesLocked(key, record)
	return cloneOrderedRecord(record), true, nil
}

// Update performs one live-record revision CAS. An absent record and a
// tombstone take precedence over candidate validation; for a live record the
// candidate is validated before a stale expected revision is reported.
func (s *orderedStore) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, err
	}

	key := orderedIdentityFor(id)
	if err := s.mu.lock(ctx); err != nil {
		return storage.OrderedRecord{}, err
	}
	defer s.mu.unlock()
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, err
	}
	record, ok := s.records[key]
	if !ok {
		return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: id}
	}
	if record.Deleted {
		return storage.OrderedRecord{}, &storage.OrderedDeletedError{ID: id}
	}
	if err := validateOrderedUpdateCandidate(value, due); err != nil {
		return storage.OrderedRecord{}, err
	}
	if expectedRevision != record.Revision {
		return storage.OrderedRecord{}, &storage.OrderedRevisionConflictError{
			ID:               id,
			ExpectedRevision: expectedRevision,
			ActualRevision:   record.Revision,
		}
	}
	if record.Revision == maxOrderedUint64 {
		return storage.OrderedRecord{}, &storage.OrderedRevisionExhaustedError{ID: id, Revision: record.Revision}
	}

	s.removeCurrentIndexesLocked(key, record)
	record.Revision++
	record.Value = cloneOrderedBytes(value)
	record.Rank = rank
	record.Due = due
	s.records[key] = record
	s.insertCurrentIndexesLocked(key, record)
	return cloneOrderedRecord(record), nil
}

// Delete turns a live record into its terminal tombstone in one revision CAS.
// It retains the authoritative record and its acceptance-order entry forever in
// the memory store so retries can return the original canonical tombstone and
// the identity can never be reused.
func (s *orderedStore) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, err
	}

	key := orderedIdentityFor(id)
	if err := s.mu.lock(ctx); err != nil {
		return storage.OrderedRecord{}, err
	}
	defer s.mu.unlock()
	if err := ctx.Err(); err != nil {
		return storage.OrderedRecord{}, err
	}
	record, ok := s.records[key]
	if !ok {
		return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: id}
	}
	if record.Deleted {
		return cloneOrderedRecord(record), nil
	}
	if expectedRevision != record.Revision {
		return storage.OrderedRecord{}, &storage.OrderedRevisionConflictError{
			ID:               id,
			ExpectedRevision: expectedRevision,
			ActualRevision:   record.Revision,
		}
	}
	if record.Revision == maxOrderedUint64 {
		return storage.OrderedRecord{}, &storage.OrderedRevisionExhaustedError{ID: id, Revision: record.Revision}
	}

	s.removeCurrentIndexesLocked(key, record)
	record.Revision++
	record.Deleted = true
	record.Rank = storage.Rank{}
	record.Due = storage.Due{State: storage.NotDue}
	s.records[key] = record
	return cloneOrderedRecord(record), nil
}

// ListOrdered reads the per-order-scope immutable index after the direct
// exclusive numeric cursor. The index includes tombstones and is append-only,
// so a page does no scan of unrelated records or historical namespaces.
func (s *orderedStore) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.OrderedPage{}, err
	}
	if err := storage.ValidateName(orderingScope); err != nil {
		return storage.OrderedPage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.OrderedPage{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.OrderedPage{}, err
	}

	if err := s.mu.lock(ctx); err != nil {
		return storage.OrderedPage{}, err
	}
	defer s.mu.unlock()
	if err := ctx.Err(); err != nil {
		return storage.OrderedPage{}, err
	}

	entries := s.ordered[orderedScope{namespace: namespace, orderingScope: orderingScope}]
	start := orderedStartAfter(s.records, entries, afterOrder)
	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}
	page := storage.OrderedPage{Records: make([]storage.OrderedRecord, 0, end-start)}
	for _, key := range entries[start:end] {
		if err := ctx.Err(); err != nil {
			return storage.OrderedPage{}, err
		}
		record := s.records[key]
		page.Records = append(page.Records, cloneOrderedRecord(record))
	}
	if len(page.Records) > 0 {
		page.NextAfterOrder = page.Records[len(page.Records)-1].Order
	}
	return page, nil
}

// ListRanked reads the maintained current ranked view for exactly one
// (namespace, ranking scope) query. It neither filters the full record map nor
// retains returned Value slices, and its continuation token binds the exact
// query plus the last descending rank tuple.
func (s *orderedStore) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.RankedPage{}, err
	}
	if err := storage.ValidateName(rankingScope); err != nil {
		return storage.RankedPage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.RankedPage{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.RankedPage{}, err
	}
	position, hasPosition, err := s.decodeRankedCursor(after, namespace, rankingScope)
	if err != nil {
		return storage.RankedPage{}, err
	}

	if err := s.mu.lock(ctx); err != nil {
		return storage.RankedPage{}, err
	}
	defer s.mu.unlock()
	if err := ctx.Err(); err != nil {
		return storage.RankedPage{}, err
	}

	entries := s.ranked[rankedScope{namespace: namespace, rankingScope: rankingScope}]
	start := 0
	if hasPosition {
		start = rankedStartAfter(s.records, entries, position)
	}
	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}
	page := storage.RankedPage{Records: make([]storage.OrderedRecord, 0, end-start)}
	for _, key := range entries[start:end] {
		if err := ctx.Err(); err != nil {
			return storage.RankedPage{}, err
		}
		page.Records = append(page.Records, cloneOrderedRecord(s.records[key]))
	}
	if end < len(entries) {
		page.NextCursor = encodeRankedCursor(rankedPositionFor(page.Records[len(page.Records)-1]))
	}
	return page, nil
}

// ListDue reads the maintained current due view for exactly one namespace. The
// sorted index lets it stop at dueAtOrBefore rather than inspecting later due
// records or any non-due/tombstoned record, while the cursor binds the fixed
// due bound and its final ascending tuple.
func (s *orderedStore) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.DuePage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.DuePage{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.DuePage{}, err
	}
	position, hasPosition, err := s.decodeDueCursor(after, namespace, dueAtOrBefore)
	if err != nil {
		return storage.DuePage{}, err
	}

	if err := s.mu.lock(ctx); err != nil {
		return storage.DuePage{}, err
	}
	defer s.mu.unlock()
	if err := ctx.Err(); err != nil {
		return storage.DuePage{}, err
	}

	entries := s.due[namespace]
	start := 0
	if hasPosition {
		start = dueStartAfter(s.records, entries, position)
	}
	eligibleEnd := dueEndAtOrBefore(s.records, entries, dueAtOrBefore)
	if start > eligibleEnd {
		start = eligibleEnd
	}
	end := start + limit
	if end > eligibleEnd {
		end = eligibleEnd
	}
	page := storage.DuePage{Records: make([]storage.OrderedRecord, 0, end-start)}
	for _, key := range entries[start:end] {
		if err := ctx.Err(); err != nil {
			return storage.DuePage{}, err
		}
		page.Records = append(page.Records, cloneOrderedRecord(s.records[key]))
	}
	if end < eligibleEnd {
		page.NextCursor = encodeDueCursor(duePositionFor(dueAtOrBefore, page.Records[len(page.Records)-1]))
	}
	return page, nil
}

// validateOrderedCandidate validates the fields that only matter for an absent
// Create candidate. Rank has no invalid representation: Ranked controls whether
// its signed Value participates in the current ranked view.
func validateOrderedCandidate(rankingScope string, value []byte, due storage.Due) error {
	if err := storage.ValidateName(rankingScope); err != nil {
		return err
	}
	return validateOrderedUpdateCandidate(value, due)
}

// validateOrderedUpdateCandidate validates the mutable fields that Update
// checks only after it has established the target is live.
func validateOrderedUpdateCandidate(value []byte, due storage.Due) error {
	if err := storage.ValidateOrderedValue(value); err != nil {
		return err
	}
	return storage.ValidateDue(due)
}

func orderedIdentityFor(id storage.OrderedID) orderedIdentity {
	return orderedIdentity{
		namespace:     id.Namespace,
		orderingScope: id.OrderingScope,
		stableKey:     id.StableKey,
	}
}

func orderedScopeFor(id storage.OrderedID) orderedScope {
	return orderedScope{namespace: id.Namespace, orderingScope: id.OrderingScope}
}

func cloneOrderedRecord(record storage.OrderedRecord) storage.OrderedRecord {
	record.Value = cloneOrderedBytes(record.Value)
	return record
}

func cloneOrderedBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	return copyValue
}

func (s *orderedStore) insertCurrentIndexesLocked(key orderedIdentity, record storage.OrderedRecord) {
	if record.Rank.Ranked {
		s.insertRankedLocked(key, record)
	}
	if record.Due.State == storage.DueAt {
		s.insertDueLocked(key, record)
	}
}

func (s *orderedStore) removeCurrentIndexesLocked(key orderedIdentity, record storage.OrderedRecord) {
	if record.Rank.Ranked {
		s.removeRankedLocked(key, record)
	}
	if record.Due.State == storage.DueAt {
		s.removeDueLocked(key, record)
	}
}

func (s *orderedStore) insertRankedLocked(key orderedIdentity, record storage.OrderedRecord) {
	scope := rankedScope{namespace: record.ID.Namespace, rankingScope: record.RankingScope}
	entries := s.ranked[scope]
	position := rankedInsertionPoint(s.records, entries, record)
	s.ranked[scope] = insertOrderedIdentity(entries, position, key)
}

func (s *orderedStore) removeRankedLocked(key orderedIdentity, record storage.OrderedRecord) {
	scope := rankedScope{namespace: record.ID.Namespace, rankingScope: record.RankingScope}
	entries, ok := s.ranked[scope]
	if !ok {
		return
	}
	if position := orderedIdentityPosition(entries, key); position >= 0 {
		entries = removeOrderedIdentity(entries, position)
	}
	if len(entries) == 0 {
		delete(s.ranked, scope)
		return
	}
	s.ranked[scope] = entries
}

func (s *orderedStore) insertDueLocked(key orderedIdentity, record storage.OrderedRecord) {
	entries := s.due[record.ID.Namespace]
	position := dueInsertionPoint(s.records, entries, record)
	s.due[record.ID.Namespace] = insertOrderedIdentity(entries, position, key)
}

func (s *orderedStore) removeDueLocked(key orderedIdentity, record storage.OrderedRecord) {
	entries, ok := s.due[record.ID.Namespace]
	if !ok {
		return
	}
	if position := orderedIdentityPosition(entries, key); position >= 0 {
		entries = removeOrderedIdentity(entries, position)
	}
	if len(entries) == 0 {
		delete(s.due, record.ID.Namespace)
		return
	}
	s.due[record.ID.Namespace] = entries
}

func insertOrderedIdentity(entries []orderedIdentity, position int, key orderedIdentity) []orderedIdentity {
	entries = append(entries, orderedIdentity{})
	copy(entries[position+1:], entries[position:])
	entries[position] = key
	return entries
}

func removeOrderedIdentity(entries []orderedIdentity, position int) []orderedIdentity {
	copy(entries[position:], entries[position+1:])
	entries[len(entries)-1] = orderedIdentity{}
	return entries[:len(entries)-1]
}

func orderedIdentityPosition(entries []orderedIdentity, want orderedIdentity) int {
	for position, key := range entries {
		if key == want {
			return position
		}
	}
	return -1
}

func orderedStartAfter(records map[orderedIdentity]storage.OrderedRecord, entries []orderedIdentity, afterOrder uint64) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		if records[entries[middle]].Order <= afterOrder {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func rankedInsertionPoint(records map[orderedIdentity]storage.OrderedRecord, entries []orderedIdentity, record storage.OrderedRecord) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		if rankedBefore(records[entries[middle]], record) {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func dueInsertionPoint(records map[orderedIdentity]storage.OrderedRecord, entries []orderedIdentity, record storage.OrderedRecord) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		if dueBefore(records[entries[middle]], record) {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// rankedBefore is the frozen descending total order: larger rank first, then
// lexicographically larger stable key, then lexicographically larger ordering
// scope. Namespace and RankingScope are fixed by a ranked index key.
func rankedBefore(left storage.OrderedRecord, right storage.OrderedRecord) bool {
	if left.Rank.Value != right.Rank.Value {
		return left.Rank.Value > right.Rank.Value
	}
	if left.ID.StableKey != right.ID.StableKey {
		return left.ID.StableKey > right.ID.StableKey
	}
	return left.ID.OrderingScope > right.ID.OrderingScope
}

// dueBefore is the frozen ascending total order: earlier due time first, then
// lexicographically smaller stable key, then lexicographically smaller ordering
// scope. Namespace is fixed by a due index key.
func dueBefore(left storage.OrderedRecord, right storage.OrderedRecord) bool {
	if left.Due.UnixMillis != right.Due.UnixMillis {
		return left.Due.UnixMillis < right.Due.UnixMillis
	}
	if left.ID.StableKey != right.ID.StableKey {
		return left.ID.StableKey < right.ID.StableKey
	}
	return left.ID.OrderingScope < right.ID.OrderingScope
}

type rankedCursorPosition struct {
	namespace     string
	rankingScope  string
	rank          int64
	stableKey     storage.StableKey
	orderingScope string
}

func rankedPositionFor(record storage.OrderedRecord) rankedCursorPosition {
	return rankedCursorPosition{
		namespace:     record.ID.Namespace,
		rankingScope:  record.RankingScope,
		rank:          record.Rank.Value,
		stableKey:     record.ID.StableKey,
		orderingScope: record.ID.OrderingScope,
	}
}

func rankedStartAfter(records map[orderedIdentity]storage.OrderedRecord, entries []orderedIdentity, position rankedCursorPosition) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		if rankedPositionBeforeRecord(position, records[entries[middle]]) {
			high = middle
		} else {
			low = middle + 1
		}
	}
	return low
}

func rankedPositionBeforeRecord(position rankedCursorPosition, record storage.OrderedRecord) bool {
	if position.rank != record.Rank.Value {
		return position.rank > record.Rank.Value
	}
	if position.stableKey != record.ID.StableKey {
		return position.stableKey > record.ID.StableKey
	}
	return position.orderingScope > record.ID.OrderingScope
}

type dueCursorPosition struct {
	namespace     string
	dueBound      int64
	dueAt         int64
	stableKey     storage.StableKey
	orderingScope string
}

func duePositionFor(dueBound int64, record storage.OrderedRecord) dueCursorPosition {
	return dueCursorPosition{
		namespace:     record.ID.Namespace,
		dueBound:      dueBound,
		dueAt:         record.Due.UnixMillis,
		stableKey:     record.ID.StableKey,
		orderingScope: record.ID.OrderingScope,
	}
}

func dueStartAfter(records map[orderedIdentity]storage.OrderedRecord, entries []orderedIdentity, position dueCursorPosition) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		if duePositionBeforeRecord(position, records[entries[middle]]) {
			high = middle
		} else {
			low = middle + 1
		}
	}
	return low
}

func duePositionBeforeRecord(position dueCursorPosition, record storage.OrderedRecord) bool {
	if position.dueAt != record.Due.UnixMillis {
		return position.dueAt < record.Due.UnixMillis
	}
	if position.stableKey != record.ID.StableKey {
		return position.stableKey < record.ID.StableKey
	}
	return position.orderingScope < record.ID.OrderingScope
}

func dueEndAtOrBefore(records map[orderedIdentity]storage.OrderedRecord, entries []orderedIdentity, dueBound int64) int {
	low, high := 0, len(entries)
	for low < high {
		middle := low + (high-low)/2
		if records[entries[middle]].Due.UnixMillis <= dueBound {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// encodeRankedCursor renders one ranked continuation position as an opaque,
// versioned, query-bound token. Encoding is total: every field is a bounded
// string or integer, so issuance cannot fail. The token carries position, never
// authority; decode re-checks every binding against the live request.
func encodeRankedCursor(position rankedCursorPosition) storage.RankedCursor {
	return storage.RankedCursor(encodeOrderedCursorToken(rankedCursorHeader, []string{
		orderedCursorVersionField,
		rankedCursorTokenKind,
		encodeOrderedCursorField(position.namespace),
		encodeOrderedCursorField(position.rankingScope),
		strconv.FormatInt(position.rank, 10),
		encodeOrderedCursorField(string(position.stableKey)),
		encodeOrderedCursorField(position.orderingScope),
	}))
}

// encodeDueCursor renders one due continuation position, including the fixed
// due bound the page was read with, under the same total encoding.
func encodeDueCursor(position dueCursorPosition) storage.DueCursor {
	return storage.DueCursor(encodeOrderedCursorToken(dueCursorHeader, []string{
		orderedCursorVersionField,
		dueCursorTokenKind,
		encodeOrderedCursorField(position.namespace),
		strconv.FormatInt(position.dueBound, 10),
		strconv.FormatInt(position.dueAt, 10),
		encodeOrderedCursorField(string(position.stableKey)),
		encodeOrderedCursorField(position.orderingScope),
	}))
}

// encodeOrderedCursorToken joins the payload fields and wraps them so callers
// observe one opaque token. Each variable-length field is encoded separately so
// the separator can never appear in a name or stable key.
func encodeOrderedCursorToken(header string, fields []string) string {
	return header + base64.RawURLEncoding.EncodeToString([]byte(strings.Join(fields, ".")))
}

func encodeOrderedCursorField(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeOrderedCursorField(field string) (string, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(field)
	if err != nil {
		return "", false
	}
	return string(decoded), true
}

func (s *orderedStore) decodeRankedCursor(cursor storage.RankedCursor, namespace string, rankingScope string) (rankedCursorPosition, bool, error) {
	if cursor == "" {
		return rankedCursorPosition{}, false, nil
	}
	fields, err := decodeOrderedCursorFields(storage.RankedCursorKind, string(cursor))
	if err != nil {
		return rankedCursorPosition{}, false, err
	}
	position, ok := parseRankedCursorFields(fields)
	if !ok {
		return rankedCursorPosition{}, false, storage.NewInvalidOrderedCursorError(storage.RankedCursorKind, string(cursor), storage.OrderedCursorMalformed)
	}
	if position.namespace != namespace || position.rankingScope != rankingScope {
		return rankedCursorPosition{}, false, storage.NewInvalidOrderedCursorError(storage.RankedCursorKind, string(cursor), storage.OrderedCursorQueryMismatch)
	}
	return position, true, nil
}

func (s *orderedStore) decodeDueCursor(cursor storage.DueCursor, namespace string, dueBound int64) (dueCursorPosition, bool, error) {
	if cursor == "" {
		return dueCursorPosition{}, false, nil
	}
	fields, err := decodeOrderedCursorFields(storage.DueCursorKind, string(cursor))
	if err != nil {
		return dueCursorPosition{}, false, err
	}
	position, ok := parseDueCursorFields(fields)
	if !ok {
		return dueCursorPosition{}, false, storage.NewInvalidOrderedCursorError(storage.DueCursorKind, string(cursor), storage.OrderedCursorMalformed)
	}
	if position.namespace != namespace || position.dueBound != dueBound {
		return dueCursorPosition{}, false, storage.NewInvalidOrderedCursorError(storage.DueCursorKind, string(cursor), storage.OrderedCursorQueryMismatch)
	}
	return position, true, nil
}

// decodeOrderedCursorFields parses an untrusted token into its payload fields.
// It applies the raw length cap before any decoding so an attacker-supplied
// token cannot drive an unbounded allocation, then classifies every rejection
// with the contract's four fail-closed rules. Nothing in the token is trusted:
// the caller compares the decoded namespace, scope, and bound with the live
// request.
func decodeOrderedCursorFields(kind storage.OrderedCursorKind, cursor string) ([]string, error) {
	if len(cursor) > maxOrderedCursorBytes {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	expectedHeader := cursorHeader(kind)
	if hasUnsupportedCursorVersion(cursor) {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorUnknownVersion)
	}
	if !strings.HasPrefix(cursor, expectedHeader) {
		if strings.HasPrefix(cursor, rankedCursorHeader) || strings.HasPrefix(cursor, dueCursorHeader) {
			return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorWrongKind)
		}
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}

	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(cursor, expectedHeader))
	if err != nil {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	fields := strings.Split(string(payload), ".")
	if len(fields) != orderedCursorFieldCount {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	if fields[0] != orderedCursorVersionField {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorUnknownVersion)
	}
	if fields[1] != cursorTokenKind(kind) {
		if fields[1] == rankedCursorTokenKind || fields[1] == dueCursorTokenKind {
			return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorWrongKind)
		}
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	return fields, nil
}

// hasUnsupportedCursorVersion recognizes the version prefix before decoding a
// cursor body. Every syntactically versioned token other than v1 is rejected as
// an unknown version rather than being conflated with a malformed token.
func hasUnsupportedCursorVersion(cursor string) bool {
	if !strings.HasPrefix(cursor, "v") {
		return false
	}
	separator := strings.IndexByte(cursor, ':')
	if separator <= 1 {
		return false
	}
	for _, digit := range cursor[1:separator] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return cursor[1:separator] != "1"
}

func cursorHeader(kind storage.OrderedCursorKind) string {
	if kind == storage.RankedCursorKind {
		return rankedCursorHeader
	}
	return dueCursorHeader
}

func cursorTokenKind(kind storage.OrderedCursorKind) string {
	if kind == storage.RankedCursorKind {
		return rankedCursorTokenKind
	}
	return dueCursorTokenKind
}

func parseRankedCursorFields(fields []string) (rankedCursorPosition, bool) {
	namespace, namespaceOK := decodeOrderedCursorField(fields[2])
	rankingScope, rankingScopeOK := decodeOrderedCursorField(fields[3])
	rank, rankErr := strconv.ParseInt(fields[4], 10, 64)
	stableKey, stableKeyOK := decodeOrderedCursorField(fields[5])
	orderingScope, orderingScopeOK := decodeOrderedCursorField(fields[6])
	if !namespaceOK || !rankingScopeOK || rankErr != nil || !stableKeyOK || !orderingScopeOK {
		return rankedCursorPosition{}, false
	}
	return rankedCursorPosition{
		namespace:     namespace,
		rankingScope:  rankingScope,
		rank:          rank,
		stableKey:     storage.StableKey(stableKey),
		orderingScope: orderingScope,
	}, true
}

func parseDueCursorFields(fields []string) (dueCursorPosition, bool) {
	namespace, namespaceOK := decodeOrderedCursorField(fields[2])
	dueBound, dueBoundErr := strconv.ParseInt(fields[3], 10, 64)
	dueAt, dueAtErr := strconv.ParseInt(fields[4], 10, 64)
	stableKey, stableKeyOK := decodeOrderedCursorField(fields[5])
	orderingScope, orderingScopeOK := decodeOrderedCursorField(fields[6])
	if !namespaceOK || dueBoundErr != nil || dueAtErr != nil || !stableKeyOK || !orderingScopeOK {
		return dueCursorPosition{}, false
	}
	return dueCursorPosition{
		namespace:     namespace,
		dueBound:      dueBound,
		dueAt:         dueAt,
		stableKey:     storage.StableKey(stableKey),
		orderingScope: orderingScope,
	}, true
}

// orderedOrderExhaustedError is fail-closed protection for the otherwise
// unreachable case where an order scope already allocated MaxUint64 entries.
// OrderedIndex's public contract has a typed revision-exhaustion error but no
// separate acceptance-order exhaustion type, so this provider-local typed error
// preserves the nonzero/no-wrap invariant without changing the frozen API.
type orderedOrderExhaustedError struct{}

func (*orderedOrderExhaustedError) Error() string {
	return "memstore: ordered acceptance order exhausted"
}
