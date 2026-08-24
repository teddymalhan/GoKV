package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
)

type DB struct {
	KV KV
}

func NewDB(dirpath string, opts ...KVOption) (*DB, error) {
	options := KVOptions{Dirpath: dirpath}
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, err
		}
	}
	db := &DB{KV: KV{Options: options}}
	return db, db.Open()
}

type DBTX struct {
	kv     *KVTX
	tables map[string]Schema
}

func (db *DB) NewTX() *DBTX {
	return &DBTX{kv: db.KV.NewTX(), tables: map[string]Schema{}}
}

func (tx *DBTX) Abort() { tx.kv.Abort() }

func (tx *DBTX) Commit() error { return tx.kv.Commit() }

func (tx *DBTX) NewTX() *DBTX {
	return &DBTX{kv: tx.kv.NewTX(), tables: map[string]Schema{}}
}

func (db *DB) Open() error { return db.KV.Open() }

func (db *DB) Close() error { return db.KV.Close() }

func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	tx := db.NewTX()
	defer tx.Abort()
	return tx.Select(schema, row)
}

func (tx *DBTX) Select(schema *Schema, row Row) (ok bool, err error) {
	key := row.EncodeKey(schema, 0)
	val, ok, err := tx.kv.Get(key)
	if err != nil || !ok {
		return ok, err
	}
	if err = row.DecodeVal(schema, val); err != nil {
		return false, err
	}
	return true, nil
}

func (tx *DBTX) update(schema *Schema, row Row, mode UpdateMode) (updated bool, err error) {
	key := row.EncodeKey(schema, 0)
	val := row.EncodeVal(schema)
	oldVal, exist, err := tx.kv.Get(key)
	if err != nil {
		return false, err
	}

	switch mode {
	case ModeUpsert:
		updated = !exist || !bytes.Equal(oldVal, val)
	case ModeInsert:
		updated = !exist
	case ModeUpdate:
		updated = exist && !bytes.Equal(oldVal, val)
	default:
		return false, errors.New("invalid update mode")
	}
	if !updated {
		return false, nil
	}

	if exist {
		oldRow := slices.Clone(row)
		if err = oldRow.DecodeVal(schema, oldVal); err != nil {
			return false, err
		}
		if _, err = tx.delete(schema, oldRow); err != nil {
			return false, err
		}
	}

	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		if i > 0 {
			key, val = row.EncodeKey(schema, i), nil
		}
		updated, err = tx.kv.SetEx(key, val, ModeInsert)
		if err == nil && !updated {
			// A raw KV writer can plant a key inside the table keyspace, so a
			// clashing index entry is bad data rather than a broken invariant.
			return false, errors.New("inconsistent index")
		}
	}
	return updated, err
}

func (tx *DBTX) Insert(schema *Schema, row Row) (updated bool, err error) {
	tx = tx.NewTX()
	updated, err = tx.update(schema, row, ModeInsert)
	return abortOrCommit(tx, updated, err)
}

func (tx *DBTX) Upsert(schema *Schema, row Row) (updated bool, err error) {
	tx = tx.NewTX()
	updated, err = tx.update(schema, row, ModeUpsert)
	return abortOrCommit(tx, updated, err)
}

func (tx *DBTX) Update(schema *Schema, row Row) (updated bool, err error) {
	tx = tx.NewTX()
	updated, err = tx.update(schema, row, ModeUpdate)
	return abortOrCommit(tx, updated, err)
}

func (tx *DBTX) delete(schema *Schema, row Row) (deleted bool, err error) {
	for i := 0; i < len(schema.Indices) && err == nil; i++ {
		key := row.EncodeKey(schema, i)
		deleted, err = tx.kv.Del(key)
		if err == nil && !deleted {
			if i != 0 {
				return false, errors.New("inconsistent index")
			}
			break
		}
	}
	return deleted, err
}

func (tx *DBTX) Delete(schema *Schema, row Row) (deleted bool, err error) {
	tx = tx.NewTX()
	deleted, err = tx.delete(schema, row)
	return abortOrCommit(tx, deleted, err)
}

func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	tx := db.NewTX()
	updated, err = tx.update(schema, row, ModeInsert)
	return abortOrCommit(tx, updated, err)
}

func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	tx := db.NewTX()
	updated, err = tx.update(schema, row, ModeUpsert)
	return abortOrCommit(tx, updated, err)
}

func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	tx := db.NewTX()
	updated, err = tx.update(schema, row, ModeUpdate)
	return abortOrCommit(tx, updated, err)
}

func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	tx := db.NewTX()
	deleted, err = tx.delete(schema, row)
	return abortOrCommit(tx, deleted, err)
}

type RowIterator struct {
	tx      *DBTX
	schema  *Schema
	indexNo int
	iter    *RangedKVIter
	valid   bool
	row     Row
}

func (iter *RowIterator) decodeKVIter() (bool, error) {
	if !iter.iter.Valid() {
		return false, nil
	}
	if err := iter.row.DecodeKey(iter.schema, iter.indexNo, iter.iter.Key()); err != nil {
		return false, err
	}
	if iter.indexNo > 0 {
		ok, err := iter.tx.Select(iter.schema, iter.row)
		if err != nil {
			return false, err
		} else if !ok {
			return false, errors.New("inconsistent index")
		}
	} else {
		if err := iter.row.DecodeVal(iter.schema, iter.iter.Val()); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (iter *RowIterator) Valid() bool {
	return iter.valid
}

func (iter *RowIterator) Row() Row {
	check(iter.valid)
	return iter.row
}

func (iter *RowIterator) Next() (err error) {
	if err = iter.iter.Next(); err != nil {
		return err
	}
	iter.valid, err = iter.decodeKVIter()
	return err
}

func (tx *DBTX) Seek(schema *Schema, row Row) (*RowIterator, error) {
	if len(row) != len(schema.Cols) {
		return nil, errors.New("row does not match the schema")
	}
	start := make([]Cell, len(schema.Indices[0]))
	for i, idx := range schema.Indices[0] {
		if row[idx].Type != schema.Cols[idx].Type {
			return nil, errors.New("row does not match the schema")
		}
		start[i] = row[idx]
	}
	return tx.Range(schema, &RangeReq{
		StartCmp: OP_GE,
		StopCmp:  OP_LE, // +inf
		Start:    start,
		Stop:     nil,
		IndexNo:  0,
	})
}

type RangeReq struct {
	StartCmp ExprOp // <= >= < >
	StopCmp  ExprOp
	Start    []Cell
	Stop     []Cell
	IndexNo  int
}

// suffixPositive reports whether the encoded bound must sort after every key
// sharing its prefix. The second result is false for an operator that cannot
// bound a range.
func suffixPositive(op ExprOp) (bool, bool) {
	switch op {
	case OP_LE, OP_GT:
		return true, true
	case OP_GE, OP_LT:
		return false, true
	default:
		return false, false
	}
}

func isDescending(op ExprOp) (bool, bool) {
	switch op {
	case OP_LE, OP_LT:
		return true, true
	case OP_GE, OP_GT:
		return false, true
	default:
		return false, false
	}
}

func (tx *DBTX) Range(schema *Schema, req *RangeReq) (out *RowIterator, err error) {
	startPos, ok1 := suffixPositive(req.StartCmp)
	stopPos, ok2 := suffixPositive(req.StopCmp)
	desc, _ := isDescending(req.StartCmp)
	stopDesc, _ := isDescending(req.StopCmp)
	if !ok1 || !ok2 || desc == stopDesc {
		return nil, errors.New("invalid range request")
	}
	start, err := EncodeKeyPrefix(schema, req.IndexNo, req.Start, startPos)
	if err != nil {
		return nil, err
	}
	stop, err := EncodeKeyPrefix(schema, req.IndexNo, req.Stop, stopPos)
	if err != nil {
		return nil, err
	}
	out = &RowIterator{tx: tx, schema: schema, indexNo: req.IndexNo, row: schema.NewRow()}
	if out.iter, err = tx.kv.Range(start, stop, desc); err != nil {
		return nil, err
	}
	if out.valid, err = out.decodeKVIter(); err != nil {
		return nil, err
	}
	return out, nil
}

// ColumnDesc describes one column of a SELECT result. It is available before
// the first row is fetched.
type ColumnDesc struct {
	Name string
	Type CellType
}

// SQLResult is the result of one statement.
//
// A SELECT streams rows: iterate with Next/Row, then check Err. It holds a read
// transaction open until Close, so Close is mandatory and must be called even
// when the rows are not drained. Every other statement is already applied when
// the result is handed out; it only carries RowsAffected and Close is a no-op.
type SQLResult struct {
	tx       *DBTX
	cols     []ColumnDesc
	cursor   *selectCursor
	affected uint64
	err      error
	closed   bool
}

// Columns returns the result columns of a SELECT, or nil for other statements.
func (r *SQLResult) Columns() []ColumnDesc { return r.cols }

// RowsAffected returns how many rows an INSERT, UPDATE or DELETE changed.
func (r *SQLResult) RowsAffected() uint64 { return r.affected }

// Next advances to the next row. It returns false at the end of the result and
// on error; the caller must then consult Err.
func (r *SQLResult) Next() bool {
	if r.cursor == nil || r.closed || r.err != nil {
		return false
	}
	ok, err := r.cursor.next()
	if err != nil {
		r.err = err
		return false
	}
	return ok
}

// Row returns the row produced by the last successful Next. The backing memory
// is reused, so clone it to keep it past the next call.
func (r *SQLResult) Row() Row {
	if r.cursor == nil {
		return nil
	}
	return r.cursor.out
}

func (r *SQLResult) Err() error { return r.err }

// Close releases the transaction held by a streaming result. It is idempotent
// and reports only failures of the release itself; a failed query keeps
// reporting through Err.
func (r *SQLResult) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.cursor = nil
	tx := r.tx
	r.tx = nil
	if tx == nil {
		return nil
	}
	if r.err != nil {
		tx.Abort()
		return nil
	}
	return tx.Commit()
}

// Rows drains the whole result. It is a convenience for callers that do not
// care about streaming; it does not close the result.
func (r *SQLResult) Rows() (out []Row, err error) {
	for r.Next() {
		out = append(out, slices.Clone(r.Row()))
	}
	return out, r.Err()
}

func (tx *DBTX) execStmt(stmt any) (*SQLResult, error) {
	switch ptr := stmt.(type) {
	case *StmtCreatTable:
		return &SQLResult{}, tx.execCreateTable(ptr)
	case *StmtDropTable:
		return &SQLResult{}, tx.execDropTable(ptr)
	case *StmtSelect:
		return tx.execSelect(ptr)
	case *StmtInsert:
		count, err := tx.execInsert(ptr)
		return &SQLResult{affected: count}, err
	case *StmtUpdate:
		count, err := tx.execUpdate(ptr)
		return &SQLResult{affected: count}, err
	case *StmtDelete:
		count, err := tx.execDelete(ptr)
		return &SQLResult{affected: count}, err
	default:
		return nil, errors.New("unsupported statement")
	}
}

// execStmtOwned runs stmt in tx and transfers the ownership of tx to the
// result: a streaming result commits on Close, anything else commits at once.
func execStmtOwned(tx *DBTX, stmt any) (*SQLResult, error) {
	r, err := tx.execStmt(stmt)
	if err != nil {
		tx.Abort()
		return nil, err
	}
	if r.cursor == nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return r, nil
	}
	r.tx = tx
	return r, nil
}

func (tx *DBTX) ExecStmt(stmt any) (*SQLResult, error) {
	return execStmtOwned(tx.NewTX(), stmt)
}

func (db *DB) ExecStmt(stmt any) (*SQLResult, error) {
	return execStmtOwned(db.NewTX(), stmt)
}

// Query parses and executes a single SQL statement. The caller must Close the
// result.
func (db *DB) Query(statement string) (*SQLResult, error) {
	stmt, err := ParseStmt(statement)
	if err != nil {
		return nil, err
	}
	return db.ExecStmt(stmt)
}

// Query parses and executes a single SQL statement inside tx.
func (tx *DBTX) Query(statement string) (*SQLResult, error) {
	stmt, err := ParseStmt(statement)
	if err != nil {
		return nil, err
	}
	return tx.ExecStmt(stmt)
}

// ReservedKeyPrefix guards the keyspace holding engine metadata such as table
// schemas. No table can collide with it: a table name always starts with a
// letter or an underscore. It is exported so the network layer can refuse to
// serve these keys -- in-process callers own their own data, but a remote KV
// client overwriting a schema would corrupt every table built on it.
const ReservedKeyPrefix = "\x00pallasdb\x00"

// IsReservedKey reports whether key belongs to the engine rather than the user.
func IsReservedKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ReservedKeyPrefix))
}

func schemaKey(table string) []byte {
	return []byte(ReservedKeyPrefix + "schema\x00" + table)
}

func (tx *DBTX) execCreateTable(stmt *StmtCreatTable) (err error) {
	if _, err := tx.GetSchema(stmt.table); err == nil {
		return errors.New("duplicate table name")
	}

	schema := Schema{
		Version: SchemaVersion,
		Table:   stmt.table,
		Cols:    stmt.cols,
	}
	if len(schema.Cols) == 0 {
		return errors.New("expect at least one column")
	}
	for i, names := range append([][]string{stmt.pkey}, stmt.indices...) {
		index, err := lookupColumns(stmt.cols, names)
		if err != nil {
			return err
		}
		if len(index) == 0 {
			return errors.New("expect at least one key column")
		}
		if i > 0 {
			index = addPKeyToIndex(index, schema.Indices[0])
		}
		schema.Indices = append(schema.Indices, index)
	}
	if err := schema.Validate(); err != nil {
		return err
	}

	val, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	if _, err = tx.kv.Set(schemaKey(stmt.table), val); err != nil {
		return err
	}

	tx.tables[schema.Table] = schema
	return nil
}

// execDropTable removes every row of the table, all of its index entries and
// finally its schema.
func (tx *DBTX) execDropTable(stmt *StmtDropTable) error {
	schema, err := tx.GetSchema(stmt.table)
	if err != nil {
		return err
	}
	err = tx.forEachMatch(&schema, nil, func(row Row) error {
		_, err := tx.Delete(&schema, row)
		return err
	})
	if err != nil {
		return err
	}
	if _, err := tx.kv.Del(schemaKey(stmt.table)); err != nil {
		return err
	}
	delete(tx.tables, stmt.table)
	return nil
}

func (tx *DBTX) GetSchema(table string) (Schema, error) {
	schema, ok := tx.tables[table]
	if !ok {
		val, ok, err := tx.kv.Get(schemaKey(table))
		if err == nil && ok {
			err = json.Unmarshal(val, &schema)
		}
		if err != nil {
			return Schema{}, err
		}
		if !ok {
			return Schema{}, errors.New("table is not found")
		}
		if err := schema.Validate(); err != nil {
			return Schema{}, err
		}
		tx.tables[table] = schema
	}
	return schema, nil
}

func (db *DB) GetSchema(table string) (Schema, error) {
	tx := db.NewTX()
	defer tx.Abort()
	return tx.GetSchema(table)
}

func addPKeyToIndex(index []int, pkey []int) []int {
	for _, idx := range pkey {
		if !slices.Contains(index, idx) {
			index = append(index, idx)
		}
	}
	return index
}

func lookupColumns(cols []Column, names []string) (indices []int, err error) {
	for _, name := range names {
		idx := slices.IndexFunc(cols, func(col Column) bool {
			return col.Name == name
		})
		if idx < 0 {
			return nil, errors.New("column is not found")
		}
		indices = append(indices, idx)
	}
	return
}

// maxPlanConjuncts bounds how many AND operands the planner inspects. Beyond
// it the rest of the tree stays one opaque residual predicate, which keeps
// planning linear on adversarial input.
const maxPlanConjuncts = 32

// flattenAnd splits a condition into its AND operands without recursing, so a
// long `a AND a AND ...` chain cannot overflow the stack.
func flattenAnd(cond any) []any {
	out := make([]any, 0, 4)
	stack := []any{cond}
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		binop, ok := node.(*ExprBinOp)
		// Splitting one node adds one operand overall; stop splitting once the
		// budget is spent and keep the rest of the tree as one operand.
		if ok && binop.op == OP_AND && len(out)+len(stack)+2 <= maxPlanConjuncts {
			stack = append(stack, binop.right, binop.left)
			continue
		}
		out = append(out, node)
	}
	return out
}

// andExcept rebuilds the conjunction of every operand whose position is not in
// used. It returns nil when nothing is left.
func andExcept(conj []any, used []int) any {
	var out any
	for i, node := range conj {
		if slices.Contains(used, i) {
			continue
		}
		if out == nil {
			out = node
		} else {
			out = &ExprBinOp{op: OP_AND, left: out, right: node}
		}
	}
	return out
}

// matchEq matches `column = value` in either operand order.
func matchEq(cond any) (NamedCell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if !ok || binop.op != OP_EQ {
		return NamedCell{}, false
	}
	left, right := binop.left, binop.right
	name, ok := left.(string)
	if !ok {
		left, right = right, left
		name, ok = left.(string)
	}
	if !ok {
		return NamedCell{}, false
	}
	cell, ok := right.(*Cell)
	if !ok {
		return NamedCell{}, false
	}
	return NamedCell{name, *cell}, true
}

func asNameList(expr any) (out []string, ok bool) {
	switch e := expr.(type) {
	case string:
		return []string{e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(string); ok {
				out = append(out, s)
			} else {
				return nil, false
			}
		}
		return out, true
	}
	return nil, false
}

func asCellList(expr any) (out []Cell, ok bool) {
	switch e := expr.(type) {
	case *Cell:
		return []Cell{*e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(*Cell); ok {
				out = append(out, *s)
			} else {
				return nil, false
			}
		}
		return out, true
	}
	return nil, false
}

func matchCmp(cond any) (ExprOp, []string, []Cell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if !ok {
		return 0, nil, nil, false
	}
	switch binop.op {
	case OP_LE, OP_GE, OP_LT, OP_GT:
	default:
		return 0, nil, nil, false
	}

	op := binop.op
	left, right := binop.left, binop.right
	names, ok := asNameList(left)
	if !ok {
		left, right = right, left
		names, ok = asNameList(left)
		switch op {
		case OP_LE:
			op = OP_GE
		case OP_GE:
			op = OP_LE
		case OP_LT:
			op = OP_GT
		case OP_GT:
			op = OP_LT
		}
	}
	if !ok {
		return 0, nil, nil, false
	}
	cells, ok := asCellList(right)
	if !ok {
		return 0, nil, nil, false
	}
	return op, names, cells, true
}

// isPKeyPrefix reports whether cols/cells name a leading subset of the given
// index. The bound is the arity of that index, not the column count of the
// table: they are unrelated and mixing them reads past the index.
func isPKeyPrefix(schema *Schema, indexNo int, cols []string, cells []Cell) bool {
	if indexNo < 0 || indexNo >= len(schema.Indices) {
		return false
	}
	index := schema.Indices[indexNo]
	if len(cols) != len(cells) || len(cols) == 0 || len(cols) > len(index) {
		return false
	}
	for i := range cols {
		col := schema.Cols[index[i]]
		if col.Name != cols[i] || col.Type != cells[i].Type {
			return false
		}
	}
	return true
}

func matchRangeByIndex(schema *Schema, indexNo int, cond any) (*RangeReq, bool) {
	binop, ok := cond.(*ExprBinOp)
	if ok && binop.op == OP_AND {
		op1, cols1, cells1, ok := matchCmp(binop.left)
		if !ok || !isPKeyPrefix(schema, indexNo, cols1, cells1) {
			return nil, false
		}
		op2, cols2, cells2, ok := matchCmp(binop.right)
		if !ok || !isPKeyPrefix(schema, indexNo, cols2, cells2) {
			return nil, false
		}
		desc1, _ := isDescending(op1)
		desc2, _ := isDescending(op2)
		if desc1 == desc2 {
			return nil, false
		}
		if desc1 {
			op1, op2, cells1, cells2 = op2, op1, cells2, cells1
		}
		return &RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     cells2,
			IndexNo:  indexNo,
		}, true
	} else if ok {
		op1, cols1, cells1, ok := matchCmp(cond)
		if !ok || !isPKeyPrefix(schema, indexNo, cols1, cells1) {
			return nil, false
		}
		op2 := OP_LE
		if desc, _ := isDescending(op1); desc {
			op2 = OP_GE
		}
		return &RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     nil,
			IndexNo:  indexNo,
		}, true
	}
	return nil, false
}

// scanPlan is the physical plan of a WHERE clause: the key range to scan plus
// the part of the predicate the range cannot express.
type scanPlan struct {
	req      *RangeReq
	residual any  // nil when the range alone answers the condition
	indexed  bool // false for a full table scan
}

func fullScanReq() *RangeReq {
	return &RangeReq{StartCmp: OP_GE, StopCmp: OP_LE, IndexNo: 0}
}

// pickEqPrefix greedily matches equality operands against the leading columns
// of an index. A full primary key match pins down a single row; a shorter match
// still narrows the scan to one key prefix.
func pickEqPrefix(schema *Schema, indexNo int, conj []any) (cells []Cell, used []int) {
	index := schema.Indices[indexNo]
	cells = make([]Cell, 0, len(index))
	used = make([]int, 0, len(index))
	for _, idx := range index {
		col := schema.Cols[idx]
		found := false
		for i, node := range conj {
			if slices.Contains(used, i) {
				continue
			}
			named, ok := matchEq(node)
			if ok && named.column == col.Name && named.value.Type == col.Type {
				cells = append(cells, named.value)
				used = append(used, i)
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	return cells, used
}

// planScan picks the tightest key range the condition allows and leaves the
// rest of the predicate as a residual filter. It never fails: an unsupported
// condition degrades to a full table scan plus filter.
func planScan(schema *Schema, cond any) scanPlan {
	if cond == nil {
		return scanPlan{req: fullScanReq()}
	}
	conj := flattenAnd(cond)

	// An equality prefix is the tightest range an index can give, so take the
	// longest one available. On the whole primary key it pins down one row.
	bestIndex, bestCells, bestUsed := -1, []Cell(nil), []int(nil)
	for indexNo := range schema.Indices {
		cells, used := pickEqPrefix(schema, indexNo, conj)
		if len(cells) > len(bestCells) {
			bestIndex, bestCells, bestUsed = indexNo, cells, used
		}
	}
	if bestIndex >= 0 {
		req := &RangeReq{
			StartCmp: OP_GE,
			StopCmp:  OP_LE,
			Start:    bestCells,
			Stop:     bestCells,
			IndexNo:  bestIndex,
		}
		return scanPlan{req: req, residual: andExcept(conj, bestUsed), indexed: true}
	}

	for indexNo := range schema.Indices {
		for i := range conj {
			for j := range conj {
				if i == j {
					continue
				}
				pair := &ExprBinOp{op: OP_AND, left: conj[i], right: conj[j]}
				if req, ok := matchRangeByIndex(schema, indexNo, pair); ok {
					return scanPlan{req: req, residual: andExcept(conj, []int{i, j}), indexed: true}
				}
			}
		}
		for i := range conj {
			if req, ok := matchRangeByIndex(schema, indexNo, conj[i]); ok {
				return scanPlan{req: req, residual: andExcept(conj, []int{i}), indexed: true}
			}
		}
	}

	return scanPlan{req: fullScanReq(), residual: cond}
}

// matchRow applies a residual predicate to a scanned row.
func matchRow(schema *Schema, row Row, residual any) (bool, error) {
	if residual == nil {
		return true, nil
	}
	cell, err := evalExpr(schema, row, residual)
	if err != nil {
		return false, err
	}
	if cell.Type != TypeI64 {
		return false, errors.New("WHERE clause is not a boolean")
	}
	return cell.I64 != 0, nil
}

// expandProjection resolves `SELECT *` and derives the result columns. The
// column types are inferred statically so that they are known before the first
// row is read.
func expandProjection(schema *Schema, cols []any) ([]any, []ColumnDesc, error) {
	proj := make([]any, 0, len(cols))
	desc := make([]ColumnDesc, 0, len(cols))
	for _, expr := range cols {
		if _, ok := expr.(*ExprStar); ok {
			for _, col := range schema.Cols {
				proj = append(proj, col.Name)
				desc = append(desc, ColumnDesc(col))
			}
			continue
		}
		proj = append(proj, expr)
		desc = append(desc, ColumnDesc{Name: expr2str(expr), Type: exprType(schema, expr)})
	}
	if len(proj) == 0 {
		return nil, nil, errors.New("expect column list")
	}
	return proj, desc, nil
}

// exprType is the static type of a projected expression. Only arithmetic
// propagates an operand type; everything else yields an integer.
func exprType(schema *Schema, expr any) CellType {
	for i := 0; i < maxExprDepth; i++ {
		switch e := expr.(type) {
		case string:
			idx := slices.IndexFunc(schema.Cols, func(col Column) bool {
				return col.Name == e
			})
			if idx < 0 {
				return TypeI64
			}
			return schema.Cols[idx].Type
		case *Cell:
			return e.Type
		case *ExprBinOp:
			switch e.op {
			case OP_ADD, OP_SUB, OP_MUL, OP_DIV:
				expr = e.left
				continue
			}
			return TypeI64
		default:
			return TypeI64
		}
	}
	return TypeI64
}

// selectCursor streams the rows of a SELECT: scan, filter, offset, project and
// stop as soon as the LIMIT is reached.
type selectCursor struct {
	schema    Schema
	plan      scanPlan
	proj      []any
	iter      *RowIterator
	started   bool
	remaining int64 // -1 means no LIMIT
	skip      int64
	out       Row
	scanned   int64 // rows pulled from storage; asserted on by the tests
}

func (c *selectCursor) next() (bool, error) {
	if c.remaining == 0 {
		return false, nil
	}
	for {
		if !c.started {
			c.started = true
		} else if err := c.iter.Next(); err != nil {
			return false, err
		}
		if !c.iter.Valid() {
			return false, nil
		}
		c.scanned++

		row := c.iter.Row()
		ok, err := matchRow(&c.schema, row, c.plan.residual)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		if c.skip > 0 {
			c.skip--
			continue
		}

		out := make(Row, len(c.proj))
		for i, expr := range c.proj {
			cell, err := evalExpr(&c.schema, row, expr)
			if err != nil {
				return false, err
			}
			out[i] = *cell
		}
		c.out = out
		if c.remaining > 0 {
			c.remaining--
		}
		return true, nil
	}
}

func (tx *DBTX) execSelect(stmt *StmtSelect) (*SQLResult, error) {
	schema, err := tx.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}
	proj, cols, err := expandProjection(&schema, stmt.cols)
	if err != nil {
		return nil, err
	}

	cursor := &selectCursor{
		schema:    schema,
		plan:      planScan(&schema, stmt.cond),
		proj:      proj,
		remaining: stmt.limit,
		skip:      stmt.offset,
	}
	if cursor.remaining < 0 {
		cursor.remaining = -1
	}
	if cursor.iter, err = tx.Range(&cursor.schema, cursor.plan.req); err != nil {
		return nil, err
	}
	return &SQLResult{cols: cols, cursor: cursor}, nil
}

func (tx *DBTX) execInsert(stmt *StmtInsert) (count uint64, err error) {
	schema, err := tx.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}
	if len(schema.Cols) != len(stmt.value) {
		return 0, errors.New("schema mismatch")
	}
	for i := range schema.Cols {
		if schema.Cols[i].Type != stmt.value[i].Type {
			return 0, errors.New("schema mismatch")
		}
	}

	updated, err := tx.Insert(&schema, stmt.value)
	if err != nil {
		return 0, err
	}
	if updated {
		count++
	}
	return count, nil
}

func fillNonPKey(schema *Schema, updates []NamedCell, out Row) error {
	for _, expr := range updates {
		idx := slices.IndexFunc(schema.Cols, func(col Column) bool {
			return col.Name == expr.column
		})
		if idx < 0 {
			return errors.New("unknown column")
		}
		if schema.Cols[idx].Type != expr.value.Type {
			return errors.New("column type mismatch")
		}
		if slices.Contains(schema.Indices[0], idx) {
			return errors.New("cannot update a primary key column")
		}
		out[idx] = expr.value
	}
	return nil
}

// mutateBatchSize bounds how many rows an UPDATE or DELETE holds in memory at
// once.
const mutateBatchSize = 512

// forEachMatch applies fn to every row matching cond. No storage iterator is
// alive while fn mutates the transaction, and at most one batch of rows is
// buffered, so a statement over a huge table stays bounded in memory.
//
// The first batch is collected through the planned (possibly index backed)
// scan. If it fills up, the whole statement is replayed as a primary key
// ordered scan before anything is mutated: primary key columns cannot be
// updated, so resuming after the last processed key then visits every matching
// row exactly once even when fn rewrites indexed columns.
func (tx *DBTX) forEachMatch(schema *Schema, cond any, fn func(row Row) error) error {
	plan := planScan(schema, cond)
	batch, done, err := tx.collectBatch(schema, plan, nil)
	if err != nil {
		return err
	}
	if done {
		for _, row := range batch {
			if err := fn(row); err != nil {
				return err
			}
		}
		return nil
	}

	scan := scanPlan{req: fullScanReq(), residual: cond}
	var resume []Cell
	for {
		batch, done, err = tx.collectBatch(schema, scan, resume)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		resume = pkeyCells(schema, batch[len(batch)-1])
		for _, row := range batch {
			if err := fn(row); err != nil {
				return err
			}
		}
		if done {
			return nil
		}
	}
}

// collectBatch reads up to mutateBatchSize matching rows. done reports that the
// scan reached its end.
func (tx *DBTX) collectBatch(schema *Schema, plan scanPlan, resume []Cell) (batch []Row, done bool, err error) {
	req := *plan.req
	if resume != nil {
		req.StartCmp = OP_GT
		req.Start = resume
	}
	iter, err := tx.Range(schema, &req)
	if err != nil {
		return nil, false, err
	}
	for iter.Valid() {
		ok, err := matchRow(schema, iter.Row(), plan.residual)
		if err != nil {
			return nil, false, err
		}
		if ok {
			batch = append(batch, slices.Clone(iter.Row()))
			if len(batch) >= mutateBatchSize {
				return batch, false, nil
			}
		}
		if err := iter.Next(); err != nil {
			return nil, false, err
		}
	}
	return batch, true, nil
}

func pkeyCells(schema *Schema, row Row) []Cell {
	pkey := schema.Indices[0]
	cells := make([]Cell, len(pkey))
	for i, idx := range pkey {
		cells[i] = row[idx]
	}
	return cells
}

func (tx *DBTX) execUpdate(stmt *StmtUpdate) (count uint64, err error) {
	schema, err := tx.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}
	err = tx.forEachMatch(&schema, stmt.cond, func(row Row) error {
		updates := make([]NamedCell, len(stmt.value))
		for i, assign := range stmt.value {
			cell, err := evalExpr(&schema, row, assign.expr)
			if err != nil {
				return err
			}
			updates[i] = NamedCell{column: assign.column, value: *cell}
		}
		if err := fillNonPKey(&schema, updates, row); err != nil {
			return err
		}
		updated, err := tx.Update(&schema, row)
		if err != nil {
			return err
		}
		if updated {
			count++
		}
		return nil
	})
	return count, err
}

func (tx *DBTX) execDelete(stmt *StmtDelete) (count uint64, err error) {
	schema, err := tx.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}
	err = tx.forEachMatch(&schema, stmt.cond, func(row Row) error {
		deleted, err := tx.Delete(&schema, row)
		if err != nil {
			return err
		}
		if deleted {
			count++
		}
		return nil
	})
	return count, err
}
