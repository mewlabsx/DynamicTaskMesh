package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

type schemaManifest struct {
	tables   []tableManifest
	triggers []schemaObjectManifest
	views    []schemaObjectManifest
}

var errSchemaManifestMismatch = errors.New("schema manifest mismatch")

type tableManifest struct {
	name          string
	columns       []columnManifest
	foreignKeys   []foreignKeyManifest
	indexes       []indexManifest
	checks        []string
	strict        bool
	withoutRowID  bool
	autoIncrement bool
}

type columnManifest struct {
	name               string
	declaredType       string
	notNull            bool
	primaryKeyPosition int
	defaultSQL         string
	hidden             int
	collation          string
}

type foreignKeyManifest struct {
	columns           []string
	referencedTable   string
	referencedColumns []string
	onUpdate          string
	onDelete          string
	match             string
	deferrable        bool
	initiallyDeferred bool
}

type indexManifest struct {
	name    string
	columns []indexColumnManifest
	unique  bool
	origin  string
	partial bool
}

type indexColumnManifest struct {
	sequence  int
	cid       int
	name      string
	desc      bool
	collation string
	key       bool
}

type schemaObjectManifest struct {
	name      string
	tableName string
}

func verifyRequiredSchema(ctx context.Context, db *sql.DB, set string, migrations []migration) error {
	if set != "core" && set != "agent" {
		return schemaMismatch("database migration set is invalid")
	}
	expected, err := expectedSchemaManifest(ctx, migrations)
	if err != nil {
		return err
	}
	actualNames, err := listSchemaTableNames(ctx, db)
	if err != nil {
		return migrationDatabaseError(ctx, "list required schema tables", err)
	}
	actual, err := readSchemaManifest(ctx, db, actualNames)
	if err != nil {
		if errors.Is(err, errSchemaManifestMismatch) {
			return schemaMismatch("database schema does not match this binary")
		}
		return migrationDatabaseError(ctx, "inspect required schema structure", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		return schemaMismatch("database schema does not match this binary")
	}
	return nil
}

func verifySchemaPrefix(ctx context.Context, db *sql.DB, set string, migrations []migration, appliedVersion int) error {
	if set != "core" && set != "agent" {
		return schemaMismatch("database migration set is invalid")
	}
	if appliedVersion < 0 || appliedVersion > len(migrations) {
		return schemaMismatch("database migration version is invalid")
	}
	expected, err := expectedSchemaManifest(ctx, migrations[:appliedVersion])
	if err != nil {
		return err
	}
	expected = withoutMigrationMetadata(expected)
	actualNames, err := listSchemaTableNames(ctx, db)
	if err != nil {
		return migrationDatabaseError(ctx, "list applied schema prefix tables", err)
	}
	actual, err := readSchemaManifest(ctx, db, withoutMigrationMetadataNames(actualNames))
	if err != nil {
		if errors.Is(err, errSchemaManifestMismatch) {
			return schemaMismatch("database schema does not match this binary: applied migration prefix differs")
		}
		return migrationDatabaseError(ctx, "inspect applied schema prefix", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		return schemaMismatch("database schema does not match this binary: applied migration prefix differs")
	}
	return nil
}

func withoutMigrationMetadata(manifest schemaManifest) schemaManifest {
	filtered := schemaManifest{tables: make([]tableManifest, 0, len(manifest.tables))}
	for _, table := range manifest.tables {
		if table.name != "schema_migrations" {
			filtered.tables = append(filtered.tables, table)
		}
	}
	return filtered
}

func withoutMigrationMetadataNames(names []string) []string {
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if name != "schema_migrations" {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

func expectedSchemaManifest(ctx context.Context, migrations []migration) (schemaManifest, error) {
	db, err := sql.Open(defaultSQLDriverName, ":memory:")
	if err != nil {
		return schemaManifest{}, migrationDatabaseError(ctx, "open expected schema manifest", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON`); err != nil {
		return schemaManifest{}, migrationDatabaseError(ctx, "configure expected schema manifest", err)
	}
	if err := applyMigrations(ctx, db, migrations, func() time.Time { return time.Unix(0, 0).UTC() }); err != nil {
		return schemaManifest{}, err
	}
	names, err := listSchemaTableNames(ctx, db)
	if err != nil {
		return schemaManifest{}, migrationDatabaseError(ctx, "list expected schema tables", err)
	}
	return readSchemaManifest(ctx, db, names)
}

func listSchemaTableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return names, nil
}

func manifestTableNames(manifest schemaManifest) []string {
	names := make([]string, 0, len(manifest.tables))
	for _, table := range manifest.tables {
		names = append(names, table.name)
	}
	return names
}

func readSchemaManifest(ctx context.Context, db *sql.DB, names []string) (schemaManifest, error) {
	result := schemaManifest{tables: make([]tableManifest, 0, len(names))}
	for _, name := range names {
		table, err := readTableManifest(ctx, db, name)
		if err != nil {
			return schemaManifest{}, err
		}
		result.tables = append(result.tables, table)
	}
	sort.Slice(result.tables, func(i, j int) bool { return result.tables[i].name < result.tables[j].name })
	var err error
	result.triggers, err = readSchemaObjects(ctx, db, "trigger")
	if err != nil {
		return schemaManifest{}, err
	}
	result.views, err = readSchemaObjects(ctx, db, "view")
	if err != nil {
		return schemaManifest{}, err
	}
	return result, nil
}

func readSchemaObjects(ctx context.Context, db *sql.DB, objectType string) ([]schemaObjectManifest, error) {
	rows, err := db.QueryContext(ctx, `SELECT name, COALESCE(tbl_name, '') FROM sqlite_schema WHERE type=? AND name NOT LIKE 'sqlite_%' ORDER BY name`, objectType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []schemaObjectManifest
	for rows.Next() {
		var item schemaObjectManifest
		if err := rows.Scan(&item.name, &item.tableName); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func readTableManifest(ctx context.Context, db *sql.DB, name string) (tableManifest, error) {
	result := tableManifest{name: name}
	var createSQL sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name=?`, name).Scan(&createSQL); err != nil {
		return tableManifest{}, err
	}
	if !createSQL.Valid || strings.TrimSpace(createSQL.String) == "" {
		return tableManifest{}, errSchemaManifestMismatch
	}
	ddlSemantics, err := parseTableDDLSemantics(createSQL.String)
	if err != nil {
		return tableManifest{}, errSchemaManifestMismatch
	}
	result.strict, result.withoutRowID, result.autoIncrement = readTableOptions(createSQL.String)
	rows, err := db.QueryContext(ctx, "PRAGMA table_xinfo("+quoteSQLiteIdentifier(name)+")")
	if err != nil {
		return tableManifest{}, err
	}
	for rows.Next() {
		var cid, notNull, primaryKey, hidden int
		var columnName, declaredType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &columnName, &declaredType, &notNull, &defaultValue, &primaryKey, &hidden); err != nil {
			_ = rows.Close()
			return tableManifest{}, err
		}
		collation, ok := ddlSemantics.columnCollations[strings.ToLower(columnName)]
		if !ok {
			_ = rows.Close()
			return tableManifest{}, errSchemaManifestMismatch
		}
		result.columns = append(result.columns, columnManifest{
			name: columnName, declaredType: normalizeDeclaredType(declaredType), notNull: notNull != 0,
			primaryKeyPosition: primaryKey, defaultSQL: normalizeDefaultSQL(defaultValue), hidden: hidden,
			collation: collation,
		})
	}
	if err := rows.Close(); err != nil {
		return tableManifest{}, err
	}
	if len(result.columns) == 0 {
		return tableManifest{}, errSchemaManifestMismatch
	}
	result.foreignKeys, err = readForeignKeyManifest(ctx, db, name)
	if err != nil {
		return tableManifest{}, err
	}
	result.indexes, err = readIndexManifest(ctx, db, name)
	if err != nil {
		return tableManifest{}, err
	}
	result.checks, err = readCheckManifestFromSQL(createSQL.String, result.columns)
	if err != nil {
		return tableManifest{}, err
	}
	return result, nil
}

func readTableOptions(createSQL string) (strict, withoutRowID, autoIncrement bool) {
	words := sqlBareWords(createSQL)
	open := strings.IndexByte(createSQL, '(')
	var suffixWords []string
	if open >= 0 {
		_, next, ok := scanParenthesizedSQL(createSQL, open)
		if ok {
			suffixWords = sqlBareWords(createSQL[next:])
		}
	}
	for _, word := range words {
		if word == "autoincrement" {
			autoIncrement = true
		}
	}
	for index, word := range suffixWords {
		if word == "strict" {
			strict = true
		}
		if word == "without" {
			withoutRowID = index+1 < len(suffixWords) && suffixWords[index+1] == "rowid"
		}
	}
	return
}

func sqlBareWords(value string) []string {
	var words []string
	for index := 0; index < len(value); {
		switch {
		case startsSQLLineComment(value, index):
			index = skipSQLLineComment(value, index)
		case startsSQLBlockComment(value, index):
			next, ok := skipSQLBlockComment(value, index)
			if !ok {
				return words
			}
			index = next
		case isSQLQuote(value[index]):
			next, ok := skipSQLQuoted(value, index)
			if !ok {
				return words
			}
			index = next
		case isSQLIdentifierStart(value[index]):
			start := index
			for index < len(value) && isSQLIdentifierPart(value[index]) {
				index++
			}
			words = append(words, strings.ToLower(value[start:index]))
		default:
			index++
		}
	}
	return words
}

func readCheckManifest(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	var createSQL sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name=?`, table).Scan(&createSQL); err != nil {
		return nil, err
	}
	if !createSQL.Valid || strings.TrimSpace(createSQL.String) == "" {
		return nil, errSchemaManifestMismatch
	}
	rows, err := db.QueryContext(ctx, "PRAGMA table_xinfo("+quoteSQLiteIdentifier(table)+")")
	if err != nil {
		return nil, err
	}
	var columns []columnManifest
	for rows.Next() {
		var cid, notNull, primaryKey, hidden int
		var name, declaredType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &declaredType, &notNull, &defaultValue, &primaryKey, &hidden); err != nil {
			_ = rows.Close()
			return nil, err
		}
		columns = append(columns, columnManifest{name: name})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return readCheckManifestFromSQL(createSQL.String, columns)
}

func readCheckManifestFromSQL(createSQL string, columns []columnManifest) ([]string, error) {
	known := make(map[string]string, len(columns))
	for _, column := range columns {
		known[strings.ToLower(column.name)] = strings.ToLower(column.name)
	}
	checks, err := extractCheckConstraintsForColumns(createSQL, known)
	if err != nil {
		return nil, errSchemaManifestMismatch
	}
	sort.Strings(checks)
	return checks, nil
}

func extractCheckConstraints(createSQL string) ([]string, error) {
	return extractCheckConstraintsForColumns(createSQL, nil)
}

func extractCheckConstraintsForColumns(createSQL string, knownColumns map[string]string) ([]string, error) {
	var checks []string
	for index := 0; index < len(createSQL); {
		switch {
		case startsSQLLineComment(createSQL, index):
			index = skipSQLLineComment(createSQL, index)
		case startsSQLBlockComment(createSQL, index):
			var ok bool
			index, ok = skipSQLBlockComment(createSQL, index)
			if !ok {
				return nil, errSchemaManifestMismatch
			}
		case isSQLQuote(createSQL[index]):
			var ok bool
			index, ok = skipSQLQuoted(createSQL, index)
			if !ok {
				return nil, errSchemaManifestMismatch
			}
		case isSQLIdentifierStart(createSQL[index]):
			start := index
			for index < len(createSQL) && isSQLIdentifierPart(createSQL[index]) {
				index++
			}
			if !strings.EqualFold(createSQL[start:index], "check") {
				continue
			}
			for index < len(createSQL) && isSQLSpace(createSQL[index]) {
				index++
			}
			if index >= len(createSQL) || createSQL[index] != '(' {
				return nil, errSchemaManifestMismatch
			}
			expression, next, ok := scanParenthesizedSQL(createSQL, index)
			if !ok {
				return nil, errSchemaManifestMismatch
			}
			canonical, err := canonicalCheckExpressionForColumns(expression, knownColumns)
			if err != nil || canonical == "" {
				return nil, errSchemaManifestMismatch
			}
			checks = append(checks, canonical)
			index = next
		default:
			index++
		}
	}
	return checks, nil
}

func scanParenthesizedSQL(value string, open int) (string, int, bool) {
	depth := 1
	for index := open + 1; index < len(value); {
		switch {
		case startsSQLLineComment(value, index):
			index = skipSQLLineComment(value, index)
		case startsSQLBlockComment(value, index):
			var ok bool
			index, ok = skipSQLBlockComment(value, index)
			if !ok {
				return "", 0, false
			}
		case isSQLQuote(value[index]):
			var ok bool
			index, ok = skipSQLQuoted(value, index)
			if !ok {
				return "", 0, false
			}
		case value[index] == '(':
			depth++
			index++
		case value[index] == ')':
			depth--
			if depth == 0 {
				return value[open+1 : index], index + 1, true
			}
			index++
		default:
			index++
		}
	}
	return "", 0, false
}

func canonicalCheckExpression(expression string) (string, error) {
	return canonicalCheckExpressionForColumns(expression, nil)
}

func canonicalCheckExpressionForColumns(expression string, knownColumns map[string]string) (string, error) {
	expression = stripOuterCheckParentheses(strings.TrimSpace(expression))
	var tokens []string
	for index := 0; index < len(expression); {
		switch {
		case isSQLSpace(expression[index]):
			index++
		case startsSQLLineComment(expression, index):
			index = skipSQLLineComment(expression, index)
		case startsSQLBlockComment(expression, index):
			var ok bool
			index, ok = skipSQLBlockComment(expression, index)
			if !ok {
				return "", errSchemaManifestMismatch
			}
		case expression[index] == '\'':
			next, ok := skipSQLQuoted(expression, index)
			if !ok {
				return "", errSchemaManifestMismatch
			}
			tokens = append(tokens, "string:"+expression[index:next])
			index = next
		case expression[index] == '"' || expression[index] == '`' || expression[index] == '[':
			next, ok := skipSQLQuoted(expression, index)
			if !ok {
				return "", errSchemaManifestMismatch
			}
			content := unescapeSQLIdentifier(expression[index:next])
			normalized := strings.ToLower(content)
			if isSimpleSQLIdentifier(content) && (knownColumns == nil || knownColumns[normalized] != "") {
				tokens = append(tokens, "identifier:"+normalized)
			} else {
				tokens = append(tokens, "quoted:"+expression[index:next])
			}
			index = next
		case isSQLIdentifierStart(expression[index]):
			start := index
			for index < len(expression) && isSQLIdentifierPart(expression[index]) {
				index++
			}
			word := strings.ToLower(expression[start:index])
			if knownColumns != nil {
				if knownColumns[word] != "" {
					tokens = append(tokens, "identifier:"+word)
				} else {
					tokens = append(tokens, "keyword:"+word)
				}
			} else {
				tokens = append(tokens, "word:"+word)
			}
		case expression[index] >= '0' && expression[index] <= '9':
			start := index
			for index < len(expression) && (expression[index] >= '0' && expression[index] <= '9' || expression[index] == '.') {
				index++
			}
			tokens = append(tokens, "number:"+expression[start:index])
		default:
			start := index
			index++
			if index < len(expression) && strings.Contains("<>=!|", string(expression[start])) && expression[index] == '=' {
				index++
			}
			if index < len(expression) && (expression[start] == '<' && expression[index] == '>' || expression[start] == '|' && expression[index] == '|') {
				index++
			}
			tokens = append(tokens, "symbol:"+expression[start:index])
		}
	}
	tokens = stripRedundantIdentifierParentheses(tokens)
	return strings.Join(tokens, "|"), nil
}

func stripRedundantIdentifierParentheses(tokens []string) []string {
	for {
		changed := false
		result := make([]string, 0, len(tokens))
		for index := 0; index < len(tokens); {
			if index+2 < len(tokens) && tokens[index] == "symbol:(" && strings.HasPrefix(tokens[index+1], "identifier:") && tokens[index+2] == "symbol:)" {
				result = append(result, tokens[index+1])
				index += 3
				changed = true
				continue
			}
			result = append(result, tokens[index])
			index++
		}
		tokens = result
		if !changed {
			return tokens
		}
	}
}

func isSimpleSQLIdentifier(value string) bool {
	if value == "" || !isSQLIdentifierStart(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !isSQLIdentifierPart(value[index]) {
			return false
		}
	}
	return true
}

func unescapeSQLIdentifier(token string) string {
	if len(token) < 2 {
		return token
	}
	open, close := token[0], token[len(token)-1]
	content := token[1 : len(token)-1]
	if open == '[' && close == ']' {
		return content
	}
	return strings.ReplaceAll(content, string(open)+string(open), string(open))
}

func stripOuterCheckParentheses(value string) string {
	for len(value) >= 2 && value[0] == '(' {
		_, next, ok := scanParenthesizedSQL(value, 0)
		if !ok || next != len(value) {
			break
		}
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	return value
}

func isSQLIdentifierStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isSQLIdentifierPart(value byte) bool {
	return isSQLIdentifierStart(value) || value >= '0' && value <= '9'
}

func isSQLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n' || value == '\f'
}

func isSQLQuote(value byte) bool {
	return value == '\'' || value == '"' || value == '`' || value == '['
}

func skipSQLQuoted(value string, start int) (int, bool) {
	quote := value[start]
	end := quote
	if quote == '[' {
		end = ']'
	}
	for index := start + 1; index < len(value); index++ {
		if value[index] != end {
			continue
		}
		if quote != '[' && index+1 < len(value) && value[index+1] == end {
			index++
			continue
		}
		return index + 1, true
	}
	return 0, false
}

func startsSQLLineComment(value string, index int) bool {
	return index+1 < len(value) && value[index] == '-' && value[index+1] == '-'
}

func skipSQLLineComment(value string, index int) int {
	for index < len(value) && value[index] != '\n' {
		index++
	}
	return index
}

func startsSQLBlockComment(value string, index int) bool {
	return index+1 < len(value) && value[index] == '/' && value[index+1] == '*'
}

func skipSQLBlockComment(value string, index int) (int, bool) {
	for index += 2; index+1 < len(value); index++ {
		if value[index] == '*' && value[index+1] == '/' {
			return index + 2, true
		}
	}
	return 0, false
}

func readForeignKeyManifest(ctx context.Context, db *sql.DB, table string) ([]foreignKeyManifest, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+quoteSQLiteIdentifier(table)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byID := map[int]*foreignKeyManifest{}
	var order []int
	for rows.Next() {
		var id, sequence int
		var referencedTable, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &sequence, &referencedTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		item := byID[id]
		if item == nil {
			item = &foreignKeyManifest{
				referencedTable: referencedTable, onUpdate: strings.ToUpper(onUpdate), onDelete: strings.ToUpper(onDelete),
				match: strings.ToUpper(match),
			}
			byID[id] = item
			order = append(order, id)
		}
		item.columns = append(item.columns, from)
		item.referencedColumns = append(item.referencedColumns, to)
	}
	sort.Ints(order)
	result := make([]foreignKeyManifest, 0, len(order))
	for _, id := range order {
		result = append(result, *byID[id])
	}
	return result, rows.Err()
}

func readIndexManifest(ctx context.Context, db *sql.DB, table string) ([]indexManifest, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+quoteSQLiteIdentifier(table)+")")
	if err != nil {
		return nil, err
	}
	type listedIndex struct {
		name            string
		unique, partial bool
		origin          string
	}
	var listed []listedIndex
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		listed = append(listed, listedIndex{name: name, unique: unique != 0, origin: origin, partial: partial != 0})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	result := make([]indexManifest, 0, len(listed))
	for _, item := range listed {
		columns, err := readIndexColumns(ctx, db, item.name)
		if err != nil {
			return nil, err
		}
		stableName := ""
		if item.origin == "c" {
			stableName = item.name
		}
		result = append(result, indexManifest{name: stableName, columns: columns, unique: item.unique, origin: item.origin, partial: item.partial})
	}
	sort.Slice(result, func(i, j int) bool { return indexManifestKey(result[i]) < indexManifestKey(result[j]) })
	return result, nil
}

func readIndexColumns(ctx context.Context, db *sql.DB, index string) ([]indexColumnManifest, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_xinfo("+quoteSQLiteIdentifier(index)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []indexColumnManifest
	for rows.Next() {
		var sequence, cid, descending, key int
		var name sql.NullString
		var collation sql.NullString
		if err := rows.Scan(&sequence, &cid, &name, &descending, &collation, &key); err != nil {
			return nil, err
		}
		result = append(result, indexColumnManifest{
			sequence: sequence, cid: cid, name: name.String, desc: descending != 0,
			collation: strings.ToUpper(collation.String), key: key != 0,
		})
	}
	return result, rows.Err()
}

func indexManifestKey(index indexManifest) string {
	return fmt.Sprintf("%s|%s|%t|%t|%v", index.origin, index.name, index.unique, index.partial, index.columns)
}

func quoteSQLiteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func normalizeDeclaredType(value string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func normalizeDefaultSQL(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	normalized := strings.TrimSpace(value.String)
	for len(normalized) >= 2 && normalized[0] == '(' && normalized[len(normalized)-1] == ')' {
		normalized = strings.TrimSpace(normalized[1 : len(normalized)-1])
	}
	if len(normalized) >= 2 && ((normalized[0] == '\'' && normalized[len(normalized)-1] == '\'') || (normalized[0] == '"' && normalized[len(normalized)-1] == '"')) {
		return "literal:" + normalized[1:len(normalized)-1]
	}
	return strings.ToUpper(strings.Join(strings.Fields(normalized), " "))
}
