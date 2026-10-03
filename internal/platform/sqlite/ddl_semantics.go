package sqlite

import (
	"errors"
	"strings"
)

type ddlTokenKind int

const (
	ddlIdentifier ddlTokenKind = iota
	ddlQuotedIdentifier
	ddlStringLiteral
	ddlNumber
	ddlKeyword
	ddlOperator
	ddlLeftParen
	ddlRightParen
	ddlComma
)

type ddlToken struct {
	kind  ddlTokenKind
	value string
}

type tableDDLSemantics struct {
	columnCollations map[string]string
}

var (
	errInvalidDDL              = errors.New("invalid sqlite ddl")
	errUnsupportedDDLSemantics = errors.New("unsupported sqlite ddl semantics")
)

var ddlKeywords = map[string]struct{}{
	"abort": {}, "action": {}, "as": {}, "asc": {}, "autoincrement": {},
	"cascade": {}, "check": {}, "collate": {}, "conflict": {}, "constraint": {}, "create": {},
	"default": {}, "deferred": {}, "deferrable": {}, "delete": {}, "desc": {},
	"fail": {}, "foreign": {}, "full": {}, "generated": {}, "ignore": {}, "immediate": {},
	"initially": {}, "key": {}, "match": {}, "no": {}, "not": {}, "null": {},
	"on": {}, "partial": {}, "primary": {}, "references": {}, "replace": {}, "restrict": {},
	"rollback": {}, "rowid": {}, "set": {}, "simple": {}, "stored": {}, "strict": {},
	"table": {}, "temporary": {}, "unique": {}, "update": {}, "virtual": {}, "without": {},
}

func parseTableDDLSemantics(createSQL string) (tableDDLSemantics, error) {
	tokens, err := scanDDLTokens(createSQL)
	if err != nil {
		return tableDDLSemantics{}, err
	}
	definitions, err := splitDDLTableDefinitions(tokens)
	if err != nil {
		return tableDDLSemantics{}, err
	}
	result := tableDDLSemantics{columnCollations: make(map[string]string)}
	for _, definition := range definitions {
		body, tableConstraint, err := normalizeDDLDefinition(definition)
		if err != nil {
			return tableDDLSemantics{}, err
		}
		if err := rejectDDLConflictPolicy(body); err != nil {
			return tableDDLSemantics{}, err
		}
		if err := rejectDDLForeignKeyPolicy(body); err != nil {
			return tableDDLSemantics{}, err
		}
		if tableConstraint {
			continue
		}
		if len(body) < 2 || !isDDLIdentifier(body[0]) {
			return tableDDLSemantics{}, errInvalidDDL
		}
		name := strings.ToLower(body[0].value)
		if _, exists := result.columnCollations[name]; exists {
			return tableDDLSemantics{}, errInvalidDDL
		}
		collation, err := parseColumnCollation(body[1:])
		if err != nil {
			return tableDDLSemantics{}, err
		}
		result.columnCollations[name] = collation
	}
	if len(result.columnCollations) == 0 {
		return tableDDLSemantics{}, errInvalidDDL
	}
	return result, nil
}

func scanDDLTokens(value string) ([]ddlToken, error) {
	var result []ddlToken
	for index := 0; index < len(value); {
		switch {
		case isSQLSpace(value[index]):
			index++
		case startsSQLLineComment(value, index):
			index = skipSQLLineComment(value, index)
		case startsSQLBlockComment(value, index):
			next, ok := skipSQLBlockComment(value, index)
			if !ok {
				return nil, errInvalidDDL
			}
			index = next
		case value[index] == '\'':
			next, ok := skipSQLQuoted(value, index)
			if !ok {
				return nil, errInvalidDDL
			}
			result = append(result, ddlToken{kind: ddlStringLiteral, value: value[index:next]})
			index = next
		case value[index] == '"' || value[index] == '`' || value[index] == '[':
			next, ok := skipSQLQuoted(value, index)
			if !ok {
				return nil, errInvalidDDL
			}
			result = append(result, ddlToken{kind: ddlQuotedIdentifier, value: unescapeSQLIdentifier(value[index:next])})
			index = next
		case isSQLIdentifierStart(value[index]):
			start := index
			for index < len(value) && isSQLIdentifierPart(value[index]) {
				index++
			}
			word := value[start:index]
			kind := ddlIdentifier
			if _, ok := ddlKeywords[strings.ToLower(word)]; ok {
				kind = ddlKeyword
			}
			result = append(result, ddlToken{kind: kind, value: word})
		case value[index] >= '0' && value[index] <= '9':
			start := index
			for index < len(value) && (value[index] >= '0' && value[index] <= '9' || value[index] == '.') {
				index++
			}
			result = append(result, ddlToken{kind: ddlNumber, value: value[start:index]})
		case value[index] == '(':
			result = append(result, ddlToken{kind: ddlLeftParen, value: "("})
			index++
		case value[index] == ')':
			result = append(result, ddlToken{kind: ddlRightParen, value: ")"})
			index++
		case value[index] == ',':
			result = append(result, ddlToken{kind: ddlComma, value: ","})
			index++
		default:
			start := index
			index++
			if index < len(value) && strings.Contains("<>=!|", string(value[start])) && (value[index] == '=' || value[start] == '<' && value[index] == '>' || value[start] == '|' && value[index] == '|') {
				index++
			}
			result = append(result, ddlToken{kind: ddlOperator, value: value[start:index]})
		}
	}
	return result, nil
}

func splitDDLTableDefinitions(tokens []ddlToken) ([][]ddlToken, error) {
	open := -1
	for index, token := range tokens {
		if token.kind == ddlLeftParen {
			open = index
			break
		}
	}
	if open < 0 {
		return nil, errInvalidDDL
	}
	depth, start := 1, open+1
	var result [][]ddlToken
	for index := open + 1; index < len(tokens); index++ {
		switch tokens[index].kind {
		case ddlLeftParen:
			depth++
		case ddlRightParen:
			depth--
			if depth < 0 {
				return nil, errInvalidDDL
			}
			if depth == 0 {
				if index == start {
					return nil, errInvalidDDL
				}
				result = append(result, tokens[start:index])
				return result, nil
			}
		case ddlComma:
			if depth == 1 {
				if index == start {
					return nil, errInvalidDDL
				}
				result = append(result, tokens[start:index])
				start = index + 1
			}
		}
	}
	return nil, errInvalidDDL
}

func normalizeDDLDefinition(tokens []ddlToken) ([]ddlToken, bool, error) {
	if len(tokens) == 0 {
		return nil, false, errInvalidDDL
	}
	first := strings.ToLower(tokens[0].value)
	if first == "constraint" {
		if len(tokens) < 3 || !isDDLIdentifier(tokens[1]) {
			return nil, false, errInvalidDDL
		}
		tokens = tokens[2:]
		first = strings.ToLower(tokens[0].value)
	}
	tableConstraint := tokens[0].kind == ddlKeyword && (first == "primary" || first == "unique" || first == "check" || first == "foreign")
	return tokens, tableConstraint, nil
}

func parseColumnCollation(tokens []ddlToken) (string, error) {
	collation := ""
	depth := 0
	for index := 0; index < len(tokens); index++ {
		switch tokens[index].kind {
		case ddlLeftParen:
			depth++
		case ddlRightParen:
			depth--
			if depth < 0 {
				return "", errInvalidDDL
			}
		default:
			if depth != 0 || !ddlWord(tokens[index], "collate") {
				continue
			}
			if collation != "" || index+1 >= len(tokens) || !isDDLIdentifier(tokens[index+1]) {
				return "", errInvalidDDL
			}
			collation = strings.ToUpper(tokens[index+1].value)
			index++
		}
	}
	if depth != 0 {
		return "", errInvalidDDL
	}
	if collation == "" {
		return "BINARY", nil
	}
	return collation, nil
}

func rejectDDLConflictPolicy(tokens []ddlToken) error {
	depth := 0
	for index := 0; index < len(tokens); index++ {
		switch tokens[index].kind {
		case ddlLeftParen:
			depth++
		case ddlRightParen:
			depth--
		default:
			if depth != 0 || !ddlWord(tokens[index], "on") {
				continue
			}
			if index+1 >= len(tokens) {
				return errInvalidDDL
			}
			if ddlWord(tokens[index+1], "conflict") {
				if index+2 >= len(tokens) || !oneOfDDLWords(tokens[index+2], "rollback", "abort", "fail", "ignore", "replace") {
					return errInvalidDDL
				}
				return errUnsupportedDDLSemantics
			}
			if !oneOfDDLWords(tokens[index+1], "delete", "update") {
				return errInvalidDDL
			}
		}
	}
	if depth != 0 {
		return errInvalidDDL
	}
	return nil
}

func rejectDDLForeignKeyPolicy(tokens []ddlToken) error {
	references := topLevelDDLWord(tokens, "references")
	if references < 0 {
		return nil
	}
	index := references + 1
	if index >= len(tokens) || !isDDLIdentifier(tokens[index]) {
		return errInvalidDDL
	}
	index++
	if index < len(tokens) && tokens[index].kind == ddlLeftParen {
		next, err := consumeDDLIdentifierList(tokens, index)
		if err != nil {
			return err
		}
		index = next
	}
	for index < len(tokens) {
		switch {
		case ddlWord(tokens[index], "match"):
			if index+1 >= len(tokens) || !isDDLIdentifier(tokens[index+1]) {
				return errInvalidDDL
			}
			return errUnsupportedDDLSemantics
		case ddlWord(tokens[index], "deferrable"):
			return errUnsupportedDDLSemantics
		case ddlWord(tokens[index], "initially"):
			if index+1 >= len(tokens) || !oneOfDDLWords(tokens[index+1], "deferred", "immediate") {
				return errInvalidDDL
			}
			return errUnsupportedDDLSemantics
		case ddlWord(tokens[index], "not") && index+1 < len(tokens) && ddlWord(tokens[index+1], "deferrable"):
			return errUnsupportedDDLSemantics
		case ddlWord(tokens[index], "on"):
			next, err := consumeDDLForeignKeyAction(tokens, index)
			if err != nil {
				return err
			}
			index = next
		default:
			return errInvalidDDL
		}
	}
	return nil
}

func consumeDDLForeignKeyAction(tokens []ddlToken, index int) (int, error) {
	if index+2 >= len(tokens) || !oneOfDDLWords(tokens[index+1], "delete", "update") {
		return 0, errInvalidDDL
	}
	index += 2
	if oneOfDDLWords(tokens[index], "cascade", "restrict") {
		return index + 1, nil
	}
	if ddlWord(tokens[index], "set") && index+1 < len(tokens) && oneOfDDLWords(tokens[index+1], "null", "default") {
		return index + 2, nil
	}
	if ddlWord(tokens[index], "no") && index+1 < len(tokens) && ddlWord(tokens[index+1], "action") {
		return index + 2, nil
	}
	return 0, errInvalidDDL
}

func consumeDDLIdentifierList(tokens []ddlToken, open int) (int, error) {
	index, needIdentifier := open+1, true
	for index < len(tokens) {
		if tokens[index].kind == ddlRightParen {
			if needIdentifier {
				return 0, errInvalidDDL
			}
			return index + 1, nil
		}
		if needIdentifier {
			if !isDDLIdentifier(tokens[index]) {
				return 0, errInvalidDDL
			}
			needIdentifier = false
		} else {
			if tokens[index].kind != ddlComma {
				return 0, errInvalidDDL
			}
			needIdentifier = true
		}
		index++
	}
	return 0, errInvalidDDL
}

func topLevelDDLWord(tokens []ddlToken, word string) int {
	depth := 0
	for index, token := range tokens {
		switch token.kind {
		case ddlLeftParen:
			depth++
		case ddlRightParen:
			depth--
		default:
			if depth == 0 && ddlWord(token, word) {
				return index
			}
		}
	}
	return -1
}

func isDDLIdentifier(token ddlToken) bool {
	return token.kind == ddlIdentifier || token.kind == ddlQuotedIdentifier || token.kind == ddlKeyword
}

func ddlWord(token ddlToken, value string) bool {
	return (token.kind == ddlIdentifier || token.kind == ddlKeyword) && strings.EqualFold(token.value, value)
}

func oneOfDDLWords(token ddlToken, values ...string) bool {
	for _, value := range values {
		if ddlWord(token, value) {
			return true
		}
	}
	return false
}
