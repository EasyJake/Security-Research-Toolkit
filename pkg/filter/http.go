package filter

import (
	"errors"
	"fmt"

	"github.com/dstotijn/hetty/pkg/http"
)

func MatchHTTPHeaders(op TokenType, expr Expression, headers []*http.Header) (bool, error) {
	if headers == nil {
		return false, nil
	}

	switch op {
	case TokOpEq:
		strLiteral, ok := expr.(StringLiteral)
		if !ok {
			return false, errors.New("filter: expression must be a string literal")
		}

		// Return `true` if at least one header (<key>: <value>) is equal to the string literal.
		for _, header := range headers {
			if strLiteral.Value == fmt.Sprintf("%v: %v", header.Key, header.Value) {
				return true, nil
			}
		}

		return false, nil
	case TokOpNotEq:
		strLiteral, ok := expr.(StringLiteral)
		if !ok {
			return false, errors.New("filter: expression must be a string literal")
		}

		// Return `true` if none of the headers (<key>: <value>) are equal to the string literal.
		for _, header := range headers {
			if strLiteral.Value == fmt.Sprintf("%v: %v", header.Key, header.Value) {
				return false, nil
			}
		}

		return true, nil
	case TokOpRe:
		re, ok := expr.(RegexpLiteral)
		if !ok {
			return false, errors.New("filter: expression must be a regular expression")
		}

		// Return `true` if at least one header (<key>: <value>) matches the regular expression.
		for _, header := range headers {
			if re.Regexp.MatchString(fmt.Sprintf("%v: %v", header.Key, header.Value)) {
				return true, nil
			}
		}

		return false, nil
	case TokOpNotRe:
		re, ok := expr.(RegexpLiteral)
		if !ok {
			return false, errors.New("filter: expression must be a regular expression")
		}

		// Return `true` if none of the headers (<key>: <value>) match the regular expression.
		for _, header := range headers {
			if re.Regexp.MatchString(fmt.Sprintf("%v: %v", header.Key, header.Value)) {
				return false, nil
			}
		}

		return true, nil
	default:
		return false, fmt.Errorf("filter: unsupported operator %q", op.String())
	}
}
