package sender

import (
	"errors"
	"fmt"
	"strings"

	"github.com/oklog/ulid/v2"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/scope"
)

var senderReqSearchKeyFns = map[string]func(req *Request) string{
	"req.id":     func(req *Request) string { return req.Id },
	"req.proto":  func(req *Request) string { return req.GetHttpRequest().GetProtocol().String() },
	"req.url":    func(req *Request) string { return req.GetHttpRequest().GetUrl() },
	"req.method": func(req *Request) string { return req.GetHttpRequest().GetMethod().String() },
	"req.body":   func(req *Request) string { return string(req.GetHttpRequest().GetBody()) },
	"req.timestamp": func(req *Request) string {
		id, err := ulid.Parse(req.Id)
		if err != nil {
			return ""
		}
		return ulid.Time(id.Time()).String()
	},
}

// TODO: Request and response headers search key functions.

// Matches returns true if the supplied search expression evaluates to true.
func (req *Request) Matches(expr filter.Expression) (bool, error) {
	switch e := expr.(type) {
	case filter.PrefixExpression:
		return req.matchPrefixExpr(e)
	case filter.InfixExpression:
		return req.matchInfixExpr(e)
	case filter.StringLiteral:
		return req.matchStringLiteral(e)
	default:
		return false, fmt.Errorf("expression type (%T) not supported", expr)
	}
}

func (req *Request) matchPrefixExpr(expr filter.PrefixExpression) (bool, error) {
	switch expr.Operator {
	case filter.TokOpNot:
		match, err := req.Matches(expr.Right)
		if err != nil {
			return false, err
		}

		return !match, nil
	default:
		return false, errors.New("operator is not supported")
	}
}

func (req *Request) matchInfixExpr(expr filter.InfixExpression) (bool, error) {
	switch expr.Operator {
	case filter.TokOpAnd:
		left, err := req.Matches(expr.Left)
		if err != nil {
			return false, err
		}

		right, err := req.Matches(expr.Right)
		if err != nil {
			return false, err
		}

		return left && right, nil
	case filter.TokOpOr:
		left, err := req.Matches(expr.Left)
		if err != nil {
			return false, err
		}

		right, err := req.Matches(expr.Right)
		if err != nil {
			return false, err
		}

		return left || right, nil
	}

	left, ok := expr.Left.(filter.StringLiteral)
	if !ok {
		return false, errors.New("left operand must be a string literal")
	}

	leftVal := req.getMappedStringLiteral(left.Value)

	if leftVal == "req.headers" {
		match, err := filter.MatchHTTPHeaders(expr.Operator, expr.Right, req.GetHttpRequest().GetHeaders())
		if err != nil {
			return false, fmt.Errorf("failed to match request HTTP headers: %w", err)
		}

		return match, nil
	}

	if leftVal == "res.headers" && req.GetHttpResponse() != nil {
		match, err := filter.MatchHTTPHeaders(expr.Operator, expr.Right, req.GetHttpResponse().GetHeaders())
		if err != nil {
			return false, fmt.Errorf("failed to match response HTTP headers: %w", err)
		}

		return match, nil
	}

	if expr.Operator == filter.TokOpRe || expr.Operator == filter.TokOpNotRe {
		right, ok := expr.Right.(filter.RegexpLiteral)
		if !ok {
			return false, errors.New("right operand must be a regular expression")
		}

		switch expr.Operator {
		case filter.TokOpRe:
			return right.MatchString(leftVal), nil
		case filter.TokOpNotRe:
			return !right.MatchString(leftVal), nil
		}
	}

	right, ok := expr.Right.(filter.StringLiteral)
	if !ok {
		return false, errors.New("right operand must be a string literal")
	}

	rightVal := req.getMappedStringLiteral(right.Value)

	switch expr.Operator {
	case filter.TokOpEq:
		return leftVal == rightVal, nil
	case filter.TokOpNotEq:
		return leftVal != rightVal, nil
	case filter.TokOpGt:
		// TODO(?) attempt to parse as int.
		return leftVal > rightVal, nil
	case filter.TokOpLt:
		// TODO(?) attempt to parse as int.
		return leftVal < rightVal, nil
	case filter.TokOpGtEq:
		// TODO(?) attempt to parse as int.
		return leftVal >= rightVal, nil
	case filter.TokOpLtEq:
		// TODO(?) attempt to parse as int.
		return leftVal <= rightVal, nil
	default:
		return false, errors.New("unsupported operator")
	}
}

func (req *Request) getMappedStringLiteral(s string) string {
	switch {
	case strings.HasPrefix(s, "req."):
		fn, ok := senderReqSearchKeyFns[s]
		if ok {
			return fn(req)
		}
	case strings.HasPrefix(s, "res."):
		fn, ok := http.ResponseSearchKeyFns[s]
		if ok {
			return fn(req.GetHttpResponse())
		}
	}

	return s
}

func (req *Request) matchStringLiteral(strLiteral filter.StringLiteral) (bool, error) {
	for _, header := range req.GetHttpRequest().GetHeaders() {
		if strings.Contains(
			strings.ToLower(fmt.Sprintf("%v: %v", header.Key, header.Value)),
			strings.ToLower(strLiteral.Value),
		) {
			return true, nil
		}
	}

	for _, fn := range senderReqSearchKeyFns {
		if strings.Contains(
			strings.ToLower(fn(req)),
			strings.ToLower(strLiteral.Value),
		) {
			return true, nil
		}
	}

	for _, header := range req.GetHttpResponse().GetHeaders() {
		if strings.Contains(
			strings.ToLower(fmt.Sprintf("%v: %v", header.Key, header.Value)),
			strings.ToLower(strLiteral.Value),
		) {
			return true, nil
		}
	}

	for _, fn := range http.ResponseSearchKeyFns {
		if strings.Contains(
			strings.ToLower(fn(req.GetHttpResponse())),
			strings.ToLower(strLiteral.Value),
		) {
			return true, nil
		}
	}

	return false, nil
}

func (req *Request) MatchScope(s *scope.Scope) bool {
	for _, rule := range s.Rules() {
		if url := req.GetHttpRequest().GetUrl(); rule.URL != nil && url != "" {
			if matches := rule.URL.MatchString(url); matches {
				return true
			}
		}

		for _, headers := range req.GetHttpRequest().GetHeaders() {
			var keyMatches, valueMatches bool

			if rule.Header.Key != nil {
				if matches := rule.Header.Key.MatchString(headers.Key); matches {
					keyMatches = true
				}
			}

			if rule.Header.Value != nil {
				if matches := rule.Header.Value.MatchString(headers.Value); matches {
					valueMatches = true
				}
			}
			// When only key or value is set, match on whatever is set.
			// When both are set, both must match.
			switch {
			case rule.Header.Key != nil && rule.Header.Value == nil && keyMatches:
				return true
			case rule.Header.Key == nil && rule.Header.Value != nil && valueMatches:
				return true
			case rule.Header.Key != nil && rule.Header.Value != nil && keyMatches && valueMatches:
				return true
			}
		}

		if rule.Body != nil {
			if matches := rule.Body.Match(req.GetHttpRequest().GetBody()); matches {
				return true
			}
		}
	}

	return false
}
