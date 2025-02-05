package reqlog

import (
	"errors"
	"fmt"
	"strings"

	"github.com/oklog/ulid/v2"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/scope"
)

var reqLogSearchKeyFns = map[string]func(rl *HttpRequestLog) string{
	"req.id":     func(rl *HttpRequestLog) string { return rl.GetId() },
	"req.proto":  func(rl *HttpRequestLog) string { return rl.GetRequest().GetProtocol().String() },
	"req.url":    func(rl *HttpRequestLog) string { return rl.GetRequest().GetUrl() },
	"req.method": func(rl *HttpRequestLog) string { return rl.GetRequest().GetMethod().String() },
	"req.body":   func(rl *HttpRequestLog) string { return string(rl.GetRequest().GetBody()) },
	"req.timestamp": func(rl *HttpRequestLog) string {
		id, err := ulid.Parse(rl.GetId())
		if err != nil {
			return ""
		}
		return ulid.Time(id.Time()).String()
	},
}

// TODO: Request and response headers search key functions.

// Matches returns true if the supplied search expression evaluates to true.
func (reqLog *HttpRequestLog) Matches(expr filter.Expression) (bool, error) {
	switch e := expr.(type) {
	case filter.PrefixExpression:
		return reqLog.matchPrefixExpr(e)
	case filter.InfixExpression:
		return reqLog.matchInfixExpr(e)
	case filter.StringLiteral:
		return reqLog.matchStringLiteral(e)
	default:
		return false, fmt.Errorf("expression type (%T) not supported", expr)
	}
}

func (reqLog *HttpRequestLog) matchPrefixExpr(expr filter.PrefixExpression) (bool, error) {
	switch expr.Operator {
	case filter.TokOpNot:
		match, err := reqLog.Matches(expr.Right)
		if err != nil {
			return false, err
		}

		return !match, nil
	default:
		return false, errors.New("operator is not supported")
	}
}

func (reqLog *HttpRequestLog) matchInfixExpr(expr filter.InfixExpression) (bool, error) {
	switch expr.Operator {
	case filter.TokOpAnd:
		left, err := reqLog.Matches(expr.Left)
		if err != nil {
			return false, err
		}

		right, err := reqLog.Matches(expr.Right)
		if err != nil {
			return false, err
		}

		return left && right, nil
	case filter.TokOpOr:
		left, err := reqLog.Matches(expr.Left)
		if err != nil {
			return false, err
		}

		right, err := reqLog.Matches(expr.Right)
		if err != nil {
			return false, err
		}

		return left || right, nil
	}

	left, ok := expr.Left.(filter.StringLiteral)
	if !ok {
		return false, errors.New("left operand must be a string literal")
	}

	leftVal := reqLog.getMappedStringLiteral(left.Value)

	if leftVal == "req.headers" {
		match, err := filter.MatchHTTPHeaders(expr.Operator, expr.Right, reqLog.Request.Headers)
		if err != nil {
			return false, fmt.Errorf("failed to match request HTTP headers: %w", err)
		}

		return match, nil
	}

	if leftVal == "res.headers" && reqLog.Response != nil {
		match, err := filter.MatchHTTPHeaders(expr.Operator, expr.Right, reqLog.Response.Headers)
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

	rightVal := reqLog.getMappedStringLiteral(right.Value)

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

func (reqLog *HttpRequestLog) getMappedStringLiteral(s string) string {
	switch {
	case strings.HasPrefix(s, "req."):
		fn, ok := reqLogSearchKeyFns[s]
		if ok {
			return fn(reqLog)
		}
	case strings.HasPrefix(s, "res."):
		fn, ok := http.ResponseSearchKeyFns[s]
		if ok {
			return fn(reqLog.GetResponse())
		}
	}

	return s
}

func (reqLog *HttpRequestLog) matchStringLiteral(strLiteral filter.StringLiteral) (bool, error) {
	for _, header := range reqLog.GetRequest().GetHeaders() {
		if strings.Contains(
			strings.ToLower(fmt.Sprintf("%v: %v", header.Key, header.Value)),
			strings.ToLower(strLiteral.Value),
		) {
			return true, nil
		}
	}

	for _, fn := range reqLogSearchKeyFns {
		if strings.Contains(
			strings.ToLower(fn(reqLog)),
			strings.ToLower(strLiteral.Value),
		) {
			return true, nil
		}
	}

	if res := reqLog.GetResponse(); res != nil {
		for _, header := range res.Headers {
			if strings.Contains(
				strings.ToLower(fmt.Sprintf("%v: %v", header.Key, header.Value)),
				strings.ToLower(strLiteral.Value),
			) {
				return true, nil
			}
		}

		for _, fn := range http.ResponseSearchKeyFns {
			if strings.Contains(
				strings.ToLower(fn(reqLog.GetResponse())),
				strings.ToLower(strLiteral.Value),
			) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (reqLog *HttpRequestLog) MatchScope(s *scope.Scope) bool {
	for _, rule := range s.Rules() {
		if rule.URL != nil {
			if matches := rule.URL.MatchString(reqLog.GetRequest().GetUrl()); matches {
				return true
			}
		}

		for _, header := range reqLog.GetRequest().GetHeaders() {
			var keyMatches, valueMatches bool

			if matches := rule.Header.Key.MatchString(header.Key); matches {
				keyMatches = true
			}

			if rule.Header.Value != nil {
				if matches := rule.Header.Value.MatchString(header.Value); matches {
					valueMatches = true
					break
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
			if matches := rule.Body.Match(reqLog.GetRequest().GetBody()); matches {
				return true
			}
		}
	}

	return false
}
