package http

import (
	"fmt"
	"io"
	nethttp "net/http"
	"strconv"
	"strings"
)

var ProtoMap = map[string]Protocol{
	"HTTP/1.0": Protocol_PROTOCOL_HTTP10,
	"HTTP/1.1": Protocol_PROTOCOL_HTTP11,
	"HTTP/2.0": Protocol_PROTOCOL_HTTP20,
}

var MethodMap = map[string]Method{
	"GET":     Method_METHOD_GET,
	"POST":    Method_METHOD_POST,
	"PUT":     Method_METHOD_PUT,
	"DELETE":  Method_METHOD_DELETE,
	"CONNECT": Method_METHOD_CONNECT,
	"OPTIONS": Method_METHOD_OPTIONS,
	"TRACE":   Method_METHOD_TRACE,
	"PATCH":   Method_METHOD_PATCH,
}

func ParseHeader(header nethttp.Header) []*Header {
	headers := []*Header{}

	for key, values := range header {
		for _, value := range values {
			headers = append(headers, &Header{Key: key, Value: value})
		}
	}

	return headers
}

var ResponseSearchKeyFns = map[string]func(rl *Response) string{
	"res.proto":      func(rl *Response) string { return rl.GetProtocol().String() },
	"res.status":     func(rl *Response) string { return rl.GetStatus() },
	"res.statusCode": func(rl *Response) string { return strconv.Itoa(int(rl.GetStatusCode())) },
	"res.statusReason": func(rl *Response) string {
		statusReasonSubs := strings.SplitN(rl.GetStatus(), " ", 2)
		if len(statusReasonSubs) != 2 {
			return ""
		}
		return statusReasonSubs[1]
	},
	"res.body": func(rl *Response) string { return string(rl.GetBody()) },
}

func ParseHTTPResponse(res *nethttp.Response) (*Response, error) {
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reqlog: could not read body: %w", err)
	}

	headers := []*Header{}
	for k, v := range res.Header {
		for _, vv := range v {
			headers = append(headers, &Header{Key: k, Value: vv})
		}
	}

	protocol, ok := ProtoMap[res.Proto]
	if !ok {
		return nil, fmt.Errorf("reqlog: invalid protocol %q", res.Proto)
	}

	return &Response{
		Protocol:   protocol,
		Status:     res.Status,
		StatusCode: int32(res.StatusCode),
		Headers:    headers,
		Body:       body,
	}, nil
}
