package reqlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"

	"github.com/dstotijn/hetty/pkg/filter"
	httppb "github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/log"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/scope"
)

type contextKey int

const (
	LogBypassedKey contextKey = iota
	ReqLogIDKey
)

var (
	ErrRequestLogNotFound = errors.New("reqlog: request not found")
	ErrProjectIDMustBeSet = errors.New("reqlog: project ID must be set")
)

type Service struct {
	bypassOutOfScopeRequests bool
	reqsFilter               *RequestLogsFilter
	activeProjectID          string
	scope                    *scope.Scope
	repo                     Repository
	logger                   log.Logger
}

type Config struct {
	ActiveProjectID string
	Scope           *scope.Scope
	Repository      Repository
	Logger          log.Logger
}

func NewService(cfg Config) *Service {
	s := &Service{
		activeProjectID: cfg.ActiveProjectID,
		repo:            cfg.Repository,
		scope:           cfg.Scope,
		logger:          cfg.Logger,
	}

	if s.logger == nil {
		s.logger = log.NewNopLogger()
	}

	return s
}

func (svc *Service) ListHttpRequestLogs(ctx context.Context, req *connect.Request[ListHttpRequestLogsRequest]) (*connect.Response[ListHttpRequestLogsResponse], error) {
	projectID := svc.activeProjectID
	if projectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrProjectIDMustBeSet)
	}

	reqLogs, err := svc.repo.FindRequestLogs(ctx, projectID, svc.filterRequestLog)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reqlog: failed to find request logs: %w", err))
	}

	return connect.NewResponse(&ListHttpRequestLogsResponse{
		HttpRequestLogs: reqLogs,
	}), nil
}

func (svc *Service) filterRequestLog(reqLog *HttpRequestLog) (bool, error) {
	if svc.reqsFilter.GetOnlyInScope() && svc.scope != nil && !reqLog.MatchScope(svc.scope) {
		return false, nil
	}

	var f filter.Expression
	var err error
	if expr := svc.reqsFilter.GetSearchExpr(); expr != "" {
		f, err = filter.ParseQuery(expr)
		if err != nil {
			return false, fmt.Errorf("failed to parse search expression: %w", err)
		}
	}

	if f == nil {
		return true, nil
	}

	match, err := reqLog.Matches(f)
	if err != nil {
		return false, fmt.Errorf("failed to match search expression for request log (id: %v): %w", reqLog.Id, err)
	}

	return match, nil
}

func (svc *Service) FindRequestLogByID(ctx context.Context, id string) (*HttpRequestLog, error) {
	if svc.activeProjectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrProjectIDMustBeSet)
	}

	return svc.repo.FindRequestLogByID(ctx, svc.activeProjectID, id)
}

// GetHttpRequestLog implements HttpRequestLogServiceHandler.
func (svc *Service) GetHttpRequestLog(ctx context.Context, req *connect.Request[GetHttpRequestLogRequest]) (*connect.Response[GetHttpRequestLogResponse], error) {
	id, err := ulid.Parse(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	reqLog, err := svc.repo.FindRequestLogByID(ctx, svc.activeProjectID, id.String())
	if errors.Is(err, ErrRequestLogNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&GetHttpRequestLogResponse{
		HttpRequestLog: reqLog,
	}), nil
}

func (svc *Service) ClearHttpRequestLogs(ctx context.Context, req *connect.Request[ClearHttpRequestLogsRequest]) (*connect.Response[ClearHttpRequestLogsResponse], error) {
	err := svc.repo.ClearRequestLogs(ctx, svc.activeProjectID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reqlog: failed to clear request logs: %w", err))
	}

	return connect.NewResponse(&ClearHttpRequestLogsResponse{}), nil
}

func (svc *Service) storeResponse(ctx context.Context, reqLogID string, res *http.Response) error {
	respb, err := httppb.ParseHTTPResponse(res)
	if err != nil {
		return err
	}

	return svc.repo.StoreResponseLog(ctx, svc.activeProjectID, reqLogID, respb)
}

func (svc *Service) RequestModifier(next proxy.RequestModifyFunc) proxy.RequestModifyFunc {
	return func(req *http.Request) {
		next(req)

		clone := req.Clone(req.Context())

		var body []byte

		if req.Body != nil {
			// TODO: Use io.LimitReader.
			var err error

			body, err = io.ReadAll(req.Body)
			if err != nil {
				svc.logger.Errorw("Failed to read request body for logging.",
					"error", err)
				return
			}

			req.Body = io.NopCloser(bytes.NewBuffer(body))
			clone.Body = io.NopCloser(bytes.NewBuffer(body))
		}

		// Bypass logging if no project is active.
		if svc.activeProjectID == "" {
			ctx := context.WithValue(req.Context(), LogBypassedKey, true)
			*req = *req.WithContext(ctx)

			svc.logger.Debugw("Bypassed logging: no active project.",
				"url", req.URL.String())

			return
		}

		// Bypass logging if this setting is enabled and the incoming request
		// doesn't match any scope rules.
		if svc.bypassOutOfScopeRequests && !svc.scope.Match(clone, body) {
			ctx := context.WithValue(req.Context(), LogBypassedKey, true)
			*req = *req.WithContext(ctx)

			svc.logger.Debugw("Bypassed logging: request doesn't match any scope rules.",
				"url", req.URL.String())

			return
		}

		reqID, ok := proxy.RequestIDFromContext(req.Context())
		if !ok {
			svc.logger.Errorw("Bypassed logging: request doesn't have an ID.")
			return
		}

		proto, ok := httppb.ProtoMap[clone.Proto]
		if !ok {
			svc.logger.Errorw("Bypassed logging: request has an invalid protocol.",
				"proto", clone.Proto)
			return
		}

		method, ok := httppb.MethodMap[clone.Method]
		if !ok {
			svc.logger.Errorw("Bypassed logging: request has an invalid method.",
				"method", clone.Method)
			return
		}

		headers := []*httppb.Header{}
		for k, v := range clone.Header {
			for _, vv := range v {
				headers = append(headers, &httppb.Header{Key: k, Value: vv})
			}
		}

		reqLog := &HttpRequestLog{
			Id:        reqID.String(),
			ProjectId: svc.activeProjectID,
			Request: &httppb.Request{
				Url:      clone.URL.String(),
				Method:   method,
				Protocol: proto,
				Headers:  headers,
				Body:     body,
			},
		}

		err := svc.repo.StoreRequestLog(req.Context(), reqLog)
		if err != nil {
			svc.logger.Errorw("Failed to store request log.",
				"error", err)
			return
		}

		svc.logger.Debugw("Stored request log.",
			"reqLogID", reqLog.Id,
			"url", reqLog.Request.Url,
		)
		ctx := context.WithValue(req.Context(), ReqLogIDKey, reqID)
		*req = *req.WithContext(ctx)
	}
}

func (svc *Service) ResponseModifier(next proxy.ResponseModifyFunc) proxy.ResponseModifyFunc {
	return func(res *http.Response) error {
		if err := next(res); err != nil {
			return err
		}

		if bypassed, _ := res.Request.Context().Value(LogBypassedKey).(bool); bypassed {
			return nil
		}

		reqLogID, ok := res.Request.Context().Value(ReqLogIDKey).(ulid.ULID)
		if !ok {
			return errors.New("reqlog: request is missing ID")
		}

		clone := *res

		if res.Body != nil {
			// TODO: Use io.LimitReader.
			body, err := io.ReadAll(res.Body)
			if err != nil {
				return fmt.Errorf("reqlog: could not read response body: %w", err)
			}

			res.Body = io.NopCloser(bytes.NewBuffer(body))
			clone.Body = io.NopCloser(bytes.NewBuffer(body))
		}

		go func() {
			if err := svc.storeResponse(context.Background(), reqLogID.String(), &clone); err != nil {
				svc.logger.Errorw("Failed to store response log.",
					"error", err)
			} else {
				svc.logger.Debugw("Stored response log.",
					"reqLogID", reqLogID.String())
			}
		}()

		return nil
	}
}

func (svc *Service) SetActiveProjectID(id string) {
	svc.activeProjectID = id
}

func (svc *Service) ActiveProjectID() string {
	return svc.activeProjectID
}

func (svc *Service) SetRequestLogsFilter(filter *RequestLogsFilter) {
	svc.reqsFilter = filter
}

func (svc *Service) SetBypassOutOfScopeRequests(bypass bool) {
	svc.bypassOutOfScopeRequests = bypass
}

func (svc *Service) BypassOutOfScopeRequests() bool {
	return svc.bypassOutOfScopeRequests
}
