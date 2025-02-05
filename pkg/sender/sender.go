package sender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	connect "connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"

	"github.com/dstotijn/hetty/pkg/filter"
	httppb "github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
)

var defaultHTTPClient = &http.Client{
	Transport: &HTTPTransport{},
	Timeout:   30 * time.Second,
}

var (
	ErrProjectIDMustBeSet = errors.New("sender: project ID must be set")
	ErrRequestNotFound    = errors.New("sender: request not found")
)

type Service struct {
	activeProjectID string
	reqsFilter      *RequestsFilter
	scope           *scope.Scope
	repo            Repository
	reqLogSvc       *reqlog.Service
	httpClient      *http.Client
}

type Config struct {
	Scope         *scope.Scope
	Repository    Repository
	ReqLogService *reqlog.Service
	HTTPClient    *http.Client
}

type SendError struct {
	err error
}

func NewService(cfg Config) *Service {
	svc := &Service{
		repo:       cfg.Repository,
		reqLogSvc:  cfg.ReqLogService,
		httpClient: defaultHTTPClient,
		scope:      cfg.Scope,
	}

	if cfg.HTTPClient != nil {
		svc.httpClient = cfg.HTTPClient
	}

	return svc
}

func (svc *Service) GetRequestByID(ctx context.Context, req *connect.Request[GetRequestByIDRequest]) (*connect.Response[GetRequestByIDResponse], error) {
	if svc.activeProjectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrProjectIDMustBeSet)
	}

	senderReq, err := svc.repo.FindSenderRequestByID(ctx, svc.activeProjectID, req.Msg.RequestId)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("sender: failed to find request: %w", err))
	}

	return &connect.Response[GetRequestByIDResponse]{
		Msg: &GetRequestByIDResponse{Request: senderReq},
	}, nil
}

func (svc *Service) ListRequests(ctx context.Context, req *connect.Request[ListRequestsRequest]) (*connect.Response[ListRequestsResponse], error) {
	reqs, err := svc.repo.FindSenderRequests(ctx, svc.activeProjectID, svc.filterRequest)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: failed to find requests: %w", err))
	}

	return &connect.Response[ListRequestsResponse]{
		Msg: &ListRequestsResponse{Requests: reqs},
	}, nil
}

func (svc *Service) filterRequest(req *Request) (bool, error) {
	if svc.reqsFilter.OnlyInScope {
		if svc.scope != nil && !req.MatchScope(svc.scope) {
			return false, nil
		}
	}

	if svc.reqsFilter.SearchExpr == "" {
		return true, nil
	}

	expr, err := filter.ParseQuery(svc.reqsFilter.SearchExpr)
	if err != nil {
		return false, fmt.Errorf("failed to parse search expression: %w", err)
	}

	match, err := req.Matches(expr)
	if err != nil {
		return false, fmt.Errorf("failed to match search expression for sender request (id: %v): %w",
			req.Id, err,
		)
	}

	return match, nil
}

func (svc *Service) CreateOrUpdateRequest(ctx context.Context, req *connect.Request[CreateOrUpdateRequestRequest]) (*connect.Response[CreateOrUpdateRequestResponse], error) {
	if svc.activeProjectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrProjectIDMustBeSet)
	}

	r := proto.Clone(req.Msg.Request).(*Request)

	if r == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("sender: request is nil"))
	}

	if r.HttpRequest == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("sender: request.http_request is nil"))
	}

	if r.Id == "" {
		r.Id = ulid.Make().String()
	}

	r.ProjectId = svc.activeProjectID

	if r.HttpRequest.Method == httppb.Method_METHOD_UNSPECIFIED {
		r.HttpRequest.Method = httppb.Method_METHOD_GET
	}

	if r.HttpRequest.Protocol == httppb.Protocol_PROTOCOL_UNSPECIFIED {
		r.HttpRequest.Protocol = httppb.Protocol_PROTOCOL_HTTP20
	}

	err := svc.repo.StoreSenderRequest(ctx, r)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: failed to store request: %w", err))
	}

	return &connect.Response[CreateOrUpdateRequestResponse]{
		Msg: &CreateOrUpdateRequestResponse{
			Request: r,
		},
	}, nil
}

func (svc *Service) CloneFromRequestLog(ctx context.Context, req *connect.Request[CloneFromRequestLogRequest]) (*connect.Response[CloneFromRequestLogResponse], error) {
	if svc.activeProjectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrProjectIDMustBeSet)
	}

	reqLog, err := svc.reqLogSvc.FindRequestLogByID(ctx, req.Msg.RequestLogId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: failed to find request log: %w", err))
	}

	clonedReqLog := proto.Clone(reqLog).(*reqlog.HttpRequestLog)

	senderReq := &Request{
		Id:                 ulid.Make().String(),
		ProjectId:          svc.activeProjectID,
		SourceRequestLogId: clonedReqLog.Id,
		HttpRequest:        clonedReqLog.Request,
	}

	err = svc.repo.StoreSenderRequest(ctx, senderReq)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: failed to store request: %w", err))
	}

	return &connect.Response[CloneFromRequestLogResponse]{Msg: &CloneFromRequestLogResponse{
		Request: senderReq,
	}}, nil
}

func (svc *Service) SetRequestsFilter(filter *RequestsFilter) {
	svc.reqsFilter = filter
}

func (svc *Service) RequestsFilter() *RequestsFilter {
	return svc.reqsFilter
}

func (svc *Service) SendRequest(ctx context.Context, connReq *connect.Request[SendRequestRequest]) (*connect.Response[SendRequestResponse], error) {
	req, err := svc.repo.FindSenderRequestByID(ctx, svc.activeProjectID, connReq.Msg.RequestId)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("sender: failed to find request: %w", err))
	}

	httpReq, err := parseHTTPRequest(ctx, req)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: failed to parse HTTP request: %w", err))
	}

	httpRes, err := svc.sendHTTPRequest(httpReq)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: could not send HTTP request: %w", err))
	}

	req.HttpResponse = httpRes

	err = svc.repo.StoreSenderRequest(ctx, req)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: failed to store sender response log: %w", err))
	}

	return &connect.Response[SendRequestResponse]{
		Msg: &SendRequestResponse{
			Request: req,
		},
	}, nil
}

func parseHTTPRequest(ctx context.Context, req *Request) (*http.Request, error) {
	ctx = context.WithValue(ctx, protoCtxKey{}, req.GetHttpRequest().GetProtocol())

	httpReq, err := http.NewRequestWithContext(ctx,
		req.GetHttpRequest().GetMethod().String(),
		req.GetHttpRequest().GetUrl(),
		bytes.NewReader(req.GetHttpRequest().GetBody()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to construct HTTP request: %w", err)
	}

	for _, header := range req.GetHttpRequest().GetHeaders() {
		httpReq.Header.Add(header.Key, header.Value)
	}

	return httpReq, nil
}

func (svc *Service) sendHTTPRequest(httpReq *http.Request) (*httppb.Response, error) {
	res, err := svc.httpClient.Do(httpReq)
	if err != nil {
		return nil, &SendError{err}
	}
	defer res.Body.Close()

	resLog, err := httppb.ParseHTTPResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to parse http response: %w", err)
	}

	return resLog, err
}

func (svc *Service) SetActiveProjectID(id string) {
	svc.activeProjectID = id
}

func (svc *Service) DeleteRequests(ctx context.Context, req *connect.Request[DeleteRequestsRequest]) (*connect.Response[DeleteRequestsResponse], error) {
	if svc.activeProjectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrProjectIDMustBeSet)
	}

	err := svc.repo.DeleteSenderRequests(ctx, svc.activeProjectID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("sender: failed to delete requests: %w", err))
	}

	return &connect.Response[DeleteRequestsResponse]{}, nil
}

func (e SendError) Error() string {
	return fmt.Sprintf("failed to send HTTP request: %v", e.err)
}

func (e SendError) Unwrap() error {
	return e.err
}
