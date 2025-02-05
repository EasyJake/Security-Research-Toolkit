package proj

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	connect "connectrpc.com/connect"
	"github.com/oklog/ulid/v2"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/proxy/intercept"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/sender"
)

type Service struct {
	repo            Repository
	interceptSvc    *intercept.Service
	reqLogSvc       *reqlog.Service
	senderSvc       *sender.Service
	scope           *scope.Scope
	activeProjectID string
	mu              sync.RWMutex
}

type Settings struct {
	// Request log settings
	ReqLogBypassOutOfScope bool
	ReqLogOnlyFindInScope  bool
	ReqLogSearchExpr       filter.Expression

	// Intercept settings
	InterceptRequests       bool
	InterceptResponses      bool
	InterceptRequestFilter  filter.Expression
	InterceptResponseFilter filter.Expression

	// Sender settings
	SenderOnlyFindInScope bool
	SenderSearchExpr      filter.Expression

	// Scope settings
	ScopeRules []scope.Rule
}

var (
	ErrProjectNotFound = errors.New("proj: project not found")
	ErrNoProject       = errors.New("proj: no open project")
	ErrNoSettings      = errors.New("proj: settings not found")
	ErrInvalidName     = errors.New("proj: invalid name, must be alphanumeric or whitespace chars")
)

var nameRegexp = regexp.MustCompile(`^[\w\d\s]+$`)

type Config struct {
	Repository       Repository
	InterceptService *intercept.Service
	ReqLogService    *reqlog.Service
	SenderService    *sender.Service
	Scope            *scope.Scope
}

// NewService returns a new Service.
func NewService(cfg Config) (*Service, error) {
	return &Service{
		repo:         cfg.Repository,
		interceptSvc: cfg.InterceptService,
		reqLogSvc:    cfg.ReqLogService,
		senderSvc:    cfg.SenderService,
		scope:        cfg.Scope,
	}, nil
}

func (svc *Service) CreateProject(ctx context.Context, req *connect.Request[CreateProjectRequest]) (*connect.Response[CreateProjectResponse], error) {
	if !nameRegexp.MatchString(req.Msg.Name) {
		return nil, ErrInvalidName
	}

	project := &Project{
		Id:   ulid.Make().String(),
		Name: req.Msg.Name,
	}

	err := svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("proj: could not create project: %w", err)
	}

	return &connect.Response[CreateProjectResponse]{
		Msg: &CreateProjectResponse{
			Project: project,
		},
	}, nil
}

// CloseProject closes the currently open project (if there is one).
func (svc *Service) CloseProject(ctx context.Context, _ *connect.Request[CloseProjectRequest]) (*connect.Response[CloseProjectResponse], error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	if svc.activeProjectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNoProject)
	}

	svc.activeProjectID = ""
	svc.reqLogSvc.SetActiveProjectID("")
	svc.reqLogSvc.SetBypassOutOfScopeRequests(false)
	svc.reqLogSvc.SetRequestLogsFilter(nil)
	svc.interceptSvc.UpdateSettings(intercept.Settings{
		RequestsEnabled:  false,
		ResponsesEnabled: false,
		RequestFilter:    nil,
		ResponseFilter:   nil,
	})
	svc.senderSvc.SetActiveProjectID("")
	svc.scope.SetRules(nil)

	return &connect.Response[CloseProjectResponse]{}, nil
}

// DeleteProject removes a project from the repository.
func (svc *Service) DeleteProject(ctx context.Context, req *connect.Request[DeleteProjectRequest]) (*connect.Response[DeleteProjectResponse], error) {
	if svc.activeProjectID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNoProject)
	}

	if err := svc.repo.DeleteProject(ctx, req.Msg.ProjectId); err != nil {
		return nil, fmt.Errorf("proj: could not delete project: %w", err)
	}

	return &connect.Response[DeleteProjectResponse]{}, nil
}

// OpenProject sets a project as the currently active project.
func (svc *Service) OpenProject(ctx context.Context, req *connect.Request[OpenProjectRequest]) (*connect.Response[OpenProjectResponse], error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	p, err := svc.repo.FindProjectByID(ctx, req.Msg.ProjectId)
	if errors.Is(err, ErrProjectNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, ErrProjectNotFound)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("proj: failed to find project: %w", err))
	}

	svc.activeProjectID = p.Id

	interceptSettings := intercept.Settings{
		RequestsEnabled:  p.InterceptRequests,
		ResponsesEnabled: p.InterceptResponses,
	}

	if p.InterceptRequestFilterExpr != "" {
		expr, err := filter.ParseQuery(p.InterceptRequestFilterExpr)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("proj: failed to parse intercept request filter: %w", err))
		}
		interceptSettings.RequestFilter = expr
	}

	if p.InterceptResponseFilterExpr != "" {
		expr, err := filter.ParseQuery(p.InterceptResponseFilterExpr)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("proj: failed to parse intercept response filter: %w", err))
		}
		interceptSettings.ResponseFilter = expr
	}

	// Request log settings.
	svc.reqLogSvc.SetActiveProjectID(p.Id)
	svc.reqLogSvc.SetBypassOutOfScopeRequests(p.ReqLogBypassOutOfScope)
	svc.reqLogSvc.SetRequestLogsFilter(p.ReqLogFilter)

	// Intercept settings.
	svc.interceptSvc.UpdateSettings(interceptSettings)

	// Sender settings.
	svc.senderSvc.SetActiveProjectID(p.Id)
	svc.senderSvc.SetRequestsFilter(&sender.RequestsFilter{
		OnlyInScope: p.SenderOnlyFindInScope,
		SearchExpr:  p.SenderSearchExpr,
	})

	// Scope settings.
	scopeRules, err := p.ParseScopeRules()
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("proj: failed to parse scope rules: %w", err))
	}
	svc.scope.SetRules(scopeRules)

	p.IsActive = true

	return &connect.Response[OpenProjectResponse]{
		Msg: &OpenProjectResponse{
			Project: p,
		},
	}, nil
}

func (svc *Service) GetActiveProject(ctx context.Context, _ *connect.Request[GetActiveProjectRequest]) (*connect.Response[GetActiveProjectResponse], error) {
	project, err := svc.activeProject(ctx)
	if errors.Is(err, ErrNoProject) {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	project.IsActive = true

	return &connect.Response[GetActiveProjectResponse]{
		Msg: &GetActiveProjectResponse{
			Project: project,
		},
	}, nil
}

func (svc *Service) activeProject(ctx context.Context) (*Project, error) {
	if svc.activeProjectID == "" {
		return nil, ErrNoProject
	}

	project, err := svc.repo.FindProjectByID(ctx, svc.activeProjectID)
	if err != nil {
		return nil, fmt.Errorf("proj: failed to get active project: %w", err)
	}

	return project, nil
}

func (svc *Service) ListProjects(ctx context.Context, _ *connect.Request[ListProjectsRequest]) (*connect.Response[ListProjectsResponse], error) {
	projects, err := svc.repo.Projects(ctx)
	if err != nil {
		return nil, fmt.Errorf("proj: could not get projects: %w", err)
	}

	for _, project := range projects {
		if svc.IsProjectActive(project.Id) {
			project.IsActive = true
		}
	}

	return &connect.Response[ListProjectsResponse]{
		Msg: &ListProjectsResponse{
			Projects: projects,
		},
	}, nil
}

func (svc *Service) Scope() *scope.Scope {
	return svc.scope
}

func (svc *Service) SetScopeRules(ctx context.Context, req *connect.Request[SetScopeRulesRequest]) (*connect.Response[SetScopeRulesResponse], error) {
	p, err := svc.activeProject(ctx)
	if errors.Is(err, ErrNoProject) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	p.ScopeRules = req.Msg.Rules

	err = svc.repo.UpsertProject(ctx, p)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("proj: failed to update project: %w", err))
	}

	scopeRules, err := p.ParseScopeRules()
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("proj: failed to parse scope rules: %w", err))
	}

	svc.scope.SetRules(scopeRules)

	return &connect.Response[SetScopeRulesResponse]{}, nil
}

func (p *Project) ParseScopeRules() ([]scope.Rule, error) {
	var err error
	scopeRules := make([]scope.Rule, len(p.ScopeRules))

	for i, rule := range p.ScopeRules {
		scopeRules[i] = scope.Rule{}
		if rule.UrlRegexp != "" {
			scopeRules[i].URL, err = regexp.Compile(rule.UrlRegexp)
			if err != nil {
				return nil, fmt.Errorf("failed to parse scope rule's URL field: %w", err)
			}
		}
		if rule.HeaderKeyRegexp != "" {
			scopeRules[i].Header.Key, err = regexp.Compile(rule.HeaderKeyRegexp)
			if err != nil {
				return nil, fmt.Errorf("failed to parse scope rule's header key field: %w", err)
			}
		}
		if rule.HeaderValueRegexp != "" {
			scopeRules[i].Header.Value, err = regexp.Compile(rule.HeaderValueRegexp)
			if err != nil {
				return nil, fmt.Errorf("failed to parse scope rule's header value field: %w", err)
			}
		}
		if rule.BodyRegexp != "" {
			scopeRules[i].Body, err = regexp.Compile(rule.BodyRegexp)
			if err != nil {
				return nil, fmt.Errorf("failed to parse scope rule's body field: %w", err)
			}
		}
	}

	return scopeRules, nil
}

func (svc *Service) SetRequestLogsFilter(ctx context.Context, req *connect.Request[SetRequestLogsFilterRequest]) (*connect.Response[SetRequestLogsFilterResponse], error) {
	project, err := svc.activeProject(ctx)
	if errors.Is(err, ErrNoProject) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	project.ReqLogFilter = req.Msg.Filter

	err = svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("proj: failed to update project: %w", err))
	}

	svc.reqLogSvc.SetRequestLogsFilter(req.Msg.Filter)

	return &connect.Response[SetRequestLogsFilterResponse]{}, nil
}

func (svc *Service) SetSenderRequestFindFilter(ctx context.Context, filter *sender.RequestsFilter) error {
	project, err := svc.activeProject(ctx)
	if err != nil {
		return err
	}

	project.SenderOnlyFindInScope = filter.OnlyInScope
	project.SenderSearchExpr = filter.SearchExpr

	err = svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return fmt.Errorf("proj: failed to update project: %w", err)
	}

	svc.senderSvc.SetRequestsFilter(filter)

	return nil
}

func (svc *Service) IsProjectActive(projectID string) bool {
	return projectID == svc.activeProjectID
}

func (svc *Service) UpdateInterceptSettings(ctx context.Context, req *connect.Request[UpdateInterceptSettingsRequest]) (*connect.Response[UpdateInterceptSettingsResponse], error) {
	project, err := svc.activeProject(ctx)
	if err != nil {
		return nil, err
	}

	project.InterceptRequests = req.Msg.RequestsEnabled
	project.InterceptResponses = req.Msg.ResponsesEnabled
	project.InterceptRequestFilterExpr = req.Msg.RequestFilterExpr
	project.InterceptResponseFilterExpr = req.Msg.ResponseFilterExpr

	err = svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("proj: failed to update project: %w", err))
	}

	reqFilterExpr, err := filter.ParseQuery(req.Msg.RequestFilterExpr)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("proj: failed to parse intercept request filter: %w", err))
	}

	respFilterExpr, err := filter.ParseQuery(req.Msg.ResponseFilterExpr)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("proj: failed to parse intercept response filter: %w", err))
	}

	svc.interceptSvc.UpdateSettings(intercept.Settings{
		RequestsEnabled:  req.Msg.RequestsEnabled,
		ResponsesEnabled: req.Msg.ResponsesEnabled,
		RequestFilter:    reqFilterExpr,
		ResponseFilter:   respFilterExpr,
	})

	return &connect.Response[UpdateInterceptSettingsResponse]{}, nil
}
