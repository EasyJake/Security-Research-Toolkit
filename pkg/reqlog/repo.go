package reqlog

import (
	"context"

	httppb "github.com/dstotijn/hetty/pkg/http"
)

type Repository interface {
	FindRequestLogs(ctx context.Context, projectID string, filterFn func(*HttpRequestLog) (bool, error)) ([]*HttpRequestLog, error)
	FindRequestLogByID(ctx context.Context, projectID, id string) (*HttpRequestLog, error)
	StoreRequestLog(ctx context.Context, reqLog *HttpRequestLog) error
	StoreResponseLog(ctx context.Context, projectID, reqLogID string, resLog *httppb.Response) error
	ClearRequestLogs(ctx context.Context, projectID string) error
}
