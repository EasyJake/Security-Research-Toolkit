package sender

import (
	"context"
)

type Repository interface {
	FindSenderRequestByID(ctx context.Context, projectID, id string) (*Request, error)
	FindSenderRequests(ctx context.Context, projectID string, filterFn func(*Request) (bool, error)) ([]*Request, error)
	StoreSenderRequest(ctx context.Context, req *Request) error
	DeleteSenderRequests(ctx context.Context, projectID string) error
}
