package proj

import (
	"context"
)

type Repository interface {
	FindProjectByID(ctx context.Context, id string) (*Project, error)
	UpsertProject(ctx context.Context, project *Project) error
	DeleteProject(ctx context.Context, id string) error
	Projects(ctx context.Context) ([]*Project, error)
	Close() error
}
