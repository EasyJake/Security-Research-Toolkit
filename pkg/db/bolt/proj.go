package bolt

import (
	"context"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dstotijn/hetty/pkg/proj"
)

var (
	ErrProjectsBucketNotFound = errors.New("bolt: projects bucket not found")
	ErrProjectBucketNotFound  = errors.New("bolt: project bucket not found")
)

var (
	projectsBucketName = []byte("projects")
	projectKey         = []byte("project")
)

func projectsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	b := tx.Bucket(projectsBucketName)
	if b == nil {
		return nil, ErrProjectsBucketNotFound
	}

	return b, nil
}

func projectBucket(tx *bolt.Tx, projectID string) (*bolt.Bucket, error) {
	pb, err := projectsBucket(tx)
	if err != nil {
		return nil, err
	}

	b := pb.Bucket([]byte(projectID))
	if b == nil {
		return nil, ErrProjectBucketNotFound
	}

	return b, nil
}

func (db *Database) UpsertProject(ctx context.Context, project *proj.Project) error {
	err := db.bolt.Update(func(tx *bolt.Tx) error {
		b, err := createNestedBucket(tx, projectsBucketName, []byte(project.Id))
		if err != nil {
			return fmt.Errorf("bolt: failed to create project bucket: %w", err)
		}

		buf, err := proto.Marshal(project)
		if err != nil {
			return fmt.Errorf("bolt: failed to marshal project: %w", err)
		}

		err = b.Put(projectKey, buf)
		if err != nil {
			return fmt.Errorf("bolt: failed to upsert project: %w", err)
		}

		_, err = b.CreateBucketIfNotExists(reqLogsBucketName)
		if err != nil {
			return fmt.Errorf("bolt: failed to create request logs bucket: %w", err)
		}

		_, err = b.CreateBucketIfNotExists(senderReqsBucketName)
		if err != nil {
			return fmt.Errorf("bolt: failed to create sender requests bucket: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return nil
}

func (db *Database) FindProjectByID(ctx context.Context, projectID string) (*proj.Project, error) {
	project := &proj.Project{}

	err := db.bolt.View(func(tx *bolt.Tx) error {
		bucket, err := projectBucket(tx, projectID)
		if errors.Is(err, ErrProjectsBucketNotFound) || errors.Is(err, ErrProjectBucketNotFound) {
			return proj.ErrProjectNotFound
		}
		if err != nil {
			return err
		}

		rawProject := bucket.Get(projectKey)
		if rawProject == nil {
			return proj.ErrProjectNotFound
		}

		err = proto.Unmarshal(rawProject, project)
		if err != nil {
			return fmt.Errorf("failed to unmarshal project: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return project, nil
}

func (db *Database) DeleteProject(ctx context.Context, projectID string) error {
	err := db.bolt.Update(func(tx *bolt.Tx) error {
		pb, err := projectsBucket(tx)
		if err != nil {
			return err
		}

		err = pb.DeleteBucket([]byte(projectID))
		if err != nil {
			return fmt.Errorf("failed to delete project bucket: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return nil
}

func (db *Database) Projects(ctx context.Context) ([]*proj.Project, error) {
	projects := make([]*proj.Project, 0)

	err := db.bolt.View(func(tx *bolt.Tx) error {
		pb, err := projectsBucket(tx)
		if err != nil {
			return err
		}

		err = pb.ForEachBucket(func(projectID []byte) error {
			bucket, err := projectBucket(tx, string(projectID))
			if err != nil {
				return err
			}

			rawProject := bucket.Get(projectKey)
			if rawProject == nil {
				return proj.ErrProjectNotFound
			}

			project := &proj.Project{}
			err = proto.Unmarshal(rawProject, project)
			if err != nil {
				return fmt.Errorf("failed to unmarshal project: %w", err)
			}
			projects = append(projects, project)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to iterate over projects: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return projects, nil
}
