package bolt

import (
	"context"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dstotijn/hetty/pkg/sender"
)

var ErrSenderRequestsBucketNotFound = errors.New("bolt: sender requests bucket not found")

var senderReqsBucketName = []byte("sender_requests")

func senderReqsBucket(tx *bolt.Tx, projectID string) (*bolt.Bucket, error) {
	pb, err := projectBucket(tx, projectID)
	if err != nil {
		return nil, err
	}

	b := pb.Bucket(senderReqsBucketName)
	if b == nil {
		return nil, ErrSenderRequestsBucketNotFound
	}

	return b, nil
}

func (db *Database) StoreSenderRequest(ctx context.Context, req *sender.Request) error {
	rawReq, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("bolt: failed to marshal sender request: %w", err)
	}

	err = db.bolt.Update(func(tx *bolt.Tx) error {
		senderReqsBucket, err := senderReqsBucket(tx, req.ProjectId)
		if err != nil {
			return fmt.Errorf("failed to get sender requests bucket: %w", err)
		}

		err = senderReqsBucket.Put([]byte(req.Id), rawReq)
		if err != nil {
			return fmt.Errorf("failed to put sender request: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return nil
}

func (db *Database) FindSenderRequestByID(ctx context.Context, projectID, senderReqID string) (req *sender.Request, err error) {
	if projectID == "" {
		return nil, sender.ErrProjectIDMustBeSet
	}

	err = db.bolt.View(func(tx *bolt.Tx) error {
		senderReqsBucket, err := senderReqsBucket(tx, projectID)
		if err != nil {
			return fmt.Errorf("failed to get sender requests bucket: %w", err)
		}

		rawSenderReq := senderReqsBucket.Get([]byte(senderReqID))
		if rawSenderReq == nil {
			return sender.ErrRequestNotFound
		}

		req = &sender.Request{}
		err = proto.Unmarshal(rawSenderReq, req)
		if err != nil {
			return fmt.Errorf("failed to unmarshal sender request: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return req, nil
}

func (db *Database) FindSenderRequests(ctx context.Context, projectID string, filterFn func(req *sender.Request) (bool, error)) (reqs []*sender.Request, err error) {
	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	b, err := senderReqsBucket(tx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender requests bucket: %w", err)
	}

	err = b.ForEach(func(senderReqID, rawSenderReq []byte) error {
		req := &sender.Request{}
		err = proto.Unmarshal(rawSenderReq, req)
		if err != nil {
			return fmt.Errorf("failed to unmarshal sender request: %w", err)
		}

		if filterFn != nil {
			match, err := filterFn(req)
			if err != nil {
				return fmt.Errorf("failed to filter sender request: %w", err)
			}

			if !match {
				return nil
			}
		}

		reqs = append(reqs, req)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	// Reverse items, so newest requests appear first.
	for i, j := 0, len(reqs)-1; i < j; i, j = i+1, j-1 {
		reqs[i], reqs[j] = reqs[j], reqs[i]
	}

	return reqs, nil
}

func (db *Database) DeleteSenderRequests(ctx context.Context, projectID string) error {
	err := db.bolt.Update(func(tx *bolt.Tx) error {
		senderReqsBucket, err := senderReqsBucket(tx, projectID)
		if err != nil {
			return fmt.Errorf("failed to get sender requests bucket: %w", err)
		}

		err = senderReqsBucket.DeleteBucket(senderReqsBucketName)
		if err != nil {
			return fmt.Errorf("failed to delete sender requests bucket: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return nil
}
