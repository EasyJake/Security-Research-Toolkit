package bolt

import (
	"context"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/reqlog"
)

var ErrRequestLogsBucketNotFound = errors.New("bolt: request logs bucket not found")

var reqLogsBucketName = []byte("request_logs")

func requestLogsBucket(tx *bolt.Tx, projectID string) (*bolt.Bucket, error) {
	pb, err := projectBucket(tx, projectID)
	if err != nil {
		return nil, err
	}

	b := pb.Bucket(reqLogsBucketName)
	if b == nil {
		return nil, ErrRequestLogsBucketNotFound
	}

	return b, nil
}

func (db *Database) FindRequestLogs(ctx context.Context, projectID string, filterFn func(*reqlog.HttpRequestLog) (bool, error)) (reqLogs []*reqlog.HttpRequestLog, err error) {
	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	b, err := requestLogsBucket(tx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to get request logs bucket: %w", err)
	}

	err = b.ForEach(func(reqLogID, rawReqLog []byte) error {
		var reqLog reqlog.HttpRequestLog
		err = proto.Unmarshal(rawReqLog, &reqLog)
		if err != nil {
			return fmt.Errorf("failed to decode request log: %w", err)
		}

		if filterFn != nil {
			match, err := filterFn(&reqLog)
			if err != nil {
				return fmt.Errorf("failed to filter request log: %w", err)
			}
			if !match {
				return nil
			}
		}

		reqLogs = append(reqLogs, &reqLog)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to iterate over request logs: %w", err)
	}

	// Reverse items, so newest requests appear first.
	for i, j := 0, len(reqLogs)-1; i < j; i, j = i+1, j-1 {
		reqLogs[i], reqLogs[j] = reqLogs[j], reqLogs[i]
	}

	return reqLogs, nil
}

func (db *Database) FindRequestLogByID(ctx context.Context, projectID, reqLogID string) (*reqlog.HttpRequestLog, error) {
	reqLog := &reqlog.HttpRequestLog{}
	err := db.bolt.View(func(tx *bolt.Tx) error {
		b, err := requestLogsBucket(tx, projectID)
		if err != nil {
			return fmt.Errorf("bolt: failed to get request logs bucket: %w", err)
		}
		rawReqLog := b.Get([]byte(reqLogID))
		if rawReqLog == nil {
			return reqlog.ErrRequestLogNotFound
		}

		err = proto.Unmarshal(rawReqLog, reqLog)
		if err != nil {
			return fmt.Errorf("failed to unmarshal request log: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bolt: failed to find request log by ID: %w", err)
	}

	return reqLog, nil
}

func (db *Database) StoreRequestLog(ctx context.Context, reqLog *reqlog.HttpRequestLog) error {
	encReqLog, err := proto.Marshal(reqLog)
	if err != nil {
		return fmt.Errorf("bolt: failed to marshal request log: %w", err)
	}

	err = db.bolt.Update(func(txn *bolt.Tx) error {
		b, err := requestLogsBucket(txn, reqLog.ProjectId)
		if err != nil {
			return fmt.Errorf("failed to get request logs bucket: %w", err)
		}

		err = b.Put([]byte(reqLog.Id), encReqLog)
		if err != nil {
			return fmt.Errorf("failed to put request log: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return nil
}

func (db *Database) StoreResponseLog(ctx context.Context, projectID, reqLogID string, resLog *http.Response) error {
	err := db.bolt.Update(func(txn *bolt.Tx) error {
		b, err := requestLogsBucket(txn, projectID)
		if err != nil {
			return fmt.Errorf("failed to get request logs bucket: %w", err)
		}

		encReqLog := b.Get([]byte(reqLogID))
		if encReqLog == nil {
			return reqlog.ErrRequestLogNotFound
		}

		var reqLog reqlog.HttpRequestLog
		err = proto.Unmarshal(encReqLog, &reqLog)
		if err != nil {
			return fmt.Errorf("failed to decode request log: %w", err)
		}

		reqLog.Response = resLog

		encReqLog, err = proto.Marshal(&reqLog)
		if err != nil {
			return fmt.Errorf("failed to encode request log: %w", err)
		}

		err = b.Put([]byte(reqLogID), encReqLog)
		if err != nil {
			return fmt.Errorf("failed to put request log: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return nil
}

func (db *Database) ClearRequestLogs(ctx context.Context, projectID string) error {
	err := db.bolt.Update(func(txn *bolt.Tx) error {
		pb, err := projectBucket(txn, projectID)
		if err != nil {
			return fmt.Errorf("failed to get project bucket: %w", err)
		}

		return pb.DeleteBucket(reqLogsBucketName)
	})
	if err != nil {
		return fmt.Errorf("bolt: failed to commit transaction: %w", err)
	}

	return nil
}
