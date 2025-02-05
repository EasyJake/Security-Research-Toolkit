package bolt_test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/testutil"
)

func TestFindRequestLogs(t *testing.T) {
	t.Parallel()

	t.Run("returns request logs and related response logs", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir() + "bolt.db"
		boltDB, err := bbolt.Open(path, 0o600, nil)
		if err != nil {
			t.Fatalf("failed to open bolt database: %v", err)
		}

		db, err := bolt.DatabaseFromBoltDB(boltDB)
		if err != nil {
			t.Fatalf("failed to create database: %v", err)
		}
		defer db.Close()

		projectID := ulid.Make().String()

		err = db.UpsertProject(context.Background(), &proj.Project{
			Id: projectID,
		})
		if err != nil {
			t.Fatalf("unexpected error upserting project: %v", err)
		}

		fixtures := []*reqlog.HttpRequestLog{
			{
				Id:        ulid.Make().String(),
				ProjectId: projectID,
				Request: &http.Request{
					Url:      "https://example.com/foobar",
					Method:   http.Method_METHOD_POST,
					Protocol: http.Protocol_PROTOCOL_HTTP11,
					Headers: []*http.Header{
						{Key: "X-Foo", Value: "baz"},
					},
					Body: []byte("foo"),
				},
				Response: &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Headers: []*http.Header{
						{Key: "X-Yolo", Value: "swag"},
					},
					Body: []byte("bar"),
				},
			},
			{
				Id:        ulid.Make().String(),
				ProjectId: projectID,
				Request: &http.Request{
					Url:      "https://example.com/foo?bar=baz",
					Method:   http.Method_METHOD_GET,
					Protocol: http.Protocol_PROTOCOL_HTTP11,
					Headers: []*http.Header{
						{Key: "X-Foo", Value: "baz"},
					},
					Body: []byte("foo"),
				},
				Response: &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Headers: []*http.Header{
						{Key: "X-Yolo", Value: "swag"},
					},
					Body: []byte("bar"),
				},
			},
		}

		// Store fixtures.
		for _, reqLog := range fixtures {
			err = db.StoreRequestLog(context.Background(), reqLog)
			if err != nil {
				t.Fatalf("unexpected error creating request log fixture: %v", err)
			}
		}

		got, err := db.FindRequestLogs(context.Background(), projectID, nil)
		if err != nil {
			t.Fatalf("unexpected error finding request logs: %v", err)
		}

		// We expect the found request logs are *reversed*, e.g. newest first.
		exp := make([]*reqlog.HttpRequestLog, len(fixtures))
		for i, j := 0, len(fixtures)-1; i < j; i, j = i+1, j-1 {
			exp[i], exp[j] = fixtures[j], fixtures[i]
		}

		testutil.ProtoSlicesDiff(t, "request logs not equal", exp, got)
	})
}
