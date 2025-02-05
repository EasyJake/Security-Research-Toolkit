package bolt_test

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/sender"
	"github.com/dstotijn/hetty/pkg/testutil"
)

var exampleURL = func() *url.URL {
	u, err := url.Parse("https://example.com/foobar")
	if err != nil {
		panic(err)
	}

	return u
}()

func TestFindRequestByID(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "bolt.db"
	boltDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}
	defer boltDB.Close()

	db, err := bolt.DatabaseFromBoltDB(boltDB)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	projectID := "foobar-project-id"
	reqID := "foobar-req-id"

	err = db.UpsertProject(context.Background(), &proj.Project{
		Id: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	// See: https://go.dev/blog/subtests#cleaning-up-after-a-group-of-parallel-tests
	t.Run("group", func(t *testing.T) {
		t.Run("sender request not found", func(t *testing.T) {
			t.Parallel()

			_, err := db.FindSenderRequestByID(context.Background(), projectID, reqID)
			if !errors.Is(err, sender.ErrRequestNotFound) {
				t.Fatalf("expected `sender.ErrRequestNotFound`, got: %v", err)
			}
		})

		t.Run("sender request found", func(t *testing.T) {
			t.Parallel()

			exp := &sender.Request{
				Id:                 "foobar-sender-req-id",
				ProjectId:          projectID,
				SourceRequestLogId: "foobar-req-log-id",
				HttpRequest: &http.Request{
					Url:      exampleURL.String(),
					Method:   http.Method_METHOD_GET,
					Protocol: http.Protocol_PROTOCOL_HTTP20,
					Headers: []*http.Header{
						{
							Key:   "X-Foo",
							Value: "bar",
						},
					},
					Body: []byte("foo"),
				},
				HttpResponse: &http.Response{
					Protocol:   http.Protocol_PROTOCOL_HTTP20,
					Status:     "200 OK",
					StatusCode: 200,
					Headers: []*http.Header{
						{
							Key:   "X-Yolo",
							Value: "swag",
						},
					},
					Body: []byte("bar"),
				},
			}

			err := db.StoreSenderRequest(context.Background(), exp)
			if err != nil {
				t.Fatalf("unexpected error (expected: nil, got: %v)", err)
			}

			got, err := db.FindSenderRequestByID(context.Background(), projectID, exp.Id)
			if err != nil {
				t.Fatalf("unexpected error (expected: nil, got: %v)", err)
			}

			testutil.ProtoDiff(t, "sender request not equal", exp, got, "id")
		})
	})
}

func TestFindSenderRequests(t *testing.T) {
	t.Parallel()

	t.Run("returns sender requests and related response logs", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir() + "bolt.db"
		boltDB, err := bbolt.Open(path, 0o600, nil)
		if err != nil {
			t.Fatalf("failed to open bolt database: %v", err)
		}
		defer boltDB.Close()

		db, err := bolt.DatabaseFromBoltDB(boltDB)
		if err != nil {
			t.Fatalf("failed to create database: %v", err)
		}
		defer db.Close()

		projectID := "foobar-project-id"

		err = db.UpsertProject(context.Background(), &proj.Project{
			Id:   projectID,
			Name: "foobar",
		})
		if err != nil {
			t.Fatalf("unexpected error creating project (expected: nil, got: %v)", err)
		}

		fixtures := []*sender.Request{
			{
				Id:                 ulid.Make().String(),
				ProjectId:          projectID,
				SourceRequestLogId: "foobar-req-log-id-1",
				HttpRequest: &http.Request{
					Url:      exampleURL.String(),
					Method:   http.Method_METHOD_POST,
					Protocol: http.Protocol_PROTOCOL_HTTP11,
					Headers: []*http.Header{
						{
							Key:   "X-Foo",
							Value: "baz",
						},
					},
					Body: []byte("foo"),
				},
				HttpResponse: &http.Response{
					Protocol:   http.Protocol_PROTOCOL_HTTP11,
					Status:     "200 OK",
					StatusCode: 200,
					Headers: []*http.Header{
						{
							Key:   "X-Yolo",
							Value: "swag",
						},
					},
					Body: []byte("bar"),
				},
			},
			{
				Id:                 ulid.Make().String(),
				ProjectId:          projectID,
				SourceRequestLogId: "foobar-req-log-id-2",
				HttpRequest: &http.Request{
					Url:      exampleURL.String(),
					Method:   http.Method_METHOD_GET,
					Protocol: http.Protocol_PROTOCOL_HTTP11,
					Headers: []*http.Header{
						{
							Key:   "X-Foo",
							Value: "baz",
						},
					},
					Body: []byte("foo"),
				},
			},
		}

		// Store fixtures.
		for _, senderReq := range fixtures {
			err = db.StoreSenderRequest(context.Background(), senderReq)
			if err != nil {
				t.Fatalf("unexpected error creating request log fixture: %v", err)
			}
		}

		got, err := db.FindSenderRequests(context.Background(), projectID, nil)
		if err != nil {
			t.Fatalf("unexpected error finding sender requests: %v", err)
		}

		// We expect the found sender requests are *reversed*, e.g. newest first.
		exp := make([]*sender.Request, len(fixtures))
		for i, j := 0, len(fixtures)-1; i < j; i, j = i+1, j-1 {
			exp[i], exp[j] = fixtures[j], fixtures[i]
		}

		testutil.ProtoSlicesDiff(t, "sender requests not equal", exp, got)
	})
}
