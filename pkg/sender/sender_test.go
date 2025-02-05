package sender_test

import (
	"context"
	"errors"
	"fmt"
	http "net/http"
	"net/http/httptest"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	httppb "github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/sender"
	"github.com/dstotijn/hetty/pkg/testutil"
	"github.com/google/go-cmp/cmp"
)

func TestStoreRequest(t *testing.T) {
	t.Parallel()

	t.Run("without active project", func(t *testing.T) {
		t.Parallel()

		svc := sender.NewService(sender.Config{})

		_, err := svc.CreateOrUpdateRequest(context.Background(), &connect.Request[sender.CreateOrUpdateRequestRequest]{
			Msg: &sender.CreateOrUpdateRequestRequest{
				Request: &sender.Request{
					HttpRequest: &httppb.Request{
						Url:    "https://example.com/foobar",
						Method: httppb.Method_METHOD_POST,
						Body:   []byte("foobar"),
					},
				},
			},
		})
		if !errors.Is(err, sender.ErrProjectIDMustBeSet) {
			t.Fatalf("expected `sender.ErrProjectIDMustBeSet`, got: %v", err)
		}
	})

	t.Run("with active project", func(t *testing.T) {
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

		svc := sender.NewService(sender.Config{
			Repository: db,
		})

		projectID := "foobar-project-id"
		err = db.UpsertProject(context.Background(), &proj.Project{
			Id:   projectID,
			Name: "foobar",
		})
		if err != nil {
			t.Fatalf("unexpected error upserting project: %v", err)
		}

		svc.SetActiveProjectID(projectID)

		exp := &sender.Request{
			ProjectId: projectID,
			HttpRequest: &httppb.Request{
				Method:   httppb.Method_METHOD_POST,
				Protocol: httppb.Protocol_PROTOCOL_HTTP20,
				Url:      "https://example.com/foobar",
				Headers: []*httppb.Header{
					{Key: "X-Foo", Value: "bar"},
				},
				Body: []byte("foobar"),
			},
		}

		createRes, err := svc.CreateOrUpdateRequest(context.Background(), &connect.Request[sender.CreateOrUpdateRequestRequest]{
			Msg: &sender.CreateOrUpdateRequestRequest{
				Request: exp,
			},
		})
		if err != nil {
			t.Fatalf("unexpected error storing request: %v", err)
		}

		if createRes.Msg.Request.Id == "" {
			t.Fatal("expected request ID to be non-empty value")
		}

		testutil.ProtoDiff(t, "request not equal", exp, createRes.Msg.Request, "id")

		got, err := db.FindSenderRequestByID(context.Background(), projectID, createRes.Msg.Request.Id)
		if err != nil {
			t.Fatalf("failed to find request by ID: %v", err)
		}

		testutil.ProtoDiff(t, "request not equal", exp, got, "id")
	})
}

func TestCloneFromRequestLog(t *testing.T) {
	t.Parallel()

	reqLogID := "foobar-req-log-id"

	t.Run("without active project", func(t *testing.T) {
		t.Parallel()

		svc := sender.NewService(sender.Config{})

		_, err := svc.CloneFromRequestLog(context.Background(), &connect.Request[sender.CloneFromRequestLogRequest]{
			Msg: &sender.CloneFromRequestLogRequest{
				RequestLogId: reqLogID,
			},
		})
		if !errors.Is(err, sender.ErrProjectIDMustBeSet) {
			t.Fatalf("expected `sender.ErrProjectIDMustBeSet`, got: %v", err)
		}
	})

	t.Run("with active project", func(t *testing.T) {
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
			t.Fatalf("unexpected error upserting project: %v", err)
		}

		reqLog := &reqlog.HttpRequestLog{
			Id:        reqLogID,
			ProjectId: projectID,
			Request: &httppb.Request{
				Url:    "https://example.com/foobar",
				Method: httppb.Method_METHOD_POST,
				Body:   []byte("foobar"),
				Headers: []*httppb.Header{
					{Key: "X-Foo", Value: "bar"},
				},
			},
			Response: &httppb.Response{
				Protocol:   httppb.Protocol_PROTOCOL_HTTP20,
				StatusCode: 200,
				Status:     "200 OK",
				Body:       []byte("foobar"),
			},
		}

		if err := db.StoreRequestLog(context.Background(), reqLog); err != nil {
			t.Fatalf("failed to store request log: %v", err)
		}

		svc := sender.NewService(sender.Config{
			ReqLogService: reqlog.NewService(reqlog.Config{
				ActiveProjectID: projectID,
				Repository:      db,
			}),
			Repository: db,
		})

		svc.SetActiveProjectID(projectID)

		exp := &sender.Request{
			SourceRequestLogId: reqLogID,
			ProjectId:          projectID,
			HttpRequest: &httppb.Request{
				Url:    "https://example.com/foobar",
				Method: httppb.Method_METHOD_POST,
				Body:   []byte("foobar"),
				Headers: []*httppb.Header{
					{Key: "X-Foo", Value: "bar"},
				},
			},
		}

		got, err := svc.CloneFromRequestLog(context.Background(), &connect.Request[sender.CloneFromRequestLogRequest]{
			Msg: &sender.CloneFromRequestLogRequest{
				RequestLogId: reqLogID,
			},
		})
		if err != nil {
			t.Fatalf("unexpected error cloning from request log: %v", err)
		}

		testutil.ProtoDiff(t, "request not equal", exp, got.Msg.Request, "id")
	})
}

func TestSendRequest(t *testing.T) {
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

	date := time.Now().Format(http.TimeFormat)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Foobar", "baz")
		w.Header().Set("Date", date)
		fmt.Fprint(w, "baz")
	}))
	defer ts.Close()

	projectID := "foobar-project-id"
	err = db.UpsertProject(context.Background(), &proj.Project{
		Id:   projectID,
		Name: "foobar",
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	reqID := "foobar-req-id"
	req := &sender.Request{
		Id:        reqID,
		ProjectId: projectID,
		HttpRequest: &httppb.Request{
			Url:    ts.URL,
			Method: httppb.Method_METHOD_POST,
			Body:   []byte("foobar"),
			Headers: []*httppb.Header{
				{Key: "X-Foo", Value: "bar"},
			},
		},
	}

	if err := db.StoreSenderRequest(context.Background(), req); err != nil {
		t.Fatalf("failed to store request: %v", err)
	}

	svc := sender.NewService(sender.Config{
		ReqLogService: reqlog.NewService(reqlog.Config{
			Repository: db,
		}),
		Repository: db,
	})
	svc.SetActiveProjectID(projectID)

	exp := &httppb.Response{
		Protocol:   httppb.Protocol_PROTOCOL_HTTP11,
		StatusCode: 200,
		Status:     "200 OK",
		Headers: []*httppb.Header{
			{Key: "Date", Value: date},
			{Key: "Foobar", Value: "baz"},
			{Key: "Content-Length", Value: "3"},
			{Key: "Content-Type", Value: "text/plain; charset=utf-8"},
		},
		Body: []byte("baz"),
	}

	got, err := svc.SendRequest(context.Background(), &connect.Request[sender.SendRequestRequest]{
		Msg: &sender.SendRequestRequest{
			RequestId: reqID,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error sending request: %v", err)
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeated(func(a, b *httppb.Header) bool {
			if a.Key != b.Key {
				return a.Key < b.Key
			}
			return a.Value < b.Value
		}),
	}
	if diff := cmp.Diff(exp, got.Msg.Request.HttpResponse, opts...); diff != "" {
		t.Fatalf("response not equal (-exp, +got):\n%v", diff)
	}
}
