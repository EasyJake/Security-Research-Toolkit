package reqlog_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	httppb "github.com/dstotijn/hetty/pkg/http"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/testutil"
)

//nolint:paralleltest
func TestRequestModifier(t *testing.T) {
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

	projectID := ulid.Make().String()
	err = db.UpsertProject(context.Background(), &proj.Project{
		Id: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	svc := reqlog.NewService(reqlog.Config{
		Repository: db,
		Scope:      &scope.Scope{},
	})
	svc.SetActiveProjectID(projectID)

	next := func(req *http.Request) {
		req.Body = io.NopCloser(strings.NewReader("modified body"))
	}
	reqModFn := svc.RequestModifier(next)
	req := httptest.NewRequest("GET", "https://example.com/", strings.NewReader("bar"))
	req.Header.Add("X-Yolo", "swag")
	reqID := ulid.Make()
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqModFn(req)

	t.Run("request log was stored in repository", func(t *testing.T) {
		exp := &reqlog.HttpRequestLog{
			Id:        reqID.String(),
			ProjectId: svc.ActiveProjectID(),
			Request: &httppb.Request{
				Url:      "https://example.com/",
				Method:   httppb.Method_METHOD_GET,
				Protocol: httppb.Protocol_PROTOCOL_HTTP11,
				Headers: []*httppb.Header{
					{
						Key:   "X-Yolo",
						Value: "swag",
					},
				},
				Body: []byte("modified body"),
			},
		}

		got, err := db.FindRequestLogByID(context.Background(), svc.ActiveProjectID(), reqID.String())
		if err != nil {
			t.Fatalf("failed to find request by id: %v", err)
		}

		testutil.ProtoDiff(t, "request log not equal", exp, got)
	})
}

func TestResponseModifier(t *testing.T) {
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
		Id: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error upserting project: %v", err)
	}

	svc := reqlog.NewService(reqlog.Config{
		Repository: db,
	})
	svc.SetActiveProjectID(projectID)

	next := func(res *http.Response) error {
		res.Body = io.NopCloser(strings.NewReader("modified body"))
		return nil
	}
	resModFn := svc.ResponseModifier(next)

	req := httptest.NewRequest("GET", "https://example.com/", strings.NewReader("bar"))
	reqLogID := ulid.Make()
	req = req.WithContext(context.WithValue(req.Context(), reqlog.ReqLogIDKey, reqLogID))

	err = db.StoreRequestLog(context.Background(), &reqlog.HttpRequestLog{
		Id:        reqLogID.String(),
		ProjectId: projectID,
	})
	if err != nil {
		t.Fatalf("failed to store request log: %v", err)
	}

	res := &http.Response{
		Request:    req,
		Proto:      "HTTP/1.1",
		Status:     "200 OK",
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("bar")),
	}

	if err := resModFn(res); err != nil {
		t.Fatalf("unexpected error (expected: nil, got: %v)", err)
	}

	// Dirty (but simple) wait for other goroutine to finish calling repository.
	time.Sleep(10 * time.Millisecond)

	got, err := db.FindRequestLogByID(context.Background(), svc.ActiveProjectID(), reqLogID.String())
	if err != nil {
		t.Fatalf("failed to find request by id: %v", err)
	}

	exp := &httppb.Response{
		Protocol:   httppb.Protocol_PROTOCOL_HTTP11,
		Status:     "200 OK",
		StatusCode: 200,
		Headers:    []*httppb.Header{},
		Body:       []byte("modified body"),
	}

	testutil.ProtoDiff(t, "response not equal", exp, got.GetResponse())
}
