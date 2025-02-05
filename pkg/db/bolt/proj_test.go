package bolt_test

import (
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/testutil"
)

func TestUpsertProject(t *testing.T) {
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

	exp := &proj.Project{
		Id:                     "foobar-project-id",
		Name:                   "foobar",
		ReqLogBypassOutOfScope: true,
		ReqLogFilter: &reqlog.RequestLogsFilter{
			OnlyInScope: true,
			SearchExpr:  "foo AND bar OR NOT baz",
		},
		ScopeRules: []*scope.ScopeRule{
			{
				UrlRegexp:         "^https://(.*)example.com(.*)$",
				HeaderKeyRegexp:   "^X-Foo(.*)$",
				HeaderValueRegexp: "^foo(.*)$",
				BodyRegexp:        "^foo(.*)",
			},
		},
	}

	err = db.UpsertProject(context.Background(), exp)
	if err != nil {
		t.Fatalf("unexpected error storing project: %v", err)
	}

	var rawProject []byte

	err = boltDB.View(func(tx *bbolt.Tx) error {
		rawProject = tx.Bucket([]byte("projects")).Bucket([]byte(exp.Id)).Get([]byte("project"))
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error retrieving project from database: %v", err)
	}
	if rawProject == nil {
		t.Fatalf("expected raw project to be retrieved, got: nil")
	}

	got := &proj.Project{}

	err = proto.Unmarshal(rawProject, got)
	if err != nil {
		t.Fatalf("unexpected error decoding project: %v", err)
	}

	testutil.ProtoDiff(t, "project not equal", exp, got, "id")
}

func TestFindProjectByID(t *testing.T) {
	t.Parallel()

	t.Run("existing project", func(t *testing.T) {
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

		exp := &proj.Project{
			Id: ulid.Make().String(),
		}

		buf, err := proto.Marshal(exp)
		if err != nil {
			t.Fatalf("unexpected error encoding project: %v", err)
		}

		err = boltDB.Update(func(tx *bbolt.Tx) error {
			b, err := tx.Bucket([]byte("projects")).CreateBucket([]byte(exp.Id))
			if err != nil {
				return err
			}
			return b.Put([]byte("project"), buf)
		})
		if err != nil {
			t.Fatalf("unexpected error setting project: %v", err)
		}

		got, err := db.FindProjectByID(context.Background(), exp.Id)
		if err != nil {
			t.Fatalf("unexpected error finding project: %v", err)
		}

		testutil.ProtoDiff(t, "project not equal", exp, got)
	})

	t.Run("project not found", func(t *testing.T) {
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

		_, err = db.FindProjectByID(context.Background(), projectID)
		if !errors.Is(err, proj.ErrProjectNotFound) {
			t.Fatalf("expected `proj.ErrProjectNotFound`, got: %v", err)
		}
	})
}

func TestDeleteProject(t *testing.T) {
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

	// Insert test fixture.
	projectID := ulid.Make().String()
	err = db.UpsertProject(context.Background(), &proj.Project{
		Id: projectID,
	})
	if err != nil {
		t.Fatalf("unexpected error storing project: %v", err)
	}

	err = db.DeleteProject(context.Background(), projectID)
	if err != nil {
		t.Fatalf("unexpected error deleting project: %v", err)
	}

	var got *bbolt.Bucket
	_ = boltDB.View(func(tx *bbolt.Tx) error {
		got = tx.Bucket([]byte("projects")).Bucket([]byte(projectID))
		return nil
	})
	if got != nil {
		t.Fatalf("expected bucket to be nil, got: %v", got)
	}
}

func TestProjects(t *testing.T) {
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

	exp := []*proj.Project{
		{
			Id:   ulid.Make().String(),
			Name: "one",
		},
		{
			Id:   ulid.Make().String(),
			Name: "two",
		},
	}

	// Store fixtures.
	for _, project := range exp {
		err = db.UpsertProject(context.Background(), project)
		if err != nil {
			t.Fatalf("unexpected error creating project fixture: %v", err)
		}
	}

	got, err := db.Projects(context.Background())
	if err != nil {
		t.Fatalf("unexpected error finding projects: %v", err)
	}

	if len(exp) != len(got) {
		t.Fatalf("expected %v projects, got: %v", len(exp), len(got))
	}

	testutil.ProtoSlicesDiff(t, "projects not equal", exp, got)
}
