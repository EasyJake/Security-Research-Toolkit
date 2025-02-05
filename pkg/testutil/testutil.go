package testutil

import (
	"testing"

	"github.com/dstotijn/hetty/pkg/log"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
)

func ProtoDiff[M proto.Message](t *testing.T, msg string, exp, got M, ignoreFields ...protoreflect.Name) {
	t.Helper()

	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(exp, ignoreFields...),
	}

	if diff := cmp.Diff(exp, got, opts...); diff != "" {
		t.Fatalf("%v (-exp, +got):\n%v", msg, diff)
	}
}

func ProtoSlicesDiff[M proto.Message](t *testing.T, msg string, exp, got []M, ignoreFields ...protoreflect.Name) {
	t.Helper()

	opts := []cmp.Option{
		protocmp.Transform(),
	}
	if len(exp) > 0 {
		opts = append(opts, protocmp.IgnoreFields(exp[0], ignoreFields...))
	}

	if diff := cmp.Diff(exp, got, opts...); diff != "" {
		t.Fatalf("%v (-exp, +got):\n%v", msg, diff)
	}
}

type testLogger struct {
	log.NopLogger
	t *testing.T
}

func (l *testLogger) Errorw(msg string, v ...interface{}) {
	l.t.Helper()
	l.t.Fatalf(msg+": %v", v...)
}

func NewLogger(t *testing.T) log.Logger {
	t.Helper()
	return &testLogger{t: t}
}
