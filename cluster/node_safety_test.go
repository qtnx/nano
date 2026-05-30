package cluster

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- C7: a panicking gRPC handler must not crash the node -------------------

func TestRecoveryInterceptorRecoversPanic(t *testing.T) {
	panicHandler := func(_ context.Context, _ interface{}) (interface{}, error) {
		panic("boom: handler panic")
	}

	resp, err := recoveryUnaryInterceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: "/cluster.Member/Test"},
		panicHandler,
	)
	if resp != nil {
		t.Fatalf("expected nil response after recovered panic, got %v", resp)
	}
	if err == nil {
		t.Fatal("expected an error after recovered panic, got nil")
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (%v)", status.Code(err), err)
	}
}

// --- H22: a gRPC serve error must be logged, never escalated to Fatal -------

func TestRunGRPCServerDoesNotFatal(t *testing.T) {
	cl, restore := installCapture(t)
	defer restore()

	// A closed listener makes Serve return immediately with an error.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = lis.Close()

	n := &Node{server: grpc.NewServer()}
	n.runGRPCServer(lis) // synchronous: returns once Serve errors out

	if cl.fatal() {
		t.Fatal("gRPC serve error was escalated to Fatal (would os.Exit the process)")
	}
}

// --- H30: a metrics-listener bind failure must surface as an error ----------

func TestStartPrometheusBindError(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()

	n := &Node{
		Options: Options{
			OpenPrometheus: true,
			PrometheusAddr: busy.Addr().String(),
		},
	}

	if err := n.startPrometheus(); err == nil {
		n.Shutdown()
		t.Fatal("expected startPrometheus to fail on an in-use address, got nil")
	}
}
