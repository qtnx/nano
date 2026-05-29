package nano

import (
	"testing"
	"time"

	"github.com/lonng/nano/cluster"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/serialize/protobuf"
	"google.golang.org/grpc"
)

// M20: an invalid heartbeat interval must not overwrite the default (it would
// otherwise panic time.NewTicker in connection/heartbeat goroutines).
func TestWithHeartbeatInterval_Invalid(t *testing.T) {
	prev := env.Heartbeat
	defer func() { env.Heartbeat = prev }()

	env.Heartbeat = 7 * time.Second
	WithHeartbeatInterval(0)(&cluster.Options{})
	if env.Heartbeat != 7*time.Second {
		t.Fatalf("zero interval overwrote heartbeat: %v", env.Heartbeat)
	}
	WithHeartbeatInterval(-5 * time.Second)(&cluster.Options{})
	if env.Heartbeat != 7*time.Second {
		t.Fatalf("negative interval overwrote heartbeat: %v", env.Heartbeat)
	}
	WithHeartbeatInterval(10 * time.Second)(&cluster.Options{})
	if env.Heartbeat != 10*time.Second {
		t.Fatalf("valid interval not applied: %v", env.Heartbeat)
	}
}

// M21: a nil serializer must not clear the default (it would otherwise nil-deref
// on the first non-raw request).
func TestWithSerializer_Nil(t *testing.T) {
	prev := env.Serializer
	defer func() { env.Serializer = prev }()

	WithSerializer(nil)(&cluster.Options{})
	if env.Serializer == nil {
		t.Fatal("nil serializer cleared the default")
	}

	s := protobuf.NewSerializer()
	WithSerializer(s)(&cluster.Options{})
	if env.Serializer == nil {
		t.Fatal("valid serializer not applied")
	}
}

// M22: a nil handshake validator must not clear the default (it would otherwise
// nil-call on the first handshake).
func TestWithHandshakeValidator_Nil(t *testing.T) {
	prev := env.HandshakeValidator
	defer func() { env.HandshakeValidator = prev }()

	WithHandshakeValidator(nil)(&cluster.Options{})
	if env.HandshakeValidator == nil {
		t.Fatal("nil handshake validator cleared the default")
	}
	if err := env.HandshakeValidator(nil, nil); err != nil {
		t.Fatalf("default handshake validator returned error: %v", err)
	}
}

// M23: a nil CheckOrigin must not clear the default (it would otherwise nil-call
// on the first WS upgrade).
func TestWithCheckOriginFunc_Nil(t *testing.T) {
	prev := env.CheckOrigin
	defer func() { env.CheckOrigin = prev }()

	WithCheckOriginFunc(nil)(&cluster.Options{})
	if env.CheckOrigin == nil {
		t.Fatal("nil origin checker cleared the default")
	}
	if !env.CheckOrigin(nil) {
		t.Fatal("default origin checker should allow")
	}
}

// M32: a non-positive retry interval must not be accepted (it disables register
// backoff and causes a tight retry/log storm).
func TestWithAdvertiseAddr_RetryInterval(t *testing.T) {
	var opt cluster.Options
	WithAdvertiseAddr("addr", -1*time.Second)(&opt)
	if opt.RetryInterval != 0 {
		t.Fatalf("negative retry interval accepted: %v", opt.RetryInterval)
	}
	if opt.AdvertiseAddr != "addr" {
		t.Fatalf("advertise addr not set: %q", opt.AdvertiseAddr)
	}

	var opt2 cluster.Options
	WithAdvertiseAddr("addr", 2*time.Second)(&opt2)
	if opt2.RetryInterval != 2*time.Second {
		t.Fatalf("valid retry interval not applied: %v", opt2.RetryInterval)
	}

	var opt3 cluster.Options
	WithAdvertiseAddr("addr", 0)(&opt3)
	if opt3.RetryInterval != 0 {
		t.Fatalf("zero retry interval should leave default zero: %v", opt3.RetryInterval)
	}
}

// M34: a nil grpc dial option must not be stored (it would otherwise panic on
// the next cluster dial).
func TestWithGrpcOptions_SkipsNil(t *testing.T) {
	prev := env.GrpcOptions
	defer func() { env.GrpcOptions = prev }()

	before := len(env.GrpcOptions)
	WithGrpcOptions(nil)(&cluster.Options{})
	for i, o := range env.GrpcOptions {
		if o == nil {
			t.Fatalf("env.GrpcOptions[%d] is nil after WithGrpcOptions(nil)", i)
		}
	}
	if len(env.GrpcOptions) != before {
		t.Fatalf("nil grpc dial option appended (%d -> %d)", before, len(env.GrpcOptions))
	}

	WithGrpcOptions(grpc.WithInsecure())(&cluster.Options{})
	if len(env.GrpcOptions) != before+1 {
		t.Fatalf("valid grpc dial option not appended (%d -> %d)", before, len(env.GrpcOptions))
	}
}

// L7: nil components must not clear the safe default (it would otherwise
// nil-panic during Startup).
func TestWithComponents_Nil(t *testing.T) {
	opt := cluster.Options{Components: &component.Components{}}
	WithComponents(nil)(&opt)
	if opt.Components == nil {
		t.Fatal("nil components cleared the default")
	}

	comps := &component.Components{}
	WithComponents(comps)(&opt)
	if opt.Components != comps {
		t.Fatal("valid components not applied")
	}
}

// L10: reserved HTTP paths must not be accepted as the WS path (they would
// silently shadow the WS route and make it unreachable).
func TestWithWSPath_Reserved(t *testing.T) {
	prev := env.WSPath
	defer func() { env.WSPath = prev }()

	for _, reserved := range []string{"/api", "/sse", "/health"} {
		env.WSPath = "/ws"
		WithWSPath(reserved)(&cluster.Options{})
		if env.WSPath == reserved {
			t.Fatalf("reserved WS path accepted: %q", reserved)
		}
	}

	env.WSPath = ""
	WithWSPath("/socket")(&cluster.Options{})
	if env.WSPath != "/socket" {
		t.Fatalf("valid WS path not applied: %q", env.WSPath)
	}
}

// H1/H11/H9: the config setters must store into env (negatives clamp safely).
func TestWithScheduler(t *testing.T) {
	prev := env.SchedulerShards
	defer func() { env.SchedulerShards = prev }()
	WithScheduler(8)(&cluster.Options{})
	if env.SchedulerShards != 8 {
		t.Fatalf("want 8, got %d", env.SchedulerShards)
	}
	WithScheduler(-1)(&cluster.Options{})
	if env.SchedulerShards != 0 {
		t.Fatalf("negative must clamp to 0, got %d", env.SchedulerShards)
	}
}

func TestWithMaxConnections(t *testing.T) {
	prev := env.MaxConnections
	defer func() { env.MaxConnections = prev }()
	WithMaxConnections(100)(&cluster.Options{})
	if env.MaxConnections != 100 {
		t.Fatalf("want 100, got %d", env.MaxConnections)
	}
	WithMaxConnections(-5)(&cluster.Options{})
	if env.MaxConnections != 0 {
		t.Fatalf("negative must clamp to 0 (unlimited), got %d", env.MaxConnections)
	}
}

func TestWithClusterAuthToken(t *testing.T) {
	prev := env.ClusterAuthToken
	defer func() { env.ClusterAuthToken = prev }()
	WithClusterAuthToken("s3cret")(&cluster.Options{})
	if env.ClusterAuthToken != "s3cret" {
		t.Fatalf("got %q", env.ClusterAuthToken)
	}
}

func TestWithInsecureCluster(t *testing.T) {
	prev := env.InsecureCluster
	defer func() { env.InsecureCluster = prev }()
	env.InsecureCluster = false
	WithInsecureCluster()(&cluster.Options{})
	if !env.InsecureCluster {
		t.Fatal("want true")
	}
}
