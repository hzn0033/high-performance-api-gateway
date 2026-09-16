package gateway

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hzn0033/high-performance-api-gateway/internal/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func serveGRPC(t *testing.T, server *grpc.Server) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(l)
	t.Cleanup(server.Stop)
	return l.Addr().String()
}

func dialTest(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestGRPCUnaryAndUnknownService(t *testing.T) {
	upstream := grpc.NewServer()
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(upstream, h)
	addr := serveGRPC(t, upstream)
	proxy, closeConnections, err := NewGRPC(map[string]string{"grpc.health.v1.Health": addr}, ratelimit.New(100, 100, 10), &Stats{}, 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeConnections)
	conn := dialTest(t, serveGRPC(t, proxy))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("response=%v err=%v", resp, err)
	}
	err = conn.Invoke(ctx, "/missing.Service/Call", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown service status: %v", err)
	}
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("upstream error lost: %v", err)
	}
}

func TestGRPCBidirectionalMetadataAndHalfClose(t *testing.T) {
	upstream := grpc.NewServer()
	upstream.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo", HandlerType: (*interface{})(nil),
		Streams: []grpc.StreamDesc{{StreamName: "Chat", ClientStreams: true, ServerStreams: true, Handler: func(_ any, s grpc.ServerStream) error {
			md, _ := metadata.FromIncomingContext(s.Context())
			if got := md.Get("x-request-id"); len(got) != 1 || got[0] != "request-42" {
				return status.Error(codes.InvalidArgument, "metadata missing")
			}
			s.SendHeader(metadata.Pairs("x-backend", "echo"))
			s.SetTrailer(metadata.Pairs("x-finished", "yes"))
			for {
				req := new(healthpb.HealthCheckRequest)
				err := s.RecvMsg(req)
				if err == io.EOF {
					return s.SendMsg(&healthpb.HealthCheckRequest{Service: "after-half-close"})
				}
				if err != nil {
					return err
				}
				if err = s.SendMsg(req); err != nil {
					return err
				}
			}
		}}},
	}, struct{}{})
	addr := serveGRPC(t, upstream)
	proxy, closeConnections, err := NewGRPC(map[string]string{"test.Echo": addr}, ratelimit.New(100, 100, 10), &Stats{}, 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeConnections)
	conn := dialTest(t, serveGRPC(t, proxy))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-request-id", "request-42"))
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.Echo/Chat")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"one", "two", "three"} {
		if err := stream.SendMsg(&healthpb.HealthCheckRequest{Service: value}); err != nil {
			t.Fatal(err)
		}
		resp := new(healthpb.HealthCheckRequest)
		if err := stream.RecvMsg(resp); err != nil {
			t.Fatal(err)
		}
		if resp.Service != value {
			t.Fatalf("got %q, want %q", resp.Service, value)
		}
	}
	stream.CloseSend()
	resp := new(healthpb.HealthCheckRequest)
	if err := stream.RecvMsg(resp); err != nil || resp.Service != "after-half-close" {
		t.Fatalf("half-close response=%v err=%v", resp, err)
	}
	if err := stream.RecvMsg(resp); err != io.EOF {
		t.Fatalf("expected EOF: %v", err)
	}
	headers, err := stream.Header()
	if err != nil || len(headers.Get("x-backend")) != 1 || len(stream.Trailer().Get("x-finished")) != 1 {
		t.Fatalf("headers/trailers lost: %v %v", headers, stream.Trailer())
	}
}

func TestGRPCCancellationAndDeadline(t *testing.T) {
	for _, clientCancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "gateway-deadline", true: "client-cancellation"}[clientCancel], func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			upstream := grpc.NewServer()
			upstream.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Wait", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Wait", ServerStreams: true, ClientStreams: true, Handler: func(_ any, s grpc.ServerStream) error {
				close(started)
				<-s.Context().Done()
				close(stopped)
				return status.FromContextError(s.Context().Err()).Err()
			}}}}, struct{}{})
			addr := serveGRPC(t, upstream)
			timeout := 200 * time.Millisecond
			if clientCancel {
				timeout = 3 * time.Second
			}
			proxy, closeConnections, err := NewGRPC(map[string]string{"test.Wait": addr}, ratelimit.New(100, 100, 10), &Stats{}, 10, timeout)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(closeConnections)
			conn := dialTest(t, serveGRPC(t, proxy))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.Wait/Wait")
			if err != nil {
				t.Fatal(err)
			}
			stream.SendMsg(&healthpb.HealthCheckRequest{})
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("upstream did not start")
			}
			if clientCancel {
				cancel()
			}
			err = stream.RecvMsg(new(healthpb.HealthCheckResponse))
			want := codes.DeadlineExceeded
			if clientCancel {
				want = codes.Canceled
			}
			if status.Code(err) != want {
				t.Fatalf("got %v want %v", err, want)
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("upstream context leaked")
			}
		})
	}
}
