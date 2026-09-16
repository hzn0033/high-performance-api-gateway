package gateway

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hzn0033/high-performance-api-gateway/internal/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// rawCodec forwards protobuf frames without needing the services' generated code.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }
func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("expected raw protobuf frame, got %T", v)
	}
	return *b, nil
}
func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("expected raw protobuf frame, got %T", v)
	}
	*b = append((*b)[:0], data...)
	return nil
}

func NewGRPC(routes map[string]string, limiter *ratelimit.Limiter, stats *Stats, maxConcurrent int, timeout time.Duration) (*grpc.Server, func(), error) {
	connections := make(map[string]*grpc.ClientConn)
	closeConnections := func() {
		for _, c := range connections {
			c.Close()
		}
	}
	for service, target := range routes {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			closeConnections()
			return nil, nil, err
		}
		connections[service] = conn
	}
	slots := make(chan struct{}, maxConcurrent)
	handler := func(_ any, incoming grpc.ServerStream) error {
		method, ok := grpc.MethodFromServerStream(incoming)
		if !ok {
			return status.Error(codes.Internal, "missing method")
		}
		parts := strings.Split(strings.TrimPrefix(method, "/"), "/")
		if len(parts) != 2 {
			return status.Error(codes.Unimplemented, "unknown method")
		}
		conn := connections[parts[0]]
		if conn == nil {
			return status.Error(codes.Unimplemented, "service is not configured")
		}
		key := "unknown"
		if p, ok := peer.FromContext(incoming.Context()); ok {
			key = ClientIP(p.Addr.String())
		}
		if ok, _ := limiter.Allow(key, time.Now()); !ok {
			stats.RateLimited.Add(1)
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			stats.Overloaded.Add(1)
			return status.Error(codes.ResourceExhausted, "gateway busy")
		}
		stats.GRPCStreams.Add(1)
		ctx, cancel := context.WithTimeout(incoming.Context(), timeout)
		defer cancel()
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			md = md.Copy()
			for k := range md {
				if strings.HasPrefix(k, ":") || k == "connection" || k == "content-type" || k == "user-agent" || strings.HasPrefix(k, "grpc-") {
					delete(md, k)
				}
			}
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		outgoing, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method, grpc.ForceCodec(rawCodec{}))
		if err != nil {
			return err
		}
		type result struct {
			request bool
			err     error
		}
		done := make(chan result, 2)
		go func() {
			for {
				var frame []byte
				if err := incoming.RecvMsg(&frame); err != nil {
					if err == io.EOF {
						err = outgoing.CloseSend()
					}
					done <- result{request: true, err: err}
					return
				}
				if err := outgoing.SendMsg(&frame); err != nil {
					// SendMsg EOF means the response pump owns the final status.
					if err == io.EOF {
						err = nil
					}
					done <- result{request: true, err: err}
					return
				}
			}
		}()
		go func() {
			headers, err := outgoing.Header()
			if err == nil {
				err = incoming.SendHeader(headers)
			}
			if err != nil {
				done <- result{err: err}
				return
			}
			for {
				var frame []byte
				err := outgoing.RecvMsg(&frame)
				if err != nil {
					incoming.SetTrailer(outgoing.Trailer())
					if err == io.EOF {
						err = nil
					}
					done <- result{err: err}
					return
				}
				if err := incoming.SendMsg(&frame); err != nil {
					done <- result{err: err}
					return
				}
			}
		}()
		for {
			select {
			case r := <-done:
				if !r.request || r.err != nil {
					return r.err
				}
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			}
		}
	}
	server := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(handler), grpc.MaxRecvMsgSize(4<<20))
	return server, closeConnections, nil
}
