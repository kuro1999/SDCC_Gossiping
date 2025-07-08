package registry

import (
	"context"

	pb "SDCC_gossiping/registry/registrypb"
)

type grpcServer struct {
	pb.UnimplementedRegistryServer
	r *Registry
}

func NewGRPCServer(r *Registry) pb.RegistryServer {
	return &grpcServer{r: r}
}

func (s *grpcServer) Register(ctx context.Context, req *pb.ServiceRequest) (*pb.Empty, error) {
	s.r.Register(ServiceInfo{ID: req.Id, Address: req.Address, Tags: req.Tags})
	return &pb.Empty{}, nil
}

func (s *grpcServer) Unregister(ctx context.Context, req *pb.UnregisterRequest) (*pb.Empty, error) {
	s.r.Unregister(req.Id)
	return &pb.Empty{}, nil
}

func (s *grpcServer) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupReply, error) {
	out := &pb.LookupReply{}
	for _, svc := range s.r.Lookup(req.Tag) {
		out.Services = append(out.Services, &pb.ServiceInfo{
			Id:      svc.ID,
			Address: svc.Address,
			Tags:    svc.Tags,
		})
	}
	return out, nil
}
