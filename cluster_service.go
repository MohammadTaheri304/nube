package main

import (
	context "context"

	"github.com/MohammadTaheri304/nube/rpc"
	"github.com/golang/protobuf/ptypes/empty"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type ClusterService struct {
	NodeId   string
	LeaderId string
	Nodes    map[string]clusterNode
}

func NewClusterService() *ClusterService {
	return &ClusterService{
		//todo
	}
}

type clusterNode struct {
	NodeId  string
	Address string
}

func (c *ClusterService) HeartBeats(ctx context.Context, req *rpc.HeartBeatMessage) (*rpc.HeartBeatMessage, error) {
	// check for leader and update it.
	// reset leader's heart beat timer.
	// respod to the request.
	return nil, status.Errorf(codes.Unimplemented, "method LeaderElection not implemented")
}

func (c *ClusterService) LeaderElection(ctx context.Context, req *rpc.LeaderElectionRequest) (*rpc.LeaderElectionResponse, error) {
	// set the leader empty
	// if the requests nodeid is bigger than this node's id, accept=true. Otherwise accept=false.
	return nil, status.Errorf(codes.Unimplemented, "method LeaderElection not implemented")
}

func (c *ClusterService) Update(ctx context.Context, req *rpc.UpdateRequest) (*empty.Empty, error) {
	// If you are not leader and request come from leader, accept it and perform necessary actions.
	// If you are not leader and request come from non-leader node ,return an error
	// If you are leader, accept it and perform necessary actions.
	return nil, status.Errorf(codes.Unimplemented, "method Update not implemented")
}

func (c *ClusterService) GetChangeLog(ctx context.Context, req *rpc.ChangeLogRequest) (*rpc.ChangeLogResponse, error) {
	//return the requested changelogs
	return nil, status.Errorf(codes.Unimplemented, "method GetChangeLog not implemented")
}
