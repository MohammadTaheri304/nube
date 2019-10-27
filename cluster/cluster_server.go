package cluster

import (
	context "context"
	"log"

	"github.com/MohammadTaheri304/nube/database"
	"github.com/MohammadTaheri304/nube/rpc"
	"github.com/golang/protobuf/ptypes/empty"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type ClusterService struct {
	database *database.Database
	manager  *ClusterManager
}

func NewClusterService(db *database.Database, manager *ClusterManager) *ClusterService {
	c := &ClusterService{
		manager:  manager,
		database: db,
	}
	return c
}

//TODO
func (c *ClusterService) HeartBeats(ctx context.Context, req *rpc.HeartBeatMessage) (*rpc.HeartBeatMessage, error) {
	// check for leader and update it.
	// reset leader's heart beat timer.
	// check for version and checksum
	// respod to the request.

	log.Printf("Recive heartbeat %+v", req)

	c.manager.BeatResetChan <- req.NodeId

	return &rpc.HeartBeatMessage{
		NodeId: c.manager.NodeId,
	}, nil
}

func (c *ClusterService) LeaderElection(ctx context.Context, req *rpc.LeaderElectionRequest) (*rpc.LeaderElectionResponse, error) {
	// set the leader empty
	// if the requests nodeid is bigger than this node's id, accept=true. Otherwise accept=false.
	if req.NodeId > c.manager.NodeId {
		return &rpc.LeaderElectionResponse{Accept: true}, nil
	}
	return &rpc.LeaderElectionResponse{Accept: false}, nil
}

func (c *ClusterService) Update(ctx context.Context, req *rpc.UpdateRequest) (*empty.Empty, error) {
	// If you are not leader and request come from leader, accept it and perform necessary actions.
	// If you are leader, accept it and perform necessary actions.
	if c.manager.IsLeader() {
		version, _ := c.database.Set(req.Key, req.Value)
		go c.manager.BroadCastUpdate(version, req.Key, req.Value)
	} else {
		if c.database.Version() == req.Version-1 {
			c.database.Set(req.Key, req.Value)
		} else {
			// error
			log.Fatalf("Database is not OK! lastVersion is %+v but local version is %+v", req.Version, c.database.Version())
		}
	}

	log.Printf("Update db to version %+v \n", c.database.Version())
	return &empty.Empty{}, nil
}

func (c *ClusterService) GetChangeLog(ctx context.Context, req *rpc.ChangeLogRequest) (*rpc.ChangeLogResponse, error) {
	//return the requested changelogs
	return nil, status.Errorf(codes.Unimplemented, "method GetChangeLog not implemented")
}
