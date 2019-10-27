package service

import (
	context "context"

	"github.com/MohammadTaheri304/nube/cluster"
	"github.com/MohammadTaheri304/nube/database"
	"github.com/MohammadTaheri304/nube/rpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type MapService struct {
	database *database.Database
	manager  *cluster.ClusterManager
}

func NewMapService(db *database.Database, manager *cluster.ClusterManager) *MapService {
	return &MapService{
		database: db,
		manager:  manager,
	}
}

func (m *MapService) Set(ctx context.Context, req *rpc.Message) (*rpc.Message, error) {
	// if m.manager.IsLeader() {
	// 	version, _ := m.database.Set(req.Key, req.Value)
	// 	go m.manager.BroadCastUpdate(version, req.Key, req.Value)
	// 	log.Printf("Update version to %+v \n", m.database.Version())
	// } else {
	m.manager.SendUpdateToLeader(req.Key, req.Value)
	// }

	return req, nil
}

func (m *MapService) Get(ctx context.Context, req *rpc.Message) (*rpc.Message, error) {
	val, ok := m.database.Get(req.Key)
	if !ok {
		return nil, status.Error(codes.NotFound, "key.not.found")
	}
	req.Value = val
	return req, nil
}
