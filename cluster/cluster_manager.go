package cluster

import (
	context "context"
	"log"
	"time"

	"github.com/MohammadTaheri304/nube/rpc"
	"google.golang.org/grpc"
)

type ClusterManager struct {
	NodeId        int64
	LeaderId      int64
	Nodes         map[int64]string
	BeatResetChan chan int64
}

func NewClusterManager(nodeId int64, nodes map[int64]string) *ClusterManager {
	c := &ClusterManager{
		NodeId:        nodeId,
		Nodes:         nodes,
		BeatResetChan: make(chan int64),
	}
	go c.heartBeatCheck()
	return c
}

func (c *ClusterManager) heartBeatCheck() {
	for {
		select {
		case <-time.After(1 * time.Second):
			log.Printf("Leader expired!")
			// send leader election request
			if c.NodeId != c.LeaderId {
				c.sendLeaderElectionRequest()
			}
		case beatFrom := <-c.BeatResetChan:
			// reset timer
			if _, ok := c.Nodes[beatFrom]; ok {
				c.LeaderId = beatFrom
				log.Printf("Leader is %+v", c.LeaderId)
			}
		}
	}
}

func connectTo(host string) rpc.ClusterServiceClient {
	connection, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error in client %v", err)
	}

	return rpc.NewClusterServiceClient(connection)
}

func (c *ClusterManager) sendLeaderElectionRequest() {
	becameLeader := true
	for k, v := range c.Nodes {
		if k != c.NodeId {
			con := connectTo(v)
			response, err := con.LeaderElection(context.Background(), &rpc.LeaderElectionRequest{NodeId: c.NodeId})
			if err != nil {
				log.Printf("Error in send leader election request  to %+v:: %+v", v, err)
			} else {
				log.Printf("Result of leader request from %+v was %+v", v, response)
				becameLeader = becameLeader && response.Accept
			}
		}
	}
	if becameLeader {
		c.LeaderId = c.NodeId
		log.Printf("I am the leader now :D")
		c.leaderShouldSendHeartBeat()
	}
}

func (c *ClusterManager) leaderShouldSendHeartBeat() {
	for {
		<-time.After(500 * time.Millisecond)
		if c.NodeId != c.LeaderId {
			return
		}

		for k, v := range c.Nodes {
			if k != c.NodeId {
				con := connectTo(v)
				//todo
				con.HeartBeats(context.Background(), &rpc.HeartBeatMessage{NodeId: c.NodeId})
			}
		}
	}
}

func (c *ClusterManager) BroadCastUpdate(version int64, key, value string) {
	for k, v := range c.Nodes {
		if k != c.NodeId {
			con := connectTo(v)
			response, err := con.Update(context.Background(), &rpc.UpdateRequest{
				Version: version,
				Key:     key,
				Value:   value,
			})
			if err != nil {
				log.Printf("Error in send update request to %+v:: %+v", v, err)
			} else {
				log.Printf("Result of send updaterequest to %+v was %+v", v, response)
			}
		}
	}
}

func (c *ClusterManager) SendUpdateToLeader(key, value string) {
	con := connectTo(c.Nodes[c.LeaderId])
	response, err := con.Update(context.Background(), &rpc.UpdateRequest{
		Version: -1,
		Key:     key,
		Value:   value,
	})
	if err != nil {
		log.Printf("Error in send update request to leader :: %+v", err)
	} else {
		log.Printf("Result of send updaterequest to leader was %+v", response)
	}
}

func (c *ClusterManager) IsLeader() bool {
	return c.NodeId == c.LeaderId
}
