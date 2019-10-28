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
	connectionMap map[int64]*grpc.ClientConn
}

func NewClusterManager(nodeId int64, nodes map[int64]string) *ClusterManager {
	c := &ClusterManager{
		NodeId:        nodeId,
		Nodes:         nodes,
		BeatResetChan: make(chan int64),
		connectionMap: make(map[int64]*grpc.ClientConn),
	}
	go c.heartBeatCheck()
	return c
}

func (c *ClusterManager) heartBeatCheck() {
	for {
		select {
		case <-time.After(1 * time.Second):
			// send leader election request
			if c.NodeId != c.LeaderId {
				log.Printf("Leader expired!")
				go c.sendLeaderElectionRequest()
			}
		case beatFrom := <-c.BeatResetChan:
			// reset timer
			if _, ok := c.Nodes[beatFrom]; ok {
				c.LeaderId = beatFrom
				//log.Printf("Leader is %+v", c.LeaderId)
			}
		}
	}
}

func (c *ClusterManager) connectTo(nodeId int64) rpc.ClusterServiceClient {
	if _, ok := c.connectionMap[nodeId]; !ok {
		conn, err := grpc.Dial(c.Nodes[nodeId], grpc.WithInsecure())
		if err != nil {
			log.Printf("Error in client %v\n", err)
		}
		c.connectionMap[nodeId] = conn
	}
	conn, _ := c.connectionMap[nodeId]
	//log.Println("Connection status is " + conn.GetState().String())
	return rpc.NewClusterServiceClient(conn)
}

func (c *ClusterManager) sendLeaderElectionRequest() {
	becameLeader := true
	for k, v := range c.Nodes {
		if k != c.NodeId {
			con := c.connectTo(k)
			response, err := con.LeaderElection(context.Background(), &rpc.LeaderElectionRequest{NodeId: c.NodeId})
			if err != nil {
				log.Printf("Error in send leader election request  to %+v:: %+v", v, err)
			} else {
				//log.Printf("Result of leader request from %+v was %+v", v, response)
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
	for i := 0; i < 25; i++ {
		<-time.After(500 * time.Millisecond)
		if c.NodeId != c.LeaderId {
			return
		}

		for k, _ := range c.Nodes {
			if k != c.NodeId {
				con := c.connectTo(k)
				//todo
				con.HeartBeats(context.Background(), &rpc.HeartBeatMessage{NodeId: c.NodeId})
			}
		}
	}
	c.LeaderId = -1
}

func (c *ClusterManager) BroadCastUpdate(version int64, key, value string) {
	for k, v := range c.Nodes {
		if k != c.NodeId {
			con := c.connectTo(k)
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
	con := c.connectTo(c.LeaderId)
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
