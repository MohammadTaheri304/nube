//go:generate protoc -I ./proto/ --go_out=plugins=grpc:./rpc/ map_service.proto
//go:generate protoc -I ./proto/ --go_out=plugins=grpc:./rpc/ cluster_service.proto
package main

import (
	context "context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MohammadTaheri304/nube/cluster"
	"github.com/MohammadTaheri304/nube/database"
	"github.com/MohammadTaheri304/nube/rpc"
	"github.com/MohammadTaheri304/nube/service"
	"google.golang.org/grpc"
)

func main() {
	address := os.Args[2]
	if os.Args[1] == "client" {
		fmt.Println("Client mode")
		startClientMode(address)
	} else {
		fmt.Println("Server mode. (server <listen-address(0.0.0.0:9090)> <nodeId(1)> <nodes(1-localhost:9090,2-localhost:9191)>)")
		nodeId, _ := strconv.ParseInt(os.Args[3], 10, 64)
		nodesString := os.Args[4]
		nodes := make(map[int64]string)
		for _, nodeString := range strings.Split(nodesString, ",") {
			parts := strings.Split(nodeString, "-")
			nodeId, _ := strconv.ParseInt(parts[0], 10, 64)
			nodes[nodeId] = parts[1]
		}
		startServerMode(address, nodeId, nodes)
	}
}

func startClientMode(address string) {
	connection, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error in client %v", err)
	}

	defer connection.Close()

	client := rpc.NewMapServiceClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err = client.Set(ctx, &rpc.Message{
		Key:   "somethingsomethingsomething",
		Value: "aaaaaaaa",
	})
	if err != nil {
		log.Fatalf("Error in set %v", err)
	}

	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			_, err = client.Get(ctx, &rpc.Message{
				Key: "somethingsomethingsomething",
			})
			if err != nil {
				log.Fatalf("Error in get %v", err)
			}
		}(i)
	}
	wg.Wait()

	fmt.Println("Total in " + time.Since(start).String())
}

func startServerMode(address string, nodeId int64, nodes map[int64]string) {
	db := database.NewDatabase()
	manager := cluster.NewClusterManager(nodeId, nodes)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Error in listener %+v", err)
	}

	server := grpc.NewServer()
	rpc.RegisterMapServiceServer(server, service.NewMapService(db, manager))
	rpc.RegisterClusterServiceServer(server, cluster.NewClusterService(db, manager))

	server.Serve(listener)
}
