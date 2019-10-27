//go:generate protoc -I ./proto/ --go_out=plugins=grpc:./rpc/ map_service.proto
//go:generate protoc -I ./proto/ --go_out=plugins=grpc:./rpc/ cluster_service.proto
package main

import (
	context "context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/MohammadTaheri304/nube/rpc"
	"google.golang.org/grpc"
)

func main() {
	if os.Args[1] == "client" {
		fmt.Println("Client mode")
		startClientMode()
	} else {
		fmt.Println("Server mode")
		startServerMode()
	}
}

func startClientMode() {
	connection, err := grpc.Dial("localhost:21212", grpc.WithInsecure())
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
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			_, err = client.Get(ctx, &rpc.Message{
				Key: "somethingsomethingsomething",
			})
			if err != nil {
				log.Fatalf("Error in get %v", err)
			}

			// _, err = client.Set(ctx, &rpc.Message{
			// 	Key:   "somethingsomethingsomething" + strconv.Itoa(index),
			// 	Value: "aaaaaaaa",
			// })
			// if err != nil {
			// 	log.Fatalf("Error in set %v", err)
			// }
		}(i)
	}
	wg.Wait()

	fmt.Println("Total in " + time.Since(start).String())
}

func startServerMode() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "0.0.0.0", 21212))
	if err != nil {
		log.Fatalf("Error in listener %+v", err)
	}

	server := grpc.NewServer()
	rpc.RegisterMapServiceServer(server, NewMapService())

	server.Serve(listener)
}
