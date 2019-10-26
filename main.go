//go:generate protoc -I ./ --go_out=plugins=grpc:./rpc/ map_service.proto
package main

import (
	context "context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/MohammadTaheri304/nube/rpc"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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
	for i := 0; i < 250000; i++ {
		go func(index int) {
			_, err = client.Set(ctx, &rpc.Message{
				Key:   "somethingsomethingsomething" + strconv.Itoa(index),
				Value: "aaaaaaaa",
			})
			if err != nil {
				log.Fatalf("Error in set %v", err)
			}
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

type MapService struct {
	Data    *sync.Map
	LogFile *os.File
}

func NewMapService() *MapService {

	return &MapService{
		Data: &sync.Map{},
	}
}

func main() {
	f, err := os.Create("test.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	l, err := f.WriteString("Hello World")
	if err != nil {
		fmt.Println(err)
		f.Close()
		return
	}
	fmt.Println(l, "bytes written successfully")
	err = f.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (m *MapService) Set(ctx context.Context, req *rpc.Message) (*rpc.Message, error) {
	m.Data.LoadOrStore(req.Key, req.Value)
	return req, nil
}
func (m *MapService) Get(ctx context.Context, req *rpc.Message) (*rpc.Message, error) {
	val, ok := m.Data.Load(req.Key)
	if !ok {
		return nil, status.Error(codes.NotFound, "key.not.found")
	}
	req.Value = val.(string)
	return req, nil
}
