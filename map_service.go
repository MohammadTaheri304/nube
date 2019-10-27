package main

import (
	context "context"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/MohammadTaheri304/nube/rpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type MapService struct {
	Data    *sync.Map
	Version int64
	LogChan chan string
}

func NewMapService() *MapService {

	chn := make(chan string)
	go fileHandler(chn)
	return &MapService{
		Data:    &sync.Map{},
		LogChan: chn,
		Version: 0,
	}
}

func (m *MapService) Set(ctx context.Context, req *rpc.Message) (*rpc.Message, error) {
	m.Version++
	m.LogChan <- strconv.FormatInt(m.Version, 16) + " set " + req.Key + " " + req.Value + "\r\n"
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

func fileHandler(fileChan chan string) {
	f, err := os.Create("changelog")
	if err != nil {
		log.Fatalf("Error in opening file %+v", err)
	}

	for {
		req := <-fileChan
		_, err := f.WriteString(req)
		if err != nil {
			log.Fatalf("Error in write into file %+v", err)
		}
		f.Sync()
	}
}
