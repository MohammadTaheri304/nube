package database

import (
	"log"
	"os"
	"strconv"
	"sync"
)

type Database struct {
	data    *sync.Map
	version int64
	logChan chan string
}

func NewDatabase() *Database {

	chn := make(chan string)

	db := &Database{
		data:    &sync.Map{},
		logChan: chn,
		version: 0,
	}

	go db.fileHandler()
	return db
}

func (db *Database) fileHandler() {
	f, err := os.Create("changelog")
	if err != nil {
		log.Fatalf("Error in opening file %+v", err)
	}
	defer f.Close()

	for {
		req := <-db.logChan
		_, err := f.WriteString(req)
		if err != nil {
			log.Fatalf("Error in write into file %+v", err)
		}
		f.Sync()
	}
}

func (db *Database) Get(key string) (string, bool) {
	val, ok := db.data.Load(key)
	if !ok {
		return "", false
	}
	c, e := val.(string)
	return c, e
}

func (db *Database) Set(key, value string) (int64, bool) {
	db.version++
	db.logChan <- strconv.FormatInt(db.version, 16) + " set " + key + " " + value + "\r\n"
	db.data.LoadOrStore(key, value)
	return db.version, true
}

func (db *Database) Version() int64 {
	return db.version
}
