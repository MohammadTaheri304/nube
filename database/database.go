package database

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
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

	db.recovery()
	go db.fileHandler()
	return db
}

func (db *Database) recovery() {
	f, err := os.Open("changelog")
	if err != nil {
		log.Printf("Error in opening file for recovery %+v", err)
		f, err = os.Create("changelog")
		if err != nil {
			log.Fatalf("Error in recovery (create recovery file): %+v", err)
		}
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		args := strings.Split(string(line), " ")
		version, _ := strconv.ParseInt(args[0], 16, 64)
		//operation := args[1]
		key := args[2]
		value := args[3]

		db.version = version
		db.data.Store(key, value)
	}
	log.Println("Recovery done. Latest version is " + strconv.FormatInt(db.version, 16))
}

func (db *Database) fileHandler() {
	f, err := os.OpenFile("changelog", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
