package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

const bufsz = 1024
const maxK = 1024

var GET = redis.NewBulkBytes([]byte("GET"))
var SET = redis.NewBulkBytes([]byte("SET"))

func main() {
	addr := os.Args[1]
	num_req, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Panic(err)
	}
	log.SetLevel(log.LevelInfo)
	conn, err := redis.DialTimeout(addr, 5*time.Second, bufsz, bufsz)
	if err != nil {
		log.Panic(err)
	}
	log.Infof("connected to %s", addr)
	var wg sync.WaitGroup

	start := time.Now()
	wg.Add(1)
	go func() {
		for i := 0; i < maxK; i++ {
			output, err := conn.Decode()
			if err != nil {
				log.Error(err)
			}
			log.Debugf("SET return %s %s", output.Type, string(output.Value))
		}
		wg.Done()
	}()
	for i := 0; i < maxK; i++ {
		key := redis.NewBulkBytes([]byte(fmt.Sprintf("key_%d", i)))
		val := redis.NewBulkBytes([]byte(fmt.Sprintf("val_%d", i)))
		err = conn.EncodeMultiBulk([]*redis.Resp{SET, key, val}, true)
		if err != nil {
			log.Error(err)
		}
	}
	wg.Wait()
	dur := time.Since(start)
	log.Infof("Finished %d SET cost %v", maxK, dur)

	start = time.Now()
	wg.Add(1)
	go func() {
		for i := 0; i < num_req; i++ {
			output, err := conn.Decode()
			if err != nil {
				log.Error(err)
			}
			log.Debugf("GET return %s %s", output.Type, string(output.Value))
		}
		wg.Done()
	}()
	for i := 0; i < num_req; i++ {
		key := redis.NewBulkBytes([]byte(fmt.Sprintf("key_%d", i%maxK)))
		err = conn.EncodeMultiBulk([]*redis.Resp{GET, key}, true)
		if err != nil {
			log.Error(err)
		}
	}
	wg.Wait()
	dur = time.Since(start)
	log.Infof("Finished %d GET cost %v", num_req, dur)
}
