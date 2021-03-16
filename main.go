package main

import (
	"context"
	"flag"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"time"
)

func main() {
	var err error
	go func(err *error) {
		if *err != nil {
			log.Println("exited with error:", (*err).Error())
			os.Exit(1)
		}
	}(&err)

	var (
		optSrc string
		optDst string
		optPfx string
	)

	flag.StringVar(&optSrc, "src", "", "source redis url")
	flag.StringVar(&optDst, "dst", "", "destination redis url")
	flag.StringVar(&optPfx, "pfx", "", "prefix")
	flag.Parse()

	var (
		optsSrc *redis.Options
		optsDst *redis.Options
	)

	if optsSrc, err = redis.ParseURL(optSrc); err != nil {
		return
	}
	if optsDst, err = redis.ParseURL(optSrc); err != nil {
		return
	}

	clientSrc := redis.NewClient(optsSrc)
	defer clientSrc.Close()
	clientDst := redis.NewClient(optsDst)
	defer clientDst.Close()

	if err = clientSrc.Ping(context.Background()).Err(); err != nil {
		return
	}

	if err = clientDst.Ping(context.Background()).Err(); err != nil {
		return
	}

	var keys []string
	var curr uint64
	for {
		if keys, curr, err = clientSrc.Scan(context.Background(), curr, optPfx+"*", 100).Result(); err != nil {
			return
		}
		for _, key := range keys {
			log.Println("KEY:", key)
			var typ string
			if typ, err = clientSrc.Type(context.Background(), key).Result(); err != nil {
				return
			}
			if typ != "string" {
				continue
			}
			var ttl time.Duration
			if ttl, err = clientSrc.TTL(context.Background(), key).Result(); err != nil {
				return
			}
			if ttl < 0 {
				ttl = 0
			}
			var val string
			if val, err = clientSrc.Get(context.Background(), key).Result(); err != nil {
				return
			}
			if err = clientDst.Set(context.Background(), key, val, ttl).Err(); err != nil {
				return
			}
		}
		if curr == 0 {
			break
		}
	}
}
