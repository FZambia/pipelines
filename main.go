package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	goredis "github.com/go-redis/redis/v9"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/rueian/rueidis"
)

var (
	maxCommandsInPipeline     = 512
	numPipelineWorkers        = 1
	parallelism               = 128
	maxFlushDelayMicroseconds = 0
)

func init() {
	if os.Getenv("PIPE_PARALLELISM") != "" {
		var err error
		parallelism, err = strconv.Atoi(os.Getenv("PIPE_PARALLELISM"))
		if err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("PIPE_WORKERS") != "" {
		var err error
		numPipelineWorkers, err = strconv.Atoi(os.Getenv("PIPE_WORKERS"))
		if err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("PIPE_MAX_COMMANDS") != "" {
		var err error
		maxCommandsInPipeline, err = strconv.Atoi(os.Getenv("PIPE_MAX_COMMANDS"))
		if err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("PIPE_MAX_FLUSH_DELAY") != "" {
		var err error
		maxFlushDelayMicroseconds, err = strconv.Atoi(os.Getenv("PIPE_MAX_FLUSH_DELAY"))
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("Parallelism: %d\n", parallelism)
	fmt.Printf("Num pipeline workers: %d\n", numPipelineWorkers)
	fmt.Printf("Rueidis max flush delay: %s\n", time.Duration(maxFlushDelayMicroseconds)*time.Microsecond)
}

func rueidisClient() rueidis.Client {
	options := rueidis.ClientOption{
		InitAddress:   []string{":6379"},
		DisableCache:  true,
		MaxFlushDelay: time.Duration(maxFlushDelayMicroseconds) * time.Microsecond,
	}
	client, err := rueidis.NewClient(options)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func goredisClient() goredis.UniversalClient {
	return goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:    []string{":6379"},
		PoolSize: 128,
	})
}

func redigoPool() *redigo.Pool {
	return &redigo.Pool{
		Wait: true,
		Dial: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", ":6379")
		},
	}
}

type command struct {
	errCh chan error
}

type sender struct {
	cmdCh         chan command
	redigoPool    *redigo.Pool
	goredisClient goredis.UniversalClient
}

func newSender(redigoPool *redigo.Pool, goredisClient goredis.UniversalClient) *sender {
	p := &sender{
		cmdCh:         make(chan command, 1024),
		redigoPool:    redigoPool,
		goredisClient: goredisClient,
	}
	for i := 0; i < numPipelineWorkers; i++ {
		go func() {
			if p.redigoPool != nil {
				p.runPipelineRoutineRedigo()
			} else {
				p.runPipelineRoutineGoredis()
			}
		}()
	}
	return p
}

func (s *sender) send() error {
	errCh := make(chan error, 1)
	cmd := command{
		errCh: errCh,
	}
	s.cmdCh <- cmd
	return <-errCh
}

func (s *sender) runPipelineRoutineRedigo() {
	conn := s.redigoPool.Get()
	defer func() { _ = conn.Close() }()
	for {
		select {
		case cmd := <-s.cmdCh:
			commands := []command{cmd}
			_ = conn.Send("publish", "pipelines", "test")
		loop:
			for i := 0; i < maxCommandsInPipeline; i++ {
				select {
				case cmd := <-s.cmdCh:
					commands = append(commands, cmd)
					_ = conn.Send("publish", "pipelines", "test")
				default:
					break loop
				}
			}
			err := conn.Flush()
			if err != nil {
				for i := 0; i < len(commands); i++ {
					commands[i].errCh <- err
				}
				continue
			}
			for i := 0; i < len(commands); i++ {
				_, err := conn.Receive()
				commands[i].errCh <- err
			}
		}
	}
}

func (s *sender) runPipelineRoutineGoredis() {
	pipe := s.goredisClient.Pipeline()
	for {
		select {
		case cmd := <-s.cmdCh:
			commands := []command{cmd}
			_ = pipe.Publish(context.Background(), "pipelines", "test")
		loop:
			for i := 0; i < maxCommandsInPipeline; i++ {
				select {
				case cmd := <-s.cmdCh:
					commands = append(commands, cmd)
					_ = pipe.Publish(context.Background(), "pipelines", "test")
				default:
					break loop
				}
			}
			results, _ := pipe.Exec(context.Background())
			for i, res := range results {
				_, err := res.(*goredis.IntCmd).Result()
				if err != nil {
					panic(err)
				}
				commands[i].errCh <- err
			}
		}
	}
}
