package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/go-redis/redis/v9"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/rueian/rueidis"
)

var (
	maxCommandsInPipeline = 512
	numPipelineWorkers    = 1
	parallelism           = 128
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
}

func rueidisClient() rueidis.Client {
	options := rueidis.ClientOption{
		InitAddress:  []string{":6379"},
		DisableCache: true,
	}
	if os.Getenv("PIPE_DELAYED") != "" {
		options.GetWriterEachConn = func(writer io.Writer) (*bufio.Writer, func()) {
			mlw := newDelayWriter(bufio.NewWriterSize(writer, 1<<19), time.Millisecond)
			w := bufio.NewWriterSize(mlw, 1<<19)
			return w, func() { mlw.close() }
		}
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

func genEvents(ctx context.Context) chan struct{} {
	const rpsLimit = 10000
	ch := make(chan struct{}, rpsLimit)
	go func() {
		for {
			time.Sleep(time.Second / rpsLimit)
			select {
			case <-ctx.Done():
				return
			case ch <- struct{}{}:
			}
		}
	}()
	return ch
}

var count int64
var prev int64

func runRedigo() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch := genEvents(ctx)

	pool := redigoPool()
	defer func() { _ = pool.Close() }()

	sender := newSender(pool, nil)

	for i := 0; i < parallelism; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
				}
				started := time.Now()
				err := sender.send()
				if err != nil {
					log.Fatal(err)
				}
				atomic.AddInt64(&count, 1)
				time.Sleep(time.Millisecond - time.Since(started))
			}
		}()
	}

	<-ctx.Done()
}

func runGoredis() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch := genEvents(ctx)

	client := goredisClient()
	defer func() { _ = client.Close() }()

	sender := newSender(nil, client)

	for i := 0; i < parallelism; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
				}
				started := time.Now()
				err := sender.send()
				if err != nil {
					log.Fatal(err)
				}
				atomic.AddInt64(&count, 1)
				time.Sleep(time.Millisecond - time.Since(started))
			}
		}()
	}

	<-ctx.Done()
}

func runRueidis() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ch := genEvents(ctx)

	client := rueidisClient()
	defer client.Close()

	for i := 0; i < parallelism; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
				}
				started := time.Now()
				cmd := client.B().Publish().Channel("pipelines").Message("test").Build()
				res := client.Do(context.Background(), cmd)
				if res.Error() != nil {
					select {
					case <-ctx.Done():
						return
					default:
						log.Fatal(res.Error())
					}
				}
				atomic.AddInt64(&count, 1)
				time.Sleep(time.Millisecond - time.Since(started))
			}
		}()
	}

	<-ctx.Done()
}

func main() {
	go func() {
		for {
			time.Sleep(time.Second)
			val := atomic.LoadInt64(&count)
			fmt.Println(val - atomic.LoadInt64(&prev))
			atomic.StoreInt64(&prev, val)
		}
	}()

	log.Println("run redigo")
	runRedigo()
	log.Println("run goredis")
	runGoredis()
	log.Println("run rueidis")
	runRueidis()
}

type writeFlusher interface {
	io.Writer
	Flush() error
}

// https://github.com/caddyserver/caddy/blob/c94f5bb7dd3c98d6573c44f06d99c7252911a9fa/modules/caddyhttp/reverseproxy/streaming.go#L88-L124
type delayWriter struct {
	dst   writeFlusher
	delay time.Duration // zero means to flush immediately

	mu           sync.Mutex // protects tm, flushPending, and dst.Flush
	tm           *time.Timer
	err          error
	flushPending bool
}

func newDelayWriter(dst writeFlusher, delay time.Duration) *delayWriter {
	return &delayWriter{dst: dst, delay: delay}
}

func (m *delayWriter) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, err
	}
	n, err = m.dst.Write(p)
	if m.delay <= 0 {
		err = m.dst.Flush()
		return
	}
	if m.flushPending {
		return
	}
	if m.tm == nil {
		m.tm = time.AfterFunc(m.delay, m.delayedFlush)
	} else {
		m.tm.Reset(m.delay)
	}
	m.flushPending = true
	return
}

func (m *delayWriter) delayedFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.flushPending { // if stop was called but AfterFunc already started this goroutine
		return
	}
	err := m.dst.Flush()
	if err != nil {
		m.err = err
	}
	m.flushPending = false
}

func (m *delayWriter) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushPending = false
	if m.tm != nil {
		m.tm.Stop()
	}
}
