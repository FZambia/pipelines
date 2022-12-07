package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/go-redis/redis/v9"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/rueian/rueidis"
)

const (
	maxCommandsInPipeline = 512
	numPipelineWorkers    = 1
	parallelism           = 128
	rpsLimit              = 10000
)

func rueidisClient() rueidis.Client {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{":6379"},
		DisableCache: true,
		GetWriterEachConn: func(writer io.Writer) (*bufio.Writer, func()) {
			mlw := &maxLatencyWriter{dst: bufio.NewWriterSize(writer, 1<<19), latency: time.Millisecond / 2}
			w := bufio.NewWriterSize(mlw, 1<<19)
			return w, func() { mlw.close() }
		},
	})
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

var count int64
var prev int64

func runRedigo() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	pool := redigoPool()
	defer func() { _ = pool.Close() }()

	sender := newSender(pool, nil)

	for i := 0; i < parallelism; i++ {
		go func() {
			for {
				//_ = limiter.Wait(context.Background())
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

	client := goredisClient()
	defer func() { _ = client.Close() }()

	sender := newSender(nil, client)

	for i := 0; i < parallelism; i++ {
		go func() {
			for {
				//_ = limiter.Wait(context.Background())
				//select {
				//case <-ctx.Done():
				//	return
				//case <-ch:
				//}
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	client := rueidisClient()
	defer client.Close()

	for i := 0; i < parallelism; i++ {
		go func() {
			for {
				//_ = limiter.Wait(context.Background())
				//select {
				//case <-ctx.Done():
				//	return
				//case <-ch:
				//}
				//_ = limiter.Wait(context.Background())
				select {
				case <-ctx.Done():
					return
				case <-ch:
				}
				started := time.Now()
				cmd := client.B().Publish().Channel("pipelines").Message("test").Build()
				res := client.Do(context.Background(), cmd)
				if res.Error() != nil {
					log.Fatal(res.Error())
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
type maxLatencyWriter struct {
	dst     writeFlusher
	latency time.Duration // non-zero; negative means to flush immediately

	mu           sync.Mutex // protects t, flushPending, and dst.Flush
	t            *time.Timer
	flushPending bool
	err          error
}

func newMaxLatencyWriter(dst writeFlusher, latency time.Duration) *maxLatencyWriter {
	return &maxLatencyWriter{dst: dst, latency: latency}
}

func (m *maxLatencyWriter) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, err
	}
	n, err = m.dst.Write(p)
	if m.latency < 0 {
		err = m.dst.Flush()
		return
	}
	if m.flushPending {
		return
	}
	if m.t == nil {
		m.t = time.AfterFunc(m.latency, m.delayedFlush)
	} else {
		m.t.Reset(m.latency)
	}
	m.flushPending = true
	return
}

func (m *maxLatencyWriter) delayedFlush() {
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

func (m *maxLatencyWriter) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushPending = false
	if m.t != nil {
		m.t.Stop()
	}
}
