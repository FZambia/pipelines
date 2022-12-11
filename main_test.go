package main

import (
	"context"
	"testing"
	"time"

	"go.uber.org/ratelimit"
)

func getLimiter() ratelimit.Limiter {
	//return ratelimit.NewUnlimited()
	return ratelimit.New(100, ratelimit.Per(time.Millisecond))
}

func BenchmarkRedigo(b *testing.B) {
	limiter := getLimiter()

	pool := redigoPool()
	defer func() { _ = pool.Close() }()

	sender := newSender(pool, nil)

	b.ResetTimer()
	b.SetParallelism(parallelism)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = limiter.Take()
			err := sender.send()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGoredis(b *testing.B) {
	limiter := getLimiter()

	client := goredisClient()
	defer func() { _ = client.Close() }()

	sender := newSender(nil, client)

	b.ResetTimer()
	b.SetParallelism(parallelism)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = limiter.Take()
			err := sender.send()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRueidis(b *testing.B) {
	limiter := getLimiter()

	client := rueidisClient()
	defer client.Close()

	b.ResetTimer()
	b.SetParallelism(parallelism)
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = limiter.Take()
			cmd := client.B().Publish().Channel("pipelines").Message("test").Build()
			res := client.Do(context.Background(), cmd)
			if res.Error() != nil {
				b.Fatal(res.Error())
			}
		}
	})
}
