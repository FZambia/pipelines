module github.com/FZambia/pipelines

go 1.19

replace github.com/rueian/rueidis => github.com/FZambia/rueidis v0.0.0-20221207163401-960c08e99a41

require (
	github.com/go-redis/redis/v9 v9.0.0-rc.2
	github.com/gomodule/redigo v1.8.9
	github.com/rueian/rueidis v0.0.88
	go.uber.org/ratelimit v0.2.0
)

require (
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
