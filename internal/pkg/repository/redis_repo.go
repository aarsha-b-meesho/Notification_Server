package repository

import (
	"context"
	"log"

	"github.com/go-redis/redis/v8"
)

type RedisRepo struct {
	client *redis.Client
}

func New_Redis_Repo(addr string) *RedisRepo {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisRepo{client: client}
}

func (r *RedisRepo) Ping(ctx context.Context) error {
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		log.Printf("Ping: Error pinging Redis server: %v", err)
	} else {
		log.Println("Ping: Successfully pinged Redis server")
	}
	return err
}

func (r *RedisRepo) FlushDB(ctx context.Context) error {
	err := r.client.FlushDB(ctx).Err()
	if err != nil {
		log.Printf("FlushDB: Error flushing Redis database: %v", err)
	} else {
		log.Println("FlushDB: Redis database flushed successfully")
	}
	return err
}

func (r *RedisRepo) SIsMember(ctx context.Context, key, member string) *redis.BoolCmd {
	log.Printf("SIsMember: Checking if member '%s' is in set '%s'", member, key)
	return r.client.SIsMember(ctx, key, member)
}

func (r *RedisRepo) SAdd(ctx context.Context, key, member string) *redis.IntCmd {
	log.Printf("SAdd: Adding member '%s' to set '%s'", member, key)
	return r.client.SAdd(ctx, key, member)
}

func (r *RedisRepo) SRem(ctx context.Context, key, member string) *redis.IntCmd {
	log.Printf("SRem: Removing member '%s' from set '%s'", member, key)
	return r.client.SRem(ctx, key, member)
}

func (r *RedisRepo) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	log.Printf("SMembers: Retrieving all members from set '%s'", key)
	return r.client.SMembers(ctx, key)
}
